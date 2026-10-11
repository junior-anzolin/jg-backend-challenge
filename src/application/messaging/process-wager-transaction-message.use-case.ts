import { Inject, Injectable } from "@nestjs/common";
import { createHash } from "node:crypto";

import { InboxMessagePayloadConflictError } from "../ports/inbox-message.repository.port";
import { UNIT_OF_WORK, UnitOfWorkPort } from "../ports/unit-of-work.port";

import { InboxMessage } from "../../domain/messaging/inbox/inbox-message";
import {
  ProcessWagerTransactionInput,
  ProcessWagerTransactionResult,
  ProcessWagerTransactionUseCase,
} from "../wagering/process-wager-transaction.use-case";

const CONSUMER_NAME = "wager-transactions";

export interface WagerTransactionRequestedMessage {
  messageId: string;
  type: "WagerTransactionRequested";
  occurredAt: string;
  data: ProcessWagerTransactionInput;
}

export interface ProcessWagerTransactionMessageResult {
  duplicateMessage: boolean;
  transactionResult?: ProcessWagerTransactionResult;
}

@Injectable()
export class ProcessWagerTransactionMessageUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
    private readonly processWagerTransaction: ProcessWagerTransactionUseCase,
  ) {}

  async execute(
    message: WagerTransactionRequestedMessage,
  ): Promise<ProcessWagerTransactionMessageResult> {
    const payloadHash = this.hashPayload(message);

    return this.unitOfWork.runInTransaction(async (context) => {
      let inbox = await context.inboxMessages.findByConsumerAndMessageId(
        CONSUMER_NAME,
        message.messageId,
      );

      if (inbox) {
        if (inbox.payloadHash !== payloadHash) {
          throw new InboxMessagePayloadConflictError(message.messageId);
        }

        if (inbox.isProcessed()) {
          return { duplicateMessage: true };
        }
      } else {
        inbox = InboxMessage.receive({
          messageId: message.messageId,
          consumerName: CONSUMER_NAME,
          payloadHash,
        });

        // Persist within this transaction before processing the wager.
        await context.inboxMessages.save(inbox);
      }

      const input: ProcessWagerTransactionInput = {
        ...message.data,
        correlationId: message.data.correlationId?.trim() || message.messageId,
      };

      const transactionResult =
        await this.processWagerTransaction.executeWithinTransaction(
          input,
          context,
        );

      inbox.markProcessed(new Date());
      await context.inboxMessages.save(inbox);

      return {
        duplicateMessage: false,
        transactionResult,
      };
    });
  }

  private hashPayload(message: WagerTransactionRequestedMessage): string {
    const serialized = JSON.stringify(this.canonicalize(message));

    if (serialized === undefined) {
      throw new Error("Could not serialize the incoming SQS message");
    }

    return createHash("sha256").update(serialized).digest("hex");
  }

  private canonicalize(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map((item) => this.canonicalize(item));
    }

    if (value !== null && typeof value === "object") {
      const record = value as Record<string, unknown>;

      return Object.fromEntries(
        Object.keys(record)
          .sort()
          .map((key) => [key, this.canonicalize(record[key])]),
      );
    }

    return value;
  }
}
