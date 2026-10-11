import { Inject, Injectable } from "@nestjs/common";

import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";
import {
  ProcessWagerTransactionInput,
  ProcessWagerTransactionUseCase,
} from "./process-wager-transaction.use-case";

import { WagerTransaction } from "../../domain/wagering/transaction/wager-transaction";
import { WagerTransactionStatus } from "../../domain/wagering/transaction/wager-transaction-status";

const DEFAULT_BATCH_SIZE = 10;

export interface RetryPendingReferencesResult {
  due: number;
  processed: number;
  rejected: number;
  stillPending: number;
  errors: Array<{
    transactionId: string;
    message: string;
  }>;
}

@Injectable()
export class RetryPendingReferencesUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
    private readonly processWagerTransaction: ProcessWagerTransactionUseCase,
  ) {}

  async execute(
    batchSize = DEFAULT_BATCH_SIZE,
  ): Promise<RetryPendingReferencesResult> {
    const dueTransactions = await this.unitOfWork.runInTransaction((context) =>
      context.wagerTransactions.findDuePendingReferences(new Date(), batchSize),
    );

    const result: RetryPendingReferencesResult = {
      due: dueTransactions.length,
      processed: 0,
      rejected: 0,
      stillPending: 0,
      errors: [],
    };

    for (const transaction of dueTransactions) {
      try {
        const outcome = await this.processWagerTransaction.execute(
          this.toInput(transaction),
        );

        switch (outcome.status) {
          case WagerTransactionStatus.Processed:
            result.processed++;
            break;

          case WagerTransactionStatus.Rejected:
          case WagerTransactionStatus.Failed:
            result.rejected++;
            break;

          default:
            result.stillPending++;
        }
      } catch (error) {
        result.errors.push({
          transactionId: transaction.id,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return result;
  }

  private toInput(transaction: WagerTransaction): ProcessWagerTransactionInput {
    return {
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      idempotencyKey: transaction.idempotencyKey,
      playerId: transaction.playerId,
      walletId: transaction.walletId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      money: transaction.money.toJSON(),
      referenceExternalTransactionId:
        transaction.referenceExternalTransactionId,
      correlationId: transaction.id,
    };
  }
}
