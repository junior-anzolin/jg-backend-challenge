import { Inject, Injectable, Logger } from "@nestjs/common";
import { randomUUID } from "node:crypto";

import type { OutboxEventPublisherPort } from "../ports/outbox-event-publisher.port";
import { OUTBOX_EVENT_PUBLISHER } from "../ports/outbox-event-publisher.port";
import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";

import { OutboxMessage } from "../../domain/messaging/outbox/outbox-message";

const DEFAULT_BATCH_SIZE = 10;
const CLAIM_LEASE_MS = 30_000;

export interface PublishOutboxMessagesResult {
  claimed: number;
  published: number;
  retriesScheduled: number;
  claimsLost: number;
  retryScheduleErrors: number;
  publicationStatusErrors: number;
}

@Injectable()
export class PublishOutboxMessagesUseCase {
  private readonly logger = new Logger(PublishOutboxMessagesUseCase.name);

  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,

    @Inject(OUTBOX_EVENT_PUBLISHER)
    private readonly publisher: OutboxEventPublisherPort,
  ) {}

  async execute(
    batchSize = DEFAULT_BATCH_SIZE,
  ): Promise<PublishOutboxMessagesResult> {
    const now = new Date();
    const claimToken = randomUUID();
    const claimUntil = new Date(now.getTime() + CLAIM_LEASE_MS);

    // Claim and persist a batch in a short SQL transaction.
    const messages = await this.unitOfWork.runInTransaction((context) =>
      context.outboxMessages.claimDueForPublishing(
        now,
        batchSize,
        claimToken,
        claimUntil,
      ),
    );

    const result: PublishOutboxMessagesResult = {
      claimed: messages.length,
      published: 0,
      retriesScheduled: 0,
      claimsLost: 0,
      retryScheduleErrors: 0,
      publicationStatusErrors: 0,
    };

    for (const state of messages) {
      try {
        // Network I/O happens outside the database transaction.
        await this.publisher.publish(state);
      } catch (error) {
        await this.scheduleRetry(state, claimToken, result, error);

        continue;
      }

      try {
        // Only acknowledge publication after SQS confirms the send.
        const markedPublished = await this.unitOfWork.runInTransaction(
          (context) =>
            context.outboxMessages.markPublished(
              state.id,
              claimToken,
              new Date(),
            ),
        );

        if (markedPublished) {
          result.published++;
        } else {
          result.claimsLost++;

          this.logger.warn(
            `Publication claim no longer belongs to this publisher: ${state.id}`,
          );
        }
      } catch (error) {
        result.publicationStatusErrors++;

        this.logger.error(
          `Event ${state.id} was sent, but its publication status could not be persisted. It may be sent again after the claim expires.`,
          error instanceof Error ? error.stack : String(error),
        );
      }
    }

    return result;
  }

  private async scheduleRetry(
    state: {
      id: string;
      eventType: string;
      aggregateId: string;
      payload: Readonly<Record<string, unknown>>;
      occurredAt: Date;
      attempts: number;
      nextAttemptAt?: Date;
      publishedAt?: Date;
    },
    claimToken: string,
    result: PublishOutboxMessagesResult,
    error: unknown,
  ): Promise<void> {
    this.logger.warn(
      `Failed to publish outbox event ${state.id} (${state.eventType}): ${
        error instanceof Error ? error.message : String(error)
      }`,
    );

    const message = OutboxMessage.rehydrate(state);
    message.scheduleRetry(new Date());

    const nextAttemptAt = message.nextAttemptAt;

    if (!nextAttemptAt) {
      result.retryScheduleErrors++;

      this.logger.error(
        `Could not calculate the next attempt for outbox event ${state.id}`,
      );

      return;
    }

    try {
      const scheduled = await this.unitOfWork.runInTransaction((context) =>
        context.outboxMessages.rescheduleClaimed(
          state.id,
          claimToken,
          message.attempts,
          nextAttemptAt,
        ),
      );

      if (scheduled) {
        result.retriesScheduled++;
      } else {
        result.claimsLost++;

        this.logger.warn(
          `Could not reschedule outbox event ${state.id}: the publication claim has changed`,
        );
      }
    } catch (persistenceError) {
      result.retryScheduleErrors++;

      this.logger.error(
        `Could not persist the retry schedule for outbox event ${state.id}. The claim will expire if it remains in the database.`,
        persistenceError instanceof Error
          ? persistenceError.stack
          : String(persistenceError),
      );
    }
  }
}
