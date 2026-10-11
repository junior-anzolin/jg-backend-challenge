import { OutboxMessage } from "../../domain/messaging/outbox/outbox-message";
import { OutboxMessageState } from "../../domain/messaging/outbox/outbox-message.types";

export interface OutboxMessageRepositoryPort {
  save(message: OutboxMessage): Promise<void>;

  /**
   * Claims due messages and persists the lease.
   * Must run inside a database transaction.
   */
  claimDueForPublishing(
    now: Date,
    limit: number,
    claimToken: string,
    claimUntil: Date,
  ): Promise<OutboxMessageState[]>;

  /**
   * Marks a message as published only if the caller still owns its claim.
   */
  markPublished(
    messageId: string,
    claimToken: string,
    publishedAt: Date,
  ): Promise<boolean>;

  /**
   * Schedules another attempt and releases the claim,
   * provided the caller still owns it.
   */
  rescheduleClaimed(
    messageId: string,
    claimToken: string,
    attempts: number,
    nextAttemptAt: Date,
  ): Promise<boolean>;
}
