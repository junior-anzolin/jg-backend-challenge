import { OutboxMessage } from "../../domain/messaging/outbox/outbox-message";
import { OutboxMessageState } from "../../domain/messaging/outbox/outbox-message.types";

export interface OutboxMessageRepositoryPort {
  save(message: OutboxMessage): Promise<void>;

  /**
   * Must run within a transaction and lock selected rows
   * to prevent concurrent publishers from claiming the same messages.
   */
  findDueForPublishing(now: Date, limit: number): Promise<OutboxMessageState[]>;
}
