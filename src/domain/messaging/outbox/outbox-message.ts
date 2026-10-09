import { IntegrationEvent } from "../events/integration-event";
import { OutboxMessageState } from "./outbox-message.types";

class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON(),
      event.occurredAt,
      0,
    );
  }

  /**
   * Reconstructs a persisted outbox message, preserving its publication state.
   */
  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      state.payload,
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }
  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this.publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return (
      this.isPending() &&
      (this.nextAttemptAt === undefined || this.nextAttemptAt <= now)
    );
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
  }

  /**
   * Schedules the next publication attempt using exponential backoff.
   */
  scheduleRetry(now: Date): void {
    this._attempts += 1;

    const baseDelayMs = 1000;
    const maxDelayMs = 5 * 60 * 1000;

    const delayMs = Math.min(
      baseDelayMs * 2 ** (this.attempts - 1),
      maxDelayMs,
    );

    this._nextAttemptAt = new Date(now.getTime() + delayMs);
  }
}
