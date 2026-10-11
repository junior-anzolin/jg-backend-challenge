import { OutboxMessageState } from "../../domain/messaging/outbox/outbox-message.types";

export interface OutboxEventPublisherPort {
  publish(message: OutboxMessageState): Promise<void>;
}

export const OUTBOX_EVENT_PUBLISHER = Symbol("OUTBOX_EVENT_PUBLISHER");
