import { InboxMessage } from "../../domain/messaging/inbox/inbox-message";

export interface InboxMessageRepositoryPort {
  findByConsumerAndMessageId(
    consumerName: string,
    messageId: string,
  ): Promise<InboxMessage | null>;

  save(message: InboxMessage): Promise<void>;
}

export class InboxMessagePayloadConflictError extends Error {
  constructor(messageId: string) {
    super(`Inbox message "${messageId}" was reused with a different payload`);
    this.name = InboxMessagePayloadConflictError.name;
  }
}
