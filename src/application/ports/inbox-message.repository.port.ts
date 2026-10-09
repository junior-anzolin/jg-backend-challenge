import { InboxMessage } from "../../domain/messaging/inbox/inbox-message";

export interface InboxMessageRepositoryPort {
  findByConsumerAndMessageId(
    consumerName: string,
    messageId: string,
  ): Promise<InboxMessage | null>;

  save(message: InboxMessage): Promise<void>;
}
