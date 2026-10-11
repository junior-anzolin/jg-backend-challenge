import { EntityManager } from "@mikro-orm/postgresql";

import {
  InboxMessagePayloadConflictError,
  InboxMessageRepositoryPort,
} from "../../../application/ports/inbox-message.repository.port";
import { InboxMessage } from "../../../domain/messaging/inbox/inbox-message";
import { InboxMessageEntity } from "../entities/inbox-message.entity";

export class InboxMessageRepository implements InboxMessageRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  async findByConsumerAndMessageId(
    consumerName: string,
    messageId: string,
  ): Promise<InboxMessage | null> {
    const entity = await this.em.findOne(InboxMessageEntity, {
      consumerName,
      messageId,
    });

    return entity ? this.toDomain(entity) : null;
  }

  async save(message: InboxMessage): Promise<void> {
    const existing = await this.em.findOne(InboxMessageEntity, {
      consumerName: message.consumerName,
      messageId: message.messageId,
    });

    if (existing) {
      if (existing.payloadHash !== message.payloadHash) {
        throw new InboxMessagePayloadConflictError(message.messageId);
      }

      if (message.processedAt) {
        existing.processedAt = message.processedAt;
      }

      return;
    }

    const entity = this.em.create(InboxMessageEntity, {
      consumerName: message.consumerName,
      messageId: message.messageId,
      payloadHash: message.payloadHash,
      receivedAt: message.receivedAt,
      processedAt: message.processedAt,
    });

    await this.em.persist(entity).flush();
  }

  private toDomain(entity: InboxMessageEntity): InboxMessage {
    return InboxMessage.rehydrate({
      consumerName: entity.consumerName,
      messageId: entity.messageId,
      payloadHash: entity.payloadHash,
      receivedAt: entity.receivedAt,
      processedAt: entity.processedAt ?? undefined,
    });
  }
}
