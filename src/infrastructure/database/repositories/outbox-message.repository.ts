import { LockMode, QueryOrder } from "@mikro-orm/core";
import { EntityManager } from "@mikro-orm/postgresql";

import { OutboxMessageRepositoryPort } from "../../../application/ports/outbox-message.repository.port";
import { OutboxMessage } from "../../../domain/messaging/outbox/outbox-message";
import { OutboxMessageState } from "../../../domain/messaging/outbox/outbox-message.types";
import { OutboxMessageEntity } from "../entities/outbox-message.entity";

export class OutboxMessageRepository implements OutboxMessageRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  async save(message: OutboxMessage): Promise<void> {
    const existing = await this.em.findOne(OutboxMessageEntity, {
      id: message.id,
    });

    if (existing) {
      this.em.assign(existing, {
        attempts: message.attempts,
        nextAttemptAt: message.nextAttemptAt,
        publishedAt: message.publishedAt,
      });

      return;
    }

    this.em.persist(
      this.em.create(OutboxMessageEntity, {
        id: message.id,
        aggregateId: message.aggregateId,
        eventType: message.eventType,
        payload: { ...message.payload },
        occurredAt: message.occurredAt,
        attempts: message.attempts,
        nextAttemptAt: message.nextAttemptAt,
        publishedAt: message.publishedAt,
      }),
    );
  }

  async findDueForPublishing(
    now: Date,
    limit: number,
  ): Promise<OutboxMessageState[]> {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError("Outbox batch limit must be a positive integer");
    }

    const entities = await this.em.find(
      OutboxMessageEntity,
      {
        publishedAt: null,
        $or: [{ nextAttemptAt: { $lte: now } }, { nextAttemptAt: null }],
      },
      {
        orderBy: {
          occurredAt: QueryOrder.ASC,
          id: QueryOrder.ASC,
        },
        limit,
        lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE,
      },
    );

    return entities.map((entity) => this.toState(entity));
  }

  private toState(entity: OutboxMessageEntity): OutboxMessageState {
    return {
      id: entity.id,
      aggregateId: entity.aggregateId,
      eventType: entity.eventType,
      payload: entity.payload,
      occurredAt: entity.occurredAt,
      attempts: entity.attempts,
      nextAttemptAt: entity.nextAttemptAt ?? undefined,
      publishedAt: entity.publishedAt ?? undefined,
    };
  }
}
