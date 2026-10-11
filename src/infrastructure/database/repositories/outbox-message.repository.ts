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

  async claimDueForPublishing(
    now: Date,
    limit: number,
    claimToken: string,
    claimUntil: Date,
  ): Promise<OutboxMessageState[]> {
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new RangeError("Outbox batch limit must be a positive integer");
    }

    if (!claimToken.trim() || claimUntil <= now) {
      throw new RangeError(
        "A valid claim token and future expiration are required",
      );
    }

    const entities = await this.em.find(
      OutboxMessageEntity,
      {
        publishedAt: null,
        $and: [
          {
            $or: [{ nextAttemptAt: null }, { nextAttemptAt: { $lte: now } }],
          },
          {
            $or: [{ claimUntil: null }, { claimUntil: { $lte: now } }],
          },
        ],
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

    for (const entity of entities) {
      this.em.assign(entity, {
        claimToken,
        claimUntil,
      });
    }

    return entities.map((entity) => this.toState(entity));
  }

  async markPublished(
    messageId: string,
    claimToken: string,
    publishedAt: Date,
  ): Promise<boolean> {
    const affected = await this.em.nativeUpdate(
      OutboxMessageEntity,
      {
        id: messageId,
        claimToken,
        publishedAt: null,
      },
      {
        publishedAt,
        claimToken: null,
        claimUntil: null,
      },
    );

    return affected === 1;
  }

  async rescheduleClaimed(
    messageId: string,
    claimToken: string,
    attempts: number,
    nextAttemptAt: Date,
  ): Promise<boolean> {
    if (!Number.isInteger(attempts) || attempts < 1) {
      throw new RangeError("Attempts must be a positive integer");
    }

    const affected = await this.em.nativeUpdate(
      OutboxMessageEntity,
      {
        id: messageId,
        claimToken,
        publishedAt: null,
      },
      {
        attempts,
        nextAttemptAt,
        claimToken: null,
        claimUntil: null,
      },
    );

    return affected === 1;
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
