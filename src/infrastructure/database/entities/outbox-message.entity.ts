import { Check, Entity, Index, PrimaryKey, Property } from "@mikro-orm/core";

@Entity({ tableName: "outbox_messages" })
@Check({
  name: "outbox_messages_attempts_non_negative",
  expression: "attempts >= 0",
})
@Index({
  name: "outbox_messages_pending_idx",
  expression:
    'create index "outbox_messages_pending_idx" ' +
    'on "outbox_messages" ("next_attempt_at", "occurred_at", "id") ' +
    'where "published_at" IS NULL',
})
export class OutboxMessageEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property({ fieldName: "aggregate_id", length: 255 })
  aggregateId!: string;

  @Property({ fieldName: "event_type", length: 128 })
  eventType!: string;

  @Property({
    type: "json",
    columnType: "jsonb",
  })
  payload!: Readonly<Record<string, unknown>>;

  @Property({
    fieldName: "occurred_at",
    columnType: "timestamptz",
  })
  occurredAt!: Date;

  @Property({ default: 0 })
  attempts = 0;

  @Property({
    fieldName: "next_attempt_at",
    columnType: "timestamptz",
    nullable: true,
  })
  nextAttemptAt?: Date;

  @Property({
    fieldName: "published_at",
    columnType: "timestamptz",
    nullable: true,
  })
  publishedAt?: Date;
}
