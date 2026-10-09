import { Entity, PrimaryKey, Property } from "@mikro-orm/core";

@Entity({ tableName: "inbox_messages" })
export class InboxMessageEntity {
  @PrimaryKey({
    fieldName: "consumer_name",
    length: 128,
  })
  consumerName!: string;

  @PrimaryKey({
    fieldName: "message_id",
    length: 128,
  })
  messageId!: string;

  @Property({
    fieldName: "payload_hash",
    length: 128,
  })
  payloadHash!: string;

  @Property({
    fieldName: "received_at",
    columnType: "timestamptz",
  })
  receivedAt!: Date;

  @Property({
    fieldName: "processed_at",
    columnType: "timestamptz",
    nullable: true,
  })
  processedAt?: Date;
}
