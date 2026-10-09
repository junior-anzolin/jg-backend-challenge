import { Migration } from "@mikro-orm/migrations";

export class Migration20261009221008 extends Migration {
  override async up(): Promise<void> {
    this.addSql(
      `create table "inbox_messages" ("consumer_name" varchar(128) not null, "message_id" varchar(128) not null, "payload_hash" varchar(128) not null, "received_at" timestamptz not null, "processed_at" timestamptz null, constraint "inbox_messages_pkey" primary key ("consumer_name", "message_id"));`,
    );
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists "inbox_messages" cascade;`);
  }
}
