import { Migration } from "@mikro-orm/migrations";

export class Migration20261009221112 extends Migration {
  override async up(): Promise<void> {
    this.addSql(
      `create table "outbox_messages" ("id" uuid not null, "aggregate_id" varchar(255) not null, "event_type" varchar(128) not null, "payload" jsonb not null, "occurred_at" timestamptz not null, "attempts" int not null default 0, "next_attempt_at" timestamptz null, "published_at" timestamptz null, constraint "outbox_messages_pkey" primary key ("id"), constraint outbox_messages_attempts_non_negative check (attempts >= 0));`,
    );
    this.addSql(
      `create index "outbox_messages_pending_idx" on "outbox_messages" ("next_attempt_at", "occurred_at", "id") where "published_at" IS NULL;`,
    );
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists "outbox_messages" cascade;`);
  }
}
