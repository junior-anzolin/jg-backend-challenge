import { Migration } from "@mikro-orm/migrations";

export class Migration20261011003712 extends Migration {
  override async up(): Promise<void> {
    this.addSql(
      `alter table "outbox_messages" add column "claim_token" uuid null, add column "claim_until" timestamptz null;`,
    );
    this.addSql(
      `alter table "outbox_messages" add constraint outbox_messages_claim_lease_consistent check((claim_token IS NULL) = (claim_until IS NULL));`,
    );
  }

  override async down(): Promise<void> {
    this.addSql(
      `alter table "outbox_messages" drop constraint outbox_messages_claim_lease_consistent;`,
    );
    this.addSql(
      `alter table "outbox_messages" drop column "claim_token", drop column "claim_until";`,
    );
  }
}
