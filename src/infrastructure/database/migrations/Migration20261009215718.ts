import { Migration } from '@mikro-orm/migrations';

export class Migration20261009215718 extends Migration {

  override async up(): Promise<void> {
    this.addSql(`create table "wallets" ("id" uuid not null, "player_id" varchar(255) not null, "currency" varchar(3) not null, "balance" numeric(20,2) not null, "version" int not null, "created_at" timestamptz not null, "updated_at" timestamptz not null, constraint "wallets_pkey" primary key ("id"), constraint wallets_version_positive check (version >= 1), constraint wallets_balance_non_negative check (balance >= 0));`);
    this.addSql(`alter table "wallets" add constraint "wallets_player_id_currency_unique" unique ("player_id", "currency");`);

    this.addSql(`create table "wager_transactions" ("id" uuid not null, "provider_id" varchar(128) not null, "external_transaction_id" varchar(255) not null, "idempotency_key" varchar(255) not null, "payload_hash" varchar(128) not null, "wallet_id" uuid not null, "player_id" varchar(255) not null, "round_id" varchar(255) not null, "game_id" varchar(255) not null, "kind" varchar(32) not null, "money_amount" numeric(20,2) not null, "money_currency" varchar(3) not null, "reference_external_transaction_id" varchar(255) null, "reference_transaction_id" uuid null, "status" varchar(32) not null, "failure_code" varchar(64) null, "created_at" timestamptz not null, "processed_at" timestamptz null, "resulting_balance_amount" numeric(20,2) null, "resulting_balance_currency" varchar(3) null, "reference_attempts" int not null default 0, "next_reference_attempt_at" timestamptz null, constraint "wager_transactions_pkey" primary key ("id"), constraint wager_transactions_resulting_balance_consistent check ((resulting_balance_amount IS NULL) = (resulting_balance_currency IS NULL)), constraint wager_transactions_reference_attempts_non_negative check (reference_attempts >= 0), constraint wager_transactions_money_non_negative check (money_amount >= 0));`);
    this.addSql(`create index "wager_transactions_pending_reference_idx" on "wager_transactions" ("status", "next_reference_attempt_at");`);
    this.addSql(`create unique index "wager_transactions_reference_kind_unique" on "wager_transactions" ("reference_transaction_id", "kind") where "kind" in ('REFUND', 'ROLLBACK');`);
    this.addSql(`alter table "wager_transactions" add constraint "wager_transactions_provider_external_unique" unique ("provider_id", "external_transaction_id");`);
    this.addSql(`alter table "wager_transactions" add constraint "wager_transactions_idempotency_key_unique" unique ("idempotency_key");`);

    this.addSql(`alter table "wager_transactions" add constraint "wager_transactions_wallet_id_foreign" foreign key ("wallet_id") references "wallets" ("id") on update cascade on delete restrict;`);
    this.addSql(`alter table "wager_transactions" add constraint "wager_transactions_reference_transaction_id_foreign" foreign key ("reference_transaction_id") references "wager_transactions" ("id") on update cascade on delete restrict;`);
  }

  override async down(): Promise<void> {
    this.addSql(`alter table "wager_transactions" drop constraint "wager_transactions_wallet_id_foreign";`);

    this.addSql(`alter table "wager_transactions" drop constraint "wager_transactions_reference_transaction_id_foreign";`);

    this.addSql(`drop table if exists "wallets" cascade;`);

    this.addSql(`drop table if exists "wager_transactions" cascade;`);
  }

}
