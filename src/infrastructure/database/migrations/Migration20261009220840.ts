import { Migration } from "@mikro-orm/migrations";

export class Migration20261009220840 extends Migration {
  override async up(): Promise<void> {
    this.addSql(
      `create table "wallet_ledger_entries" ("id" uuid not null, "wallet_id" uuid not null, "transaction_id" uuid not null, "direction" varchar(10) not null, "money_amount" numeric(20,2) not null, "money_currency" varchar(3) not null, "balance_before_amount" numeric(20,2) not null, "balance_before_currency" varchar(3) not null, "balance_after_amount" numeric(20,2) not null, "balance_after_currency" varchar(3) not null, "created_at" timestamptz not null, constraint "wallet_ledger_entries_pkey" primary key ("id"), constraint wallet_ledger_entries_balanced check ((direction = 'DEBIT' AND balance_after_amount = balance_before_amount - money_amount) OR (direction = 'CREDIT' AND balance_after_amount = balance_before_amount + money_amount)), constraint wallet_ledger_entries_currency_consistency check (money_currency = balance_before_currency AND money_currency = balance_after_currency), constraint wallet_ledger_entries_balances_non_negative check (balance_before_amount >= 0 AND balance_after_amount >= 0), constraint wallet_ledger_entries_amount_non_negative check (money_amount >= 0));`,
    );
    this.addSql(
      `create index "wallet_ledger_entries_wallet_created_id_idx" on "wallet_ledger_entries" ("wallet_id", "created_at", "id");`,
    );
    this.addSql(
      `alter table "wallet_ledger_entries" add constraint "wallet_ledger_entries_wallet_transaction_unique" unique ("wallet_id", "transaction_id");`,
    );

    this.addSql(
      `alter table "wallet_ledger_entries" add constraint "wallet_ledger_entries_wallet_id_foreign" foreign key ("wallet_id") references "wallets" ("id") on update cascade on delete restrict;`,
    );
    this.addSql(
      `alter table "wallet_ledger_entries" add constraint "wallet_ledger_entries_transaction_id_foreign" foreign key ("transaction_id") references "wager_transactions" ("id") on update cascade on delete restrict;`,
    );
  }

  override async down(): Promise<void> {
    this.addSql(`drop table if exists "wallet_ledger_entries" cascade;`);
  }
}
