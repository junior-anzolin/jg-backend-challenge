import {
  Check,
  DecimalType,
  Entity,
  Index,
  ManyToOne,
  PrimaryKey,
  Property,
  Unique,
} from "@mikro-orm/core";

import { LedgerDirection } from "../../../domain/wagering/ledger/ledger-direction";
import { WagerTransactionEntity } from "./wager-transaction.entity";
import { WalletEntity } from "./wallet.entity";

@Entity({ tableName: "wallet_ledger_entries" })
@Unique({
  name: "wallet_ledger_entries_wallet_transaction_unique",
  properties: ["wallet", "transaction"],
})
@Index({
  name: "wallet_ledger_entries_wallet_created_id_idx",
  properties: ["wallet", "createdAt", "id"],
})
@Check({
  name: "wallet_ledger_entries_amount_non_negative",
  expression: "money_amount >= 0",
})
@Check({
  name: "wallet_ledger_entries_balances_non_negative",
  expression: "balance_before_amount >= 0 AND balance_after_amount >= 0",
})
@Check({
  name: "wallet_ledger_entries_currency_consistency",
  expression:
    "money_currency = balance_before_currency AND money_currency = balance_after_currency",
})
@Check({
  name: "wallet_ledger_entries_balanced",
  expression:
    "(direction = 'DEBIT' AND balance_after_amount = balance_before_amount - money_amount) OR " +
    "(direction = 'CREDIT' AND balance_after_amount = balance_before_amount + money_amount)",
})
export class WalletLedgerEntryEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @ManyToOne(() => WalletEntity, {
    fieldName: "wallet_id",
    deleteRule: "restrict",
  })
  wallet!: WalletEntity;

  @ManyToOne(() => WagerTransactionEntity, {
    fieldName: "transaction_id",
    deleteRule: "restrict",
  })
  transaction!: WagerTransactionEntity;

  @Property({ length: 10 })
  direction!: LedgerDirection;

  @Property({
    fieldName: "money_amount",
    type: DecimalType,
    columnType: "numeric(20,2)",
  })
  moneyAmount!: string;

  @Property({ fieldName: "money_currency", length: 3 })
  moneyCurrency!: string;

  @Property({
    fieldName: "balance_before_amount",
    type: DecimalType,
    columnType: "numeric(20,2)",
  })
  balanceBeforeAmount!: string;

  @Property({ fieldName: "balance_before_currency", length: 3 })
  balanceBeforeCurrency!: string;

  @Property({
    fieldName: "balance_after_amount",
    type: DecimalType,
    columnType: "numeric(20,2)",
  })
  balanceAfterAmount!: string;

  @Property({ fieldName: "balance_after_currency", length: 3 })
  balanceAfterCurrency!: string;

  @Property({ columnType: "timestamptz" })
  createdAt!: Date;
}
