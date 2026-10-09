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

import { FailureCode } from "../../../domain/wagering/transaction/failure-code";
import { WagerTransactionKind } from "../../../domain/wagering/transaction/wager-transaction-kind";
import { WagerTransactionStatus } from "../../../domain/wagering/transaction/wager-transaction-status";
import { WalletEntity } from "./wallet.entity";

@Entity({ tableName: "wager_transactions" })
@Unique({
  name: "wager_transactions_idempotency_key_unique",
  properties: ["idempotencyKey"],
})
@Unique({
  name: "wager_transactions_provider_external_unique",
  properties: ["providerId", "externalTransactionId"],
})
@Index({
  name: "wager_transactions_reference_kind_unique",
  expression:
    `create unique index "wager_transactions_reference_kind_unique" ` +
    `on "wager_transactions" ("reference_transaction_id", "kind") ` +
    `where "kind" in ('REFUND', 'ROLLBACK')`,
})
@Index({
  name: "wager_transactions_pending_reference_idx",
  properties: ["status", "nextReferenceAttemptAt"],
})
@Check({
  name: "wager_transactions_money_non_negative",
  expression: "money_amount >= 0",
})
@Check({
  name: "wager_transactions_reference_attempts_non_negative",
  expression: "reference_attempts >= 0",
})
@Check({
  name: "wager_transactions_resulting_balance_consistent",
  expression:
    "(resulting_balance_amount IS NULL) = (resulting_balance_currency IS NULL)",
})
export class WagerTransactionEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property({ fieldName: "provider_id", length: 128 })
  providerId!: string;

  @Property({ fieldName: "external_transaction_id", length: 255 })
  externalTransactionId!: string;

  @Property({ fieldName: "idempotency_key", length: 255 })
  idempotencyKey!: string;

  @Property({ fieldName: "payload_hash", length: 128 })
  payloadHash!: string;

  @ManyToOne(() => WalletEntity, {
    fieldName: "wallet_id",
    deleteRule: "restrict",
  })
  wallet!: WalletEntity;

  @Property({ fieldName: "player_id", length: 255 })
  playerId!: string;

  @Property({ fieldName: "round_id", length: 255 })
  roundId!: string;

  @Property({ fieldName: "game_id", length: 255 })
  gameId!: string;

  @Property({ length: 32 })
  kind!: WagerTransactionKind;

  @Property({
    fieldName: "money_amount",
    type: DecimalType,
    columnType: "numeric(20,2)",
  })
  moneyAmount!: string;

  @Property({ fieldName: "money_currency", length: 3 })
  moneyCurrency!: string;

  @Property({
    fieldName: "reference_external_transaction_id",
    length: 255,
    nullable: true,
  })
  referenceExternalTransactionId?: string;

  @ManyToOne(() => WagerTransactionEntity, {
    fieldName: "reference_transaction_id",
    nullable: true,
    deleteRule: "restrict",
  })
  referenceTransaction?: WagerTransactionEntity;

  @Property({ length: 32 })
  status!: WagerTransactionStatus;

  @Property({ fieldName: "failure_code", length: 64, nullable: true })
  failureCode?: FailureCode;

  @Property({ fieldName: "created_at", columnType: "timestamptz" })
  createdAt!: Date;

  @Property({
    fieldName: "processed_at",
    columnType: "timestamptz",
    nullable: true,
  })
  processedAt?: Date;

  @Property({
    fieldName: "resulting_balance_amount",
    type: DecimalType,
    columnType: "numeric(20,2)",
    nullable: true,
  })
  resultingBalanceAmount?: string;

  @Property({
    fieldName: "resulting_balance_currency",
    length: 3,
    nullable: true,
  })
  resultingBalanceCurrency?: string;

  @Property({ fieldName: "reference_attempts", default: 0 })
  referenceAttempts = 0;

  @Property({
    fieldName: "next_reference_attempt_at",
    columnType: "timestamptz",
    nullable: true,
  })
  nextReferenceAttemptAt?: Date;
}
