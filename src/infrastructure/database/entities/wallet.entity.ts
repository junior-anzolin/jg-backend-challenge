import {
  Check,
  DecimalType,
  Entity,
  PrimaryKey,
  Property,
  Unique,
} from "@mikro-orm/core";

@Entity({ tableName: "wallets" })
@Unique({
  name: "wallets_player_id_currency_unique",
  properties: ["playerId", "currency"],
})
@Check({
  name: "wallets_balance_non_negative",
  expression: "balance >= 0",
})
@Check({
  name: "wallets_version_positive",
  expression: "version >= 1",
})
export class WalletEntity {
  @PrimaryKey({ type: "uuid" })
  id!: string;

  @Property({ fieldName: "player_id", length: 255 })
  playerId!: string;

  @Property({ length: 3 })
  currency!: string;

  @Property({
    type: DecimalType,
    columnType: "numeric(20,2)",
  })
  balance!: string;

  @Property()
  version!: number;

  @Property({ fieldName: "created_at", columnType: "timestamptz" })
  createdAt!: Date;

  @Property({ fieldName: "updated_at", columnType: "timestamptz" })
  updatedAt!: Date;
}
