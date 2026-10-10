import { FilterQuery, QueryOrder } from "@mikro-orm/core";
import { EntityManager } from "@mikro-orm/postgresql";

import {
  LedgerEntryPage,
  LedgerPagination,
  WalletLedgerEntryRepositoryPort,
} from "../../../application/ports/wallet-ledger-entry.repository.port";
import { Money } from "../../../domain/shared/money/money";
import { LedgerDirection } from "../../../domain/wagering/ledger/ledger-direction";
import { WalletLedgerEntry } from "../../../domain/wagering/ledger/wallet-ledger-entry";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity";
import { WalletLedgerEntryEntity } from "../entities/wallet-ledger-entry.entity";
import { WalletEntity } from "../entities/wallet.entity";

interface LedgerCursor {
  createdAt: Date;
  id: string;
}

export class WalletLedgerEntryRepository implements WalletLedgerEntryRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  async save(entry: WalletLedgerEntry): Promise<void> {
    const entity = this.em.create(WalletLedgerEntryEntity, {
      id: entry.id,
      wallet: this.em.getReference(WalletEntity, entry.walletId),
      transaction: this.em.getReference(
        WagerTransactionEntity,
        entry.transactionId,
      ),
      direction: entry.direction,
      moneyAmount: entry.money.toString(),
      moneyCurrency: entry.money.currency,
      balanceBeforeAmount: entry.balanceBefore.toString(),
      balanceBeforeCurrency: entry.balanceBefore.currency,
      balanceAfterAmount: entry.balanceAfter.toString(),
      balanceAfterCurrency: entry.balanceAfter.currency,
      createdAt: entry.createdAt,
    });

    this.em.persist(entity);
  }

  async findByWalletId(
    walletId: string,
    pagination: LedgerPagination,
  ): Promise<LedgerEntryPage> {
    if (!Number.isInteger(pagination.limit) || pagination.limit <= 0) {
      throw new RangeError("Ledger page limit must be a positive integer");
    }

    const cursor = pagination.cursor
      ? this.decodeCursor(pagination.cursor)
      : undefined;

    const criteria: FilterQuery<WalletLedgerEntryEntity> = {
      wallet: this.em.getReference(WalletEntity, walletId),
    };

    if (cursor) {
      criteria.$or = [
        { createdAt: { $lt: cursor.createdAt } },
        {
          createdAt: cursor.createdAt,
          id: { $lt: cursor.id },
        },
      ];
    }

    const entities = await this.em.find(WalletLedgerEntryEntity, criteria, {
      orderBy: {
        createdAt: QueryOrder.DESC,
        id: QueryOrder.DESC,
      },
      limit: pagination.limit + 1,
    });

    const hasNextPage = entities.length > pagination.limit;
    const pageEntities = entities.slice(0, pagination.limit);
    const lastEntity = pageEntities.at(-1);

    return {
      items: pageEntities.map((entity) => this.toDomain(entity)),
      nextCursor:
        hasNextPage && lastEntity ? this.encodeCursor(lastEntity) : undefined,
    };
  }

  async calculateBalanceFromLedger(
    walletId: string,
    currency: string,
  ): Promise<Money> {
    const rows = await this.em
      .getConnection()
      .execute<{ balance: string | null }[]>(
        `
        SELECT
          COALESCE(
            SUM(
              CASE
                WHEN direction = ? THEN money_amount
                WHEN direction = ? THEN -money_amount
                ELSE 0
              END
            ),
            0
          )::numeric(20, 2)::text AS balance
        FROM wallet_ledger_entries
        WHERE wallet_id = ?
          AND money_currency = ?
      `,
        [LedgerDirection.Credit, LedgerDirection.Debit, walletId, currency],
      );

    return Money.from({
      amount: rows[0]?.balance ?? "0.00",
      currency,
    });
  }

  async countByWalletId(walletId: string): Promise<number> {
    return this.em.count(WalletLedgerEntryEntity, {
      wallet: this.em.getReference(WalletEntity, walletId),
    });
  }

  private toDomain(entity: WalletLedgerEntryEntity): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: entity.id,
      walletId: entity.wallet.id,
      transactionId: entity.transaction.id,
      direction: entity.direction,
      money: Money.from({
        amount: entity.moneyAmount,
        currency: entity.moneyCurrency,
      }),
      balanceBefore: Money.from({
        amount: entity.balanceBeforeAmount,
        currency: entity.balanceBeforeCurrency,
      }),
      balanceAfter: Money.from({
        amount: entity.balanceAfterAmount,
        currency: entity.balanceAfterCurrency,
      }),
      createdAt: entity.createdAt,
    });
  }

  private encodeCursor(entity: WalletLedgerEntryEntity): string {
    const payload = JSON.stringify({
      createdAt: entity.createdAt.toISOString(),
      id: entity.id,
    });

    return Buffer.from(payload).toString("base64url");
  }

  private decodeCursor(cursor: string): LedgerCursor {
    try {
      const payload = JSON.parse(
        Buffer.from(cursor, "base64url").toString("utf8"),
      ) as { createdAt?: unknown; id?: unknown };

      if (
        typeof payload.createdAt !== "string" ||
        typeof payload.id !== "string" ||
        !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
          payload.id,
        )
      ) {
        throw new Error();
      }

      const createdAt = new Date(payload.createdAt);

      if (Number.isNaN(createdAt.getTime())) {
        throw new Error();
      }

      return { createdAt, id: payload.id };
    } catch {
      throw new RangeError("Invalid ledger cursor");
    }
  }
}
