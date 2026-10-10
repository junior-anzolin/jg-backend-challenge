import { QueryOrder } from "@mikro-orm/core";
import { EntityManager } from "@mikro-orm/postgresql";

import { WagerTransactionRepositoryPort } from "../../../application/ports/wager-transaction.repository.port";
import { Money } from "../../../domain/shared/money/money";
import { WagerTransaction } from "../../../domain/wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../../domain/wagering/transaction/wager-transaction-kind";
import { WagerTransactionStatus } from "../../../domain/wagering/transaction/wager-transaction-status";
import { WagerTransactionEntity } from "../entities/wager-transaction.entity";
import { WalletEntity } from "../entities/wallet.entity";

export class WagerTransactionRepository implements WagerTransactionRepositoryPort {
  constructor(private readonly em: EntityManager) {}

  async findById(id: string): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, { id });

    return entity ? this.toDomain(entity) : null;
  }

  async findByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      idempotencyKey,
    });

    return entity ? this.toDomain(entity) : null;
  }

  async findByProviderAndExternalTransactionId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      providerId,
      externalTransactionId,
    });

    return entity ? this.toDomain(entity) : null;
  }

  async findReversalByReferenceAndKind(
    referenceTransactionId: string,
    kind: WagerTransactionKind.Refund | WagerTransactionKind.Rollback,
  ): Promise<WagerTransaction | null> {
    const entity = await this.em.findOne(WagerTransactionEntity, {
      referenceTransaction: referenceTransactionId,
      kind,
    });

    return entity ? this.toDomain(entity) : null;
  }

  async findDuePendingReferences(
    now: Date,
    limit: number,
  ): Promise<WagerTransaction[]> {
    const entities = await this.em.find(
      WagerTransactionEntity,
      {
        status: WagerTransactionStatus.PendingReference,
        nextReferenceAttemptAt: { $lte: now },
      },
      {
        orderBy: {
          nextReferenceAttemptAt: QueryOrder.ASC,
          createdAt: QueryOrder.ASC,
        },
        limit,
      },
    );

    return entities.map((entity) => this.toDomain(entity));
  }

  async save(transaction: WagerTransaction): Promise<void> {
    const data = {
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      idempotencyKey: transaction.idempotencyKey,
      payloadHash: transaction.payloadHash,
      wallet: this.em.getReference(WalletEntity, transaction.walletId),
      playerId: transaction.playerId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      moneyAmount: transaction.money.toString(),
      moneyCurrency: transaction.money.currency,
      referenceExternalTransactionId:
        transaction.referenceExternalTransactionId,
      referenceTransaction: transaction.referenceTransactionId
        ? this.em.getReference(
            WagerTransactionEntity,
            transaction.referenceTransactionId,
          )
        : undefined,
      status: transaction.status,
      failureCode: transaction.failureCode,
      createdAt: transaction.createdAt,
      processedAt: transaction.processedAt,
      resultingBalanceAmount: transaction.resultingBalance?.toString(),
      resultingBalanceCurrency: transaction.resultingBalance?.currency,
      referenceAttempts: transaction.referenceAttempts,
      nextReferenceAttemptAt: transaction.nextReferenceAttemptAt,
    };

    const entity = await this.em.findOne(WagerTransactionEntity, {
      id: transaction.id,
    });

    if (entity) {
      this.em.assign(entity, data);
      return;
    }

    this.em.persist(
      this.em.create(WagerTransactionEntity, {
        id: transaction.id,
        ...data,
      }),
    );
  }

  private toDomain(entity: WagerTransactionEntity): WagerTransaction {
    const resultingBalance =
      entity.resultingBalanceAmount != null &&
      entity.resultingBalanceCurrency != null
        ? Money.from({
            amount: entity.resultingBalanceAmount,
            currency: entity.resultingBalanceCurrency,
          })
        : undefined;

    return WagerTransaction.rehydrate({
      id: entity.id,
      providerId: entity.providerId,
      externalTransactionId: entity.externalTransactionId,
      idempotencyKey: entity.idempotencyKey,
      payloadHash: entity.payloadHash,
      walletId: entity.wallet.id,
      playerId: entity.playerId,
      roundId: entity.roundId,
      gameId: entity.gameId,
      kind: entity.kind,
      money: Money.from({
        amount: entity.moneyAmount,
        currency: entity.moneyCurrency,
      }),
      referenceExternalTransactionId:
        entity.referenceExternalTransactionId ?? undefined,
      referenceTransactionId: entity.referenceTransaction?.id,
      createdAt: entity.createdAt,
      status: entity.status,
      failureCode: entity.failureCode ?? undefined,
      processedAt: entity.processedAt ?? undefined,
      resultingBalance,
      referenceAttempts: entity.referenceAttempts,
      nextReferenceAttemptAt: entity.nextReferenceAttemptAt ?? undefined,
    });
  }
}
