import { Inject, Injectable } from "@nestjs/common";

import type { MoneyProps } from "../../domain/shared/money/money.type";
import type { FailureCode } from "../../domain/wagering/transaction/failure-code";
import type { WagerTransaction } from "../../domain/wagering/transaction/wager-transaction";
import type { WagerTransactionStatus } from "../../domain/wagering/transaction/wager-transaction-status";
import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";

export interface WagerTransactionDetails {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  status: WagerTransactionStatus;
  failureCode?: FailureCode;
  createdAt: string;
  processedAt?: string;
  balance?: MoneyProps;
  referenceAttempts: number;
  nextReferenceAttemptAt?: string;
}

@Injectable()
export class GetWagerTransactionUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async executeById(
    transactionId: string,
  ): Promise<WagerTransactionDetails | null> {
    const transaction = await this.unitOfWork.runInTransaction((context) =>
      context.wagerTransactions.findById(transactionId),
    );

    return transaction ? this.toDetails(transaction) : null;
  }

  async executeByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransactionDetails | null> {
    const transaction = await this.unitOfWork.runInTransaction((context) =>
      context.wagerTransactions.findByProviderAndExternalTransactionId(
        providerId,
        externalTransactionId,
      ),
    );

    return transaction ? this.toDetails(transaction) : null;
  }

  private toDetails(transaction: WagerTransaction): WagerTransactionDetails {
    return {
      transactionId: transaction.id,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      idempotencyKey: transaction.idempotencyKey,
      playerId: transaction.playerId,
      walletId: transaction.walletId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      money: transaction.money.toJSON(),
      ...(transaction.referenceExternalTransactionId
        ? {
            referenceExternalTransactionId:
              transaction.referenceExternalTransactionId,
          }
        : {}),
      ...(transaction.referenceTransactionId
        ? { referenceTransactionId: transaction.referenceTransactionId }
        : {}),
      status: transaction.status,
      ...(transaction.failureCode
        ? { failureCode: transaction.failureCode }
        : {}),
      createdAt: transaction.createdAt.toISOString(),
      ...(transaction.processedAt
        ? { processedAt: transaction.processedAt.toISOString() }
        : {}),
      ...(transaction.resultingBalance
        ? { balance: transaction.resultingBalance.toJSON() }
        : {}),
      referenceAttempts: transaction.referenceAttempts,
      ...(transaction.nextReferenceAttemptAt
        ? {
            nextReferenceAttemptAt:
              transaction.nextReferenceAttemptAt.toISOString(),
          }
        : {}),
    };
  }
}
