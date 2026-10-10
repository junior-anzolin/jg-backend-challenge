import { Inject, Injectable } from "@nestjs/common";
import { createHash, randomUUID } from "node:crypto";

import {
  UNIT_OF_WORK,
  UnitOfWorkContext,
  UnitOfWorkPort,
} from "../ports/unit-of-work.port";

import { Money } from "../../domain/shared/money/money";
import { MoneyProps } from "../../domain/shared/money/money.type";
import { LedgerDirection } from "../../domain/wagering/ledger/ledger-direction";
import { WalletLedgerEntry } from "../../domain/wagering/ledger/wallet-ledger-entry";
import { FailureCode } from "../../domain/wagering/transaction/failure-code";
import { WagerTransaction } from "../../domain/wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../domain/wagering/transaction/wager-transaction-kind";
import { WagerTransactionStatus } from "../../domain/wagering/transaction/wager-transaction-status";
import { InsufficientBalanceError } from "../../domain/wagering/wallet/wallet.errors";

import { EventContext } from "../../domain/messaging/events/event-context";
import { WagerTransactionPendingReference } from "../../domain/messaging/events/wager-transaction-pending-reference";
import { WagerTransactionProcessed } from "../../domain/messaging/events/wager-transaction-processed";
import { WagerTransactionRejected } from "../../domain/messaging/events/wager-transaction-rejected";
import { WalletBalanceChanged } from "../../domain/messaging/events/wallet-balance-changed";
import { OutboxMessage } from "../../domain/messaging/outbox/outbox-message";

export interface ProcessWagerTransactionInput {
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  causationId?: string;
}

export interface ProcessWagerTransactionResult {
  transactionId: string;
  status: WagerTransactionStatus;
  balance?: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}

export class IdempotencyConflictError extends Error {
  constructor() {
    super("Idempotency key was already used with a different payload");
    this.name = IdempotencyConflictError.name;
  }
}

export class WagerTransactionAlreadyExistsError extends Error {
  constructor() {
    super("Provider transaction already exists with a different payload");
    this.name = WagerTransactionAlreadyExistsError.name;
  }
}

export class WalletNotFoundError extends Error {
  constructor(walletId: string) {
    super(`Wallet ${walletId} was not found`);
    this.name = WalletNotFoundError.name;
  }
}

@Injectable()
export class ProcessWagerTransactionUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(
    input: ProcessWagerTransactionInput,
  ): Promise<ProcessWagerTransactionResult> {
    if (!input.idempotencyKey.trim()) {
      throw new Error("Idempotency-Key is required");
    }

    if (input.kind === WagerTransactionKind.Opening) {
      throw new Error("OPENING transactions are internal only");
    }

    const money = Money.from(input.money);

    if (!money.isPositive()) {
      throw new Error("Transaction amount must be greater than zero");
    }

    const payloadHash = this.createPayloadHash(input, money);
    const eventContext: EventContext = {
      correlationId: input.correlationId?.trim() || randomUUID(),
      causationId: input.causationId,
    };

    return this.unitOfWork.runInTransaction(async (context) => {
      const existing = await context.wagerTransactions.findByIdempotencyKey(
        input.idempotencyKey,
      );

      if (existing) {
        return this.replay(existing, payloadHash);
      }

      const existingExternal =
        await context.wagerTransactions.findByProviderAndExternalTransactionId(
          input.providerId,
          input.externalTransactionId,
        );

      if (existingExternal) {
        if (!existingExternal.matchesPayload(payloadHash)) {
          throw new WagerTransactionAlreadyExistsError();
        }

        return this.toResult(existingExternal, true);
      }

      const wallet = await context.wallets.findByIdForUpdate(input.walletId);

      if (!wallet) {
        throw new WalletNotFoundError(input.walletId);
      }

      if (wallet.playerId !== input.playerId) {
        throw new Error("Wallet does not belong to the supplied player");
      }

      // Recheck after acquiring the wallet lock: a concurrent operation
      // may have committed while this operation was waiting.
      const concurrentExisting =
        await context.wagerTransactions.findByIdempotencyKey(
          input.idempotencyKey,
        );

      if (concurrentExisting) {
        return this.replay(concurrentExisting, payloadHash);
      }

      const transaction = WagerTransaction.create({
        id: randomUUID(),
        providerId: input.providerId,
        externalTransactionId: input.externalTransactionId,
        idempotencyKey: input.idempotencyKey,
        payloadHash,
        walletId: wallet.id,
        playerId: input.playerId,
        roundId: input.roundId,
        gameId: input.gameId,
        kind: input.kind,
        money,
        referenceExternalTransactionId: input.referenceExternalTransactionId,
      });

      if (wallet.currency !== money.currency) {
        return this.reject(
          context,
          transaction,
          FailureCode.CurrencyMismatch,
          eventContext,
          undefined,
        );
      }

      let reference: WagerTransaction | undefined;

      if (transaction.requiresReference()) {
        reference =
          (await context.wagerTransactions.findByProviderAndExternalTransactionId(
            transaction.providerId,
            transaction.referenceExternalTransactionId!,
          )) ?? undefined;

        if (!reference) {
          const now = new Date();
          transaction.markPendingReference();
          transaction.scheduleReferenceRetry(now);

          await context.wagerTransactions.save(transaction);

          await context.outboxMessages.save(
            OutboxMessage.enqueue(
              WagerTransactionPendingReference.from(
                transaction,
                eventContext,
                now,
              ),
            ),
          );

          return this.toResult(transaction);
        }

        const referenceFailure = await this.validateReference(
          context,
          transaction,
          reference,
        );

        if (referenceFailure) {
          return this.reject(
            context,
            transaction,
            referenceFailure,
            eventContext,
            wallet.balance,
          );
        }
      }

      const balanceBefore = wallet.balance;

      if (transaction.affectsBalance()) {
        let direction: LedgerDirection;

        try {
          direction = transaction.ledgerDirectionFor(reference);

          if (direction === LedgerDirection.Debit) {
            wallet.debit(transaction.money);
          } else {
            wallet.credit(transaction.money);
          }
        } catch (error) {
          if (error instanceof InsufficientBalanceError) {
            const code =
              transaction.kind === WagerTransactionKind.Rollback
                ? FailureCode.ReversalWouldOverdraw
                : FailureCode.InsufficientBalance;

            return this.reject(
              context,
              transaction,
              code,
              eventContext,
              wallet.balance,
            );
          }

          throw error;
        }

        const balanceAfter = wallet.balance;

        const entry = WalletLedgerEntry.create({
          id: randomUUID(),
          walletId: wallet.id,
          transactionId: transaction.id,
          direction,
          money: transaction.money,
          balanceBefore,
          balanceAfter,
        });

        transaction.markProcessed(reference?.id, balanceAfter, new Date());

        await context.wallets.save(wallet);
        await context.ledgerEntries.save(entry);

        await context.outboxMessages.save(
          OutboxMessage.enqueue(
            WalletBalanceChanged.from(wallet, entry, eventContext),
          ),
        );
      } else {
        // LOSS records the result but does not move the wallet balance.
        transaction.markProcessed(undefined, wallet.balance, new Date());
      }

      await context.wagerTransactions.save(transaction);

      await context.outboxMessages.save(
        OutboxMessage.enqueue(
          WagerTransactionProcessed.from(transaction, eventContext),
        ),
      );

      return this.toResult(transaction);
    });
  }

  private async validateReference(
    context: UnitOfWorkContext,
    transaction: WagerTransaction,
    reference: WagerTransaction,
  ): Promise<FailureCode | undefined> {
    if (reference.status !== WagerTransactionStatus.Processed) {
      return FailureCode.InvalidReference;
    }

    if (
      reference.playerId !== transaction.playerId ||
      reference.walletId !== transaction.walletId ||
      reference.roundId !== transaction.roundId
    ) {
      return FailureCode.InvalidReference;
    }

    if (reference.money.currency !== transaction.money.currency) {
      return FailureCode.CurrencyMismatch;
    }

    if (!reference.money.equals(transaction.money)) {
      return FailureCode.ReferenceAmountMismatch;
    }

    if (
      transaction.kind === WagerTransactionKind.Refund &&
      reference.kind !== WagerTransactionKind.Bet
    ) {
      return FailureCode.InvalidReference;
    }

    if (
      transaction.kind === WagerTransactionKind.Rollback &&
      ![
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
      ].includes(reference.kind)
    ) {
      return FailureCode.InvalidReference;
    }

    const previousReversal =
      await context.wagerTransactions.findReversalByReferenceAndKind(
        reference.id,
        transaction.kind as
          | WagerTransactionKind.Refund
          | WagerTransactionKind.Rollback,
      );

    if (previousReversal) {
      return FailureCode.ReferenceAlreadyReversed;
    }

    return undefined;
  }

  private async reject(
    context: UnitOfWorkContext,
    transaction: WagerTransaction,
    failureCode: FailureCode,
    eventContext: EventContext,
    resultingBalance: Money | undefined,
  ): Promise<ProcessWagerTransactionResult> {
    transaction.reject(failureCode, resultingBalance, new Date());

    await context.wagerTransactions.save(transaction);

    await context.outboxMessages.save(
      OutboxMessage.enqueue(
        WagerTransactionRejected.from(transaction, eventContext),
      ),
    );

    return this.toResult(transaction);
  }

  private replay(
    transaction: WagerTransaction,
    payloadHash: string,
  ): ProcessWagerTransactionResult {
    if (!transaction.matchesPayload(payloadHash)) {
      throw new IdempotencyConflictError();
    }

    return this.toResult(transaction, true);
  }

  private toResult(
    transaction: WagerTransaction,
    idempotentReplay = false,
  ): ProcessWagerTransactionResult {
    return {
      transactionId: transaction.id,
      status: transaction.status,
      ...(transaction.resultingBalance
        ? { balance: transaction.resultingBalance.toJSON() }
        : {}),
      ...(transaction.failureCode
        ? { failureCode: transaction.failureCode }
        : {}),
      idempotentReplay,
    };
  }

  private createPayloadHash(
    input: ProcessWagerTransactionInput,
    money: Money,
  ): string {
    // Object keys are constructed in a fixed order to ensure deterministic hashing.
    const canonicalPayload = JSON.stringify({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.playerId,
      walletId: input.walletId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      money: money.toJSON(),
      referenceExternalTransactionId:
        input.referenceExternalTransactionId ?? null,
    });

    return createHash("sha256").update(canonicalPayload).digest("hex");
  }
}
