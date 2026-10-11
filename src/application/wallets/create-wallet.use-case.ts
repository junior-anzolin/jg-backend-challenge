import { Inject, Injectable } from "@nestjs/common";
import { createHash, randomUUID } from "node:crypto";

import { WagerTransactionProcessed } from "../../domain/messaging/events/wager-transaction-processed";
import { WalletBalanceChanged } from "../../domain/messaging/events/wallet-balance-changed";
import { OutboxMessage } from "../../domain/messaging/outbox/outbox-message";
import { Money } from "../../domain/shared/money/money";
import { MoneyProps } from "../../domain/shared/money/money.type";
import { LedgerDirection } from "../../domain/wagering/ledger/ledger-direction";
import { WalletLedgerEntry } from "../../domain/wagering/ledger/wallet-ledger-entry";
import { WagerTransaction } from "../../domain/wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../domain/wagering/transaction/wager-transaction-kind";
import { Wallet } from "../../domain/wagering/wallet/wallet";
import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";

export interface CreateWalletInput {
  playerId: string;
  initialBalance: MoneyProps;
  correlationId?: string;
}

export class WalletAlreadyExistsError extends Error {
  constructor(playerId: string, currency: string) {
    super(
      `Wallet already exists for player ${playerId} and currency ${currency}`,
    );
    this.name = WalletAlreadyExistsError.name;
  }
}

@Injectable()
export class CreateWalletUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(input: CreateWalletInput): Promise<Wallet> {
    const initialBalance = Money.from(input.initialBalance);

    return this.unitOfWork.runInTransaction(async (context) => {
      const existingWallet = await context.wallets.findByPlayerIdAndCurrency(
        input.playerId,
        initialBalance.currency,
      );

      if (existingWallet) {
        throw new WalletAlreadyExistsError(
          input.playerId,
          initialBalance.currency,
        );
      }

      const wallet = Wallet.open({
        id: randomUUID(),
        playerId: input.playerId,
        initialBalance,
      });

      await context.wallets.save(wallet);

      if (initialBalance.isPositive()) {
        const openingTransaction = this.createOpeningTransaction(
          wallet,
          initialBalance,
        );

        openingTransaction.markProcessed(undefined, initialBalance, new Date());

        const ledgerEntry = WalletLedgerEntry.create({
          id: randomUUID(),
          walletId: wallet.id,
          transactionId: openingTransaction.id,
          direction: LedgerDirection.Credit,
          money: initialBalance,
          balanceBefore: Money.zero(initialBalance.currency),
          balanceAfter: initialBalance,
        });

        await context.wagerTransactions.save(openingTransaction);
        await context.ledgerEntries.save(ledgerEntry);

        const eventContext = {
          correlationId: input.correlationId?.trim() || randomUUID(),
        };

        const events = [
          WagerTransactionProcessed.from(openingTransaction, eventContext),
          WalletBalanceChanged.from(wallet, ledgerEntry, eventContext),
        ];

        for (const event of events) {
          await context.outboxMessages.save(OutboxMessage.enqueue(event));
        }
      }

      return wallet;
    });
  }

  private createOpeningTransaction(
    wallet: Wallet,
    initialBalance: Money,
  ): WagerTransaction {
    const providerId = "internal";
    const externalTransactionId = `wallet-opening:${wallet.id}`;
    const idempotencyKey = `wallet-opening:${wallet.id}`;
    const roundId = "wallet-opening";
    const gameId = "internal";

    const payloadHash = createHash("sha256")
      .update(
        JSON.stringify({
          gameId,
          kind: WagerTransactionKind.Opening,
          money: initialBalance.toJSON(),
          playerId: wallet.playerId,
          roundId,
          walletId: wallet.id,
        }),
      )
      .digest("hex");

    return WagerTransaction.create({
      id: randomUUID(),
      providerId,
      externalTransactionId,
      idempotencyKey,
      payloadHash,
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId,
      gameId,
      kind: WagerTransactionKind.Opening,
      money: initialBalance,
    });
  }
}
