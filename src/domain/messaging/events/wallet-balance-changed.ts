import type { EventContext } from "./event-context";
import { IntegrationEvent } from "./integration-event";

import { Money } from "../../shared/money/money";
import { LedgerDirection } from "../../wagering/ledger/ledger-direction";
import { WalletLedgerEntry } from "../../wagering/ledger/wallet-ledger-entry";
import { Wallet } from "../../wagering/wallet/wallet";

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: ReturnType<Money["toJSON"]>;
  balanceBefore: ReturnType<Money["toJSON"]>;
  balanceAfter: ReturnType<Money["toJSON"]>;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: crypto.randomUUID(),
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: entry.createdAt,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
