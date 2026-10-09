import type { EventContext } from "./event-context";
import { IntegrationEvent } from "./integration-event";

import { Money } from "../../shared/money/money";
import { WagerTransaction } from "../../wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../wagering/transaction/wager-transaction-kind";
import { WagerTransactionStatus } from "../../wagering/transaction/wager-transaction-status";
import { InvalidWagerTransactionError } from "../../wagering/transaction/wager-transaction.errors";

export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: ReturnType<Money["toJSON"]>;
  resultingBalance: ReturnType<Money["toJSON"]>;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    const resultingBalance = transaction.resultingBalance;

    if (
      transaction.status !== WagerTransactionStatus.Processed ||
      !resultingBalance
    ) {
      throw new InvalidWagerTransactionError(
        "Cannot create a processed event from an unprocessed transaction",
      );
    }

    return new WagerTransactionProcessed({
      eventId: crypto.randomUUID(),
      aggregateId: transaction.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: transaction.processedAt ?? new Date(),
      data: {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        resultingBalance: resultingBalance.toJSON(),
      },
    });
  }
}
