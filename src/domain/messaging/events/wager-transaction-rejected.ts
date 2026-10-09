import type { EventContext } from "./event-context";
import { IntegrationEvent } from "./integration-event";

import { Money } from "../../shared/money/money";
import { FailureCode } from "../../wagering/transaction/failure-code";
import { WagerTransaction } from "../../wagering/transaction/wager-transaction";
import { WagerTransactionStatus } from "../../wagering/transaction/wager-transaction-status";
import { InvalidWagerTransactionError } from "../../wagering/transaction/wager-transaction.errors";

export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  money: ReturnType<Money["toJSON"]>;
  failureCode: FailureCode;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    ctx: EventContext,
    occurredAt: Date = new Date(),
  ): WagerTransactionRejected {
    const failureCode = transaction.failureCode;

    if (
      transaction.status !== WagerTransactionStatus.Rejected ||
      !failureCode
    ) {
      throw new InvalidWagerTransactionError(
        "Cannot create a rejected event from a transaction that was not rejected",
      );
    }

    return new WagerTransactionRejected({
      eventId: crypto.randomUUID(),
      aggregateId: transaction.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt,
      data: {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        money: transaction.money.toJSON(),
        failureCode,
      },
    });
  }
}
