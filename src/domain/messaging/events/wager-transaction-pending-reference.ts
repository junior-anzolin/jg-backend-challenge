import { IntegrationEvent } from "./integration-event";
import type { EventContext } from "./event-context";

import { Money } from "../../shared/money/money";
import { WagerTransaction } from "../../wagering/transaction/wager-transaction";
import { WagerTransactionStatus } from "../../wagering/transaction/wager-transaction-status";
import { InvalidWagerTransactionError } from "../../wagering/transaction/wager-transaction.errors";

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  kind: string;
  money: ReturnType<Money["toJSON"]>;
  referenceExternalTransactionId: string;
  referenceAttempts: number;
  nextReferenceAttemptAt?: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    ctx: EventContext,
    occurredAt: Date = new Date(),
  ): WagerTransactionPendingReference {
    const referenceExternalTransactionId =
      transaction.referenceExternalTransactionId;

    if (
      transaction.status !== WagerTransactionStatus.PendingReference ||
      !referenceExternalTransactionId
    ) {
      throw new InvalidWagerTransactionError(
        "Cannot create a pending-reference event for this transaction",
      );
    }

    return new WagerTransactionPendingReference({
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
        kind: transaction.kind,
        money: transaction.money.toJSON(),
        referenceExternalTransactionId,
        referenceAttempts: transaction.referenceAttempts,
        nextReferenceAttemptAt:
          transaction.nextReferenceAttemptAt?.toISOString(),
      },
    });
  }
}
