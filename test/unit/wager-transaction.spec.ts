import { describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { Money } from "../../src/domain/shared/money/money";
import { LedgerDirection } from "../../src/domain/wagering/ledger/ledger-direction";
import { FailureCode } from "../../src/domain/wagering/transaction/failure-code";
import { WagerTransaction } from "../../src/domain/wagering/transaction/wager-transaction";
import { WagerTransactionKind } from "../../src/domain/wagering/transaction/wager-transaction-kind";
import { WagerTransactionStatus } from "../../src/domain/wagering/transaction/wager-transaction-status";
import {
  InvalidTransactionStateError,
  InvalidWagerTransactionError,
} from "../../src/domain/wagering/transaction/wager-transaction.errors";

const createMoney = (amount: string, currency = "BRL"): Money =>
  Money.from({ amount, currency });

function createTransaction(
  kind: WagerTransactionKind = WagerTransactionKind.Bet,
  referenceExternalTransactionId?: string,
): WagerTransaction {
  return WagerTransaction.create({
    id: randomUUID(),
    providerId: "provider-1",
    externalTransactionId: randomUUID(),
    idempotencyKey: randomUUID(),
    payloadHash: "payload-hash",
    walletId: randomUUID(),
    playerId: randomUUID(),
    roundId: "round-1",
    gameId: "game-1",
    kind,
    money: createMoney("25.00"),
    ...(referenceExternalTransactionId !== undefined
      ? { referenceExternalTransactionId }
      : {}),
  });
}

describe("WagerTransaction", () => {
  describe("creation", () => {
    it("starts in PENDING and preserves the business identity", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);

      expect(transaction.status).toBe(WagerTransactionStatus.Pending);
      expect(transaction.kind).toBe(WagerTransactionKind.Bet);
      expect(transaction.providerId).toBe("provider-1");
      expect(transaction.payloadHash).toBe("payload-hash");
      expect(transaction.isTerminal()).toBe(false);
      expect(transaction.referenceAttempts).toBe(0);
      expect(transaction.nextReferenceAttemptAt).toBeUndefined();
    });

    it("requires a non-blank external reference for REFUND and ROLLBACK", () => {
      for (const kind of [
        WagerTransactionKind.Refund,
        WagerTransactionKind.Rollback,
      ]) {
        for (const reference of [undefined, "", "   "]) {
          expect(() => createTransaction(kind, reference)).toThrow(
            InvalidWagerTransactionError,
          );
        }
      }
    });

    it("allows transactions without a reference when the kind does not require one", () => {
      for (const kind of [
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Loss,
      ]) {
        const transaction = createTransaction(kind);

        expect(transaction.status).toBe(WagerTransactionStatus.Pending);
        expect(transaction.referenceExternalTransactionId).toBeUndefined();
      }
    });
  });

  describe("reference ordering and retry scheduling", () => {
    it("moves a reference-dependent transaction to PENDING_REFERENCE", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-bet-123",
      );

      transaction.markPendingReference();

      expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
      expect(transaction.isTerminal()).toBe(false);
    });

    it("does not allow transactions without a required reference to become PENDING_REFERENCE", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);

      expect(() => transaction.markPendingReference()).toThrow(
        InvalidWagerTransactionError,
      );
      expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    });

    it("schedules exponential retries beginning at one second", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-bet-123",
      );
      const now = new Date("2026-01-01T00:00:00.000Z");

      transaction.markPendingReference();
      transaction.scheduleReferenceRetry(now);

      expect(transaction.referenceAttempts).toBe(1);
      expect(transaction.nextReferenceAttemptAt?.getTime()).toBe(
        now.getTime() + 1_000,
      );

      transaction.scheduleReferenceRetry(now);

      expect(transaction.referenceAttempts).toBe(2);
      expect(transaction.nextReferenceAttemptAt?.getTime()).toBe(
        now.getTime() + 2_000,
      );
    });

    it("caps the reference retry delay at five minutes", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Refund,
        "provider-bet-123",
      );
      const now = new Date("2026-01-01T00:00:00.000Z");

      transaction.markPendingReference();

      for (let attempt = 0; attempt < 10; attempt++) {
        transaction.scheduleReferenceRetry(now);
      }

      expect(transaction.referenceAttempts).toBe(10);
      expect(transaction.nextReferenceAttemptAt?.getTime()).toBe(
        now.getTime() + 300_000,
      );
    });

    it("rejects retry scheduling before entering PENDING_REFERENCE", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-reference",
      );

      expect(() => transaction.scheduleReferenceRetry(new Date())).toThrow(
        InvalidWagerTransactionError,
      );

      expect(transaction.referenceAttempts).toBe(0);
      expect(transaction.nextReferenceAttemptAt).toBeUndefined();
    });

    it("clears the scheduled retry when the reference is resolved and the transaction is processed", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-bet-123",
      );
      const now = new Date("2026-01-01T00:00:00.000Z");

      transaction.markPendingReference();
      transaction.scheduleReferenceRetry(now);

      transaction.markProcessed(
        randomUUID(),
        createMoney("100.00"),
        new Date("2026-01-01T00:00:02.000Z"),
      );

      expect(transaction.status).toBe(WagerTransactionStatus.Processed);
      expect(transaction.referenceTransactionId).toBeDefined();
      expect(transaction.resultingBalance?.toString()).toBe("100.00");
      expect(transaction.processedAt).toEqual(
        new Date("2026-01-01T00:00:02.000Z"),
      );
      expect(transaction.nextReferenceAttemptAt).toBeUndefined();
      expect(transaction.isTerminal()).toBe(true);
    });

    it("clears the scheduled retry when a pending reference is rejected", () => {
      const transaction = createTransaction(
        WagerTransactionKind.Rollback,
        "missing-reference",
      );
      const now = new Date("2026-01-01T00:00:00.000Z");

      transaction.markPendingReference();
      transaction.scheduleReferenceRetry(now);
      transaction.reject(
        FailureCode.ReferenceNotFound,
        createMoney("100.00"),
        new Date("2026-01-01T00:00:02.000Z"),
      );

      expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(transaction.failureCode).toBe(FailureCode.ReferenceNotFound);
      expect(transaction.nextReferenceAttemptAt).toBeUndefined();
      expect(transaction.isTerminal()).toBe(true);
    });
  });

  describe("state transitions", () => {
    it("requires a resolved reference before processing REFUND or ROLLBACK", () => {
      for (const kind of [
        WagerTransactionKind.Refund,
        WagerTransactionKind.Rollback,
      ]) {
        const transaction = createTransaction(kind, "provider-reference");

        expect(() =>
          transaction.markProcessed(
            undefined,
            createMoney("75.00"),
            new Date(),
          ),
        ).toThrow(InvalidWagerTransactionError);

        expect(transaction.status).toBe(WagerTransactionStatus.Pending);
      }
    });

    it("rejects processing when the resulting balance uses a different currency", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);

      expect(() =>
        transaction.markProcessed(
          undefined,
          createMoney("75.00", "USD"),
          new Date(),
        ),
      ).toThrow(InvalidWagerTransactionError);

      expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    });

    it("records rejection details and resulting balance", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);
      const at = new Date("2026-01-01T00:00:00.000Z");

      transaction.reject(
        FailureCode.InsufficientBalance,
        createMoney("10.00"),
        at,
      );

      expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(transaction.failureCode).toBe(FailureCode.InsufficientBalance);
      expect(transaction.resultingBalance?.toString()).toBe("10.00");
      expect(transaction.processedAt).toEqual(at);
      expect(transaction.isTerminal()).toBe(true);
    });

    it("allows rejection without a resulting balance", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);

      transaction.reject(FailureCode.CurrencyMismatch, undefined, new Date());

      expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(transaction.resultingBalance).toBeUndefined();
    });

    it("records permanent failure and its failure code", () => {
      const transaction = createTransaction(WagerTransactionKind.Bet);

      transaction.fail(FailureCode.PermanentInfrastructureFailure);

      expect(transaction.status).toBe(WagerTransactionStatus.Failed);
      expect(transaction.failureCode).toBe(
        FailureCode.PermanentInfrastructureFailure,
      );
      expect(transaction.isTerminal()).toBe(true);
    });

    it("does not allow any terminal transaction to transition again", () => {
      const terminalTransactions = [
        (() => {
          const transaction = createTransaction(WagerTransactionKind.Bet);
          transaction.markProcessed(
            undefined,
            createMoney("75.00"),
            new Date(),
          );
          return transaction;
        })(),
        (() => {
          const transaction = createTransaction(WagerTransactionKind.Bet);
          transaction.reject(
            FailureCode.InsufficientBalance,
            undefined,
            new Date(),
          );
          return transaction;
        })(),
        (() => {
          const transaction = createTransaction(WagerTransactionKind.Bet);
          transaction.fail(FailureCode.PermanentInfrastructureFailure);
          return transaction;
        })(),
      ];

      for (const transaction of terminalTransactions) {
        const originalStatus = transaction.status;

        expect(() =>
          transaction.markProcessed(
            undefined,
            createMoney("75.00"),
            new Date(),
          ),
        ).toThrow(InvalidTransactionStateError);

        expect(() => transaction.markPendingReference()).toThrow(
          InvalidTransactionStateError,
        );

        expect(() =>
          transaction.reject(
            FailureCode.InsufficientBalance,
            undefined,
            new Date(),
          ),
        ).toThrow(InvalidTransactionStateError);

        expect(() =>
          transaction.fail(FailureCode.PermanentInfrastructureFailure),
        ).toThrow(InvalidTransactionStateError);

        expect(transaction.status).toBe(originalStatus);
      }
    });
  });

  describe("business rules", () => {
    it("identifies LOSS as a transaction that does not affect the balance", () => {
      expect(
        createTransaction(WagerTransactionKind.Loss).affectsBalance(),
      ).toBe(false);

      for (const kind of [
        WagerTransactionKind.Opening,
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Refund,
        WagerTransactionKind.Rollback,
      ]) {
        const transaction =
          kind === WagerTransactionKind.Refund ||
          kind === WagerTransactionKind.Rollback
            ? createTransaction(kind, "provider-reference")
            : createTransaction(kind);

        expect(transaction.affectsBalance()).toBe(true);
      }
    });

    it("matches transactions only when the payload hash is identical", () => {
      const transaction = createTransaction();

      expect(transaction.matchesPayload("payload-hash")).toBe(true);
      expect(transaction.matchesPayload("different-payload-hash")).toBe(false);
    });

    it("maps normal transaction kinds to their ledger directions", () => {
      expect(
        createTransaction(WagerTransactionKind.Opening).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Credit);

      expect(
        createTransaction(WagerTransactionKind.Bet).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Debit);

      expect(
        createTransaction(WagerTransactionKind.Win).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Credit);

      expect(
        createTransaction(
          WagerTransactionKind.Refund,
          "provider-bet",
        ).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Credit);
    });

    it("reverses the direction based on the referenced transaction kind", () => {
      const rollback = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-reference",
      );

      expect(
        rollback.ledgerDirectionFor(
          createTransaction(WagerTransactionKind.Bet),
        ),
      ).toBe(LedgerDirection.Credit);

      expect(
        rollback.ledgerDirectionFor(
          createTransaction(WagerTransactionKind.Win),
        ),
      ).toBe(LedgerDirection.Debit);

      expect(
        rollback.ledgerDirectionFor(
          createTransaction(WagerTransactionKind.Refund, "provider-bet"),
        ),
      ).toBe(LedgerDirection.Debit);
    });

    it("rejects ledger direction for LOSS and ROLLBACK without a valid reference", () => {
      const loss = createTransaction(WagerTransactionKind.Loss);
      const rollback = createTransaction(
        WagerTransactionKind.Rollback,
        "provider-reference",
      );

      expect(() => loss.ledgerDirectionFor()).toThrow(
        InvalidWagerTransactionError,
      );

      expect(() => rollback.ledgerDirectionFor()).toThrow(
        InvalidWagerTransactionError,
      );

      expect(() =>
        rollback.ledgerDirectionFor(
          createTransaction(WagerTransactionKind.Loss),
        ),
      ).toThrow(InvalidWagerTransactionError);
    });
  });
});
