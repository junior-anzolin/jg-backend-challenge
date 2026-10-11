import { describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { Money } from "../../src/domain/shared/money/money";
import { LedgerDirection } from "../../src/domain/wagering/ledger/ledger-direction";
import { WalletLedgerEntry } from "../../src/domain/wagering/ledger/wallet-ledger-entry";
import { InvalidLedgerEntryError } from "../../src/domain/wagering/ledger/wallet-ledger-entry.errors";
import type { CreateLedgerEntryProps } from "../../src/domain/wagering/ledger/wallet-ledger-entry.types";

function createMoney(amount: string, currency = "BRL"): Money {
  return Money.from({ amount, currency });
}

function createEntry(
  overrides: Partial<CreateLedgerEntryProps> = {},
): WalletLedgerEntry {
  return WalletLedgerEntry.create({
    id: randomUUID(),
    walletId: randomUUID(),
    transactionId: randomUUID(),
    direction: LedgerDirection.Credit,
    money: createMoney("25.00"),
    balanceBefore: createMoney("100.00"),
    balanceAfter: createMoney("125.00"),
    ...overrides,
  });
}

describe("WalletLedgerEntry", () => {
  describe("creation", () => {
    it("creates a balanced DEBIT entry", () => {
      const entry = createEntry({
        direction: LedgerDirection.Debit,
        money: createMoney("25.00"),
        balanceBefore: createMoney("100.00"),
        balanceAfter: createMoney("75.00"),
      });

      expect(entry.direction).toBe(LedgerDirection.Debit);
      expect(entry.money.toString()).toBe("25.00");
      expect(entry.balanceBefore.toString()).toBe("100.00");
      expect(entry.balanceAfter.toString()).toBe("75.00");
      expect(entry.isBalanced()).toBe(true);
    });

    it("creates a balanced CREDIT entry", () => {
      const entry = createEntry();

      expect(entry.direction).toBe(LedgerDirection.Credit);
      expect(entry.money.toString()).toBe("25.00");
      expect(entry.balanceBefore.toString()).toBe("100.00");
      expect(entry.balanceAfter.toString()).toBe("125.00");
      expect(entry.isBalanced()).toBe(true);
    });

    it("creates an entry with a zero amount when the balance is unchanged", () => {
      const entry = createEntry({
        money: createMoney("0.00"),
        balanceBefore: createMoney("100.00"),
        balanceAfter: createMoney("100.00"),
      });

      expect(entry.money.toString()).toBe("0.00");
      expect(entry.isBalanced()).toBe(true);
    });

    it("assigns a creation timestamp", () => {
      const entry = createEntry();

      expect(entry.createdAt).toBeInstanceOf(Date);
      expect(Number.isNaN(entry.createdAt.getTime())).toBe(false);
    });

    it("preserves the entry identity", () => {
      const id = randomUUID();
      const walletId = randomUUID();
      const transactionId = randomUUID();

      const entry = createEntry({
        id,
        walletId,
        transactionId,
      });

      expect(entry.id).toBe(id);
      expect(entry.walletId).toBe(walletId);
      expect(entry.transactionId).toBe(transactionId);
    });
  });

  describe("balance validation", () => {
    it("rejects a DEBIT whose resulting balance is incorrect", () => {
      expect(() =>
        createEntry({
          direction: LedgerDirection.Debit,
          money: createMoney("25.00"),
          balanceBefore: createMoney("100.00"),
          balanceAfter: createMoney("80.00"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });

    it("rejects a CREDIT whose resulting balance is incorrect", () => {
      expect(() =>
        createEntry({
          direction: LedgerDirection.Credit,
          money: createMoney("25.00"),
          balanceBefore: createMoney("100.00"),
          balanceAfter: createMoney("120.00"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });

    it("rejects a negative transaction amount", () => {
      expect(() =>
        createEntry({
          money: createMoney("1.00").negate(),
          balanceBefore: createMoney("100.00"),
          balanceAfter: createMoney("99.00"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });

    it("rejects a negative balance before the transaction", () => {
      expect(() =>
        createEntry({
          direction: LedgerDirection.Credit,
          money: createMoney("25.00"),
          balanceBefore: createMoney("0.00").negate(),
          balanceAfter: createMoney("25.00"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });

    it("rejects a negative balance after the transaction", () => {
      expect(() =>
        createEntry({
          direction: LedgerDirection.Debit,
          money: createMoney("25.00"),
          balanceBefore: createMoney("10.00"),
          balanceAfter: createMoney("15.00").negate(),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });
  });

  describe("currency validation", () => {
    it("rejects a transaction amount in a different currency from the balances", () => {
      expect(() =>
        createEntry({
          money: createMoney("25.00", "USD"),
          balanceBefore: createMoney("100.00", "BRL"),
          balanceAfter: createMoney("125.00", "BRL"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });

    it("rejects balances that use different currencies", () => {
      expect(() =>
        createEntry({
          money: createMoney("25.00", "BRL"),
          balanceBefore: createMoney("100.00", "BRL"),
          balanceAfter: createMoney("125.00", "USD"),
        }),
      ).toThrow(InvalidLedgerEntryError);
    });
  });

  describe("rehydration", () => {
    it("restores a persisted entry without changing its identity or timestamp", () => {
      const createdAt = new Date("2026-01-01T12:00:00.000Z");

      const state: CreateLedgerEntryProps & { createdAt: Date } = {
        id: "ledger-entry-1",
        walletId: "wallet-1",
        transactionId: "transaction-1",
        direction: LedgerDirection.Debit,
        money: createMoney("25.00"),
        balanceBefore: createMoney("100.00"),
        balanceAfter: createMoney("75.00"),
        createdAt,
      };

      const entry = WalletLedgerEntry.rehydrate(state);

      expect(entry.id).toBe(state.id);
      expect(entry.walletId).toBe(state.walletId);
      expect(entry.transactionId).toBe(state.transactionId);
      expect(entry.direction).toBe(LedgerDirection.Debit);
      expect(entry.money.toJSON()).toEqual({
        amount: "25.00",
        currency: "BRL",
      });
      expect(entry.balanceBefore.toString()).toBe("100.00");
      expect(entry.balanceAfter.toString()).toBe("75.00");
      expect(entry.createdAt).toBe(createdAt);
      expect(entry.isBalanced()).toBe(true);
    });

    it("reports an unbalanced rehydrated entry without creating a new entry", () => {
      const state: CreateLedgerEntryProps & { createdAt: Date } = {
        id: "ledger-entry-invalid",
        walletId: "wallet-1",
        transactionId: "transaction-1",
        direction: LedgerDirection.Debit,
        money: createMoney("25.00"),
        balanceBefore: createMoney("100.00"),
        balanceAfter: createMoney("90.00"),
        createdAt: new Date("2026-01-01T12:00:00.000Z"),
      };

      const entry = WalletLedgerEntry.rehydrate(state);

      expect(entry.id).toBe(state.id);
      expect(entry.isBalanced()).toBe(false);
    });
  });
});
