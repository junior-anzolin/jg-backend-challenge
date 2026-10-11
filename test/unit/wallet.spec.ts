import { describe, expect, it } from "bun:test";

import { DomainError } from "../../src/domain/shared/errors/domain.error";
import { Money } from "../../src/domain/shared/money/money";
import { CurrencyMismatchError } from "../../src/domain/shared/money/money.errors";
import { Wallet } from "../../src/domain/wagering/wallet/wallet";
import { InsufficientBalanceError } from "../../src/domain/wagering/wallet/wallet.errors";

function createMoney(amount: string, currency = "BRL"): Money {
  return Money.from({ amount, currency });
}

function openWallet(
  input: {
    id?: string;
    playerId?: string;
    initialBalance?: Money;
  } = {},
): Wallet {
  return Wallet.open({
    id: input.id ?? "wallet-1",
    playerId: input.playerId ?? "player-1",
    initialBalance: input.initialBalance ?? createMoney("100.00"),
  });
}

describe("Wallet", () => {
  describe("opening", () => {
    it("opens with the initial balance and version one", () => {
      const wallet = openWallet();

      expect(wallet.id).toBe("wallet-1");
      expect(wallet.playerId).toBe("player-1");
      expect(wallet.currency).toBe("BRL");
      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.createdAt).toBeInstanceOf(Date);
      expect(wallet.updatedAt).toBe(wallet.createdAt);
    });

    it("allows opening a zero-balance wallet", () => {
      const wallet = openWallet({
        initialBalance: createMoney("0.00"),
      });

      expect(wallet.balance.toString()).toBe("0.00");
      expect(wallet.version).toBe(1);
    });

    it("rejects a blank wallet identifier", () => {
      expect(() => openWallet({ id: "" })).toThrow(DomainError);
      expect(() => openWallet({ id: "   " })).toThrow(DomainError);
    });

    it("rejects a blank player identifier", () => {
      expect(() => openWallet({ playerId: "" })).toThrow(DomainError);
      expect(() => openWallet({ playerId: "   " })).toThrow(DomainError);
    });

    it("rejects a negative initial balance", () => {
      expect(() =>
        openWallet({
          initialBalance: createMoney("1.00").negate(),
        }),
      ).toThrow(DomainError);
    });
  });

  describe("debit", () => {
    it("debits the balance and increments the version", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      wallet.debit(createMoney("25.00"));

      expect(wallet.balance.toString()).toBe("75.00");
      expect(wallet.version).toBe(2);
      expect(wallet.updatedAt).not.toBe(previousUpdatedAt);
    });

    it("allows debiting the entire balance without making it negative", () => {
      const wallet = openWallet({
        initialBalance: createMoney("25.00"),
      });

      wallet.debit(createMoney("25.00"));

      expect(wallet.balance.toString()).toBe("0.00");
      expect(wallet.version).toBe(2);
    });

    it("rejects a debit that exceeds the available balance without changing wallet state", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      expect(() => wallet.debit(createMoney("100.01"))).toThrow(
        InsufficientBalanceError,
      );

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.updatedAt).toBe(previousUpdatedAt);
    });

    it("rejects a negative debit without changing wallet state", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      expect(() => wallet.debit(createMoney("1.00").negate())).toThrow(
        DomainError,
      );

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.updatedAt).toBe(previousUpdatedAt);
    });
  });

  describe("credit", () => {
    it("credits the balance and increments the version", () => {
      const wallet = openWallet();

      wallet.credit(createMoney("25.50"));

      expect(wallet.balance.toString()).toBe("125.50");
      expect(wallet.version).toBe(2);
    });

    it("rejects a negative credit without changing wallet state", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      expect(() => wallet.credit(createMoney("1.00").negate())).toThrow(
        DomainError,
      );

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.updatedAt).toBe(previousUpdatedAt);
    });
  });

  describe("currency validation", () => {
    it("rejects debits in a different currency", () => {
      const wallet = openWallet();

      expect(() => wallet.debit(createMoney("10.00", "USD"))).toThrow(
        CurrencyMismatchError,
      );

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
    });

    it("rejects credits in a different currency", () => {
      const wallet = openWallet();

      expect(() => wallet.credit(createMoney("10.00", "USD"))).toThrow(
        CurrencyMismatchError,
      );

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
    });
  });

  describe("version invariant", () => {
    it("does not increment the version when debiting zero", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      wallet.debit(createMoney("0.00"));

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.updatedAt).toBe(previousUpdatedAt);
    });

    it("does not increment the version when crediting zero", () => {
      const wallet = openWallet();
      const previousUpdatedAt = wallet.updatedAt;

      wallet.credit(createMoney("0.00"));

      expect(wallet.balance.toString()).toBe("100.00");
      expect(wallet.version).toBe(1);
      expect(wallet.updatedAt).toBe(previousUpdatedAt);
    });

    it("increments the version exactly once for each balance-changing operation", () => {
      const wallet = openWallet();

      wallet.debit(createMoney("10.00"));
      expect(wallet.version).toBe(2);

      wallet.credit(createMoney("5.00"));
      expect(wallet.version).toBe(3);

      wallet.debit(createMoney("5.00"));
      expect(wallet.version).toBe(4);

      expect(wallet.balance.toString()).toBe("90.00");
    });
  });
});
