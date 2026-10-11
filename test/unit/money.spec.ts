import { describe, expect, it } from "bun:test";

import { Money } from "../../src/domain/shared/money/money";
import {
  CurrencyMismatchError,
  InvalidMoneyError,
} from "../../src/domain/shared/money/money.errors";

describe("Money", () => {
  describe("creation and serialization", () => {
    it("creates monetary values with exactly two decimal places", () => {
      const values = [
        { amount: "0.00", expected: "0.00" },
        { amount: "0.10", expected: "0.10" },
        { amount: "25.00", expected: "25.00" },
        { amount: "999999.99", expected: "999999.99" },
      ];

      for (const value of values) {
        const money = Money.from({
          amount: value.amount,
          currency: "BRL",
        });

        expect(money.toString()).toBe(value.expected);
        expect(money.toJSON()).toEqual({
          amount: value.expected,
          currency: "BRL",
        });
      }
    });

    it("creates a zero value for the requested currency", () => {
      const money = Money.zero("BRL");

      expect(money.toString()).toBe("0.00");
      expect(money.currency).toBe("BRL");
      expect(money.isZero()).toBe(true);
      expect(money.isPositive()).toBe(false);
      expect(money.isNegative()).toBe(false);
    });

    it("rejects invalid monetary amount formats", () => {
      const invalidAmounts = [
        "",
        " ",
        "NaN",
        "Infinity",
        "-Infinity",
        "1e2",
        "25",
        "25.0",
        "25.000",
        "-1.00",
        "-0.01",
        "+25.00",
        " 25.00",
        "25.00 ",
        ".50",
        "25,00",
        "1.2.3",
      ];

      for (const amount of invalidAmounts) {
        expect(() =>
          Money.from({
            amount,
            currency: "BRL",
          }),
        ).toThrow(InvalidMoneyError);
      }
    });

    it("rejects invalid currency codes", () => {
      const invalidCurrencies = [
        "",
        " ",
        "brl",
        "BrL",
        "BR",
        "BRLL",
        "1RL",
        "R$",
      ];

      for (const currency of invalidCurrencies) {
        expect(() =>
          Money.from({
            amount: "25.00",
            currency,
          }),
        ).toThrow(InvalidMoneyError);
      }
    });
  });

  describe("arithmetic and precision", () => {
    it("adds decimal values exactly without floating-point precision errors", () => {
      const left = Money.from({
        amount: "0.10",
        currency: "BRL",
      });
      const right = Money.from({
        amount: "0.20",
        currency: "BRL",
      });

      const result = left.add(right);

      expect(result.toString()).toBe("0.30");
      expect(result.toJSON()).toEqual({
        amount: "0.30",
        currency: "BRL",
      });
    });

    it("subtracts decimal values while preserving the fixed scale", () => {
      const left = Money.from({
        amount: "10.30",
        currency: "BRL",
      });
      const right = Money.from({
        amount: "0.20",
        currency: "BRL",
      });

      expect(left.subtract(right).toString()).toBe("10.10");
    });

    it("preserves negative results from subtraction without mutating the operands", () => {
      const left = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const right = Money.from({
        amount: "15.25",
        currency: "BRL",
      });

      const result = left.subtract(right);

      expect(result.toString()).toBe("-5.25");
      expect(result.isNegative()).toBe(true);
      expect(result.isPositive()).toBe(false);
      expect(result.isZero()).toBe(false);
      expect(left.toString()).toBe("10.00");
      expect(right.toString()).toBe("15.25");
    });

    it("negates a monetary value without changing the original", () => {
      const original = Money.from({
        amount: "12.50",
        currency: "BRL",
      });

      const negative = original.negate();
      const positiveAgain = negative.negate();

      expect(negative.toString()).toBe("-12.50");
      expect(negative.isNegative()).toBe(true);
      expect(positiveAgain.toString()).toBe("12.50");
      expect(original.toString()).toBe("12.50");
    });

    it("returns a new instance for arithmetic operations", () => {
      const left = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const right = Money.from({
        amount: "5.00",
        currency: "BRL",
      });

      const sum = left.add(right);
      const difference = left.subtract(right);
      const negated = left.negate();

      expect(sum).not.toBe(left);
      expect(difference).not.toBe(left);
      expect(negated).not.toBe(left);

      expect(left.toString()).toBe("10.00");
      expect(right.toString()).toBe("5.00");
    });
  });

  describe("comparisons", () => {
    it("compares values by amount and currency", () => {
      const ten = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const tenAgain = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const twenty = Money.from({
        amount: "20.00",
        currency: "BRL",
      });

      expect(ten.equals(tenAgain)).toBe(true);
      expect(ten.equals(twenty)).toBe(false);
      expect(ten.isLessThan(twenty)).toBe(true);
      expect(twenty.isLessThan(ten)).toBe(false);
      expect(ten.isLessThan(tenAgain)).toBe(false);
    });

    it("does not consider equal amounts in different currencies equal", () => {
      const real = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const dollar = Money.from({
        amount: "10.00",
        currency: "USD",
      });

      expect(real.equals(dollar)).toBe(false);
    });

    it("rejects arithmetic and ordering comparisons between different currencies", () => {
      const real = Money.from({
        amount: "10.00",
        currency: "BRL",
      });
      const dollar = Money.from({
        amount: "5.00",
        currency: "USD",
      });

      expect(() => real.add(dollar)).toThrow(CurrencyMismatchError);
      expect(() => real.subtract(dollar)).toThrow(CurrencyMismatchError);
      expect(() => real.isLessThan(dollar)).toThrow(CurrencyMismatchError);
    });
  });
});
