import Decimal from "decimal.js";

import { CurrencyMismatchError, InvalidMoneyError } from "./money.errors";
import { MoneyProps } from "./money.type";

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {}

  static from({ currency, amount }: MoneyProps): Money {
    if (typeof amount !== "string" || !/^\d+\.\d{2}$/.test(amount))
      throw new InvalidMoneyError(`Invalid monetary amount: ${amount}`);

    if (typeof currency !== "string" || !/^[A-Z]{3}$/.test(currency))
      throw new InvalidMoneyError(`Invalid currency: ${currency}`);

    return new Money(new Decimal(amount), currency);
  }

  static zero(currency: string): Money {
    return this.from({ amount: "0.00", currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);

    return new Money(this.value.add(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);

    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    return new Money(this.value.negated(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.isPositive();
  }

  isNegative(): boolean {
    return this.value.isNegative();
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);

    return this.value.lessThan(other.value);
  }

  equals({ currency, value }: Money): boolean {
    return currency === this.currency && this.value.equals(value);
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    return this.value.toFixed(2);
  }

  private assertSameCurrency({ currency }: Money): void {
    if (currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, currency);
    }
  }
}
