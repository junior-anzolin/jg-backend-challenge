import { DomainError } from "src/domain/shared/errors/domain.error";
import { Money } from "src/domain/shared/money/money";
import { CurrencyMismatchError } from "src/domain/shared/money/money.errors";
import { InsufficientBalanceError } from "./wallet.errors";
import { OpenWalletProps, WalletState } from "./wallet.type";

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open({ id, playerId, initialBalance }: OpenWalletProps): Wallet {
    if (!id.trim()) throw new DomainError("Wallet id is required");

    if (!playerId.trim()) throw new DomainError("Player id is required");

    if (initialBalance.isNegative())
      throw new DomainError("Wallet initial balance cannot be negative");

    const now = new Date();

    return new Wallet(
      id,
      playerId,
      initialBalance.currency,
      initialBalance,
      1,
      now,
      now,
    );
  }

  /**
   * Reconstructs persisted state without revalidating domain transitions.
   */
  static rehydrate({
    id,
    playerId,
    currency,
    balance,
    version,
    createdAt,
    updatedAt,
  }: WalletState): Wallet {
    return new Wallet(
      id,
      playerId,
      currency,
      balance,
      version,
      createdAt,
      updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(money: Money): void {
    this.assertSameCurrency(money);

    if (money.isNegative())
      throw new DomainError("Debit amount cannot be negative");

    if (this.balance.isLessThan(money)) throw new InsufficientBalanceError();

    const newBalance = this.balance.subtract(money);

    if (newBalance.equals(this.balance)) return;

    this._balance = newBalance;
    this._version++;
    this._updatedAt = new Date();
  }
  credit(money: Money): void {
    this.assertSameCurrency(money);

    if (money.isNegative())
      throw new DomainError("Credit amount cannot be negative");

    const newBalance = this.balance.add(money);

    if (newBalance.equals(this.balance)) return;

    this._balance = newBalance;
    this._version++;
    this._updatedAt = new Date();
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
