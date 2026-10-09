import { Money } from "../../shared/money/money";
import { LedgerDirection } from "./ledger-direction";
import { InvalidLedgerEntryError } from "./wallet-ledger-entry.errors";
import {
  CreateLedgerEntryProps,
  LedgerEntryState,
} from "./wallet-ledger-entry.types";

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {}

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    const { money, balanceBefore, balanceAfter } = props;

    if (
      money.currency !== balanceBefore.currency ||
      money.currency !== balanceAfter.currency
    ) {
      throw new InvalidLedgerEntryError(
        "All ledger amounts must use the same currency",
      );
    }

    if (
      money.isNegative() ||
      balanceBefore.isNegative() ||
      balanceAfter.isNegative()
    ) {
      throw new InvalidLedgerEntryError(
        "Ledger amounts and balances cannot be negative",
      );
    }

    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      money,
      balanceBefore,
      balanceAfter,
      new Date(),
    );

    if (!entry.isBalanced()) {
      throw new InvalidLedgerEntryError("Ledger entry is not balanced");
    }

    return entry;
  }

  /**
   * Reconstructs a persisted ledger entry without creating a new entry.
   */
  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /**
   * Checks whether the balance before, transaction amount and balance after are consistent.
   */
  isBalanced(): boolean {
    const expectedBalance =
      this.direction === LedgerDirection.Debit
        ? this.balanceBefore.subtract(this.money)
        : this.balanceBefore.add(this.money);

    return expectedBalance.equals(this.balanceAfter);
  }
}
