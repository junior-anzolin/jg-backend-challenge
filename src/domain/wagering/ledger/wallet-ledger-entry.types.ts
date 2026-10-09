import { Money } from "../../shared/money/money";
import { LedgerDirection } from "./ledger-direction";

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
}

export interface LedgerEntryState extends CreateLedgerEntryProps {
  createdAt: Date;
}
