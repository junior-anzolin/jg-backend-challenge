import { Money } from "../../domain/shared/money/money";
import { WalletLedgerEntry } from "../../domain/wagering/ledger/wallet-ledger-entry";

export interface LedgerPagination {
  cursor?: string;
  limit: number;
}

export interface LedgerEntryPage {
  items: WalletLedgerEntry[];
  nextCursor?: string;
}

export interface WalletLedgerEntryRepositoryPort {
  save(entry: WalletLedgerEntry): Promise<void>;

  findByWalletId(
    walletId: string,
    pagination: LedgerPagination,
  ): Promise<LedgerEntryPage>;

  calculateBalanceFromLedger(
    walletId: string,
    currency: string,
  ): Promise<Money>;

  countByWalletId(walletId: string): Promise<number>;
}
