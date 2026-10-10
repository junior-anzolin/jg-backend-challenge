import { InboxMessageRepositoryPort } from "./inbox-message.repository.port";
import { OutboxMessageRepositoryPort } from "./outbox-message.repository.port";
import { WagerTransactionRepositoryPort } from "./wager-transaction.repository.port";
import { WalletLedgerEntryRepositoryPort } from "./wallet-ledger-entry.repository.port";
import { WalletRepositoryPort } from "./wallet.repository.port";

export interface UnitOfWorkContext {
  wallets: WalletRepositoryPort;
  wagerTransactions: WagerTransactionRepositoryPort;
  ledgerEntries: WalletLedgerEntryRepositoryPort;
  inboxMessages: InboxMessageRepositoryPort;
  outboxMessages: OutboxMessageRepositoryPort;
}

export interface UnitOfWorkPort {
  runInTransaction<T>(
    operation: (context: UnitOfWorkContext) => Promise<T>,
  ): Promise<T>;
}

export const UNIT_OF_WORK = Symbol("UNIT_OF_WORK");
