import { EntityManager } from "@mikro-orm/postgresql";
import { Injectable } from "@nestjs/common";

import type {
  UnitOfWorkContext,
  UnitOfWorkPort,
} from "../../application/ports/unit-of-work.port";
import { InboxMessageRepository } from "./repositories/inbox-message.repository";
import { OutboxMessageRepository } from "./repositories/outbox-message.repository";
import { WagerTransactionRepository } from "./repositories/wager-transaction.repository";
import { WalletLedgerEntryRepository } from "./repositories/wallet-ledger-entry.repository";
import { WalletRepository } from "./repositories/wallet.repository";

@Injectable()
export class UnitOfWork implements UnitOfWorkPort {
  constructor(private readonly em: EntityManager) {}

  async runInTransaction<T>(
    operation: (context: UnitOfWorkContext) => Promise<T>,
  ): Promise<T> {
    return this.em.transactional(async (transactionalEm) => {
      const context: UnitOfWorkContext = {
        wallets: new WalletRepository(transactionalEm),
        wagerTransactions: new WagerTransactionRepository(transactionalEm),
        ledgerEntries: new WalletLedgerEntryRepository(transactionalEm),
        inboxMessages: new InboxMessageRepository(transactionalEm),
        outboxMessages: new OutboxMessageRepository(transactionalEm),
      };

      return operation(context);
    });
  }
}
