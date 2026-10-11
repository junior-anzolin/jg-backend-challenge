import { Inject, Injectable } from "@nestjs/common";

import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";
import type {
  LedgerEntryPage,
  LedgerPagination,
} from "../ports/wallet-ledger-entry.repository.port";

@Injectable()
export class GetWalletLedgerUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(
    walletId: string,
    pagination: LedgerPagination,
  ): Promise<LedgerEntryPage | null> {
    return this.unitOfWork.runInTransaction(async (context) => {
      const wallet = await context.wallets.findById(walletId);

      if (!wallet) {
        return null;
      }

      return context.ledgerEntries.findByWalletId(walletId, pagination);
    });
  }
}
