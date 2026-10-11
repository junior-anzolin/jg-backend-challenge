import { Inject, Injectable, Logger } from "@nestjs/common";

import type { MoneyProps } from "../../domain/shared/money/money.type";
import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";

export interface WalletReconciliationResult {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

@Injectable()
export class ReconcileWalletUseCase {
  private readonly logger = new Logger(ReconcileWalletUseCase.name);

  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(walletId: string): Promise<WalletReconciliationResult | null> {
    return this.unitOfWork.runInTransaction(async (context) => {
      const wallet = await context.wallets.findByIdForUpdate(walletId);

      if (!wallet) {
        return null;
      }

      const storedBalance = wallet.balance;

      const calculatedBalance =
        await context.ledgerEntries.calculateBalanceFromLedger(
          wallet.id,
          wallet.currency,
        );

      const checkedEntries = await context.ledgerEntries.countByWalletId(
        wallet.id,
      );

      const consistent = storedBalance.equals(calculatedBalance);
      const difference = storedBalance.subtract(calculatedBalance);

      if (!consistent) {
        this.logger.error(
          `Wallet reconciliation mismatch: walletId=${wallet.id}, ` +
            `storedBalance=${storedBalance.toString()} ${wallet.currency}, ` +
            `calculatedBalance=${calculatedBalance.toString()} ${wallet.currency}, ` +
            `difference=${difference.toString()} ${wallet.currency}, ` +
            `checkedEntries=${checkedEntries}`,
        );
      }

      return {
        walletId: wallet.id,
        storedBalance: storedBalance.toJSON(),
        calculatedBalance: calculatedBalance.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries,
      };
    });
  }
}
