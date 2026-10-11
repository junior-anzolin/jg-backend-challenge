import { Inject, Injectable } from "@nestjs/common";

import { Wallet } from "../../domain/wagering/wallet/wallet";
import type { UnitOfWorkPort } from "../ports/unit-of-work.port";
import { UNIT_OF_WORK } from "../ports/unit-of-work.port";

@Injectable()
export class GetWalletUseCase {
  constructor(
    @Inject(UNIT_OF_WORK)
    private readonly unitOfWork: UnitOfWorkPort,
  ) {}

  async execute(walletId: string): Promise<Wallet | null> {
    return this.unitOfWork.runInTransaction(async (context) => {
      return context.wallets.findById(walletId);
    });
  }
}
