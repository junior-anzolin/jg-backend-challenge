import { Module } from "@nestjs/common";

import { CreateWalletUseCase } from "../../../application/wallets/create-wallet.use-case";
import { GetWalletUseCase } from "../../../application/wallets/get-wallet.use-case";
import { DatabaseModule } from "../../database/database.module";
import { WalletsController } from "./wallets.controller";

@Module({
  imports: [DatabaseModule],
  controllers: [WalletsController],
  providers: [CreateWalletUseCase, GetWalletUseCase],
})
export class WalletsModule {}
