import { Module } from "@nestjs/common";

import { ProcessWagerTransactionUseCase } from "../../../application/wagering/process-wager-transaction.use-case";
import { DatabaseModule } from "../../database/database.module";
import { WageringController } from "./wagering.controller";

@Module({
  imports: [DatabaseModule],
  controllers: [WageringController],
  providers: [ProcessWagerTransactionUseCase],
  exports: [ProcessWagerTransactionUseCase],
})
export class WageringModule {}
