import { Module } from "@nestjs/common";

import { ProcessWagerTransactionUseCase } from "../../../application/wagering/process-wager-transaction.use-case";
import { DatabaseModule } from "../../database/database.module";

@Module({
  imports: [DatabaseModule],
  providers: [ProcessWagerTransactionUseCase],
  exports: [ProcessWagerTransactionUseCase],
})
export class WageringModule {}
