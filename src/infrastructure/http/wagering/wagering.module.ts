import { Module } from "@nestjs/common";

import { GetWagerTransactionUseCase } from "../../../application/wagering/get-wager-transaction.use-case";
import { ProcessWagerTransactionUseCase } from "../../../application/wagering/process-wager-transaction.use-case";
import { DatabaseModule } from "../../database/database.module";
import { ObservabilityModule } from "../../observability/observability.module";
import { ProviderWageringTransactionsController } from "./provider-wagering-transactions.controller";
import { WageringController } from "./wagering.controller";

@Module({
  imports: [DatabaseModule, ObservabilityModule],
  controllers: [WageringController, ProviderWageringTransactionsController],
  providers: [ProcessWagerTransactionUseCase, GetWagerTransactionUseCase],
  exports: [ProcessWagerTransactionUseCase],
})
export class WageringModule {}
