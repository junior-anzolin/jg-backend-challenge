import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { ProcessWagerTransactionUseCase } from "./application/wagering/process-wager-transaction.use-case";
import { CreateWalletUseCase } from "./application/wallets/create-wallet.use-case";
import appConfig from "./config/app.config";
import databaseConfig from "./config/database.config";
import { DatabaseModule } from "./infrastructure/database/database.module";
import { HealthModule } from "./infrastructure/health/health.module";
import { WageringModule } from "./infrastructure/http/wagering/wagering.module";
import { WalletsModule } from "./infrastructure/http/wallets/wallets.module";

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [appConfig, databaseConfig],
    }),
    DatabaseModule,
    HealthModule,
    WalletsModule,
    WageringModule,
  ],
  providers: [CreateWalletUseCase, ProcessWagerTransactionUseCase],
})
export class AppModule {}
