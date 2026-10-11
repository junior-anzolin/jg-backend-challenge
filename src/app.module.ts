import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import appConfig from "./config/app.config";
import databaseConfig from "./config/database.config";
import { DatabaseModule } from "./infrastructure/database/database.module";
import { HealthModule } from "./infrastructure/health/health.module";
import { WageringModule } from "./infrastructure/http/wagering/wagering.module";
import { WalletsModule } from "./infrastructure/http/wallets/wallets.module";
import { MessagingModule } from "./infrastructure/messaging/messaging.module";
@Module({
  imports: [
    // Mantenha os imports que já existem.
    ConfigModule.forRoot({
      isGlobal: true,
      load: [appConfig, databaseConfig],
    }),
    DatabaseModule,
    HealthModule,
    WalletsModule,
    WageringModule,
    MessagingModule,
  ],
})
export class AppModule {}
