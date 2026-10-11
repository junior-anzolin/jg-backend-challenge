import { Migrator } from "@mikro-orm/migrations";
import { MikroOrmModule } from "@mikro-orm/nestjs";
import { PostgreSqlDriver } from "@mikro-orm/postgresql";
import { Module } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";

import { UNIT_OF_WORK } from "../../application/ports/unit-of-work.port";
import { UnitOfWork } from "./unit-of-work";

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      driver: PostgreSqlDriver,
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        driver: PostgreSqlDriver,
        host: configService.get<string>("database.host"),
        port: configService.get<number>("database.port"),
        dbName: configService.get<string>("database.name"),
        user: configService.get<string>("database.user"),
        password: configService.get<string>("database.password"),
        entities: ["dist/**/*.entity.js"],
        extensions: [Migrator],
        discovery: {
          warnWhenNoEntities: false,
        },
        migrations: {
          path: "dist/infrastructure/database/migrations",
          pathTs: "src/infrastructure/database/migrations",
          glob: "!(*.d).{js,ts}",
          transactional: true,
          disableForeignKeys: true,
          allOrNothing: true,
        },
      }),
    }),
  ],
  providers: [
    {
      provide: UNIT_OF_WORK,
      useClass: UnitOfWork,
    },
  ],
  exports: [UNIT_OF_WORK],
})
export class DatabaseModule {}
