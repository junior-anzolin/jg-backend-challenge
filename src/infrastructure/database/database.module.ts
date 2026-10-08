import { Module } from '@nestjs/common';
import { MikroOrmModule } from '@mikro-orm/nestjs';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';

@Module({
  imports: [
    MikroOrmModule.forRootAsync({
      driver: PostgreSqlDriver,
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        driver: PostgreSqlDriver,
        host: configService.get<string>('database.host'),
        port: configService.get<number>('database.port'),
        dbName: configService.get<string>('database.name'),
        user: configService.get<string>('database.user'),
        password: configService.get<string>('database.password'),
        entities: ['dist/**/*.entity.js'],
        entitiesTs: ['src/**/*.entity.ts'],
        extensions: [Migrator],
        discovery: {
          warnWhenNoEntities: false,
        },
        migrations: {
          path: 'dist/infrastructure/database/migrations',
          pathTs: 'src/infrastructure/database/migrations',
          glob: '!(*.d).{js,ts}',
          transactional: true,
          disableForeignKeys: true,
          allOrNothing: true,
        },
      }),
    }),
  ],
})
export class DatabaseModule {}
