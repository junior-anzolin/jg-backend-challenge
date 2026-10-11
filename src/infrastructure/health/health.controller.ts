import { EntityManager } from "@mikro-orm/postgresql";
import {
  Controller,
  Get,
  Logger,
  ServiceUnavailableException,
} from "@nestjs/common";

import { SqsReadinessService } from "./sqs-readiness.service";

@Controller("health")
export class HealthController {
  private readonly logger = new Logger(HealthController.name);

  constructor(
    private readonly em: EntityManager,
    private readonly sqsReadinessService: SqsReadinessService,
  ) {}

  @Get("live")
  getLive() {
    return {
      status: "UP",
      timestamp: new Date().toISOString(),
    };
  }

  @Get("ready")
  async getReady() {
    let databaseReady = false;
    let sqsReady = false;

    try {
      await this.em.execute("SELECT 1");
      databaseReady = true;
    } catch (error) {
      this.logger.error(
        "PostgreSQL readiness check failed",
        error instanceof Error ? error.stack : String(error),
      );
    }

    try {
      await this.sqsReadinessService.check();
      sqsReady = true;
    } catch (error) {
      this.logger.error(
        "SQS readiness check failed",
        error instanceof Error ? error.stack : String(error),
      );
    }

    const timestamp = new Date().toISOString();

    if (!databaseReady || !sqsReady) {
      throw new ServiceUnavailableException({
        status: "DOWN",
        database: databaseReady ? "CONNECTED" : "DISCONNECTED",
        sqs: sqsReady ? "CONNECTED" : "DISCONNECTED",
        timestamp,
      });
    }

    return {
      status: "UP",
      database: "CONNECTED",
      sqs: "CONNECTED",
      timestamp,
    };
  }
}
