import { Module } from "@nestjs/common";

import { HealthController } from "./health.controller";
import { SqsReadinessService } from "./sqs-readiness.service";

@Module({
  controllers: [HealthController],
  providers: [SqsReadinessService],
})
export class HealthModule {}
