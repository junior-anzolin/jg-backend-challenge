import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";

import { RetryPendingReferencesUseCase } from "../../application/wagering/retry-pending-references.use-case";

const POLL_INTERVAL_MS = 1_000;

@Injectable()
export class PendingReferenceRetryWorker
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(PendingReferenceRetryWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private activeRun?: Promise<void>;
  private stopping = false;

  constructor(
    private readonly retryPendingReferences: RetryPendingReferencesUseCase,
  ) {}

  onModuleInit(): void {
    void this.tick();
    this.timer = setInterval(() => void this.tick(), POLL_INTERVAL_MS);

    this.logger.log("Pending reference retry worker started");
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;

    if (this.timer) {
      clearInterval(this.timer);
    }

    await this.activeRun;

    this.logger.log("Pending reference retry worker stopped");
  }

  private async tick(): Promise<void> {
    if (this.stopping || this.activeRun) {
      return;
    }

    this.activeRun = this.runBatch();

    try {
      await this.activeRun;
    } finally {
      this.activeRun = undefined;
    }
  }

  private async runBatch(): Promise<void> {
    try {
      const result = await this.retryPendingReferences.execute();

      if (result.due === 0 && result.errors.length === 0) {
        return;
      }

      this.logger.log(
        `Reference retry batch: due=${result.due}, ` +
          `processed=${result.processed}, rejected=${result.rejected}, ` +
          `stillPending=${result.stillPending}, errors=${result.errors.length}`,
      );

      for (const error of result.errors) {
        this.logger.error(
          `Could not retry pending transaction ${error.transactionId}: ${error.message}`,
        );
      }
    } catch (error) {
      this.logger.error(
        "Unexpected error while retrying pending references",
        error instanceof Error ? error.stack : String(error),
      );
    }
  }
}
