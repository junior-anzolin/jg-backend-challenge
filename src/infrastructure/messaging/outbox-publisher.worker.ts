import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";

import { PublishOutboxMessagesUseCase } from "../../application/messaging/publish-outbox-messages.use-case";

const POLL_INTERVAL_MS = 1_000;

@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxPublisherWorker.name);
  private timer?: ReturnType<typeof setInterval>;
  private activeRun?: Promise<void>;
  private stopping = false;

  constructor(
    private readonly publishOutboxMessages: PublishOutboxMessagesUseCase,
  ) {}

  onModuleInit(): void {
    void this.tick();
    this.timer = setInterval(() => void this.tick(), POLL_INTERVAL_MS);

    this.logger.log("Outbox publisher worker started");
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }

    await this.activeRun;

    this.logger.log("Outbox publisher worker stopped");
  }

  private async tick(): Promise<void> {
    if (this.stopping || this.activeRun) {
      return;
    }

    const run = this.runBatch();
    this.activeRun = run;

    try {
      await run;
    } finally {
      if (this.activeRun === run) {
        this.activeRun = undefined;
      }
    }
  }

  private async runBatch(): Promise<void> {
    try {
      const result = await this.publishOutboxMessages.execute();

      if (result.claimed > 0) {
        this.logger.log(
          `Outbox batch finished: claimed=${result.claimed}, ` +
            `published=${result.published}, ` +
            `retriesScheduled=${result.retriesScheduled}, ` +
            `claimsLost=${result.claimsLost}`,
        );
      }
    } catch (error) {
      this.logger.error(
        "Unexpected error while processing the outbox",
        error instanceof Error ? error.stack : String(error),
      );
    }
  }
}
