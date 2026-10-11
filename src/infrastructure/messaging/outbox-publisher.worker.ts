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
  private running = false;

  constructor(
    private readonly publishOutboxMessages: PublishOutboxMessagesUseCase,
  ) {}

  onModuleInit(): void {
    void this.tick();
    this.timer = setInterval(() => void this.tick(), POLL_INTERVAL_MS);

    this.logger.log("Outbox publisher worker started");
  }

  onModuleDestroy(): void {
    if (this.timer) {
      clearInterval(this.timer);
    }

    this.logger.log("Outbox publisher worker stopped");
  }

  private async tick(): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;

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
    } finally {
      this.running = false;
    }
  }
}
