import { Module } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";

import { ProcessWagerTransactionMessageUseCase } from "../../application/messaging/process-wager-transaction-message.use-case";
import { PublishOutboxMessagesUseCase } from "../../application/messaging/publish-outbox-messages.use-case";
import { OUTBOX_EVENT_PUBLISHER } from "../../application/ports/outbox-event-publisher.port";
import { DatabaseModule } from "../database/database.module";
import { WageringModule } from "../http/wagering/wagering.module";
import { OutboxPublisherWorker } from "./outbox-publisher.worker";
import { SqsOutboxEventPublisher } from "./sqs/sqs-outbox-event-publisher";
import { SqsWagerTransactionConsumer } from "./sqs/sqs-wager-transaction.consumer";

@Module({
  imports: [ConfigModule, DatabaseModule, WageringModule],
  providers: [
    PublishOutboxMessagesUseCase,
    ProcessWagerTransactionMessageUseCase,
    SqsOutboxEventPublisher,
    {
      provide: OUTBOX_EVENT_PUBLISHER,
      useExisting: SqsOutboxEventPublisher,
    },
    OutboxPublisherWorker,
    SqsWagerTransactionConsumer,
  ],
})
export class MessagingModule {}
