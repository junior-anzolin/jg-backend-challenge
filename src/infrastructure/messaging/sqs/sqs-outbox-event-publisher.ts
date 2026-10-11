import {
  GetQueueUrlCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Injectable, OnModuleDestroy } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";

import { OutboxEventPublisherPort } from "../../../application/ports/outbox-event-publisher.port";
import { OutboxMessageState } from "../../../domain/messaging/outbox/outbox-message.types";

@Injectable()
export class SqsOutboxEventPublisher
  implements OutboxEventPublisherPort, OnModuleDestroy
{
  private readonly client: SQSClient;
  private readonly queueName: string;
  private queueUrlPromise?: Promise<string>;

  constructor(private readonly configService: ConfigService) {
    const endpoint = this.configService.get<string>("SQS_ENDPOINT");

    this.queueName =
      this.configService.get<string>("SQS_WAGER_EVENTS_QUEUE_NAME") ??
      "wager-events.fifo";

    this.client = new SQSClient({
      region: this.configService.get<string>("AWS_REGION") ?? "us-east-1",
      ...(endpoint
        ? {
            endpoint,
            credentials: {
              accessKeyId:
                this.configService.get<string>("AWS_ACCESS_KEY_ID") ?? "test",
              secretAccessKey:
                this.configService.get<string>("AWS_SECRET_ACCESS_KEY") ??
                "test",
            },
          }
        : {}),
    });
  }

  async publish(message: OutboxMessageState): Promise<void> {
    const queueUrl = await this.getQueueUrl();

    await this.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(message.payload),
        MessageGroupId: message.aggregateId,
        MessageDeduplicationId: message.id,
      }),
    );
  }

  private getQueueUrl(): Promise<string> {
    if (!this.queueUrlPromise) {
      this.queueUrlPromise = this.client
        .send(
          new GetQueueUrlCommand({
            QueueName: this.queueName,
          }),
        )
        .then(({ QueueUrl }) => {
          if (!QueueUrl) {
            throw new Error(
              `SQS did not return a URL for queue "${this.queueName}"`,
            );
          }

          return QueueUrl;
        })
        .catch((error: unknown) => {
          // Allow a future attempt if SQS is temporarily unavailable.
          this.queueUrlPromise = undefined;
          throw error;
        });
    }

    return this.queueUrlPromise;
  }

  onModuleDestroy(): void {
    this.client.destroy();
  }
}
