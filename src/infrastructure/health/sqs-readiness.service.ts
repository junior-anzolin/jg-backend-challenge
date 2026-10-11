import { GetQueueUrlCommand, SQSClient } from "@aws-sdk/client-sqs";
import { Injectable, OnModuleDestroy } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";

@Injectable()
export class SqsReadinessService implements OnModuleDestroy {
  private readonly client: SQSClient;
  private readonly queueNames: string[];

  constructor(private readonly configService: ConfigService) {
    const endpoint = this.configService.get<string>("SQS_ENDPOINT");

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

    this.queueNames = [
      this.configService.get<string>("SQS_WAGER_TRANSACTIONS_QUEUE_NAME") ??
        "wager-transactions.fifo",
      this.configService.get<string>("SQS_WAGER_EVENTS_QUEUE_NAME") ??
        "wager-events.fifo",
    ];
  }

  async check(): Promise<void> {
    await Promise.all(
      this.queueNames.map(async (queueName) => {
        const response = await this.client.send(
          new GetQueueUrlCommand({
            QueueName: queueName,
          }),
        );

        if (!response.QueueUrl) {
          throw new Error(`SQS did not return a URL for queue "${queueName}"`);
        }
      }),
    );
  }

  onModuleDestroy(): void {
    this.client.destroy();
  }
}
