import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";

import {
  ProcessWagerTransactionMessageUseCase,
  WagerTransactionRequestedMessage,
} from "../../../application/messaging/process-wager-transaction-message.use-case";
import { WagerTransactionKind } from "../../../domain/wagering/transaction/wager-transaction-kind";

const WAIT_TIME_SECONDS = 20;
const VISIBILITY_TIMEOUT_SECONDS = 60;
const MAX_NUMBER_OF_MESSAGES = 1;
const POLL_ERROR_DELAY_MS = 1_000;

@Injectable()
export class SqsWagerTransactionConsumer
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(SqsWagerTransactionConsumer.name);
  private readonly client: SQSClient;
  private readonly queueName: string;

  private queueUrlPromise?: Promise<string>;
  private stopping = false;
  private loopPromise?: Promise<void>;
  private receiveAbortController?: AbortController;

  constructor(
    private readonly configService: ConfigService,
    private readonly processMessage: ProcessWagerTransactionMessageUseCase,
  ) {
    const endpoint = this.configService.get<string>("SQS_ENDPOINT");

    this.queueName =
      this.configService.get<string>("SQS_WAGER_TRANSACTIONS_QUEUE_NAME") ??
      "wager-transactions.fifo";

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

  onModuleInit(): void {
    this.loopPromise = this.poll();
    this.logger.log("SQS wager transaction consumer started");
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    this.receiveAbortController?.abort();

    await this.loopPromise;
    this.client.destroy();

    this.logger.log("SQS wager transaction consumer stopped");
  }

  private async poll(): Promise<void> {
    while (!this.stopping) {
      try {
        const queueUrl = await this.getQueueUrl();

        const receiveController = new AbortController();
        this.receiveAbortController = receiveController;

        let response;

        try {
          response = await this.client.send(
            new ReceiveMessageCommand({
              QueueUrl: queueUrl,
              WaitTimeSeconds: WAIT_TIME_SECONDS,
              VisibilityTimeout: VISIBILITY_TIMEOUT_SECONDS,
              MaxNumberOfMessages: MAX_NUMBER_OF_MESSAGES,
            }),
            {
              abortSignal: receiveController.signal,
            },
          );
        } finally {
          if (this.receiveAbortController === receiveController) {
            this.receiveAbortController = undefined;
          }
        }

        for (const message of response.Messages ?? []) {
          if (this.stopping) {
            break;
          }

          await this.handleMessage(message, queueUrl);
        }
      } catch (error) {
        if (this.stopping) {
          break;
        }

        this.logger.error(
          "Failed to poll the SQS wager transaction queue",
          error instanceof Error ? error.stack : String(error),
        );

        await new Promise((resolve) =>
          setTimeout(resolve, POLL_ERROR_DELAY_MS),
        );
      }
    }
  }

  private async handleMessage(
    message: {
      MessageId?: string;
      Body?: string;
      ReceiptHandle?: string;
    },
    queueUrl: string,
  ): Promise<void> {
    if (!message.Body || !message.ReceiptHandle) {
      this.logger.warn(
        `Ignoring malformed SQS delivery metadata: ${message.MessageId ?? "unknown"}`,
      );
      return;
    }

    try {
      const payload = this.parseMessage(message.Body);

      await this.processMessage.execute(payload);

      // Ack only after the application transaction has committed.
      await this.client.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    } catch (error) {
      // Leave the message in the queue. SQS redelivery and the configured
      // redrive policy determine whether it will be retried or sent to the DLQ.
      this.logger.error(
        `Could not process SQS message ${message.MessageId ?? "unknown"}; it will remain available for retry`,
        error instanceof Error ? error.stack : String(error),
      );
    }
  }

  private parseMessage(body: string): WagerTransactionRequestedMessage {
    let value: unknown;

    try {
      value = JSON.parse(body);
    } catch {
      throw new Error("SQS message body is not valid JSON");
    }

    if (!this.isRecord(value)) {
      throw new Error("SQS message must be a JSON object");
    }

    if (
      typeof value.messageId !== "string" ||
      value.messageId.trim().length === 0 ||
      value.type !== "WagerTransactionRequested" ||
      typeof value.occurredAt !== "string" ||
      !Number.isFinite(Date.parse(value.occurredAt)) ||
      !this.isRecord(value.data)
    ) {
      throw new Error("SQS message envelope is invalid");
    }

    const data = value.data;
    const requiredStrings = [
      "providerId",
      "externalTransactionId",
      "idempotencyKey",
      "playerId",
      "walletId",
      "roundId",
      "gameId",
    ];

    for (const field of requiredStrings) {
      if (
        typeof data[field] !== "string" ||
        (data[field] as string).trim().length === 0
      ) {
        throw new Error(`SQS transaction data has an invalid ${field}`);
      }
    }

    if (
      ![
        WagerTransactionKind.Bet,
        WagerTransactionKind.Win,
        WagerTransactionKind.Loss,
        WagerTransactionKind.Refund,
        WagerTransactionKind.Rollback,
      ].includes(data.kind as WagerTransactionKind)
    ) {
      throw new Error("SQS transaction kind is invalid");
    }

    if (
      !this.isRecord(data.money) ||
      typeof data.money.amount !== "string" ||
      typeof data.money.currency !== "string"
    ) {
      throw new Error("SQS transaction money is invalid");
    }

    if (
      data.referenceExternalTransactionId !== undefined &&
      typeof data.referenceExternalTransactionId !== "string"
    ) {
      throw new Error("SQS transaction reference is invalid");
    }

    return value as unknown as WagerTransactionRequestedMessage;
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return value !== null && typeof value === "object" && !Array.isArray(value);
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
          this.queueUrlPromise = undefined;
          throw error;
        });
    }

    return this.queueUrlPromise;
  }
}
