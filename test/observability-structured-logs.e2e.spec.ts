import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication, LoggerService } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import type { WagerTransactionRequestedMessage } from "../src/application/messaging/process-wager-transaction-message.use-case";
import { WagerTransactionKind } from "../src/domain/wagering/transaction/wager-transaction-kind";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../src/infrastructure/messaging/pending-reference-retry.worker";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "37.91";
const VISIBILITY_TIMEOUT_SECONDS = 5;
const CONSUMER_NAME = "wager-transactions";
const TIMEOUT_MS = 15_000;

const QUEUE_SUFFIX = randomUUID();
const SOURCE_QUEUE_NAME = `wager-observability-${QUEUE_SUFFIX}.fifo`;

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type TransactionRow = {
  id: string;
  status: string;
  resulting_balance_amount: string | null;
};

type InboxRow = {
  message_id: string;
  processed_at: Date | string | null;
};

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type QueueCounts = {
  visible: number;
  inFlight: number;
};

type CapturedLog = {
  level: string;
  message: string;
  output: string;
};

type StructuredLogRecord = Record<string, unknown>;

type RunningApplication = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
};

class CapturingLogger implements LoggerService {
  readonly entries: CapturedLog[] = [];

  log(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("log", message, optionalParams);
  }

  error(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("error", message, optionalParams);
  }

  warn(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("warn", message, optionalParams);
  }

  debug(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("debug", message, optionalParams);
  }

  verbose(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("verbose", message, optionalParams);
  }

  fatal(message: unknown, ...optionalParams: unknown[]): void {
    this.capture("fatal", message, optionalParams);
  }

  private capture(
    level: string,
    message: unknown,
    optionalParams: unknown[],
  ): void {
    const normalizedMessage =
      typeof message === "string"
        ? message
        : (JSON.stringify(message) ?? String(message));

    const output = [
      normalizedMessage,
      ...optionalParams.map((param) =>
        typeof param === "string"
          ? param
          : (JSON.stringify(param) ?? String(param)),
      ),
    ].join(" ");

    this.entries.push({
      level,
      message: normalizedMessage,
      output,
    });
  }
}

const capturingLogger = new CapturingLogger();

let runningApplication: RunningApplication | undefined;
let sqs: SQSClient | undefined;
let sourceQueueUrl: string | undefined;

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "SQS_WAGER_TRANSACTIONS_QUEUE_NAME",
  "SQS_WAGER_EVENTS_QUEUE_NAME",
  "SQS_VISIBILITY_TIMEOUT_SECONDS",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

function restoreEnvironment(): void {
  for (const key of managedEnvironmentKeys) {
    const previousValue = previousEnvironment.get(key);

    if (previousValue === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = previousValue;
    }
  }
}

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  return (await runningApplication.entityManager
    .getConnection()
    .execute(sql, params)) as T[];
}

async function waitUntil(
  condition: () => Promise<boolean>,
  description: string,
  timeoutMs = TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await condition()) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error(`Timed out waiting for ${description}`);
}

function parseStructuredLog(
  entry: CapturedLog,
): StructuredLogRecord | undefined {
  try {
    const parsed: unknown = JSON.parse(entry.message);

    if (
      parsed !== null &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
    ) {
      return parsed as StructuredLogRecord;
    }
  } catch {}

  return undefined;
}

function hasRequiredTransactionContext(
  entry: CapturedLog,
  expected: {
    correlationId: string;
    messageId: string;
    transactionId: string;
    walletId: string;
    providerId: string;
  },
): boolean {
  const record = parseStructuredLog(entry);

  return (
    record?.correlationId === expected.correlationId &&
    record?.messageId === expected.messageId &&
    record?.transactionId === expected.transactionId &&
    record?.walletId === expected.walletId &&
    record?.providerId === expected.providerId
  );
}

async function startApplication(): Promise<RunningApplication> {
  const moduleBuilder = Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(OutboxPublisherWorker)
    .useValue({})
    .overrideProvider(PendingReferenceRetryWorker)
    .useValue({});

  const moduleFixture = await moduleBuilder.compile();
  const app = moduleFixture.createNestApplication();

  app.useLogger(capturingLogger);

  try {
    await app.listen(0);

    return {
      app,
      entityManager: app.get(EntityManager),
      serverUrl: await app.getUrl(),
    };
  } catch (error) {
    await app.close();
    throw error;
  }
}

async function createWallet(runId: string): Promise<WalletResponse> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const response = await fetch(`${runningApplication.serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId: `e2e-player-${runId}`,
      initialBalance: {
        amount: INITIAL_BALANCE,
        currency: "BRL",
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.balance).toEqual({
    amount: INITIAL_BALANCE,
    currency: "BRL",
  });

  return wallet;
}

async function sendMessage(
  message: WagerTransactionRequestedMessage,
  runId: string,
): Promise<void> {
  if (!sqs || !sourceQueueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: sourceQueueUrl,
      MessageBody: JSON.stringify(message),
      MessageGroupId: `e2e-group-${runId}`,
      MessageDeduplicationId: `e2e-dedup-${runId}`,
    }),
  );
}

async function findTransaction(
  providerId: string,
  externalTransactionId: string,
): Promise<TransactionRow[]> {
  return query<TransactionRow>(
    `SELECT id,
            status,
            resulting_balance_amount::text AS resulting_balance_amount
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id = ?`,
    [providerId, externalTransactionId],
  );
}

async function findInboxMessage(messageId: string): Promise<InboxRow[]> {
  return query<InboxRow>(
    `SELECT message_id, processed_at
       FROM inbox_messages
       WHERE consumer_name = ?
         AND message_id = ?`,
    [CONSUMER_NAME, messageId],
  );
}

async function getQueueCounts(): Promise<QueueCounts> {
  if (!sqs || !sourceQueueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  const response = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: sourceQueueUrl,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
      ],
    }),
  );

  return {
    visible: Number(response.Attributes?.ApproximateNumberOfMessages ?? "-1"),
    inFlight: Number(
      response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "-1",
    ),
  };
}

async function waitForQueueToDrain(): Promise<void> {
  let consecutiveEmptyReads = 0;

  await waitUntil(async () => {
    const counts = await getQueueCounts();

    if (counts.visible === 0 && counts.inFlight === 0) {
      consecutiveEmptyReads++;
    } else {
      consecutiveEmptyReads = 0;
    }

    return consecutiveEmptyReads >= 3;
  }, "the SQS message to be acknowledged");
}

async function expectWalletBalanceMatchesLedger(
  walletId: string,
  expectedBalance: string,
): Promise<void> {
  const wallets = await query<BalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(wallets).toHaveLength(1);
  expect(wallets[0].balance).toBe(expectedBalance);

  const ledgerBalances = await query<BalanceRow>(
    `SELECT COALESCE(
              SUM(
                CASE
                  WHEN direction = 'CREDIT' THEN money_amount
                  WHEN direction = 'DEBIT' THEN -money_amount
                  ELSE 0
                END
              ),
              0
            )::numeric(20, 2)::text AS balance
       FROM wallet_ledger_entries
       WHERE wallet_id = ?
         AND money_currency = 'BRL'`,
    [walletId],
  );

  expect(ledgerBalances[0].balance).toBe(expectedBalance);
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

describe("Structured observability logs E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_VISIBILITY_TIMEOUT_SECONDS = String(
      VISIBILITY_TIMEOUT_SECONDS,
    );
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-observability-${QUEUE_SUFFIX}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-observability-${QUEUE_SUFFIX}.fifo`;

    sqs = new SQSClient({
      region: "us-east-1",
      endpoint: SQS_ENDPOINT,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    });

    try {
      const queue = await sqs.send(
        new CreateQueueCommand({
          QueueName: process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME,
          Attributes: {
            FifoQueue: "true",
            ContentBasedDeduplication: "false",
            VisibilityTimeout: String(VISIBILITY_TIMEOUT_SECONDS),
          },
        }),
      );

      if (!queue.QueueUrl) {
        throw new Error("MiniStack did not return the E2E queue URL");
      }

      sourceQueueUrl = queue.QueueUrl;
      runningApplication = await startApplication();
    } catch (error) {
      await runningApplication?.app.close();
      runningApplication = undefined;

      try {
        if (sqs && sourceQueueUrl) {
          await sqs.send(
            new DeleteQueueCommand({
              QueueUrl: sourceQueueUrl,
            }),
          );
        }
      } finally {
        sqs?.destroy();
        sqs = undefined;
        sourceQueueUrl = undefined;
        restoreEnvironment();
      }

      throw error;
    }
  });

  afterAll(async () => {
    try {
      await runningApplication?.app.close();
    } finally {
      runningApplication = undefined;

      try {
        if (sqs && sourceQueueUrl) {
          await sqs.send(
            new DeleteQueueCommand({
              QueueUrl: sourceQueueUrl,
            }),
          );
        }
      } finally {
        sqs?.destroy();
        sqs = undefined;
        sourceQueueUrl = undefined;
        restoreEnvironment();
      }
    }
  });

  it("emits JSON processing logs with required context and without financial payload data", async () => {
    capturingLogger.entries.length = 0;

    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-bet-${runId}`;
    const messageId = `e2e-message-${runId}`;
    const correlationId = `e2e-correlation-${runId}`;

    const message: WagerTransactionRequestedMessage = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId,
        externalTransactionId,
        idempotencyKey: `e2e-idempotency-${runId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `e2e-round-${runId}`,
        gameId: "e2e-game",
        kind: WagerTransactionKind.Bet,
        money: {
          amount: BET_AMOUNT,
          currency: "BRL",
        },
        correlationId,
      },
    };

    await sendMessage(message, runId);

    let transaction: TransactionRow | undefined;

    await waitUntil(async () => {
      const transactions = await findTransaction(
        providerId,
        externalTransactionId,
      );

      transaction = transactions.find((row) => row.status === "PROCESSED");

      return (
        transaction !== undefined &&
        capturingLogger.entries.some((entry) =>
          hasRequiredTransactionContext(entry, {
            correlationId,
            messageId,
            transactionId: transaction!.id,
            walletId: wallet.id,
            providerId,
          }),
        )
      );
    }, "a structured transaction log containing the required context");

    expect(transaction).toBeDefined();
    expect(transaction!.status).toBe("PROCESSED");
    expect(transaction!.resulting_balance_amount).toBe("62.09");

    const matchingLogs = capturingLogger.entries.filter((entry) =>
      hasRequiredTransactionContext(entry, {
        correlationId,
        messageId,
        transactionId: transaction!.id,
        walletId: wallet.id,
        providerId,
      }),
    );

    expect(matchingLogs.length).toBeGreaterThan(0);

    for (const entry of matchingLogs) {
      expect(parseStructuredLog(entry)).toBeDefined();
    }

    const inboxRows = await findInboxMessage(messageId);

    expect(inboxRows).toHaveLength(1);
    expect(inboxRows[0].processed_at).not.toBeNull();

    await expectWalletBalanceMatchesLedger(wallet.id, "62.09");
    await waitForQueueToDrain();

    const allLogOutput = capturingLogger.entries
      .map((entry) => entry.output)
      .join("\n");

    expect(allLogOutput).not.toContain(JSON.stringify(message));
    expect(allLogOutput).not.toContain(wallet.playerId);
    expect(allLogOutput).not.toContain(BET_AMOUNT);
  }, 30_000);
});
