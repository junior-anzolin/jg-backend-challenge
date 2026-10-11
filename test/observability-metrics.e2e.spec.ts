import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../src/infrastructure/messaging/pending-reference-retry.worker";
import { SqsWagerTransactionConsumer } from "../src/infrastructure/messaging/sqs/sqs-wager-transaction.consumer";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "25.00";
const TIMEOUT_MS = 15_000;

const METRIC_FAMILIES = [
  { name: "wager_transactions_total", type: "counter" },
  { name: "wager_duplicates_total", type: "counter" },
  { name: "wager_retries_total", type: "counter" },
  { name: "wager_dlq_messages_total", type: "counter" },
  { name: "wager_lock_conflicts_total", type: "counter" },
  { name: "wager_outbox_lag_seconds", type: "gauge" },
  { name: "wager_processing_duration_seconds", type: "histogram" },
] as const;

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type WagerResponse = {
  transactionId: string;
  status: string;
  balance?: {
    amount: string;
    currency: string;
  };
  idempotentReplay: boolean;
};

type RunningApplication = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
};

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "SQS_WAGER_TRANSACTIONS_QUEUE_NAME",
  "SQS_WAGER_EVENTS_QUEUE_NAME",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

let runningApplication: RunningApplication | undefined;

async function startApplication(): Promise<RunningApplication> {
  const moduleFixture = await Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(OutboxPublisherWorker)
    .useValue({})
    .overrideProvider(PendingReferenceRetryWorker)
    .useValue({})
    .overrideProvider(SqsWagerTransactionConsumer)
    .useValue({})
    .compile();

  const app = moduleFixture.createNestApplication();

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

async function getMetrics(): Promise<string> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const response = await fetch(`${runningApplication.serverUrl}/metrics`);

  expect(response.status).toBe(200);

  return response.text();
}

function expectMetricFamilies(metrics: string): void {
  for (const family of METRIC_FAMILIES) {
    expect(metrics).toMatch(
      new RegExp(`^# TYPE ${family.name} ${family.type}$`, "m"),
    );
  }
}

function readMetricValue(
  metrics: string,
  metricName: string,
  requiredLabels: Record<string, string> = {},
): number {
  let total = 0;

  for (const line of metrics.split("\n")) {
    const match = line.match(
      /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?|NaN|[+-]?Inf)(?:\s+\d+)?$/,
    );

    if (!match || match[1] !== metricName) {
      continue;
    }

    const labels = Object.fromEntries(
      [
        ...(match[2] ?? "").matchAll(
          /([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"/g,
        ),
      ].map((labelMatch) => [
        labelMatch[1],
        labelMatch[2].replace(/\\"/g, '"').replace(/\\\\/g, "\\"),
      ]),
    );

    const matchesRequiredLabels = Object.entries(requiredLabels).every(
      ([key, value]) => labels[key] === value,
    );

    if (matchesRequiredLabels) {
      total += Number(match[3]);
    }
  }

  return total;
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

async function submitBet(
  wallet: WalletResponse,
  runId: string,
): Promise<WagerResponse> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const response = await fetch(
    `${runningApplication.serverUrl}/wagering/transactions`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": `e2e-idempotency-${runId}`,
      },
      body: JSON.stringify({
        providerId: `e2e-provider-${runId}`,
        externalTransactionId: `e2e-bet-${runId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `e2e-round-${runId}`,
        gameId: "e2e-game",
        kind: "BET",
        money: {
          amount: BET_AMOUNT,
          currency: "BRL",
        },
      }),
    },
  );

  expect(response.status).toBe(200);

  return (await response.json()) as WagerResponse;
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

async function expectWalletBalance(
  walletId: string,
  expectedBalance: string,
): Promise<void> {
  const rows = await query<{ balance: string }>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(rows).toHaveLength(1);
  expect(rows[0].balance).toBe(expectedBalance);
}

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

describe("Observability metrics E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-metrics-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-metrics-${randomUUID()}.fifo`;

    try {
      runningApplication = await startApplication();
    } catch (error) {
      await runningApplication?.app.close();
      runningApplication = undefined;
      restoreEnvironment();
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await runningApplication?.app.close();
    } finally {
      runningApplication = undefined;
      restoreEnvironment();
    }
  });

  it("exposes all metric families required by the README", async () => {
    const metrics = await getMetrics();

    expectMetricFamilies(metrics);
  });

  it("updates transaction, duplicate, and processing latency metrics", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const baselineMetrics = await getMetrics();

    expectMetricFamilies(baselineMetrics);

    const baselineProcessedTransactions = readMetricValue(
      baselineMetrics,
      "wager_transactions_total",
      { status: "PROCESSED" },
    );
    const baselineDuplicates = readMetricValue(
      baselineMetrics,
      "wager_duplicates_total",
    );
    const baselineProcessingCount = readMetricValue(
      baselineMetrics,
      "wager_processing_duration_seconds_count",
    );

    const firstResult = await submitBet(wallet, runId);

    expect(firstResult.status).toBe("PROCESSED");
    expect(firstResult.idempotentReplay).toBe(false);
    expect(firstResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const replayResult = await submitBet(wallet, runId);

    expect(replayResult.transactionId).toBe(firstResult.transactionId);
    expect(replayResult.status).toBe("PROCESSED");
    expect(replayResult.idempotentReplay).toBe(true);
    expect(replayResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    await expectWalletBalance(wallet.id, "75.00");

    let metricsAfter: string | undefined;

    await waitUntil(async () => {
      metricsAfter = await getMetrics();

      const processedTransactions = readMetricValue(
        metricsAfter,
        "wager_transactions_total",
        { status: "PROCESSED" },
      );
      const duplicates = readMetricValue(
        metricsAfter,
        "wager_duplicates_total",
      );
      const processingCount = readMetricValue(
        metricsAfter,
        "wager_processing_duration_seconds_count",
      );

      return (
        processedTransactions > baselineProcessedTransactions &&
        duplicates > baselineDuplicates &&
        processingCount > baselineProcessingCount
      );
    }, "metrics to reflect transaction processing and idempotent replay");

    expect(metricsAfter).toBeDefined();
    expectMetricFamilies(metricsAfter!);

    expect(
      readMetricValue(metricsAfter!, "wager_transactions_total", {
        status: "PROCESSED",
      }),
    ).toBeGreaterThan(baselineProcessedTransactions);

    expect(
      readMetricValue(metricsAfter!, "wager_duplicates_total"),
    ).toBeGreaterThan(baselineDuplicates);

    expect(
      readMetricValue(metricsAfter!, "wager_processing_duration_seconds_count"),
    ).toBeGreaterThan(baselineProcessingCount);
  }, 30_000);
});
