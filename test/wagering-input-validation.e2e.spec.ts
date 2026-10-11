import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../src/infrastructure/messaging/pending-reference-retry.worker";
import { SqsWagerTransactionConsumer } from "../src/infrastructure/messaging/sqs/sqs-wager-transaction.consumer";

const SQS_ENDPOINT = "http://localhost:4566";

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type CountRow = {
  count: string;
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

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
    }),
  );

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

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  return (await runningApplication.entityManager
    .getConnection()
    .execute(sql, params)) as T[];
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
        amount: "100.00",
        currency: "BRL",
      },
    }),
  });

  expect(response.status).toBe(201);

  return (await response.json()) as WalletResponse;
}

function createPayload(input: {
  providerId: string;
  externalTransactionId: string;
  wallet: WalletResponse;
  kind: string;
  amount?: string;
  referenceExternalTransactionId?: string;
}): Record<string, unknown> {
  return {
    providerId: input.providerId,
    externalTransactionId: input.externalTransactionId,
    playerId: input.wallet.playerId,
    walletId: input.wallet.id,
    roundId: `e2e-round-${input.externalTransactionId}`,
    gameId: "e2e-game",
    kind: input.kind,
    money: {
      amount: input.amount ?? "25.00",
      currency: "BRL",
    },
    ...(input.referenceExternalTransactionId !== undefined
      ? {
          referenceExternalTransactionId: input.referenceExternalTransactionId,
        }
      : {}),
  };
}

async function submitTransaction(
  payload: Record<string, unknown>,
  idempotencyKey?: string,
): Promise<Response> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (idempotencyKey !== undefined) {
    headers["Idempotency-Key"] = idempotencyKey;
  }

  return fetch(`${runningApplication.serverUrl}/wagering/transactions`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
}

async function expectNoTransactions(providerId: string): Promise<void> {
  const rows = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM wager_transactions
       WHERE provider_id = ?`,
    [providerId],
  );

  expect(Number(rows[0].count)).toBe(0);
}

describe("Wagering input validation E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-validation-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-validation-${randomUUID()}.fifo`;

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

  it("rejects invalid monetary formats without persisting transactions or changing the wallet balance", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const invalidAmounts = [
      "",
      "NaN",
      "Infinity",
      "1e2",
      "25",
      "25.000",
      "-1.00",
      "1234567890123456789.00",
    ];

    for (const [index, amount] of invalidAmounts.entries()) {
      const response = await submitTransaction(
        createPayload({
          providerId,
          externalTransactionId: `e2e-invalid-amount-${index}-${runId}`,
          wallet,
          kind: "BET",
          amount,
        }),
        `e2e-invalid-amount-key-${index}-${runId}`,
      );

      expect(response.status).toBe(400);
    }

    await expectNoTransactions(providerId);

    const walletResponse = await fetch(
      `${runningApplication!.serverUrl}/wallets/${wallet.id}`,
    );

    expect(walletResponse.status).toBe(200);

    const walletAfter = (await walletResponse.json()) as WalletResponse;

    expect(walletAfter.balance).toEqual({
      amount: "100.00",
      currency: "BRL",
    });
  });

  it("rejects missing, blank, and oversized Idempotency-Key values without persisting transactions", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const idempotencyKeys = [undefined, "", "x".repeat(256)];

    for (const [index, idempotencyKey] of idempotencyKeys.entries()) {
      const response = await submitTransaction(
        createPayload({
          providerId,
          externalTransactionId: `e2e-invalid-key-${index}-${runId}`,
          wallet,
          kind: "BET",
        }),
        idempotencyKey,
      );

      expect(response.status).toBe(400);
    }

    await expectNoTransactions(providerId);
  });

  it("rejects OPENING submitted through the public API without persisting a transaction", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;

    const response = await submitTransaction(
      createPayload({
        providerId,
        externalTransactionId: `e2e-opening-${runId}`,
        wallet,
        kind: "OPENING",
      }),
      `e2e-opening-key-${runId}`,
    );

    expect(response.status).toBe(400);
    await expectNoTransactions(providerId);
  });

  it("requires referenceExternalTransactionId for REFUND and ROLLBACK without persisting transactions", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;

    for (const kind of ["REFUND", "ROLLBACK"]) {
      const response = await submitTransaction(
        createPayload({
          providerId,
          externalTransactionId: `e2e-missing-reference-${kind}-${runId}`,
          wallet,
          kind,
        }),
        `e2e-missing-reference-key-${kind}-${runId}`,
      );

      expect(response.status).toBe(400);
    }

    await expectNoTransactions(providerId);
  });
});
