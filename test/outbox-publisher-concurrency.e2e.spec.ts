import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import { PublishOutboxMessagesUseCase } from "../src/application/messaging/publish-outbox-messages.use-case";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../src/infrastructure/messaging/pending-reference-retry.worker";
import { SqsWagerTransactionConsumer } from "../src/infrastructure/messaging/sqs/sqs-wager-transaction.consumer";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "25.00";
const PUBLISH_BATCH_SIZE = 500;
const TIMEOUT_MS = 20_000;

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
  failureCode?: string;
  idempotentReplay: boolean;
};

type OutboxRow = {
  id: string;
  event_type: string;
  published_at: string | null;
  claim_token: string | null;
  claim_until: string | null;
};

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type RunningApplication = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
  publishOutboxMessages: PublishOutboxMessagesUseCase;
};

let applications: RunningApplication[] = [];

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "SQS_WAGER_EVENTS_QUEUE_NAME",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

async function startApplication(): Promise<RunningApplication> {
  const moduleFixture = await Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(OutboxPublisherWorker)
    .useValue({})
    .overrideProvider(SqsWagerTransactionConsumer)
    .useValue({})
    .overrideProvider(PendingReferenceRetryWorker)
    .useValue({})
    .compile();

  const app = moduleFixture.createNestApplication();

  try {
    await app.listen(0);

    return {
      app,
      entityManager: app.get(EntityManager),
      serverUrl: await app.getUrl(),
      publishOutboxMessages: app.get(PublishOutboxMessagesUseCase),
    };
  } catch (error) {
    await app.close();
    throw error;
  }
}

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (applications.length === 0) {
    throw new Error("E2E applications have not been initialized");
  }

  return (await applications[0].entityManager
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

async function createWallet(runId: string): Promise<WalletResponse> {
  if (applications.length === 0) {
    throw new Error("E2E applications have not been initialized");
  }

  const response = await fetch(`${applications[0].serverUrl}/wallets`, {
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
  if (applications.length === 0) {
    throw new Error("E2E applications have not been initialized");
  }

  const response = await fetch(
    `${applications[0].serverUrl}/wagering/transactions`,
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

async function findOutboxEvents(transactionId: string): Promise<OutboxRow[]> {
  return query<OutboxRow>(
    `SELECT id,
            event_type,
            published_at::text AS published_at,
            claim_token,
            claim_until::text AS claim_until
       FROM outbox_messages
       WHERE payload->'data'->>'transactionId' = ?
       ORDER BY event_type`,
    [transactionId],
  );
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

describe("Outbox publisher concurrency E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = "wager-events.fifo";

    try {
      applications.push(await startApplication());
      applications.push(await startApplication());
    } catch (error) {
      await Promise.all(applications.map(({ app }) => app.close()));
      applications = [];
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await Promise.all(applications.map(({ app }) => app.close()));
    } finally {
      applications = [];

      for (const key of managedEnvironmentKeys) {
        const previousValue = previousEnvironment.get(key);

        if (previousValue === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = previousValue;
        }
      }
    }
  });

  it("publishes each transaction event when two publishers run concurrently", async () => {
    expect(applications).toHaveLength(2);
    expect(new Set(applications.map(({ serverUrl }) => serverUrl)).size).toBe(
      2,
    );

    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const betResult = await submitBet(wallet, runId);

    expect(betResult.status).toBe("PROCESSED");
    expect(betResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const initialEvents = await findOutboxEvents(betResult.transactionId);

    expect(initialEvents).toHaveLength(2);
    expect(initialEvents.map((event) => event.event_type).sort()).toEqual([
      "WagerTransactionProcessed",
      "WalletBalanceChanged",
    ]);

    const results = await Promise.all(
      applications.map(({ publishOutboxMessages }) =>
        publishOutboxMessages.execute(PUBLISH_BATCH_SIZE),
      ),
    );

    expect(results).toHaveLength(2);

    await waitUntil(async () => {
      const rows = await findOutboxEvents(betResult.transactionId);

      return (
        rows.length === 2 &&
        rows.every(
          (event) =>
            event.published_at !== null &&
            event.claim_token === null &&
            event.claim_until === null,
        )
      );
    }, "both transaction events to be published and their claims released");

    const finalEvents = await findOutboxEvents(betResult.transactionId);

    expect(finalEvents).toHaveLength(2);
    expect(finalEvents.map((event) => event.event_type).sort()).toEqual([
      "WagerTransactionProcessed",
      "WalletBalanceChanged",
    ]);

    expect(finalEvents.every((event) => event.published_at !== null)).toBe(
      true,
    );

    expect(
      finalEvents.every(
        (event) => event.claim_token === null && event.claim_until === null,
      ),
    ).toBe(true);

    const eventCounts = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE payload->'data'->>'transactionId' = ?
           AND event_type IN (
             'WalletBalanceChanged',
             'WagerTransactionProcessed'
           )`,
      [betResult.transactionId],
    );

    expect(Number(eventCounts[0].count)).toBe(2);

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);
});
