import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { SqsWagerTransactionConsumer } from "../src/infrastructure/messaging/sqs/sqs-wager-transaction.consumer";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const TRANSACTION_AMOUNT = "25.00";
const TIMEOUT_MS = 15_000;
const TEST_TIMEOUT_MS = 30_000;

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

type PendingReferenceRow = {
  id: string;
  status: string;
  reference_attempts: number | string;
  next_reference_attempt_at: Date | string | null;
  remaining_delay_ms: number | string | null;
  failure_code: string | null;
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
};

let runningApplication: RunningApplication | undefined;

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

async function startApplication(): Promise<RunningApplication> {
  const moduleBuilder = Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(OutboxPublisherWorker)
    .useValue({})
    .overrideProvider(SqsWagerTransactionConsumer)
    .useValue({});

  const moduleFixture = await moduleBuilder.compile();
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

async function submitTransaction(
  payload: Record<string, unknown>,
  idempotencyKey: string,
): Promise<Response> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  return fetch(`${runningApplication.serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify(payload),
  });
}

async function findPendingReference(
  transactionId: string,
): Promise<PendingReferenceRow[]> {
  return query<PendingReferenceRow>(
    `SELECT id,
            status,
            reference_attempts,
            next_reference_attempt_at,
            CASE
              WHEN next_reference_attempt_at IS NULL THEN NULL
              ELSE (
                EXTRACT(
                  EPOCH FROM (next_reference_attempt_at - NOW())
                ) * 1000
              )::int
            END AS remaining_delay_ms,
            failure_code
       FROM wager_transactions
       WHERE id = ?`,
    [transactionId],
  );
}

async function countOutboxEvents(
  eventType: string,
  transactionId: string,
): Promise<number> {
  const rows = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = ?
         AND payload->'data'->>'transactionId' = ?`,
    [eventType, transactionId],
  );

  return Number(rows[0]?.count ?? "0");
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

function expectScheduledDelay(
  row: PendingReferenceRow,
  expectedAttempt: number,
  minimumDelayMs: number,
  maximumDelayMs: number,
): void {
  expect(row.status).toBe("PENDING_REFERENCE");
  expect(Number(row.reference_attempts)).toBe(expectedAttempt);
  expect(row.next_reference_attempt_at).not.toBeNull();
  expect(row.failure_code).toBeNull();

  const remainingDelayMs = Number(row.remaining_delay_ms);

  expect(remainingDelayMs).toBeGreaterThanOrEqual(minimumDelayMs);
  expect(remainingDelayMs).toBeLessThanOrEqual(maximumDelayMs);
}

describe("Pending reference exponential backoff E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-transactions-backoff-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-backoff-${randomUUID()}.fifo`;

    try {
      runningApplication = await startApplication();
    } catch (error) {
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

  it(
    "increases the scheduled delay exponentially while a reference remains absent",
    async () => {
      const runId = randomUUID();
      const wallet = await createWallet(runId);

      const providerId = `e2e-provider-${runId}`;
      const externalTransactionId = `e2e-rollback-${runId}`;
      const missingReferenceId = `e2e-missing-bet-${runId}`;

      const response = await submitTransaction(
        {
          providerId,
          externalTransactionId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `e2e-round-${runId}`,
          gameId: "e2e-game",
          kind: "ROLLBACK",
          money: {
            amount: TRANSACTION_AMOUNT,
            currency: "BRL",
          },
          referenceExternalTransactionId: missingReferenceId,
        },
        `e2e-idempotency-${runId}`,
      );

      expect(response.status).toBe(202);

      const initialResult = (await response.json()) as WagerResponse;

      expect(initialResult.status).toBe("PENDING_REFERENCE");
      expect(initialResult.idempotentReplay).toBe(false);

      let rows = await findPendingReference(initialResult.transactionId);

      expect(rows).toHaveLength(1);
      expectScheduledDelay(rows[0], 1, 500, 1_800);

      await waitUntil(async () => {
        const current = await findPendingReference(initialResult.transactionId);

        return (
          current.length === 1 && Number(current[0].reference_attempts) >= 2
        );
      }, "the second scheduled reference attempt");

      rows = await findPendingReference(initialResult.transactionId);

      expect(rows).toHaveLength(1);

      expectScheduledDelay(rows[0], 2, 1_200, 2_800);

      await waitUntil(async () => {
        const current = await findPendingReference(initialResult.transactionId);

        return (
          current.length === 1 && Number(current[0].reference_attempts) >= 3
        );
      }, "the third scheduled reference attempt");

      rows = await findPendingReference(initialResult.transactionId);

      expect(rows).toHaveLength(1);

      expectScheduledDelay(rows[0], 3, 2_800, 5_000);

      expect(
        await countOutboxEvents(
          "WagerTransactionPendingReference",
          initialResult.transactionId,
        ),
      ).toBe(1);

      expect(
        await countOutboxEvents(
          "WagerTransactionRejected",
          initialResult.transactionId,
        ),
      ).toBe(0);

      const ledgerRows = await query<CountRow>(
        `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
        [initialResult.transactionId],
      );

      expect(ledgerRows[0].count).toBe("0");

      await expectWalletBalanceMatchesLedger(wallet.id, INITIAL_BALANCE);
    },
    TEST_TIMEOUT_MS,
  );
});
