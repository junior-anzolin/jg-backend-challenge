import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const CURRENCY = "BRL";
const INITIAL_BALANCE = "100.00";
const AMOUNT = "25.00";

type WagerKind = "BET" | "WIN" | "REFUND" | "ROLLBACK";

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type WagerPayload = {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: {
    amount: string;
    currency: string;
  };
  referenceExternalTransactionId?: string;
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

type TransactionRow = {
  id: string;
  external_transaction_id: string;
  kind: string;
  status: string;
  failure_code: string | null;
};

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type RunningEnvironment = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
};

let environment: RunningEnvironment | undefined;

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!environment) {
    throw new Error("E2E environment has not been initialized");
  }

  return (await environment.entityManager
    .getConnection()
    .execute(sql, params)) as T[];
}

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);

  return Number(rows[0]?.count ?? "0");
}

async function createWallet(runId: string): Promise<WalletResponse> {
  if (!environment) {
    throw new Error("E2E environment has not been initialized");
  }

  const playerId = `e2e-player-${runId}`;

  const response = await fetch(`${environment.serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId,
      initialBalance: {
        amount: INITIAL_BALANCE,
        currency: CURRENCY,
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.playerId).toBe(playerId);
  expect(wallet.balance).toEqual({
    amount: INITIAL_BALANCE,
    currency: CURRENCY,
  });

  return wallet;
}

function createPayload(
  wallet: WalletResponse,
  runId: string,
  suffix: string,
  kind: WagerKind,
  referenceExternalTransactionId?: string,
): WagerPayload {
  return {
    providerId: `e2e-provider-${runId}`,
    externalTransactionId: `e2e-${suffix}-${runId}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: `e2e-round-${runId}`,
    gameId: "e2e-game",
    kind,
    money: {
      amount: AMOUNT,
      currency: CURRENCY,
    },
    ...(referenceExternalTransactionId
      ? { referenceExternalTransactionId }
      : {}),
  };
}

async function submitTransaction(payload: WagerPayload): Promise<Response> {
  if (!environment) {
    throw new Error("E2E environment has not been initialized");
  }

  return fetch(`${environment.serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${payload.externalTransactionId}`,
    },
    body: JSON.stringify(payload),
  });
}

async function expectProcessed(
  payload: WagerPayload,
  response: Response,
  expectedBalance: string,
): Promise<WagerResponse> {
  expect(response.status).toBe(200);

  const result = (await response.json()) as WagerResponse;

  expect(result.status).toBe("PROCESSED");
  expect(result.idempotentReplay).toBe(false);
  expect(result.balance).toEqual({
    amount: expectedBalance,
    currency: CURRENCY,
  });

  const transactions = await query<TransactionRow>(
    `SELECT id, external_transaction_id, kind, status, failure_code
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id = ?`,
    [payload.providerId, payload.externalTransactionId],
  );

  expect(transactions).toHaveLength(1);
  expect(transactions[0].id).toBe(result.transactionId);
  expect(transactions[0].kind).toBe(payload.kind);
  expect(transactions[0].status).toBe("PROCESSED");
  expect(transactions[0].failure_code).toBeNull();

  return result;
}

async function expectReconciliation(
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

  const ledger = await query<BalanceRow>(
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
         AND money_currency = ?`,
    [walletId, CURRENCY],
  );

  expect(ledger).toHaveLength(1);
  expect(ledger[0].balance).toBe(expectedBalance);
  expect(wallets[0].balance).toBe(ledger[0].balance);
}

async function expectOnlyOneReversalProcessed(
  walletId: string,
  providerId: string,
  reversalIds: string[],
  expectedBalance: string,
): Promise<void> {
  const transactions = await query<TransactionRow>(
    `SELECT id, external_transaction_id, kind, status, failure_code
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id IN (?, ?)
       ORDER BY external_transaction_id`,
    [providerId, ...reversalIds],
  );

  expect(transactions).toHaveLength(2);

  const processed = transactions.filter(
    (transaction) => transaction.status === "PROCESSED",
  );

  const rejected = transactions.filter(
    (transaction) => transaction.status === "REJECTED",
  );

  expect(processed).toHaveLength(1);
  expect(rejected).toHaveLength(1);
  expect(rejected[0].failure_code).toBe("REFERENCE_ALREADY_REVERSED");

  expect(
    await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id IN (?, ?)`,
      [processed[0].id, rejected[0].id],
    ),
  ).toBe(1);

  expect(
    await countRows(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WalletBalanceChanged'
           AND payload->'data'->>'transactionId' IN (?, ?)`,
      [processed[0].id, rejected[0].id],
    ),
  ).toBe(1);

  expect(
    await countRows(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WagerTransactionRejected'
           AND payload->'data'->>'externalTransactionId' IN (?, ?)`,
      reversalIds,
    ),
  ).toBe(1);

  await expectReconciliation(walletId, expectedBalance);
}

describe("Wagering reversal concurrency E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    let app: INestApplication | undefined;

    try {
      const moduleFixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();

      app = moduleFixture.createNestApplication();

      await app.listen(0);

      environment = {
        app,
        entityManager: app.get(EntityManager),
        serverUrl: await app.getUrl(),
      };
    } catch (error) {
      await app?.close();
      restoreEnvironment();
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await environment?.app.close();
    } finally {
      environment = undefined;
      restoreEnvironment();
    }
  });

  it("processes only one of two concurrent REFUNDs for the same BET", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const bet = createPayload(wallet, runId, "bet", "BET");

    await expectProcessed(bet, await submitTransaction(bet), "75.00");

    const firstRefund = createPayload(
      wallet,
      runId,
      "refund-first",
      "REFUND",
      bet.externalTransactionId,
    );

    const secondRefund = createPayload(
      wallet,
      runId,
      "refund-second",
      "REFUND",
      bet.externalTransactionId,
    );

    const responses = await Promise.all([
      submitTransaction(firstRefund),
      submitTransaction(secondRefund),
    ]);

    expect(
      responses.map((response) => response.status).sort((a, b) => a - b),
    ).toEqual([200, 422]);

    const results = await Promise.all(
      responses.map(
        async (response) => (await response.json()) as WagerResponse,
      ),
    );

    expect(
      results.filter((result) => result.status === "PROCESSED"),
    ).toHaveLength(1);

    expect(
      results.filter((result) => result.status === "REJECTED"),
    ).toHaveLength(1);

    await expectOnlyOneReversalProcessed(
      wallet.id,
      firstRefund.providerId,
      [firstRefund.externalTransactionId, secondRefund.externalTransactionId],
      "100.00",
    );
  }, 30_000);

  it("processes only one of two concurrent ROLLBACKs for the same WIN", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const win = createPayload(wallet, runId, "win", "WIN");

    await expectProcessed(win, await submitTransaction(win), "125.00");

    const firstRollback = createPayload(
      wallet,
      runId,
      "rollback-first",
      "ROLLBACK",
      win.externalTransactionId,
    );

    const secondRollback = createPayload(
      wallet,
      runId,
      "rollback-second",
      "ROLLBACK",
      win.externalTransactionId,
    );

    const responses = await Promise.all([
      submitTransaction(firstRollback),
      submitTransaction(secondRollback),
    ]);

    expect(
      responses.map((response) => response.status).sort((a, b) => a - b),
    ).toEqual([200, 422]);

    const results = await Promise.all(
      responses.map(
        async (response) => (await response.json()) as WagerResponse,
      ),
    );

    expect(
      results.filter((result) => result.status === "PROCESSED"),
    ).toHaveLength(1);

    expect(
      results.filter((result) => result.status === "REJECTED"),
    ).toHaveLength(1);

    await expectOnlyOneReversalProcessed(
      wallet.id,
      firstRollback.providerId,
      [
        firstRollback.externalTransactionId,
        secondRollback.externalTransactionId,
      ],
      "100.00",
    );
  }, 30_000);
});

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
