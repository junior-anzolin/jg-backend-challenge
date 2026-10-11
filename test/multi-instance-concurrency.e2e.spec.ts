import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const INSTANCE_COUNT = 3;
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "40.00";
const EXPECTED_BALANCE = "20.00";

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type BetPayload = {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: "BET";
  money: {
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

type TransactionRow = {
  id: string;
  external_transaction_id: string;
  status: string;
  failure_code: string | null;
  resulting_balance_amount: string | null;
};

type LedgerRow = {
  transaction_id: string;
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
};

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type RunningInstance = {
  app: INestApplication;
  serverUrl: string;
};

let instances: RunningInstance[] = [];
let entityManager: EntityManager | undefined;

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
  if (!entityManager) {
    throw new Error("E2E EntityManager has not been initialized");
  }

  return (await entityManager.getConnection().execute(sql, params)) as T[];
}

async function startInstance(): Promise<RunningInstance> {
  const moduleFixture = await Test.createTestingModule({
    imports: [AppModule],
  }).compile();

  const app = moduleFixture.createNestApplication();

  try {
    await app.listen(0);

    return {
      app,
      serverUrl: await app.getUrl(),
    };
  } catch (error) {
    await app.close();
    throw error;
  }
}

async function createWallet(
  serverUrl: string,
  runId: string,
): Promise<WalletResponse> {
  const response = await fetch(`${serverUrl}/wallets`, {
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

function createBetPayload(
  wallet: WalletResponse,
  runId: string,
  instanceIndex: number,
): BetPayload {
  return {
    providerId: `e2e-provider-${runId}`,
    externalTransactionId: `e2e-bet-${runId}-${instanceIndex}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: `e2e-round-${runId}`,
    gameId: "e2e-game",
    kind: "BET",
    money: {
      amount: BET_AMOUNT,
      currency: "BRL",
    },
  };
}

async function submitBet(
  serverUrl: string,
  payload: BetPayload,
  idempotencyKey: string,
): Promise<Response> {
  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify(payload),
  });
}

async function expectWalletBalanceMatchesLedger(
  walletId: string,
): Promise<void> {
  const walletRows = await query<BalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(walletRows).toHaveLength(1);
  expect(walletRows[0].balance).toBe(EXPECTED_BALANCE);

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

  expect(ledgerBalances[0].balance).toBe(EXPECTED_BALANCE);
  expect(walletRows[0].balance).toBe(ledgerBalances[0].balance);
}

async function countOutboxEvents(
  eventType: string,
  externalTransactionId: string,
): Promise<number> {
  const rows = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = ?
         AND payload->'data'->>'externalTransactionId' = ?`,
    [eventType, externalTransactionId],
  );

  return Number(rows[0]?.count ?? "0");
}

async function countBalanceChangedEvents(
  transactionId: string,
): Promise<number> {
  const rows = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = 'WalletBalanceChanged'
         AND payload->'data'->>'transactionId' = ?`,
    [transactionId],
  );

  return Number(rows[0]?.count ?? "0");
}

describe("Multi-instance concurrency E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    try {
      for (let index = 0; index < INSTANCE_COUNT; index++) {
        const instance = await startInstance();
        instances.push(instance);
      }

      if (instances.length !== INSTANCE_COUNT) {
        throw new Error(
          `Expected ${INSTANCE_COUNT} application instances, got ${instances.length}`,
        );
      }

      entityManager = instances[0].app.get(EntityManager);
    } catch (error) {
      await Promise.all(instances.map(({ app }) => app.close()));
      instances = [];
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await Promise.all(instances.map(({ app }) => app.close()));
    } finally {
      instances = [];

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

  it("preserves wallet consistency across three simultaneous application instances", async () => {
    expect(instances).toHaveLength(INSTANCE_COUNT);

    const runId = randomUUID();

    const wallet = await createWallet(instances[0].serverUrl, runId);

    const submissions = instances.map((instance, index) => ({
      serverUrl: instance.serverUrl,
      payload: createBetPayload(wallet, runId, index),
      idempotencyKey: `e2e-idempotency-${runId}-${index}`,
    }));

    expect(
      new Set(submissions.map((submission) => submission.serverUrl)).size,
    ).toBe(INSTANCE_COUNT);

    const responses = await Promise.all(
      submissions.map(({ serverUrl, payload, idempotencyKey }) =>
        submitBet(serverUrl, payload, idempotencyKey),
      ),
    );

    expect(responses).toHaveLength(INSTANCE_COUNT);

    const results = await Promise.all(
      responses.map(async (response) => ({
        httpStatus: response.status,
        ok: response.ok,
        body: (await response.json()) as Partial<WagerResponse>,
      })),
    );

    expect(
      results.filter((result) => result.body.status === "PROCESSED"),
    ).toHaveLength(2);

    expect(
      results.filter((result) => result.body.status === "REJECTED"),
    ).toHaveLength(1);

    expect(results.filter((result) => result.ok)).toHaveLength(2);
    expect(results.filter((result) => !result.ok)).toHaveLength(1);

    expect(
      results
        .filter((result) => result.body.status === "PROCESSED")
        .every(
          (result) =>
            result.body.balance?.currency === "BRL" &&
            ["60.00", EXPECTED_BALANCE].includes(result.body.balance.amount),
        ),
    ).toBe(true);

    const transactions = await query<TransactionRow>(
      `SELECT id,
              external_transaction_id,
              status,
              failure_code,
              resulting_balance_amount::text AS resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id IN (?, ?, ?)`,
      [
        submissions[0].payload.providerId,
        submissions[0].payload.externalTransactionId,
        submissions[1].payload.externalTransactionId,
        submissions[2].payload.externalTransactionId,
      ],
    );

    expect(transactions).toHaveLength(INSTANCE_COUNT);

    const processed = transactions.filter(
      (transaction) => transaction.status === "PROCESSED",
    );

    const rejected = transactions.filter(
      (transaction) => transaction.status === "REJECTED",
    );

    expect(processed).toHaveLength(2);
    expect(rejected).toHaveLength(1);

    expect(
      processed
        .map((transaction) => transaction.resulting_balance_amount)
        .sort(),
    ).toEqual(["20.00", "60.00"]);

    expect(rejected[0].failure_code).toBeTruthy();

    const debitRows = await query<LedgerRow>(
      `SELECT transaction_id,
              direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE wallet_id = ?
           AND direction = 'DEBIT'
         ORDER BY balance_before_amount DESC`,
      [wallet.id],
    );

    expect(debitRows).toHaveLength(2);

    expect(debitRows).toEqual([
      {
        transaction_id: expect.any(String),
        direction: "DEBIT",
        amount: BET_AMOUNT,
        balance_before: INITIAL_BALANCE,
        balance_after: "60.00",
      },
      {
        transaction_id: expect.any(String),
        direction: "DEBIT",
        amount: BET_AMOUNT,
        balance_before: "60.00",
        balance_after: EXPECTED_BALANCE,
      },
    ]);

    expect(new Set(debitRows.map((entry) => entry.transaction_id))).toEqual(
      new Set(processed.map((transaction) => transaction.id)),
    );

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        processed[0].external_transaction_id,
      ),
    ).toBe(1);

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        processed[1].external_transaction_id,
      ),
    ).toBe(1);

    expect(
      await countOutboxEvents(
        "WagerTransactionRejected",
        rejected[0].external_transaction_id,
      ),
    ).toBe(1);

    for (const transaction of processed) {
      expect(await countBalanceChangedEvents(transaction.id)).toBe(1);
    }

    expect(await countBalanceChangedEvents(rejected[0].id)).toBe(0);

    const rejectedLedgerRows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [rejected[0].id],
    );

    expect(Number(rejectedLedgerRows[0].count)).toBe(0);

    await expectWalletBalanceMatchesLedger(wallet.id);
  }, 30_000);
});
