import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const INSTANCE_COUNT = 3;
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "30.00";
const EXPECTED_BALANCE = "70.00";

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
  if (!entityManager) {
    throw new Error("E2E EntityManager has not been initialized");
  }

  return (await entityManager.getConnection().execute(sql, params)) as T[];
}

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);

  return Number(rows[0]?.count ?? "0");
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
  const playerId = `e2e-player-${runId}`;

  const response = await fetch(`${serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId,
      initialBalance: {
        amount: INITIAL_BALANCE,
        currency: "BRL",
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.playerId).toBe(playerId);
  expect(wallet.balance).toEqual({
    amount: INITIAL_BALANCE,
    currency: "BRL",
  });

  return wallet;
}

function createBetPayload(wallet: WalletResponse, runId: string): BetPayload {
  return {
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

  const ledgerRows = await query<BalanceRow>(
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

  expect(ledgerRows).toHaveLength(1);
  expect(ledgerRows[0].balance).toBe(EXPECTED_BALANCE);
  expect(walletRows[0].balance).toBe(ledgerRows[0].balance);
}

describe("Multi-instance idempotency E2E", () => {
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
        instances.push(await startInstance());
      }

      if (instances.length !== INSTANCE_COUNT) {
        throw new Error(
          `Expected ${INSTANCE_COUNT} instances, got ${instances.length}`,
        );
      }

      entityManager = instances[0].app.get(EntityManager);
    } catch (error) {
      await Promise.all(instances.map(({ app }) => app.close()));
      instances = [];
      restoreEnvironment();
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await Promise.all(instances.map(({ app }) => app.close()));
    } finally {
      instances = [];
      entityManager = undefined;
      restoreEnvironment();
    }
  });

  it("processes the same bet only once across three simultaneous instances", async () => {
    expect(instances).toHaveLength(INSTANCE_COUNT);

    const runId = randomUUID();
    const wallet = await createWallet(instances[0].serverUrl, runId);
    const payload = createBetPayload(wallet, runId);
    const idempotencyKey = `e2e-idempotency-${runId}`;

    expect(new Set(instances.map((instance) => instance.serverUrl)).size).toBe(
      INSTANCE_COUNT,
    );

    const responses = await Promise.all(
      instances.map((instance) =>
        submitBet(instance.serverUrl, payload, idempotencyKey),
      ),
    );

    expect(responses).toHaveLength(INSTANCE_COUNT);

    const results = await Promise.all(
      responses.map(async (response) => ({
        httpStatus: response.status,
        body: (await response.json()) as WagerResponse,
      })),
    );

    expect(results.every((result) => result.httpStatus === 200)).toBe(true);
    expect(results.every((result) => result.body.status === "PROCESSED")).toBe(
      true,
    );

    expect(
      new Set(results.map((result) => result.body.transactionId)).size,
    ).toBe(1);

    expect(
      results.filter((result) => result.body.idempotentReplay === false),
    ).toHaveLength(1);

    expect(
      results.filter((result) => result.body.idempotentReplay === true),
    ).toHaveLength(INSTANCE_COUNT - 1);

    expect(
      results.every(
        (result) =>
          result.body.balance?.amount === EXPECTED_BALANCE &&
          result.body.balance.currency === "BRL",
      ),
    ).toBe(true);

    const transactions = await query<TransactionRow>(
      `SELECT id,
              status,
              failure_code,
              resulting_balance_amount::text AS resulting_balance_amount
         FROM wager_transactions
        WHERE provider_id = ?
          AND external_transaction_id = ?`,
      [payload.providerId, payload.externalTransactionId],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0].id).toBe(results[0].body.transactionId);
    expect(transactions[0].status).toBe("PROCESSED");
    expect(transactions[0].failure_code).toBeNull();
    expect(transactions[0].resulting_balance_amount).toBe(EXPECTED_BALANCE);

    const ledgerRows = await query<LedgerRow>(
      `SELECT transaction_id,
              direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
        WHERE transaction_id = ?`,
      [transactions[0].id],
    );

    expect(ledgerRows).toHaveLength(1);
    expect(ledgerRows[0]).toEqual({
      transaction_id: transactions[0].id,
      direction: "DEBIT",
      amount: BET_AMOUNT,
      balance_before: INITIAL_BALANCE,
      balance_after: EXPECTED_BALANCE,
    });

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
           FROM outbox_messages
          WHERE event_type = 'WagerTransactionProcessed'
            AND payload->'data'->>'externalTransactionId' = ?`,
        [payload.externalTransactionId],
      ),
    ).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
           FROM outbox_messages
          WHERE event_type = 'WalletBalanceChanged'
            AND payload->'data'->>'transactionId' = ?`,
        [transactions[0].id],
      ),
    ).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
           FROM wallet_ledger_entries
          WHERE wallet_id = ?
            AND direction = 'DEBIT'`,
        [wallet.id],
      ),
    ).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id);
  }, 30_000);
});
