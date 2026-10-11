import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const CURRENCY = "BRL";
const INITIAL_BALANCE = "100.00";

type WagerKind = "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK";

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

type BalanceRow = {
  balance: string;
};

type CountRow = {
  count: string;
};

let app: INestApplication | undefined;
let entityManager: EntityManager | undefined;
let serverUrl: string | undefined;

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

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);

  return Number(rows[0]?.count ?? "0");
}

async function createWallet(runId: string): Promise<WalletResponse> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

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
  amount: string,
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
      amount,
      currency: CURRENCY,
    },
    ...(referenceExternalTransactionId
      ? { referenceExternalTransactionId }
      : {}),
  };
}

async function submitTransaction(payload: WagerPayload): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${payload.externalTransactionId}`,
    },
    body: JSON.stringify(payload),
  });
}

async function readWalletBalance(walletId: string): Promise<string> {
  const rows = await query<BalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
      WHERE id = ?`,
    [walletId],
  );

  expect(rows).toHaveLength(1);

  return rows[0].balance;
}

async function readLedgerBalance(walletId: string): Promise<string> {
  const rows = await query<BalanceRow>(
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

  expect(rows).toHaveLength(1);

  return rows[0].balance;
}

async function expectConsistentBalance(
  walletId: string,
  expectedBalance: string,
): Promise<void> {
  const walletBalance = await readWalletBalance(walletId);
  const ledgerBalance = await readLedgerBalance(walletId);

  expect(walletBalance).toBe(expectedBalance);
  expect(ledgerBalance).toBe(expectedBalance);
  expect(walletBalance).toBe(ledgerBalance);
}

async function submitAndAssert(
  wallet: WalletResponse,
  payload: WagerPayload,
  expectedStatus: number,
  expectedTransactionStatus: "PROCESSED" | "REJECTED",
  expectedBalance: string,
  expectedFailureCode?: string,
): Promise<WagerResponse> {
  const response = await submitTransaction(payload);

  expect(response.status).toBe(expectedStatus);

  const result = (await response.json()) as WagerResponse;

  expect(result.status).toBe(expectedTransactionStatus);
  expect(result.idempotentReplay).toBe(false);

  if (expectedTransactionStatus === "PROCESSED") {
    expect(result.balance).toEqual({
      amount: expectedBalance,
      currency: CURRENCY,
    });
  }

  if (expectedFailureCode) {
    expect(result.failureCode).toBe(expectedFailureCode);
  }

  const transactionRows = await query<{ count: string }>(
    `SELECT COUNT(*)::text AS count
       FROM wager_transactions
      WHERE id = ?`,
    [result.transactionId],
  );

  expect(Number(transactionRows[0]?.count ?? "0")).toBe(1);

  await expectConsistentBalance(wallet.id, expectedBalance);

  return result;
}

describe("Wagering ledger consistency E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    const moduleFixture = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();

    try {
      await app.listen(0);
      serverUrl = await app.getUrl();
      entityManager = app.get(EntityManager);
    } catch (error) {
      await app.close();
      app = undefined;
      serverUrl = undefined;
      entityManager = undefined;

      for (const key of managedEnvironmentKeys) {
        const previousValue = previousEnvironment.get(key);

        if (previousValue === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = previousValue;
        }
      }

      throw error;
    }
  });

  afterAll(async () => {
    try {
      await app?.close();
    } finally {
      app = undefined;
      serverUrl = undefined;
      entityManager = undefined;

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

  it("preserves the wallet and ledger invariant throughout a sequence of wagers and reversals", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    await expectConsistentBalance(wallet.id, INITIAL_BALANCE);

    const bet = createPayload(wallet, runId, "bet", "BET", "30.00");

    await submitAndAssert(wallet, bet, 200, "PROCESSED", "70.00");

    const win = createPayload(wallet, runId, "win", "WIN", "15.00");

    await submitAndAssert(wallet, win, 200, "PROCESSED", "85.00");

    const loss = createPayload(wallet, runId, "loss", "LOSS", "10.00");

    await submitAndAssert(wallet, loss, 200, "PROCESSED", "85.00");

    const refund = createPayload(
      wallet,
      runId,
      "refund",
      "REFUND",
      "30.00",
      bet.externalTransactionId,
    );

    await submitAndAssert(wallet, refund, 200, "PROCESSED", "115.00");

    const rollback = createPayload(
      wallet,
      runId,
      "rollback",
      "ROLLBACK",
      "15.00",
      win.externalTransactionId,
    );

    await submitAndAssert(wallet, rollback, 200, "PROCESSED", "100.00");

    const duplicateRefund = createPayload(
      wallet,
      runId,
      "duplicate-refund",
      "REFUND",
      "30.00",
      bet.externalTransactionId,
    );

    await submitAndAssert(wallet, duplicateRefund, 422, "REJECTED", "100.00");

    const duplicateRollback = createPayload(
      wallet,
      runId,
      "duplicate-rollback",
      "ROLLBACK",
      "15.00",
      win.externalTransactionId,
    );

    await submitAndAssert(wallet, duplicateRollback, 422, "REJECTED", "100.00");

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
           FROM wallet_ledger_entries
          WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).toBe(5);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
       FROM wager_transactions
      WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).toBe(8);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
           FROM outbox_messages
          WHERE event_type = 'WalletBalanceChanged'
            AND payload->'data'->>'walletId' = ?`,
        [wallet.id],
      ),
    ).toBe(5);

    await expectConsistentBalance(wallet.id, "100.00");
  });
});
