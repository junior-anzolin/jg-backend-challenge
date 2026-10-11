import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const TRANSACTION_AMOUNT = "25.00";
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

type TransactionRow = {
  id: string;
  external_transaction_id: string;
  status: string;
  resulting_balance_amount: string | null;
  failure_code: string | null;
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
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

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

async function submitTransaction(
  payload: Record<string, unknown>,
  idempotencyKey: string,
): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify(payload),
  });
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

describe("Wagering reference order E2E", () => {
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
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await app?.close();
    } finally {
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

  it("processes a ROLLBACK after its referenced BET arrives", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const providerId = `e2e-provider-${runId}`;
    const betExternalTransactionId = `e2e-bet-${runId}`;
    const rollbackExternalTransactionId = `e2e-rollback-${runId}`;
    const roundId = `e2e-round-${runId}`;

    const rollbackPayload = {
      providerId,
      externalTransactionId: rollbackExternalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId,
      gameId: "e2e-game",
      kind: "ROLLBACK",
      money: {
        amount: TRANSACTION_AMOUNT,
        currency: "BRL",
      },
      referenceExternalTransactionId: betExternalTransactionId,
    };

    // The rollback is deliberately submitted before the referenced bet.
    const rollbackResponse = await submitTransaction(
      rollbackPayload,
      `e2e-idempotency-rollback-${runId}`,
    );

    expect(rollbackResponse.status).toBe(202);

    const initialRollback = (await rollbackResponse.json()) as WagerResponse;

    expect(initialRollback.status).toBe("PENDING_REFERENCE");
    expect(initialRollback.idempotentReplay).toBe(false);

    const pendingRows = await query<TransactionRow>(
      `SELECT id,
              external_transaction_id,
              status,
              resulting_balance_amount::text AS resulting_balance_amount,
              failure_code
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [providerId, rollbackExternalTransactionId],
    );

    expect(pendingRows).toHaveLength(1);
    expect(pendingRows[0].id).toBe(initialRollback.transactionId);
    expect(pendingRows[0].status).toBe("PENDING_REFERENCE");

    const betPayload = {
      providerId,
      externalTransactionId: betExternalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId,
      gameId: "e2e-game",
      kind: "BET",
      money: {
        amount: TRANSACTION_AMOUNT,
        currency: "BRL",
      },
    };

    const betResponse = await submitTransaction(
      betPayload,
      `e2e-idempotency-bet-${runId}`,
    );

    expect(betResponse.status).toBe(200);

    const betResult = (await betResponse.json()) as WagerResponse;

    expect(betResult.status).toBe("PROCESSED");
    expect(betResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    // The scheduled worker must resume the rollback after its retry becomes due.
    await waitUntil(async () => {
      const rows = await query<TransactionRow>(
        `SELECT id,
                  external_transaction_id,
                  status,
                  resulting_balance_amount::text AS resulting_balance_amount,
                  failure_code
             FROM wager_transactions
             WHERE provider_id = ?
               AND external_transaction_id = ?`,
        [providerId, rollbackExternalTransactionId],
      );

      return rows.length === 1 && rows[0].status === "PROCESSED";
    }, "the pending rollback to be processed by the scheduled worker");

    const transactions = await query<TransactionRow>(
      `SELECT id,
              external_transaction_id,
              status,
              resulting_balance_amount::text AS resulting_balance_amount,
              failure_code
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id IN (?, ?)`,
      [providerId, betExternalTransactionId, rollbackExternalTransactionId],
    );

    expect(transactions).toHaveLength(2);

    const betTransaction = transactions.find(
      (transaction) =>
        transaction.external_transaction_id === betExternalTransactionId,
    );

    const rollbackTransaction = transactions.find(
      (transaction) =>
        transaction.external_transaction_id === rollbackExternalTransactionId,
    );

    expect(betTransaction).toBeDefined();
    expect(rollbackTransaction).toBeDefined();

    expect(betTransaction!.status).toBe("PROCESSED");
    expect(betTransaction!.resulting_balance_amount).toBe("75.00");

    expect(rollbackTransaction!.status).toBe("PROCESSED");
    expect(rollbackTransaction!.resulting_balance_amount).toBe("100.00");
    expect(rollbackTransaction!.failure_code).toBeNull();

    const ledgerRows = await query<LedgerRow>(
      `SELECT transaction_id,
              direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE transaction_id IN (?, ?)`,
      [betTransaction!.id, rollbackTransaction!.id],
    );

    expect(ledgerRows).toHaveLength(2);

    const betLedgerEntry = ledgerRows.find(
      (entry) => entry.transaction_id === betTransaction!.id,
    );

    const rollbackLedgerEntry = ledgerRows.find(
      (entry) => entry.transaction_id === rollbackTransaction!.id,
    );

    expect(betLedgerEntry).toEqual({
      transaction_id: betTransaction!.id,
      direction: "DEBIT",
      amount: TRANSACTION_AMOUNT,
      balance_before: "100.00",
      balance_after: "75.00",
    });

    expect(rollbackLedgerEntry).toEqual({
      transaction_id: rollbackTransaction!.id,
      direction: "CREDIT",
      amount: TRANSACTION_AMOUNT,
      balance_before: "75.00",
      balance_after: "100.00",
    });

    expect(
      await countOutboxEvents(
        "WagerTransactionPendingReference",
        rollbackExternalTransactionId,
      ),
    ).toBe(1);

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        betExternalTransactionId,
      ),
    ).toBe(1);

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        rollbackExternalTransactionId,
      ),
    ).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, "100.00");
  }, 30_000);
});
