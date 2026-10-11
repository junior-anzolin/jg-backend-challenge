import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";

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

type TransactionRow = {
  id: string;
  status: string;
  resulting_balance_amount: string | null;
};

type LedgerRow = {
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
};

type CountRow = {
  count: string;
};

type WalletBalanceRow = {
  balance: string;
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
        amount: "100.00",
        currency: "BRL",
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.balance).toEqual({
    amount: "100.00",
    currency: "BRL",
  });

  return wallet;
}

function createBetPayload(
  wallet: WalletResponse,
  runId: string,
  amount = "25.00",
): BetPayload {
  return {
    providerId: `e2e-provider-${runId}`,
    externalTransactionId: `e2e-bet-${runId}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: `e2e-round-${runId}`,
    gameId: "e2e-game",
    kind: "BET",
    money: {
      amount,
      currency: "BRL",
    },
  };
}

async function submitBet(
  payload: BetPayload,
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

async function expectBetEffects(input: {
  walletId: string;
  providerId: string;
  externalTransactionId: string;
  expectedBalance: string;
}): Promise<void> {
  const transactions = await query<TransactionRow>(
    `SELECT id,
            status,
            resulting_balance_amount::text AS resulting_balance_amount
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id = ?`,
    [input.providerId, input.externalTransactionId],
  );

  expect(transactions).toHaveLength(1);
  expect(transactions[0].status).toBe("PROCESSED");
  expect(transactions[0].resulting_balance_amount).toBe(input.expectedBalance);

  const ledgerRows = await query<LedgerRow>(
    `SELECT direction,
            money_amount::text AS amount,
            balance_before_amount::text AS balance_before,
            balance_after_amount::text AS balance_after
       FROM wallet_ledger_entries
       WHERE transaction_id = ?`,
    [transactions[0].id],
  );

  expect(ledgerRows).toHaveLength(1);
  expect(ledgerRows[0]).toEqual({
    direction: "DEBIT",
    amount: "25.00",
    balance_before: "100.00",
    balance_after: input.expectedBalance,
  });

  const processedEvents = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = 'WagerTransactionProcessed'
         AND payload->'data'->>'externalTransactionId' = ?`,
    [input.externalTransactionId],
  );

  expect(Number(processedEvents[0].count)).toBe(1);

  const balanceChangedEvents = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = 'WalletBalanceChanged'
         AND payload->'data'->>'transactionId' = ?`,
    [transactions[0].id],
  );

  expect(Number(balanceChangedEvents[0].count)).toBe(1);

  const wallets = await query<WalletBalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [input.walletId],
  );

  expect(wallets).toHaveLength(1);
  expect(wallets[0].balance).toBe(input.expectedBalance);

  const ledgerBalances = await query<WalletBalanceRow>(
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
    [input.walletId],
  );

  expect(ledgerBalances[0].balance).toBe(input.expectedBalance);
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

describe("Wagering API idempotency E2E", () => {
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
    await app.listen(0);

    serverUrl = await app.getUrl();
    entityManager = app.get(EntityManager);
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

  it("returns the original result for an identical idempotent replay", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const payload = createBetPayload(wallet, runId);
    const idempotencyKey = `e2e-idempotency-${runId}`;

    const firstResponse = await submitBet(payload, idempotencyKey);

    expect(firstResponse.ok).toBe(true);

    const firstResult = (await firstResponse.json()) as WagerResponse;

    expect(firstResult.status).toBe("PROCESSED");
    expect(firstResult.idempotentReplay).toBe(false);
    expect(firstResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const replayResponse = await submitBet(payload, idempotencyKey);

    expect(replayResponse.ok).toBe(true);

    const replayResult = (await replayResponse.json()) as WagerResponse;

    expect(replayResult.transactionId).toBe(firstResult.transactionId);
    expect(replayResult.status).toBe(firstResult.status);
    expect(replayResult.balance).toEqual(firstResult.balance);
    expect(replayResult.idempotentReplay).toBe(true);

    await expectBetEffects({
      walletId: wallet.id,
      providerId: payload.providerId,
      externalTransactionId: payload.externalTransactionId,
      expectedBalance: "75.00",
    });
  }, 30_000);

  it("returns a conflict when an idempotency key is reused with a different payload", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const originalPayload = createBetPayload(wallet, runId, "25.00");
    const idempotencyKey = `e2e-idempotency-${runId}`;

    const firstResponse = await submitBet(originalPayload, idempotencyKey);

    expect(firstResponse.ok).toBe(true);

    const firstResult = (await firstResponse.json()) as WagerResponse;

    expect(firstResult.status).toBe("PROCESSED");
    expect(firstResult.idempotentReplay).toBe(false);
    expect(firstResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const conflictingPayload = {
      ...originalPayload,
      money: {
        amount: "40.00",
        currency: "BRL",
      },
    };

    const conflictResponse = await submitBet(
      conflictingPayload,
      idempotencyKey,
    );

    expect(conflictResponse.status).toBe(409);

    await expectBetEffects({
      walletId: wallet.id,
      providerId: originalPayload.providerId,
      externalTransactionId: originalPayload.externalTransactionId,
      expectedBalance: "75.00",
    });
  }, 30_000);
});
