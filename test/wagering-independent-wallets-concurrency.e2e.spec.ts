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

type BalanceRow = {
  balance: string;
};

type CountRow = {
  count: string;
};

type LedgerRow = {
  direction: string;
  amount: string;
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

let app: INestApplication | undefined;
let entityManager: EntityManager | undefined;
let serverUrl: string | undefined;

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

  return (await response.json()) as WalletResponse;
}

async function submitBet(
  wallet: WalletResponse,
  runId: string,
): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const externalTransactionId = `e2e-bet-${runId}`;

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${runId}`,
    },
    body: JSON.stringify({
      providerId: `e2e-provider-${runId}`,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `e2e-round-${runId}`,
      gameId: "e2e-game",
      kind: "BET",
      money: {
        amount: "30.00",
        currency: "BRL",
      },
    }),
  });
}

async function getWalletLedgerBalance(walletId: string): Promise<string> {
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
         AND money_currency = 'BRL'`,
    [walletId],
  );

  return rows[0].balance;
}

describe("Independent wallets concurrency E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-independent-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-independent-${randomUUID()}.fifo`;

    try {
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

      app = moduleFixture.createNestApplication();
      await app.listen(0);

      serverUrl = await app.getUrl();
      entityManager = app.get(EntityManager);
    } catch (error) {
      await app?.close();
      app = undefined;
      entityManager = undefined;
      serverUrl = undefined;
      restoreEnvironment();
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await app?.close();
    } finally {
      app = undefined;
      entityManager = undefined;
      serverUrl = undefined;
      restoreEnvironment();
    }
  });

  it("processes bets on distinct wallets in parallel without mixing balances or ledger entries", async () => {
    const runId = randomUUID();
    const wallets = await Promise.all(
      Array.from({ length: 3 }, (_, index) =>
        createWallet(`${runId}-${index}`),
      ),
    );

    expect(new Set(wallets.map((wallet) => wallet.id)).size).toBe(3);

    const responses = await Promise.all(
      wallets.map((wallet, index) => submitBet(wallet, `${runId}-${index}`)),
    );

    const results = await Promise.all(
      responses.map(async (response) => {
        expect(response.status).toBe(200);
        return (await response.json()) as WagerResponse;
      }),
    );

    for (const result of results) {
      expect(result.status).toBe("PROCESSED");
      expect(result.idempotentReplay).toBe(false);
      expect(result.balance).toEqual({
        amount: "70.00",
        currency: "BRL",
      });
    }

    for (let index = 0; index < wallets.length; index++) {
      const wallet = wallets[index];
      const result = results[index];

      const walletRows = await query<BalanceRow>(
        `SELECT balance::text AS balance
           FROM wallets
           WHERE id = ?`,
        [wallet.id],
      );

      expect(walletRows).toHaveLength(1);
      expect(walletRows[0].balance).toBe("70.00");
      expect(await getWalletLedgerBalance(wallet.id)).toBe(
        walletRows[0].balance,
      );

      const ledgerRows = await query<LedgerRow>(
        `SELECT direction,
                money_amount::text AS amount
           FROM wallet_ledger_entries
           WHERE transaction_id = ?`,
        [result.transactionId],
      );

      expect(ledgerRows).toEqual([
        {
          direction: "DEBIT",
          amount: "30.00",
        },
      ]);

      const transactionRows = await query<CountRow>(
        `SELECT COUNT(*)::text AS count
           FROM wager_transactions
           WHERE id = ?`,
        [result.transactionId],
      );

      expect(Number(transactionRows[0].count)).toBe(1);
    }
  }, 30_000);
});
