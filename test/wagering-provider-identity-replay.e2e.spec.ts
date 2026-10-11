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

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type LedgerRow = {
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
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

async function submitBet(input: {
  payload: Record<string, unknown>;
  idempotencyKey: string;
}): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": input.idempotencyKey,
    },
    body: JSON.stringify(input.payload),
  });
}

describe("Wagering provider identity replay E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    try {
      const moduleFixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();

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

  it("replays the existing provider transaction when the same payload arrives with a new idempotency key", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-bet-${runId}`;

    const payload = {
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `e2e-round-${runId}`,
      gameId: "e2e-game",
      kind: "BET",
      money: {
        amount: "25.00",
        currency: "BRL",
      },
    };

    const firstResponse = await submitBet({
      payload,
      idempotencyKey: `e2e-idempotency-first-${runId}`,
    });

    expect(firstResponse.status).toBe(200);

    const firstResult = (await firstResponse.json()) as WagerResponse;

    expect(firstResult.status).toBe("PROCESSED");
    expect(firstResult.idempotentReplay).toBe(false);
    expect(firstResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const replayResponse = await submitBet({
      payload,
      idempotencyKey: `e2e-idempotency-second-${runId}`,
    });

    expect(replayResponse.status).toBe(200);

    const replayResult = (await replayResponse.json()) as WagerResponse;

    expect(replayResult).toEqual({
      ...firstResult,
      idempotentReplay: true,
    });

    const transactions = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [providerId, externalTransactionId],
    );

    expect(Number(transactions[0].count)).toBe(1);

    const ledgerEntries = await query<LedgerRow>(
      `SELECT direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [firstResult.transactionId],
    );

    expect(ledgerEntries).toEqual([
      {
        direction: "DEBIT",
        amount: "25.00",
        balance_before: "100.00",
        balance_after: "75.00",
      },
    ]);

    const processedEvents = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WagerTransactionProcessed'
           AND payload->'data'->>'externalTransactionId' = ?`,
      [externalTransactionId],
    );

    expect(Number(processedEvents[0].count)).toBe(1);

    const balanceChangedEvents = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WalletBalanceChanged'
           AND payload->'data'->>'transactionId' = ?`,
      [firstResult.transactionId],
    );

    expect(Number(balanceChangedEvents[0].count)).toBe(1);

    const walletRows = await query<BalanceRow>(
      `SELECT balance::text AS balance
         FROM wallets
         WHERE id = ?`,
      [wallet.id],
    );

    expect(walletRows).toHaveLength(1);
    expect(walletRows[0].balance).toBe("75.00");
  }, 30_000);
});
