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

  expect(response.ok).toBe(true);

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
  suffix: string,
  amount: string,
): BetPayload {
  return {
    providerId: `e2e-provider-${runId}`,
    externalTransactionId: `e2e-bet-${suffix}-${runId}`,
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

async function expectWalletBalanceMatchesLedger(
  walletId: string,
  expectedBalance: string,
): Promise<void> {
  const walletRows = await query<BalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(walletRows).toHaveLength(1);
  expect(walletRows[0].balance).toBe(expectedBalance);

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

  expect(ledgerRows[0].balance).toBe(expectedBalance);
  expect(walletRows[0].balance).toBe(ledgerRows[0].balance);
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

describe("Wagering concurrency E2E", () => {
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

  it("processes 50 parallel submissions of the same bet with only one debit", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const payload = createBetPayload(wallet, runId, "same", "25.00");

    const idempotencyKey = `e2e-idempotency-${runId}`;
    const submissionCount = 50;

    // All requests are issued concurrently against the real HTTP application.
    const responses = await Promise.all(
      Array.from({ length: submissionCount }, () =>
        submitBet(payload, idempotencyKey),
      ),
    );

    expect(responses).toHaveLength(submissionCount);
    expect(responses.every((response) => response.ok)).toBe(true);

    const results = await Promise.all(
      responses.map(
        async (response) => (await response.json()) as WagerResponse,
      ),
    );

    expect(results).toHaveLength(submissionCount);
    expect(results.every((result) => result.status === "PROCESSED")).toBe(true);
    expect(
      results.every(
        (result) =>
          result.balance?.amount === "75.00" &&
          result.balance.currency === "BRL",
      ),
    ).toBe(true);

    expect(new Set(results.map((result) => result.transactionId)).size).toBe(1);

    expect(
      results.filter((result) => result.idempotentReplay === false),
    ).toHaveLength(1);

    expect(
      results.filter((result) => result.idempotentReplay === true),
    ).toHaveLength(submissionCount - 1);

    const transactions = await query<TransactionRow>(
      `SELECT id,
              external_transaction_id,
              status,
              failure_code,
              resulting_balance_amount::text AS resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [payload.providerId, payload.externalTransactionId],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0].status).toBe("PROCESSED");
    expect(transactions[0].resulting_balance_amount).toBe("75.00");

    const debitRows = await query<LedgerRow>(
      `SELECT transaction_id,
              direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE wallet_id = ?
           AND direction = 'DEBIT'`,
      [wallet.id],
    );

    expect(debitRows).toHaveLength(1);
    expect(debitRows[0]).toEqual({
      transaction_id: transactions[0].id,
      direction: "DEBIT",
      amount: "25.00",
      balance_before: "100.00",
      balance_after: "75.00",
    });

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        payload.externalTransactionId,
      ),
    ).toBe(1);

    expect(await countBalanceChangedEvents(transactions[0].id)).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);

  it("allows only one of two parallel 80.00 bets against a 100.00 balance", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const firstPayload = createBetPayload(wallet, runId, "first", "80.00");

    const secondPayload = createBetPayload(wallet, runId, "second", "80.00");

    const submissions = [
      {
        payload: firstPayload,
        idempotencyKey: `e2e-idempotency-first-${runId}`,
      },
      {
        payload: secondPayload,
        idempotencyKey: `e2e-idempotency-second-${runId}`,
      },
    ];

    // Distinct bets compete for the same wallet at the same time.
    const responses = await Promise.all(
      submissions.map(({ payload, idempotencyKey }) =>
        submitBet(payload, idempotencyKey),
      ),
    );

    expect(responses).toHaveLength(2);
    expect(responses.filter((response) => response.ok)).toHaveLength(1);
    expect(responses.filter((response) => !response.ok)).toHaveLength(1);

    const responseBodies = await Promise.all(
      responses.map(async (response) => ({
        ok: response.ok,
        body: (await response.json()) as Partial<WagerResponse>,
      })),
    );

    const processedResponse = responseBodies.find(
      (result) => result.body.status === "PROCESSED",
    );

    const rejectedResponse = responseBodies.find(
      (result) => result.body.status === "REJECTED",
    );

    expect(processedResponse).toBeDefined();
    expect(rejectedResponse).toBeDefined();
    expect(processedResponse!.ok).toBe(true);
    expect(rejectedResponse!.ok).toBe(false);

    const externalTransactionIds = submissions.map(
      ({ payload }) => payload.externalTransactionId,
    );

    const transactions = await query<TransactionRow>(
      `SELECT id,
              external_transaction_id,
              status,
              failure_code,
              resulting_balance_amount::text AS resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id IN (?, ?)`,
      [
        firstPayload.providerId,
        externalTransactionIds[0],
        externalTransactionIds[1],
      ],
    );

    expect(transactions).toHaveLength(2);
    expect(
      transactions.filter((transaction) => transaction.status === "PROCESSED"),
    ).toHaveLength(1);
    expect(
      transactions.filter((transaction) => transaction.status === "REJECTED"),
    ).toHaveLength(1);

    const processed = transactions.find(
      (transaction) => transaction.status === "PROCESSED",
    )!;

    const rejected = transactions.find(
      (transaction) => transaction.status === "REJECTED",
    )!;

    expect(processed.resulting_balance_amount).toBe("20.00");
    expect(rejected.failure_code).toBeTruthy();

    const debitRows = await query<LedgerRow>(
      `SELECT transaction_id,
              direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE wallet_id = ?
           AND direction = 'DEBIT'`,
      [wallet.id],
    );

    expect(debitRows).toHaveLength(1);
    expect(debitRows[0]).toEqual({
      transaction_id: processed.id,
      direction: "DEBIT",
      amount: "80.00",
      balance_before: "100.00",
      balance_after: "20.00",
    });

    const rejectedLedgerRows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [rejected.id],
    );

    expect(Number(rejectedLedgerRows[0].count)).toBe(0);

    expect(
      await countOutboxEvents(
        "WagerTransactionProcessed",
        processed.external_transaction_id,
      ),
    ).toBe(1);

    expect(
      await countOutboxEvents(
        "WagerTransactionRejected",
        rejected.external_transaction_id,
      ),
    ).toBe(1);

    expect(await countBalanceChangedEvents(processed.id)).toBe(1);
    expect(await countBalanceChangedEvents(rejected.id)).toBe(0);

    await expectWalletBalanceMatchesLedger(wallet.id, "20.00");
  }, 30_000);
});
