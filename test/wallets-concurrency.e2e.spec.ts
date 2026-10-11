import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const WALLET_COUNT = 6;
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "25.00";
const EXPECTED_BALANCE = "75.00";

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
  idempotentReplay: boolean;
};

type TransactionRow = {
  id: string;
  status: string;
  resulting_balance_amount: string | null;
};

type LedgerRow = {
  transaction_id: string;
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
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

async function createWallet(
  runId: string,
  index: number,
): Promise<WalletResponse> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const response = await fetch(`${serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId: `e2e-player-${runId}-${index}`,
      initialBalance: {
        amount: INITIAL_BALANCE,
        currency: "BRL",
      },
    }),
  });

  expect(response.ok).toBe(true);

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
  index: number,
): BetPayload {
  return {
    providerId: `e2e-provider-${runId}`,
    externalTransactionId: `e2e-bet-${runId}-${index}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId: `e2e-round-${runId}-${index}`,
    gameId: "e2e-game",
    kind: "BET",
    money: {
      amount: BET_AMOUNT,
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
): Promise<void> {
  const wallets = await query<BalanceRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(wallets).toHaveLength(1);
  expect(wallets[0].balance).toBe(EXPECTED_BALANCE);

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
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

describe("Wallets concurrency E2E", () => {
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

  it("processes bets on different wallets in parallel without cross-wallet interference", async () => {
    const runId = randomUUID();

    // Each wallet belongs to a different player and starts with 100.00 BRL.
    const wallets = await Promise.all(
      Array.from({ length: WALLET_COUNT }, (_, index) =>
        createWallet(runId, index),
      ),
    );

    expect(wallets).toHaveLength(WALLET_COUNT);
    expect(new Set(wallets.map((wallet) => wallet.id)).size).toBe(WALLET_COUNT);

    const submissions = wallets.map((wallet, index) => {
      const payload = createBetPayload(wallet, runId, index);

      return {
        wallet,
        payload,
        idempotencyKey: `e2e-idempotency-${runId}-${index}`,
      };
    });

    // Issue requests concurrently; no mocks or sequential submission loop.
    const responses = await Promise.all(
      submissions.map(({ payload, idempotencyKey }) =>
        submitBet(payload, idempotencyKey),
      ),
    );

    expect(responses).toHaveLength(WALLET_COUNT);
    expect(responses.every((response) => response.ok)).toBe(true);

    const results = await Promise.all(
      responses.map(
        async (response) => (await response.json()) as WagerResponse,
      ),
    );

    expect(results).toHaveLength(WALLET_COUNT);
    expect(results.every((result) => result.status === "PROCESSED")).toBe(true);
    expect(results.every((result) => result.idempotentReplay === false)).toBe(
      true,
    );
    expect(
      results.every(
        (result) =>
          result.balance?.amount === EXPECTED_BALANCE &&
          result.balance.currency === "BRL",
      ),
    ).toBe(true);

    expect(new Set(results.map((result) => result.transactionId)).size).toBe(
      WALLET_COUNT,
    );

    // Verify persisted transaction and ledger effects independently per wallet.
    for (const [index, submission] of submissions.entries()) {
      const { wallet, payload } = submission;
      const result = results[index];

      const transactions = await query<TransactionRow>(
        `SELECT id,
                status,
                resulting_balance_amount::text AS resulting_balance_amount
           FROM wager_transactions
           WHERE provider_id = ?
             AND external_transaction_id = ?`,
        [payload.providerId, payload.externalTransactionId],
      );

      expect(transactions).toHaveLength(1);
      expect(transactions[0].id).toBe(result.transactionId);
      expect(transactions[0].status).toBe("PROCESSED");
      expect(transactions[0].resulting_balance_amount).toBe(EXPECTED_BALANCE);

      const debitEntries = await query<LedgerRow>(
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

      expect(debitEntries).toHaveLength(1);
      expect(debitEntries[0]).toEqual({
        transaction_id: transactions[0].id,
        direction: "DEBIT",
        amount: BET_AMOUNT,
        balance_before: INITIAL_BALANCE,
        balance_after: EXPECTED_BALANCE,
      });

      await expectWalletBalanceMatchesLedger(wallet.id);
    }
  }, 30_000);
});
