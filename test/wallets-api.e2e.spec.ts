import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const CURRENCY = "BRL";

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
  version: number;
};

type OpeningTransactionRow = {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  kind: string;
  status: string;
  money_amount: string;
  money_currency: string;
  resulting_balance_amount: string | null;
  resulting_balance_currency: string | null;
};

type LedgerRow = {
  id: string;
  wallet_id: string;
  transaction_id: string;
  direction: string;
  money_amount: string;
  money_currency: string;
  balance_before_amount: string;
  balance_before_currency: string;
  balance_after_amount: string;
  balance_after_currency: string;
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

async function createWallet(
  playerId: string,
  initialBalance = INITIAL_BALANCE,
): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId,
      initialBalance: {
        amount: initialBalance,
        currency: CURRENCY,
      },
    }),
  });
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
         AND money_currency = ?`,
    [walletId, CURRENCY],
  );

  expect(ledgerBalances).toHaveLength(1);
  expect(ledgerBalances[0].balance).toBe(expectedBalance);
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

describe("Wallets API E2E", () => {
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

  it("creates a wallet with an atomic OPENING transaction and balanced credit ledger entry", async () => {
    const playerId = `e2e-player-${randomUUID()}`;

    const response = await createWallet(playerId);

    expect(response.status).toBe(201);

    const wallet = (await response.json()) as WalletResponse;

    expect(wallet.id).toBeTruthy();
    expect(wallet.playerId).toBe(playerId);
    expect(wallet.balance).toEqual({
      amount: INITIAL_BALANCE,
      currency: CURRENCY,
    });
    expect(wallet.version).toBe(1);

    const wallets = await query<BalanceRow>(
      `SELECT balance::text AS balance
           FROM wallets
           WHERE id = ?`,
      [wallet.id],
    );

    expect(wallets).toHaveLength(1);
    expect(wallets[0].balance).toBe(INITIAL_BALANCE);

    const openingTransactions = await query<OpeningTransactionRow>(
      `SELECT id,
                provider_id,
                external_transaction_id,
                idempotency_key,
                kind,
                status,
                money_amount::text AS money_amount,
                money_currency,
                resulting_balance_amount::text
                  AS resulting_balance_amount,
                resulting_balance_currency
           FROM wager_transactions
           WHERE wallet_id = ?
             AND kind = 'OPENING'`,
      [wallet.id],
    );

    expect(openingTransactions).toHaveLength(1);

    const opening = openingTransactions[0];

    expect(opening.provider_id).toBe("internal");
    expect(opening.external_transaction_id).toBe(`wallet-opening:${wallet.id}`);
    expect(opening.idempotency_key).toBe(`wallet-opening:${wallet.id}`);
    expect(opening.status).toBe("PROCESSED");
    expect(opening.money_amount).toBe(INITIAL_BALANCE);
    expect(opening.money_currency).toBe(CURRENCY);
    expect(opening.resulting_balance_amount).toBe(INITIAL_BALANCE);
    expect(opening.resulting_balance_currency).toBe(CURRENCY);

    const ledgerEntries = await query<LedgerRow>(
      `SELECT id,
                wallet_id,
                transaction_id,
                direction,
                money_amount::text AS money_amount,
                money_currency,
                balance_before_amount::text AS balance_before_amount,
                balance_before_currency,
                balance_after_amount::text AS balance_after_amount,
                balance_after_currency
           FROM wallet_ledger_entries
           WHERE wallet_id = ?
             AND transaction_id = ?`,
      [wallet.id, opening.id],
    );

    expect(ledgerEntries).toHaveLength(1);

    expect(ledgerEntries[0]).toMatchObject({
      wallet_id: wallet.id,
      transaction_id: opening.id,
      direction: "CREDIT",
      money_amount: INITIAL_BALANCE,
      money_currency: CURRENCY,
      balance_before_amount: "0.00",
      balance_before_currency: CURRENCY,
      balance_after_amount: INITIAL_BALANCE,
      balance_after_currency: CURRENCY,
    });

    const balanceChangedEvents = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
           FROM outbox_messages
           WHERE event_type = 'WalletBalanceChanged'
             AND payload->'data'->>'transactionId' = ?`,
      [opening.id],
    );

    expect(Number(balanceChangedEvents[0].count)).toBe(1);

    const processedEvents = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
           FROM outbox_messages
           WHERE event_type = 'WagerTransactionProcessed'
             AND payload->'data'->>'externalTransactionId' = ?`,
      [opening.external_transaction_id],
    );

    expect(Number(processedEvents[0].count)).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, INITIAL_BALANCE);
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
