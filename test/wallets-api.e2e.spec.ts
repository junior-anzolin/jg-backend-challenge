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

type WalletRow = {
  id: string;
  balance: string;
  version: number;
};

type LedgerEntryResponse = {
  id: string;
  walletId: string;
  transactionId: string;
  direction: string;
  money: {
    amount: string;
    currency: string;
  };
  balanceBefore: {
    amount: string;
    currency: string;
  };
  balanceAfter: {
    amount: string;
    currency: string;
  };
  createdAt: string;
};

type LedgerPageResponse = {
  walletId: string;
  items: LedgerEntryResponse[];
  nextCursor: string | null;
};

type ReconciliationResponse = {
  walletId: string;
  storedBalance: {
    amount: string;
    currency: string;
  };
  calculatedBalance: {
    amount: string;
    currency: string;
  };
  difference: {
    amount: string;
    currency: string;
  };
  consistent: boolean;
  checkedEntries: number;
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

async function getWallet(walletId: string): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wallets/${walletId}`);
}

async function getLedger(
  walletId: string,
  queryParameters = "",
): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wallets/${walletId}/ledger${queryParameters}`);
}

async function reconcileWallet(walletId: string): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wallets/${walletId}/reconciliation`, {
    method: "POST",
  });
}

async function submitBet(
  wallet: WalletResponse,
  runId: string,
  amount: string,
): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${runId}`,
    },
    body: JSON.stringify({
      providerId: `e2e-provider-${runId}`,
      externalTransactionId: `e2e-bet-${runId}`,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: `e2e-round-${runId}`,
      gameId: "e2e-game",
      kind: "BET",
      money: {
        amount,
        currency: CURRENCY,
      },
    }),
  });
}

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);
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
         AND money_currency = ?`,
    [walletId, CURRENCY],
  );

  expect(ledgerBalances).toHaveLength(1);
  expect(ledgerBalances[0].balance).toBe(expectedBalance);
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

async function createWalletOrFail(
  playerId: string,
  initialBalance = INITIAL_BALANCE,
): Promise<WalletResponse> {
  const response = await createWallet(playerId, initialBalance);

  expect(response.status).toBe(201);

  return (await response.json()) as WalletResponse;
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

  it("creates a positive-balance wallet with OPENING, a balanced credit entry, and outbox events", async () => {
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

    const walletRows = await query<BalanceRow>(
      `SELECT balance::text AS balance
           FROM wallets
           WHERE id = ?`,
      [wallet.id],
    );

    expect(walletRows).toHaveLength(1);
    expect(walletRows[0].balance).toBe(INITIAL_BALANCE);

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

    const balanceChangedEvents = await countRows(
      `SELECT COUNT(*)::text AS count
           FROM outbox_messages
           WHERE event_type = 'WalletBalanceChanged'
             AND payload->'data'->>'transactionId' = ?`,
      [opening.id],
    );

    expect(balanceChangedEvents).toBe(1);

    const processedEvents = await countRows(
      `SELECT COUNT(*)::text AS count
           FROM outbox_messages
           WHERE event_type = 'WagerTransactionProcessed'
             AND payload->'data'->>'externalTransactionId' = ?`,
      [opening.external_transaction_id],
    );

    expect(processedEvents).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, INITIAL_BALANCE);
  }, 30_000);

  it("creates a zero-balance wallet without an OPENING transaction or ledger entry", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const wallet = await createWalletOrFail(playerId, "0.00");

    expect(wallet.balance).toEqual({
      amount: "0.00",
      currency: CURRENCY,
    });
    expect(wallet.version).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wager_transactions
             WHERE wallet_id = ?
               AND kind = 'OPENING'`,
        [wallet.id],
      ),
    ).toBe(0);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wallet_ledger_entries
             WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).toBe(0);

    await expectWalletBalanceMatchesLedger(wallet.id, "0.00");
  }, 30_000);

  it("rejects duplicate wallet creation for the same player and currency without changing the original", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const original = await createWalletOrFail(playerId);

    const duplicateResponse = await createWallet(playerId);

    expect(duplicateResponse.status).toBe(409);

    const wallets = await query<WalletRow>(
      `SELECT id, balance::text AS balance, version
           FROM wallets
           WHERE player_id = ?
             AND currency = ?`,
      [playerId, CURRENCY],
    );

    expect(wallets).toHaveLength(1);
    expect(wallets[0]).toEqual({
      id: original.id,
      balance: INITIAL_BALANCE,
      version: 1,
    });

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wager_transactions
             WHERE wallet_id = ?
               AND kind = 'OPENING'`,
        [original.id],
      ),
    ).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wallet_ledger_entries
             WHERE wallet_id = ?`,
        [original.id],
      ),
    ).toBe(1);

    await expectWalletBalanceMatchesLedger(original.id, INITIAL_BALANCE);
  }, 30_000);

  it("allows only one wallet to be created when duplicate requests race concurrently", async () => {
    const playerId = `e2e-player-${randomUUID()}`;

    const responses = await Promise.all([
      createWallet(playerId),
      createWallet(playerId),
    ]);

    const statuses = responses
      .map((response) => response.status)
      .sort((left, right) => left - right);

    expect(statuses).toEqual([201, 409]);

    const successfulResponse = responses.find(
      (response) => response.status === 201,
    );

    expect(successfulResponse).toBeDefined();

    const wallet = (await successfulResponse!.json()) as WalletResponse;

    const wallets = await query<WalletRow>(
      `SELECT id, balance::text AS balance, version
           FROM wallets
           WHERE player_id = ?
             AND currency = ?`,
      [playerId, CURRENCY],
    );

    expect(wallets).toHaveLength(1);
    expect(wallets[0].id).toBe(wallet.id);
    expect(wallets[0].balance).toBe(INITIAL_BALANCE);
    expect(wallets[0].version).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wager_transactions
             WHERE wallet_id = ?
               AND kind = 'OPENING'`,
        [wallet.id],
      ),
    ).toBe(1);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wallet_ledger_entries
             WHERE wallet_id = ?`,
        [wallet.id],
      ),
    ).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, INITIAL_BALANCE);
  }, 30_000);

  it("returns an existing wallet and returns 404 for an unknown wallet", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const wallet = await createWalletOrFail(playerId);

    const response = await getWallet(wallet.id);

    expect(response.status).toBe(200);

    const result = (await response.json()) as WalletResponse;

    expect(result).toEqual({
      id: wallet.id,
      playerId,
      balance: {
        amount: INITIAL_BALANCE,
        currency: CURRENCY,
      },
      version: 1,
    });

    const missingResponse = await getWallet(randomUUID());

    expect(missingResponse.status).toBe(404);
  }, 30_000);

  it("paginates ledger entries with a cursor without losing or duplicating entries", async () => {
    const runId = randomUUID();
    const playerId = `e2e-player-${runId}`;
    const wallet = await createWalletOrFail(playerId);

    const firstBetResponse = await submitBet(wallet, `${runId}-first`, "10.00");

    expect(firstBetResponse.status).toBe(200);

    const firstBet = (await firstBetResponse.json()) as WagerResponse;
    expect(firstBet.status).toBe("PROCESSED");

    const secondBetResponse = await submitBet(
      wallet,
      `${runId}-second`,
      "15.00",
    );

    expect(secondBetResponse.status).toBe(200);

    const secondBet = (await secondBetResponse.json()) as WagerResponse;
    expect(secondBet.status).toBe("PROCESSED");
    expect(secondBet.balance).toEqual({
      amount: "75.00",
      currency: CURRENCY,
    });

    const firstPageResponse = await getLedger(wallet.id, "?limit=2");

    expect(firstPageResponse.status).toBe(200);

    const firstPage = (await firstPageResponse.json()) as LedgerPageResponse;

    expect(firstPage.walletId).toBe(wallet.id);
    expect(firstPage.items).toHaveLength(2);
    expect(firstPage.nextCursor).toEqual(expect.any(String));
    expect(firstPage.nextCursor!.length).toBeGreaterThan(0);

    const secondPageResponse = await getLedger(
      wallet.id,
      `?limit=2&cursor=${encodeURIComponent(firstPage.nextCursor!)}`,
    );

    expect(secondPageResponse.status).toBe(200);

    const secondPage = (await secondPageResponse.json()) as LedgerPageResponse;

    expect(secondPage.walletId).toBe(wallet.id);
    expect(secondPage.items).toHaveLength(1);
    expect(secondPage.nextCursor).toBeNull();

    const allReturnedIds = [
      ...firstPage.items.map((entry) => entry.id),
      ...secondPage.items.map((entry) => entry.id),
    ];

    expect(new Set(allReturnedIds).size).toBe(3);

    const persistedEntries = await query<{ id: string }>(
      `SELECT id
           FROM wallet_ledger_entries
           WHERE wallet_id = ?`,
      [wallet.id],
    );

    expect(persistedEntries).toHaveLength(3);

    expect([...allReturnedIds].sort()).toEqual(
      persistedEntries.map((entry) => entry.id).sort(),
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);

  it("rejects invalid ledger cursors and out-of-range page limits", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const wallet = await createWalletOrFail(playerId);

    const invalidCursorResponse = await getLedger(
      wallet.id,
      "?cursor=not-a-valid-cursor&limit=10",
    );

    expect(invalidCursorResponse.status).toBe(400);

    const tooLargeLimitResponse = await getLedger(wallet.id, "?limit=101");

    expect(tooLargeLimitResponse.status).toBe(400);

    const zeroLimitResponse = await getLedger(wallet.id, "?limit=0");

    expect(zeroLimitResponse.status).toBe(400);

    const missingWalletResponse = await getLedger(randomUUID(), "?limit=10");

    expect(missingWalletResponse.status).toBe(404);
  }, 30_000);

  it("reconciles a consistent wallet against its ledger", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const wallet = await createWalletOrFail(playerId);

    const response = await reconcileWallet(wallet.id);

    expect(response.status).toBe(200);

    const result = (await response.json()) as ReconciliationResponse;

    expect(result).toEqual({
      walletId: wallet.id,
      storedBalance: {
        amount: INITIAL_BALANCE,
        currency: CURRENCY,
      },
      calculatedBalance: {
        amount: INITIAL_BALANCE,
        currency: CURRENCY,
      },
      difference: {
        amount: "0.00",
        currency: CURRENCY,
      },
      consistent: true,
      checkedEntries: 1,
    });

    await expectWalletBalanceMatchesLedger(wallet.id, INITIAL_BALANCE);
  }, 30_000);

  it("reports a reconciliation divergence without silently correcting the stored balance", async () => {
    const playerId = `e2e-player-${randomUUID()}`;
    const wallet = await createWalletOrFail(playerId);

    // Corrompe deliberadamente o saldo materializado para simular divergência.
    // A alteração é feita apenas como preparação do cenário E2E.
    await query(
      `UPDATE wallets
            SET balance = ?
          WHERE id = ?`,
      ["99.00", wallet.id],
    );

    const response = await reconcileWallet(wallet.id);

    expect(response.status).toBe(200);

    const result = (await response.json()) as ReconciliationResponse;

    expect(result).toEqual({
      walletId: wallet.id,
      storedBalance: {
        amount: "99.00",
        currency: CURRENCY,
      },
      calculatedBalance: {
        amount: INITIAL_BALANCE,
        currency: CURRENCY,
      },
      difference: {
        amount: "-1.00",
        currency: CURRENCY,
      },
      consistent: false,
      checkedEntries: 1,
    });

    const storedWallets = await query<BalanceRow>(
      `SELECT balance::text AS balance
           FROM wallets
           WHERE id = ?`,
      [wallet.id],
    );

    expect(storedWallets).toHaveLength(1);
    expect(storedWallets[0].balance).toBe("99.00");
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
