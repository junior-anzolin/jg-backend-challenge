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
  version: number;
};

type MoneyResponse = {
  amount: string;
  currency: string;
};

type WagerResponse = {
  transactionId: string;
  status: string;
  balance?: MoneyResponse;
  failureCode?: string;
  idempotentReplay: boolean;
};

type WagerPayload = {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: MoneyResponse;
  referenceExternalTransactionId?: string;
};

type TransactionRow = {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  kind: string;
  status: string;
  failure_code: string | null;
  money_amount: string;
  money_currency: string;
  resulting_balance_amount: string | null;
  resulting_balance_currency: string | null;
  reference_transaction_id: string | null;
};

type LedgerRow = {
  direction: string;
  amount: string;
  currency: string;
  balance_before: string;
  balance_before_currency: string;
  balance_after: string;
  balance_after_currency: string;
};

type BalanceRow = {
  balance: string;
};

type CountRow = {
  count: string;
};

type ExpectedLedgerEntry = {
  direction: "DEBIT" | "CREDIT";
  amount: string;
  currency: string;
  balanceBefore: string;
  balanceAfter: string;
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
  currency = CURRENCY,
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
      playerId,
      initialBalance: {
        amount: initialBalance,
        currency,
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.playerId).toBe(playerId);
  expect(wallet.balance).toEqual({
    amount: initialBalance,
    currency,
  });

  return wallet;
}

function createPayload(
  wallet: WalletResponse,
  runId: string,
  suffix: string,
  kind: WagerKind,
  amount: string,
  roundId: string,
  options: {
    currency?: string;
    providerId?: string;
    referenceExternalTransactionId?: string;
  } = {},
): WagerPayload {
  return {
    providerId: options.providerId ?? `e2e-provider-${runId}`,
    externalTransactionId: `e2e-${suffix}-${runId}`,
    playerId: wallet.playerId,
    walletId: wallet.id,
    roundId,
    gameId: "e2e-game",
    kind,
    money: {
      amount,
      currency: options.currency ?? CURRENCY,
    },
    ...(options.referenceExternalTransactionId
      ? {
          referenceExternalTransactionId:
            options.referenceExternalTransactionId,
        }
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

async function findTransaction(
  providerId: string,
  externalTransactionId: string,
): Promise<TransactionRow[]> {
  return query<TransactionRow>(
    `SELECT id,
            provider_id,
            external_transaction_id,
            kind,
            status,
            failure_code,
            money_amount::text AS money_amount,
            money_currency,
            resulting_balance_amount::text AS resulting_balance_amount,
            resulting_balance_currency,
            reference_transaction_id
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id = ?`,
    [providerId, externalTransactionId],
  );
}

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);
  return Number(rows[0]?.count ?? "0");
}

async function expectWalletBalanceMatchesLedger(
  walletId: string,
  expectedBalance: string,
  currency = CURRENCY,
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
         AND money_currency = ?`,
    [walletId, currency],
  );

  expect(ledgerRows).toHaveLength(1);
  expect(ledgerRows[0].balance).toBe(expectedBalance);
  expect(walletRows[0].balance).toBe(ledgerRows[0].balance);
}

async function expectProcessedTransaction(
  payload: WagerPayload,
  response: Response,
  expectedBalance: string,
  expectedLedgerEntry: ExpectedLedgerEntry | null,
): Promise<WagerResponse> {
  expect(response.status).toBe(200);

  const result = (await response.json()) as WagerResponse;

  expect(result.status).toBe("PROCESSED");
  expect(result.idempotentReplay).toBe(false);
  expect(result.balance).toEqual({
    amount: expectedBalance,
    currency: payload.money.currency,
  });

  const transactions = await findTransaction(
    payload.providerId,
    payload.externalTransactionId,
  );

  expect(transactions).toHaveLength(1);

  const transaction = transactions[0];

  expect(transaction.id).toBe(result.transactionId);
  expect(transaction.kind).toBe(payload.kind);
  expect(transaction.status).toBe("PROCESSED");
  expect(transaction.failure_code).toBeNull();
  expect(transaction.resulting_balance_amount).toBe(expectedBalance);
  expect(transaction.resulting_balance_currency).toBe(payload.money.currency);

  if (payload.referenceExternalTransactionId) {
    const referenceTransactions = await findTransaction(
      payload.providerId,
      payload.referenceExternalTransactionId,
    );

    expect(referenceTransactions).toHaveLength(1);
    expect(transaction.reference_transaction_id).toBe(
      referenceTransactions[0].id,
    );
  }

  const ledgerEntries = await query<LedgerRow>(
    `SELECT direction,
            money_amount::text AS amount,
            money_currency AS currency,
            balance_before_amount::text AS balance_before,
            balance_before_currency,
            balance_after_amount::text AS balance_after,
            balance_after_currency
       FROM wallet_ledger_entries
       WHERE transaction_id = ?`,
    [transaction.id],
  );

  if (expectedLedgerEntry) {
    expect(ledgerEntries).toHaveLength(1);
    expect(ledgerEntries[0]).toEqual({
      direction: expectedLedgerEntry.direction,
      amount: expectedLedgerEntry.amount,
      currency: expectedLedgerEntry.currency,
      balance_before: expectedLedgerEntry.balanceBefore,
      balance_before_currency: expectedLedgerEntry.currency,
      balance_after: expectedLedgerEntry.balanceAfter,
      balance_after_currency: expectedLedgerEntry.currency,
    });
  } else {
    expect(ledgerEntries).toHaveLength(0);
  }

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
      [transaction.id],
    ),
  ).toBe(expectedLedgerEntry ? 1 : 0);

  await expectWalletBalanceMatchesLedger(
    payload.walletId,
    expectedBalance,
    payload.money.currency,
  );

  return result;
}

async function expectRejectedTransaction(
  payload: WagerPayload,
  response: Response,
  expectedFailureCode: string,
  expectedWalletBalance: string,
  walletCurrency = CURRENCY,
): Promise<WagerResponse> {
  expect(response.status).toBe(422);

  const result = (await response.json()) as WagerResponse;

  expect(result.status).toBe("REJECTED");
  expect(result.failureCode).toBe(expectedFailureCode);
  expect(result.idempotentReplay).toBe(false);

  const transactions = await findTransaction(
    payload.providerId,
    payload.externalTransactionId,
  );

  expect(transactions).toHaveLength(1);

  const transaction = transactions[0];

  expect(transaction.id).toBe(result.transactionId);
  expect(transaction.kind).toBe(payload.kind);
  expect(transaction.status).toBe("REJECTED");
  expect(transaction.failure_code).toBe(expectedFailureCode);

  expect(
    await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [transaction.id],
    ),
  ).toBe(0);

  expect(
    await countRows(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WagerTransactionRejected'
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
      [transaction.id],
    ),
  ).toBe(0);

  await expectWalletBalanceMatchesLedger(
    payload.walletId,
    expectedWalletBalance,
    walletCurrency,
  );

  return result;
}

describe("Wagering financial rules E2E", () => {
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

  it("credits the wallet and creates a CREDIT ledger entry for WIN", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);

    const win = createPayload(
      wallet,
      runId,
      "win",
      "WIN",
      "30.00",
      `e2e-round-${runId}`,
    );

    const response = await submitTransaction(win);

    await expectProcessedTransaction(win, response, "130.00", {
      direction: "CREDIT",
      amount: "30.00",
      currency: CURRENCY,
      balanceBefore: "100.00",
      balanceAfter: "130.00",
    });
  }, 30_000);

  it("refunds a processed BET exactly once with a CREDIT ledger entry", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);
    const roundId = `e2e-round-${runId}`;

    const bet = createPayload(wallet, runId, "bet", "BET", "25.00", roundId);

    const betResponse = await submitTransaction(bet);

    const betResult = await expectProcessedTransaction(
      bet,
      betResponse,
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    expect(betResult.transactionId).toBeTruthy();

    const refund = createPayload(
      wallet,
      runId,
      "refund",
      "REFUND",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    const refundResponse = await submitTransaction(refund);

    const refundResult = await expectProcessedTransaction(
      refund,
      refundResponse,
      "100.00",
      {
        direction: "CREDIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "75.00",
        balanceAfter: "100.00",
      },
    );

    const refundRows = await findTransaction(
      refund.providerId,
      refund.externalTransactionId,
    );

    expect(refundRows[0].reference_transaction_id).toBe(
      betResult.transactionId,
    );
    expect(refundResult.transactionId).toBe(refundRows[0].id);

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wallet_ledger_entries
             WHERE transaction_id = ?`,
        [refundResult.transactionId],
      ),
    ).toBe(1);
  }, 30_000);

  it("rejects a second REFUND of the same BET without a second credit", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);
    const roundId = `e2e-round-${runId}`;

    const bet = createPayload(wallet, runId, "bet", "BET", "25.00", roundId);

    await expectProcessedTransaction(
      bet,
      await submitTransaction(bet),
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    const firstRefund = createPayload(
      wallet,
      runId,
      "refund-first",
      "REFUND",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectProcessedTransaction(
      firstRefund,
      await submitTransaction(firstRefund),
      "100.00",
      {
        direction: "CREDIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "75.00",
        balanceAfter: "100.00",
      },
    );

    const secondRefund = createPayload(
      wallet,
      runId,
      "refund-second",
      "REFUND",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      secondRefund,
      await submitTransaction(secondRefund),
      "REFERENCE_ALREADY_REVERSED",
      "100.00",
    );

    expect(
      await countRows(
        `SELECT COUNT(*)::text AS count
             FROM wallet_ledger_entries
             WHERE wallet_id = ?
               AND direction = 'CREDIT'
               AND transaction_id IN (
                 SELECT id
                   FROM wager_transactions
                   WHERE provider_id = ?
                     AND external_transaction_id = ?
               )`,
        [wallet.id, firstRefund.providerId, firstRefund.externalTransactionId],
      ),
    ).toBe(1);

    await expectWalletBalanceMatchesLedger(wallet.id, "100.00");
  }, 30_000);

  it("rolls back a WIN with a DEBIT entry and rejects a duplicate ROLLBACK", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);
    const roundId = `e2e-round-${runId}`;

    const win = createPayload(wallet, runId, "win", "WIN", "25.00", roundId);

    await expectProcessedTransaction(
      win,
      await submitTransaction(win),
      "125.00",
      {
        direction: "CREDIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "125.00",
      },
    );

    const rollback = createPayload(
      wallet,
      runId,
      "rollback-win-first",
      "ROLLBACK",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: win.externalTransactionId,
      },
    );

    await expectProcessedTransaction(
      rollback,
      await submitTransaction(rollback),
      "100.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "125.00",
        balanceAfter: "100.00",
      },
    );

    const secondRollback = createPayload(
      wallet,
      runId,
      "rollback-win-second",
      "ROLLBACK",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: win.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      secondRollback,
      await submitTransaction(secondRollback),
      "REFERENCE_ALREADY_REVERSED",
      "100.00",
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "100.00");
  }, 30_000);

  it("rolls back a REFUND by debiting the refunded amount", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);
    const roundId = `e2e-round-${runId}`;

    const bet = createPayload(wallet, runId, "bet", "BET", "25.00", roundId);

    await expectProcessedTransaction(
      bet,
      await submitTransaction(bet),
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    const refund = createPayload(
      wallet,
      runId,
      "refund",
      "REFUND",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectProcessedTransaction(
      refund,
      await submitTransaction(refund),
      "100.00",
      {
        direction: "CREDIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "75.00",
        balanceAfter: "100.00",
      },
    );

    const rollbackRefund = createPayload(
      wallet,
      runId,
      "rollback-refund",
      "ROLLBACK",
      "25.00",
      roundId,
      {
        referenceExternalTransactionId: refund.externalTransactionId,
      },
    );

    await expectProcessedTransaction(
      rollbackRefund,
      await submitTransaction(rollbackRefund),
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);

  it("rejects REFUND of WIN and ROLLBACK of LOSS without changing the balance", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);

    const winRoundId = `e2e-win-round-${runId}`;
    const win = createPayload(wallet, runId, "win", "WIN", "20.00", winRoundId);

    await expectProcessedTransaction(
      win,
      await submitTransaction(win),
      "120.00",
      {
        direction: "CREDIT",
        amount: "20.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "120.00",
      },
    );

    const refundWin = createPayload(
      wallet,
      runId,
      "refund-win",
      "REFUND",
      "20.00",
      winRoundId,
      {
        referenceExternalTransactionId: win.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      refundWin,
      await submitTransaction(refundWin),
      "INVALID_REFERENCE",
      "120.00",
    );

    const lossRoundId = `e2e-loss-round-${runId}`;
    const loss = createPayload(
      wallet,
      runId,
      "loss",
      "LOSS",
      "10.00",
      lossRoundId,
    );

    await expectProcessedTransaction(
      loss,
      await submitTransaction(loss),
      "120.00",
      null,
    );

    const rollbackLoss = createPayload(
      wallet,
      runId,
      "rollback-loss",
      "ROLLBACK",
      "10.00",
      lossRoundId,
      {
        referenceExternalTransactionId: loss.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      rollbackLoss,
      await submitTransaction(rollbackLoss),
      "INVALID_REFERENCE",
      "120.00",
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "120.00");
  }, 30_000);

  it("rejects references belonging to a different wallet, player, or round", async () => {
    const runId = randomUUID();
    const playerId = `e2e-player-${runId}`;
    const otherPlayerId = `e2e-other-player-${runId}`;
    const providerId = `e2e-provider-${runId}`;
    const roundId = `e2e-round-${runId}`;

    const brlWallet = await createWallet(playerId, INITIAL_BALANCE, "BRL");

    const usdWallet = await createWallet(playerId, INITIAL_BALANCE, "USD");
    const otherPlayerWallet = await createWallet(
      otherPlayerId,
      INITIAL_BALANCE,
      "BRL",
    );

    const bet = createPayload(
      brlWallet,
      runId,
      "reference-bet",
      "BET",
      "25.00",
      roundId,
      { providerId },
    );

    await expectProcessedTransaction(
      bet,
      await submitTransaction(bet),
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    const wrongWalletRefund = createPayload(
      usdWallet,
      runId,
      "refund-wrong-wallet",
      "REFUND",
      "25.00",
      roundId,
      {
        providerId,
        currency: "USD",
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      wrongWalletRefund,
      await submitTransaction(wrongWalletRefund),
      "INVALID_REFERENCE",
      "100.00",
      "USD",
    );

    const wrongPlayerRefund = createPayload(
      otherPlayerWallet,
      runId,
      "refund-wrong-player",
      "REFUND",
      "25.00",
      roundId,
      {
        providerId,
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      wrongPlayerRefund,
      await submitTransaction(wrongPlayerRefund),
      "INVALID_REFERENCE",
      "100.00",
    );

    const wrongRoundRefund = createPayload(
      brlWallet,
      runId,
      "refund-wrong-round",
      "REFUND",
      "25.00",
      `e2e-different-round-${runId}`,
      {
        providerId,
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      wrongRoundRefund,
      await submitTransaction(wrongRoundRefund),
      "INVALID_REFERENCE",
      "75.00",
    );

    await expectWalletBalanceMatchesLedger(brlWallet.id, "75.00", "BRL");
    await expectWalletBalanceMatchesLedger(usdWallet.id, "100.00", "USD");
    await expectWalletBalanceMatchesLedger(
      otherPlayerWallet.id,
      "100.00",
      "BRL",
    );
  }, 30_000);

  it("rejects a REFUND whose amount differs from the referenced BET", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`);
    const roundId = `e2e-round-${runId}`;

    const bet = createPayload(wallet, runId, "bet", "BET", "25.00", roundId);

    await expectProcessedTransaction(
      bet,
      await submitTransaction(bet),
      "75.00",
      {
        direction: "DEBIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "100.00",
        balanceAfter: "75.00",
      },
    );

    const refund = createPayload(
      wallet,
      runId,
      "refund-different-amount",
      "REFUND",
      "20.00",
      roundId,
      {
        referenceExternalTransactionId: bet.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      refund,
      await submitTransaction(refund),
      "REFERENCE_AMOUNT_MISMATCH",
      "75.00",
    );
  }, 30_000);

  it("rejects a transaction whose currency differs from the wallet currency", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(
      `e2e-player-${runId}`,
      INITIAL_BALANCE,
      "USD",
    );

    const win = createPayload(
      wallet,
      runId,
      "win-currency-mismatch",
      "WIN",
      "10.00",
      `e2e-round-${runId}`,
      { currency: "BRL" },
    );

    await expectRejectedTransaction(
      win,
      await submitTransaction(win),
      "CURRENCY_MISMATCH",
      INITIAL_BALANCE,
      "USD",
    );
  }, 30_000);

  it("rejects a ROLLBACK that would make the wallet balance negative", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(`e2e-player-${runId}`, "0.00", CURRENCY);

    const winRoundId = `e2e-win-round-${runId}`;

    const win = createPayload(wallet, runId, "win", "WIN", "25.00", winRoundId);

    await expectProcessedTransaction(
      win,
      await submitTransaction(win),
      "25.00",
      {
        direction: "CREDIT",
        amount: "25.00",
        currency: CURRENCY,
        balanceBefore: "0.00",
        balanceAfter: "25.00",
      },
    );

    const bet = createPayload(
      wallet,
      runId,
      "spending-bet",
      "BET",
      "20.00",
      `e2e-spending-round-${runId}`,
    );

    await expectProcessedTransaction(
      bet,
      await submitTransaction(bet),
      "5.00",
      {
        direction: "DEBIT",
        amount: "20.00",
        currency: CURRENCY,
        balanceBefore: "25.00",
        balanceAfter: "5.00",
      },
    );

    const rollbackWin = createPayload(
      wallet,
      runId,
      "rollback-win-overdraw",
      "ROLLBACK",
      "25.00",
      winRoundId,
      {
        referenceExternalTransactionId: win.externalTransactionId,
      },
    );

    await expectRejectedTransaction(
      rollbackWin,
      await submitTransaction(rollbackWin),
      "REVERSAL_WOULD_OVERDRAW",
      "5.00",
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "5.00");
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
