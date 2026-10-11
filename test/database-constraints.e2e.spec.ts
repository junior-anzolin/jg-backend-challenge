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
  version: number;
};

type CountRow = {
  count: string;
};

type NameRow = {
  name: string;
};

type IdRow = {
  id: string;
};

type WalletStateRow = {
  balance: string;
  version: number;
};

const requiredMigrations = [
  "Migration20261009215718",
  "Migration20261009220840",
  "Migration20261009221008",
  "Migration20261009221112",
  "Migration20261011003712",
];

const requiredConstraints = [
  "wallets_pkey",
  "wallets_player_id_currency_unique",
  "wallets_balance_non_negative",
  "wallets_version_positive",
  "wager_transactions_pkey",
  "wager_transactions_resulting_balance_consistent",
  "wager_transactions_reference_attempts_non_negative",
  "wager_transactions_money_non_negative",
  "wager_transactions_provider_external_unique",
  "wager_transactions_idempotency_key_unique",
  "wager_transactions_wallet_id_foreign",
  "wager_transactions_reference_transaction_id_foreign",
  "wallet_ledger_entries_pkey",
  "wallet_ledger_entries_wallet_transaction_unique",
  "wallet_ledger_entries_wallet_id_foreign",
  "wallet_ledger_entries_transaction_id_foreign",
  "wallet_ledger_entries_balanced",
  "wallet_ledger_entries_currency_consistency",
  "wallet_ledger_entries_balances_non_negative",
  "wallet_ledger_entries_amount_non_negative",
  "inbox_messages_pkey",
  "outbox_messages_pkey",
  "outbox_messages_attempts_non_negative",
  "outbox_messages_claim_lease_consistent",
];

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

async function expectQueryRejected(
  sql: string,
  params: unknown[] = [],
): Promise<void> {
  let rejected = false;

  try {
    await query(sql, params);
  } catch {
    rejected = true;
  }

  expect(rejected).toBe(true);
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

async function findOpeningLedgerEntry(walletId: string): Promise<string> {
  const rows = await query<IdRow>(
    `SELECT id
       FROM wallet_ledger_entries
       WHERE wallet_id = ?
       ORDER BY created_at ASC, id ASC
       LIMIT 1`,
    [walletId],
  );

  expect(rows).toHaveLength(1);

  return rows[0].id;
}

describe("Database migrations and constraints E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-db-constraints-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-db-constraints-${randomUUID()}.fifo`;

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

  it("has the required migrations applied and database constraints installed", async () => {
    const migrations = await query<NameRow>(
      `SELECT name
         FROM mikro_orm_migrations
        WHERE name IN (${requiredMigrations.map(() => "?").join(", ")})`,
      requiredMigrations,
    );

    const appliedMigrations = migrations.map((row) => row.name);

    for (const migration of requiredMigrations) {
      expect(appliedMigrations).toContain(migration);
    }

    const constraints = await query<NameRow>(
      `SELECT conname AS name
         FROM pg_constraint
        WHERE conname IN (${requiredConstraints.map(() => "?").join(", ")})`,
      requiredConstraints,
    );

    const installedConstraints = new Set(constraints.map((row) => row.name));

    for (const constraint of requiredConstraints) {
      expect(installedConstraints.has(constraint)).toBe(true);
    }

    const partialIndexes = await query<NameRow>(
      `SELECT indexname AS name
         FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = ?`,
      ["wager_transactions_reference_kind_unique"],
    );

    expect(partialIndexes.map((row) => row.name)).toContain(
      "wager_transactions_reference_kind_unique",
    );
  });

  it("rejects negative wallet balances and non-positive wallet versions at the database level", async () => {
    const wallet = await createWallet(randomUUID());

    await expectQueryRejected(
      `UPDATE wallets
          SET balance = -1.00
        WHERE id = ?`,
      [wallet.id],
    );

    await expectQueryRejected(
      `UPDATE wallets
          SET version = 0
        WHERE id = ?`,
      [wallet.id],
    );

    const state = await query<WalletStateRow>(
      `SELECT balance::text AS balance, version
         FROM wallets
        WHERE id = ?`,
      [wallet.id],
    );

    expect(state).toEqual([
      {
        balance: "100.00",
        version: 1,
      },
    ]);
  });

  it("rejects unbalanced, negative, or currency-inconsistent ledger entries", async () => {
    const wallet = await createWallet(randomUUID());
    const ledgerEntryId = await findOpeningLedgerEntry(wallet.id);

    await expectQueryRejected(
      `UPDATE wallet_ledger_entries
          SET balance_after_amount = balance_after_amount + 1.00
        WHERE id = ?`,
      [ledgerEntryId],
    );

    await expectQueryRejected(
      `UPDATE wallet_ledger_entries
          SET money_amount = -1.00
        WHERE id = ?`,
      [ledgerEntryId],
    );

    await expectQueryRejected(
      `UPDATE wallet_ledger_entries
          SET money_currency = 'USD'
        WHERE id = ?`,
      [ledgerEntryId],
    );

    await expectQueryRejected(
      `UPDATE wallet_ledger_entries
          SET balance_before_amount = -1.00
        WHERE id = ?`,
      [ledgerEntryId],
    );

    const currentEntry = await query<WalletStateRow>(
      `SELECT balance_after_amount::text AS balance, 1 AS version
         FROM wallet_ledger_entries
        WHERE id = ?`,
      [ledgerEntryId],
    );

    expect(currentEntry).toEqual([
      {
        balance: "100.00",
        version: 1,
      },
    ]);
  });

  it("prevents duplicate ledger entries for the same wallet and transaction", async () => {
    const wallet = await createWallet(randomUUID());
    const ledgerEntryId = await findOpeningLedgerEntry(wallet.id);

    await expectQueryRejected(
      `INSERT INTO wallet_ledger_entries (
         id,
         wallet_id,
         transaction_id,
         direction,
         money_amount,
         money_currency,
         balance_before_amount,
         balance_before_currency,
         balance_after_amount,
         balance_after_currency,
         created_at
       )
       SELECT
         ?,
         wallet_id,
         transaction_id,
         direction,
         money_amount,
         money_currency,
         balance_before_amount,
         balance_before_currency,
         balance_after_amount,
         balance_after_currency,
         created_at
       FROM wallet_ledger_entries
       WHERE id = ?`,
      [randomUUID(), ledgerEntryId],
    );

    const count = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
        WHERE wallet_id = ?`,
      [wallet.id],
    );

    expect(Number(count[0].count)).toBe(1);
  });

  it("rejects negative Outbox attempts and inconsistent claim leases", async () => {
    const wallet = await createWallet(randomUUID());

    const outboxEntries = await query<IdRow>(
      `SELECT id
         FROM outbox_messages
        WHERE payload->'data'->>'walletId' = ?
        LIMIT 1`,
      [wallet.id],
    );

    expect(outboxEntries).not.toHaveLength(0);

    const outboxId = outboxEntries[0].id;

    await expectQueryRejected(
      `UPDATE outbox_messages
          SET attempts = -1
        WHERE id = ?`,
      [outboxId],
    );

    await expectQueryRejected(
      `UPDATE outbox_messages
          SET claim_token = ?
        WHERE id = ?`,
      [randomUUID(), outboxId],
    );

    const attempts = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
        WHERE id = ?
          AND attempts >= 0
          AND (claim_token IS NULL) = (claim_until IS NULL)`,
      [outboxId],
    );

    expect(Number(attempts[0].count)).toBe(1);
  });

  it("enforces Inbox message identity by consumer and message ID", async () => {
    const consumerName = `e2e-consumer-${randomUUID()}`;
    const messageId = randomUUID();

    await query(
      `INSERT INTO inbox_messages (
         consumer_name,
         message_id,
         payload_hash,
         received_at,
         processed_at
       )
       VALUES (?, ?, ?, NOW(), NULL)`,
      [consumerName, messageId, "e2e-payload-hash"],
    );

    await expectQueryRejected(
      `INSERT INTO inbox_messages (
         consumer_name,
         message_id,
         payload_hash,
         received_at,
         processed_at
       )
       VALUES (?, ?, ?, NOW(), NULL)`,
      [consumerName, messageId, "e2e-payload-hash"],
    );

    const rows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM inbox_messages
        WHERE consumer_name = ?
          AND message_id = ?`,
      [consumerName, messageId],
    );

    expect(Number(rows[0].count)).toBe(1);
  });
});
