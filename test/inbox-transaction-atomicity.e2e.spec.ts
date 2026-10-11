import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";
import {
  ProcessWagerTransactionMessageUseCase,
  WagerTransactionRequestedMessage,
} from "../src/application/messaging/process-wager-transaction-message.use-case";
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

type WalletSnapshot = {
  balance: string;
  version: number;
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
let failureTriggerName: string | undefined;
let failureFunctionName: string | undefined;

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

async function countRows(sql: string, params: unknown[] = []): Promise<number> {
  const rows = await query<CountRow>(sql, params);

  return Number(rows[0]?.count ?? "0");
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

async function readWalletSnapshot(walletId: string): Promise<WalletSnapshot[]> {
  return query<WalletSnapshot>(
    `SELECT balance::text AS balance, version
       FROM wallets
      WHERE id = ?`,
    [walletId],
  );
}

async function installFailureTrigger(walletId: string): Promise<void> {
  const suffix = randomUUID().replaceAll("-", "");

  failureTriggerName = `e2e_fail_outbox_${suffix}`;
  failureFunctionName = `e2e_fail_outbox_fn_${suffix}`;

  await query(`
    CREATE FUNCTION ${failureFunctionName}()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
      IF NEW.payload->'data'->>'walletId' = TG_ARGV[0] THEN
        RAISE EXCEPTION 'Injected failure to verify inbox transaction atomicity';
      END IF;

      RETURN NEW;
    END;
    $$
  `);

  try {
    await query(`
      CREATE TRIGGER ${failureTriggerName}
      BEFORE INSERT ON outbox_messages
      FOR EACH ROW
      EXECUTE FUNCTION ${failureFunctionName}('${walletId}')
    `);
  } catch (error) {
    await query(`DROP FUNCTION IF EXISTS ${failureFunctionName}()`);
    failureFunctionName = undefined;
    failureTriggerName = undefined;
    throw error;
  }
}

async function removeFailureTrigger(): Promise<void> {
  if (!entityManager) {
    return;
  }

  if (failureTriggerName) {
    await query(
      `DROP TRIGGER IF EXISTS ${failureTriggerName} ON outbox_messages`,
    );
    failureTriggerName = undefined;
  }

  if (failureFunctionName) {
    await query(`DROP FUNCTION IF EXISTS ${failureFunctionName}()`);
    failureFunctionName = undefined;
  }
}

describe("Inbox and financial transaction atomicity E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-inbox-atomicity-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-inbox-atomicity-${randomUUID()}.fifo`;

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
      await removeFailureTrigger();
    } finally {
      try {
        await app?.close();
      } finally {
        app = undefined;
        entityManager = undefined;
        serverUrl = undefined;
        restoreEnvironment();
      }
    }
  });

  it("rolls back the inbox record and all financial changes when outbox persistence fails", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const messageId = `e2e-message-${runId}`;
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-bet-${runId}`;
    const idempotencyKey = `e2e-idempotency-${runId}`;

    const baselineWallet = await readWalletSnapshot(wallet.id);

    const baselineLedgerCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
        WHERE wallet_id = ?`,
      [wallet.id],
    );

    const baselineOutboxCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
        WHERE payload->'data'->>'walletId' = ?`,
      [wallet.id],
    );

    const message: WagerTransactionRequestedMessage = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId,
        externalTransactionId,
        idempotencyKey,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `e2e-round-${runId}`,
        gameId: "e2e-game",
        kind: "BET",
        money: {
          amount: "25.00",
          currency: "BRL",
        },
      },
    };

    await installFailureTrigger(wallet.id);

    try {
      const processMessage = app!.get(ProcessWagerTransactionMessageUseCase);

      await expect(processMessage.execute(message)).rejects.toThrow(
        "Injected failure to verify inbox transaction atomicity",
      );
    } finally {
      await removeFailureTrigger();
    }

    expect(await readWalletSnapshot(wallet.id)).toEqual(baselineWallet);

    const inboxCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM inbox_messages
        WHERE consumer_name = ?
          AND message_id = ?`,
      ["wager-transactions", messageId],
    );

    expect(inboxCount).toBe(0);

    const transactionCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wager_transactions
        WHERE provider_id = ?
          AND external_transaction_id = ?`,
      [providerId, externalTransactionId],
    );

    expect(transactionCount).toBe(0);

    const idempotencyCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wager_transactions
        WHERE idempotency_key = ?`,
      [idempotencyKey],
    );

    expect(idempotencyCount).toBe(0);

    const ledgerCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
        WHERE wallet_id = ?`,
      [wallet.id],
    );

    expect(ledgerCount).toBe(baselineLedgerCount);

    const outboxCount = await countRows(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
        WHERE payload->'data'->>'walletId' = ?`,
      [wallet.id],
    );

    expect(outboxCount).toBe(baselineOutboxCount);
  }, 30_000);
});
