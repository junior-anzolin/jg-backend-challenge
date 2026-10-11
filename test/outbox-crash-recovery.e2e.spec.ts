import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
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
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "25.00";
const TIMEOUT_MS = 20_000;

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
  failureCode?: string;
  idempotentReplay: boolean;
};

type OutboxRow = {
  id: string;
  event_type: string;
  published_at: string | null;
  claim_token: string | null;
  claim_until: string | null;
  transaction_id: string | null;
};

type PublishedEvent = {
  eventId: string;
  eventType: string;
  aggregateId: string;
  data: {
    transactionId?: string;
    walletId?: string;
    kind?: string;
  };
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

type RunningApplication = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
};

let runningApplication: RunningApplication | undefined;
let sqs: SQSClient | undefined;
let queueUrl: string | undefined;

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "SQS_WAGER_EVENTS_QUEUE_NAME",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

async function startApplication(
  enableOutboxPublisher: boolean,
): Promise<RunningApplication> {
  const builder = Test.createTestingModule({
    imports: [AppModule],
  })
    // The test drives publication recovery explicitly. The actual SQS
    // publisher remains available in the recovery application.
    .overrideProvider(SqsWagerTransactionConsumer)
    .useValue({})
    .overrideProvider(PendingReferenceRetryWorker)
    .useValue({});

  if (!enableOutboxPublisher) {
    builder.overrideProvider(OutboxPublisherWorker).useValue({});
  }

  const moduleFixture = await builder.compile();
  const app = moduleFixture.createNestApplication();

  try {
    await app.listen(0);

    return {
      app,
      entityManager: app.get(EntityManager),
      serverUrl: await app.getUrl(),
    };
  } catch (error) {
    await app.close();
    throw error;
  }
}

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  return (await runningApplication.entityManager
    .getConnection()
    .execute(sql, params)) as T[];
}

async function waitUntil(
  condition: () => Promise<boolean>,
  description: string,
  timeoutMs = TIMEOUT_MS,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await condition()) {
      return;
    }

    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  throw new Error(`Timed out waiting for ${description}`);
}

async function createWallet(runId: string): Promise<WalletResponse> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const response = await fetch(`${runningApplication.serverUrl}/wallets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      playerId: `e2e-player-${runId}`,
      initialBalance: {
        amount: INITIAL_BALANCE,
        currency: "BRL",
      },
    }),
  });

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as WalletResponse;

  expect(wallet.balance).toEqual({
    amount: INITIAL_BALANCE,
    currency: "BRL",
  });

  return wallet;
}

async function submitBet(
  wallet: WalletResponse,
  runId: string,
): Promise<WagerResponse> {
  if (!runningApplication) {
    throw new Error("E2E application has not been initialized");
  }

  const response = await fetch(
    `${runningApplication.serverUrl}/wagering/transactions`,
    {
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
          amount: BET_AMOUNT,
          currency: "BRL",
        },
      }),
    },
  );

  expect(response.status).toBe(200);

  return (await response.json()) as WagerResponse;
}

async function findWalletOutboxEvents(walletId: string): Promise<OutboxRow[]> {
  return query<OutboxRow>(
    `SELECT id,
            event_type,
            published_at::text AS published_at,
            claim_token,
            claim_until::text AS claim_until,
            payload->'data'->>'transactionId' AS transaction_id
       FROM outbox_messages
       WHERE payload->'data'->>'walletId' = ?
       ORDER BY event_type, id`,
    [walletId],
  );
}

async function receiveExpectedEvents(
  expectedEvents: OutboxRow[],
): Promise<PublishedEvent[]> {
  if (!sqs || !queueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  const expectedIds = new Set(expectedEvents.map((event) => event.id));
  const receivedById = new Map<string, PublishedEvent>();

  await waitUntil(async () => {
    const response = await sqs!.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl!,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 5,
      }),
    );

    for (const message of response.Messages ?? []) {
      if (!message.Body || !message.ReceiptHandle) {
        continue;
      }

      const event = JSON.parse(message.Body) as PublishedEvent;

      if (!expectedIds.has(event.eventId)) {
        // Do not delete events that do not belong to this test.
        continue;
      }

      receivedById.set(event.eventId, event);

      await sqs!.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl!,
          ReceiptHandle: message.ReceiptHandle,
        }),
      );
    }

    return receivedById.size === expectedIds.size;
  }, "all persisted Outbox events to arrive in SQS");

  return Array.from(receivedById.values());
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
         AND money_currency = 'BRL'`,
    [walletId],
  );

  expect(ledgerBalances[0].balance).toBe(expectedBalance);
  expect(wallets[0].balance).toBe(ledgerBalances[0].balance);
}

describe("Outbox crash recovery E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    const queueName = `wager-events-crash-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = queueName;

    sqs = new SQSClient({
      region: "us-east-1",
      endpoint: SQS_ENDPOINT,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    });

    try {
      const queue = await sqs.send(
        new CreateQueueCommand({
          QueueName: queueName,
          Attributes: {
            FifoQueue: "true",
            ContentBasedDeduplication: "false",
          },
        }),
      );

      if (!queue.QueueUrl) {
        throw new Error("MiniStack did not return the E2E queue URL");
      }

      queueUrl = queue.QueueUrl;

      // First application persists financial state and Outbox records, but
      // does not run the publisher. This deterministically establishes the
      // committed-but-unpublished state before stopping the application.
      runningApplication = await startApplication(false);
    } catch (error) {
      await runningApplication?.app.close();
      runningApplication = undefined;

      if (sqs && queueUrl) {
        await sqs.send(
          new DeleteQueueCommand({
            QueueUrl: queueUrl,
          }),
        );
      }

      sqs?.destroy();
      sqs = undefined;
      throw error;
    }
  });

  afterAll(async () => {
    try {
      await runningApplication?.app.close();
    } finally {
      runningApplication = undefined;

      try {
        if (sqs && queueUrl) {
          await sqs.send(
            new DeleteQueueCommand({
              QueueUrl: queueUrl,
            }),
          );
        }
      } finally {
        sqs?.destroy();
        sqs = undefined;
        queueUrl = undefined;

        for (const key of managedEnvironmentKeys) {
          const previousValue = previousEnvironment.get(key);

          if (previousValue === undefined) {
            delete process.env[key];
          } else {
            process.env[key] = previousValue;
          }
        }
      }
    }
  });

  it("publishes committed Outbox events after the original application stops", async () => {
    expect(runningApplication).toBeDefined();

    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const betResult = await submitBet(wallet, runId);

    expect(betResult.status).toBe("PROCESSED");
    expect(betResult.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const pendingEventsBeforeRestart = await findWalletOutboxEvents(wallet.id);

    // Wallet creation creates an OPENING transaction, and the BET creates
    // another transaction. Both change the balance and produce two events.
    expect(pendingEventsBeforeRestart).toHaveLength(4);

    const initialEventTypes = pendingEventsBeforeRestart.map(
      (event) => event.event_type,
    );

    expect(
      initialEventTypes.filter(
        (eventType) => eventType === "WalletBalanceChanged",
      ),
    ).toHaveLength(2);

    expect(
      initialEventTypes.filter(
        (eventType) => eventType === "WagerTransactionProcessed",
      ),
    ).toHaveLength(2);

    expect(
      pendingEventsBeforeRestart.every(
        (event) =>
          event.published_at === null &&
          event.claim_token === null &&
          event.claim_until === null,
      ),
    ).toBe(true);

    const expectedEventIds = pendingEventsBeforeRestart
      .map((event) => event.id)
      .sort();

    // Stop the original application after the financial transaction has
    // committed, but before any Outbox event is published.
    const originalApplication = runningApplication;
    runningApplication = undefined;

    await originalApplication!.app.close();

    // Start a new instance with the real Outbox worker and SQS publisher.
    runningApplication = await startApplication(true);

    await waitUntil(async () => {
      const currentEvents = await findWalletOutboxEvents(wallet.id);

      if (currentEvents.length !== pendingEventsBeforeRestart.length) {
        return false;
      }

      const currentIds = currentEvents.map((event) => event.id).sort();

      if (JSON.stringify(currentIds) !== JSON.stringify(expectedEventIds)) {
        return false;
      }

      return currentEvents.every(
        (event) =>
          event.published_at !== null &&
          event.claim_token === null &&
          event.claim_until === null,
      );
    }, "all committed Outbox records to be marked published after restart");

    const receivedEvents = await receiveExpectedEvents(
      pendingEventsBeforeRestart,
    );

    expect(receivedEvents).toHaveLength(pendingEventsBeforeRestart.length);

    expect(new Set(receivedEvents.map((event) => event.eventId)).size).toBe(
      pendingEventsBeforeRestart.length,
    );

    expect(receivedEvents.map((event) => event.eventId).sort()).toEqual(
      expectedEventIds,
    );

    const expectedEventsById = new Map(
      pendingEventsBeforeRestart.map((event) => [event.id, event]),
    );

    for (const receivedEvent of receivedEvents) {
      const expectedEvent = expectedEventsById.get(receivedEvent.eventId);

      expect(expectedEvent).toBeDefined();
      expect(receivedEvent.eventType).toBe(expectedEvent!.event_type);
      expect(receivedEvent.data.walletId).toBe(wallet.id);
    }

    const betOutboxEvents = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE payload->'data'->>'transactionId' = ?
           AND event_type IN (
             'WagerTransactionProcessed',
             'WalletBalanceChanged'
           )`,
      [betResult.transactionId],
    );

    expect(Number(betOutboxEvents[0].count)).toBe(2);

    const debitRows = await query<LedgerRow>(
      `SELECT direction,
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
      direction: "DEBIT",
      amount: BET_AMOUNT,
      balance_before: INITIAL_BALANCE,
      balance_after: "75.00",
    });

    const finalEvents = await findWalletOutboxEvents(wallet.id);
    const finalEventTypes = finalEvents.map((event) => event.event_type);

    expect(finalEvents).toHaveLength(4);

    expect(
      finalEventTypes.filter(
        (eventType) => eventType === "WalletBalanceChanged",
      ),
    ).toHaveLength(2);

    expect(
      finalEventTypes.filter(
        (eventType) => eventType === "WagerTransactionProcessed",
      ),
    ).toHaveLength(2);

    expect(
      finalEvents.every(
        (event) =>
          event.published_at !== null &&
          event.claim_token === null &&
          event.claim_until === null,
      ),
    ).toBe(true);

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);
});
