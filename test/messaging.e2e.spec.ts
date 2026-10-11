import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { EntityManager } from "@mikro-orm/postgresql";
import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";
const CONSUMER_NAME = "wager-transactions";
const TEST_QUEUE_NAME = `wager-transactions-e2e-${randomUUID()}.fifo`;

type InboxRow = {
  message_id: string;
  payload_hash: string;
  processed_at: Date | string | null;
};

type TransactionRow = {
  id: string;
  status: string;
  resulting_balance_amount: string;
};

type CountRow = {
  count: string;
};

type WalletRow = {
  balance: string;
};

let app: INestApplication | undefined;
let entityManager: EntityManager | undefined;
let sqs: SQSClient | undefined;
let queueUrl: string | undefined;
let serverUrl: string | undefined;

let previousQueueName: string | undefined;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
  if (!entityManager) {
    throw new Error("E2E EntityManager has not been initialized");
  }

  return (await entityManager.getConnection().execute(sql, params)) as T[];
}

async function waitUntil(
  condition: () => Promise<boolean>,
  description: string,
  timeoutMs = 15_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (await condition()) {
      return;
    }

    await sleep(100);
  }

  throw new Error(`Timed out waiting for: ${description}`);
}

describe("SQS messaging E2E", () => {
  beforeAll(async () => {
    // These tests always target the local MiniStack, never real AWS.
    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    previousQueueName = process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME;
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = TEST_QUEUE_NAME;

    sqs = new SQSClient({
      region: "us-east-1",
      endpoint: SQS_ENDPOINT,
      credentials: {
        accessKeyId: "test",
        secretAccessKey: "test",
      },
    });

    const queue = await sqs.send(
      new CreateQueueCommand({
        QueueName: TEST_QUEUE_NAME,
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

        if (previousQueueName === undefined) {
          delete process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME;
        } else {
          process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = previousQueueName;
        }
      }
    }
  });

  it("processes duplicate deliveries once and acknowledges both messages", async () => {
    if (!app || !sqs || !queueUrl || !serverUrl) {
      throw new Error("E2E setup is incomplete");
    }

    const runId = randomUUID();
    const playerId = `e2e-player-${runId}`;
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-loss-${runId}`;
    const messageId = `e2e-message-${runId}`;

    // Create a fresh wallet through the real HTTP endpoint.
    const walletResponse = await fetch(`${serverUrl}/wallets`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        playerId,
        initialBalance: {
          amount: "100.00",
          currency: "BRL",
        },
      }),
    });

    expect(walletResponse.status).toBe(201);

    const wallet = (await walletResponse.json()) as {
      id: string;
      playerId: string;
      balance: {
        amount: string;
        currency: string;
      };
    };

    expect(wallet.balance.amount).toBe("100.00");

    const message = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId,
        externalTransactionId,
        idempotencyKey: `e2e-idempotency-${runId}`,
        playerId,
        walletId: wallet.id,
        roundId: `e2e-round-${runId}`,
        gameId: "e2e-game",
        kind: "LOSS",
        money: {
          amount: "1.00",
          currency: "BRL",
        },
      },
    };

    const body = JSON.stringify(message);

    // Same application message, but separate SQS deduplication IDs:
    // both deliveries should reach the consumer.
    for (const delivery of ["first", "second"]) {
      await sqs.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: body,
          MessageGroupId: wallet.id,
          MessageDeduplicationId: `${delivery}-${runId}`,
        }),
      );
    }

    // Wait until the first committed processing is visible in the Inbox.
    await waitUntil(async () => {
      const rows = await query<InboxRow>(
        `SELECT message_id, payload_hash, processed_at
           FROM inbox_messages
           WHERE consumer_name = ?
             AND message_id = ?`,
        [CONSUMER_NAME, messageId],
      );

      return rows.length === 1 && rows[0].processed_at !== null;
    }, "the Inbox record to be committed");

    // Both deliveries must be gone, not merely invisible during processing.
    let consecutiveEmptyReads = 0;

    await waitUntil(async () => {
      const response = await sqs!.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl!,
          AttributeNames: [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
          ],
        }),
      );

      const attributes = response.Attributes ?? {};
      const visible = Number(attributes.ApproximateNumberOfMessages);
      const inFlight = Number(attributes.ApproximateNumberOfMessagesNotVisible);

      if (visible === 0 && inFlight === 0) {
        consecutiveEmptyReads++;
      } else {
        consecutiveEmptyReads = 0;
      }

      return consecutiveEmptyReads >= 3;
    }, "both SQS deliveries to be acknowledged");

    const inboxRows = await query<InboxRow>(
      `SELECT message_id, payload_hash, processed_at
         FROM inbox_messages
         WHERE consumer_name = ?
           AND message_id = ?`,
      [CONSUMER_NAME, messageId],
    );

    expect(inboxRows).toHaveLength(1);
    expect(inboxRows[0].processed_at).not.toBeNull();

    const transactions = await query<TransactionRow>(
      `SELECT id, status, resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [providerId, externalTransactionId],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0].status).toBe("PROCESSED");
    expect(transactions[0].resulting_balance_amount).toBe("100.00");

    const ledgerRows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [transactions[0].id],
    );

    // LOSS records the operation, but does not move money.
    expect(Number(ledgerRows[0].count)).toBe(0);

    const outboxRows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM outbox_messages
         WHERE event_type = 'WagerTransactionProcessed'
           AND payload->'data'->>'externalTransactionId' = ?`,
      [externalTransactionId],
    );

    expect(Number(outboxRows[0].count)).toBe(1);

    const wallets = await query<WalletRow>(
      `SELECT balance::text AS balance
         FROM wallets
         WHERE id = ?`,
      [wallet.id],
    );

    expect(wallets).toHaveLength(1);
    expect(wallets[0].balance).toBe("100.00");
  }, 30_000);
});
