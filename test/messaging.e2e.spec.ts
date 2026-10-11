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
  processed_at: Date | string | null;
};

type TransactionRow = {
  id: string;
  status: string;
  resulting_balance_amount: string | null;
};

type CountRow = {
  count: string;
};

type WalletRow = {
  balance: string;
};

type LedgerRow = {
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
};

type Wallet = {
  id: string;
  playerId: string;
};

type WagerKind = "BET" | "LOSS";

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

async function createWallet(runId: string): Promise<Wallet> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const playerId = `e2e-player-${runId}`;

  const response = await fetch(`${serverUrl}/wallets`, {
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

  expect(response.status).toBe(201);

  const wallet = (await response.json()) as {
    id: string;
    playerId: string;
    balance: {
      amount: string;
      currency: string;
    };
  };

  expect(wallet.balance).toEqual({
    amount: "100.00",
    currency: "BRL",
  });

  return {
    id: wallet.id,
    playerId: wallet.playerId,
  };
}

function createMessage(input: {
  runId: string;
  wallet: Wallet;
  kind: WagerKind;
  amount: string;
}) {
  return {
    messageId: `e2e-message-${input.runId}`,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId: `e2e-provider-${input.runId}`,
      externalTransactionId: `e2e-${input.kind.toLowerCase()}-${input.runId}`,
      idempotencyKey: `e2e-idempotency-${input.runId}`,
      playerId: input.wallet.playerId,
      walletId: input.wallet.id,
      roundId: `e2e-round-${input.runId}`,
      gameId: "e2e-game",
      kind: input.kind,
      money: {
        amount: input.amount,
        currency: "BRL",
      },
    },
  };
}

async function sendDuplicateDeliveries(
  message: ReturnType<typeof createMessage>,
  walletId: string,
  runId: string,
): Promise<void> {
  if (!sqs || !queueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  const body = JSON.stringify(message);

  // Identical application messages use different SQS deduplication IDs.
  // This ensures both deliveries reach the queue.
  await Promise.all(
    ["first", "second"].map((delivery) =>
      sqs!.send(
        new SendMessageCommand({
          QueueUrl: queueUrl!,
          MessageBody: body,
          MessageGroupId: walletId,
          MessageDeduplicationId: `${delivery}-${runId}`,
        }),
      ),
    ),
  );
}

async function waitForProcessedInbox(messageId: string): Promise<void> {
  await waitUntil(async () => {
    const rows = await query<InboxRow>(
      `SELECT message_id, processed_at
         FROM inbox_messages
         WHERE consumer_name = ?
           AND message_id = ?`,
      [CONSUMER_NAME, messageId],
    );

    return rows.length === 1 && rows[0].processed_at !== null;
  }, "the Inbox record to be committed");
}

async function waitForQueueToDrain(
  description: string,
  timeoutMs = 15_000,
): Promise<void> {
  if (!sqs || !queueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  let consecutiveEmptyReads = 0;

  await waitUntil(
    async () => {
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
    },
    description,
    timeoutMs,
  );
}

async function countOutboxEvents(eventType: string): Promise<number> {
  const rows = await query<CountRow>(
    `SELECT COUNT(*)::text AS count
       FROM outbox_messages
       WHERE event_type = ?`,
    [eventType],
  );

  return Number(rows[0]?.count ?? "0");
}

async function expectWalletBalanceMatchesLedger(
  walletId: string,
  expectedBalance: string,
): Promise<void> {
  const wallets = await query<WalletRow>(
    `SELECT balance::text AS balance
       FROM wallets
       WHERE id = ?`,
    [walletId],
  );

  expect(wallets).toHaveLength(1);
  expect(wallets[0].balance).toBe(expectedBalance);

  const ledgerBalances = await query<WalletRow>(
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

  it("processes duplicate LOSS deliveries once without changing the balance", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const message = createMessage({
      runId,
      wallet,
      kind: "LOSS",
      amount: "1.00",
    });

    const processedEventsBefore = await countOutboxEvents(
      "WagerTransactionProcessed",
    );
    const balanceEventsBefore = await countOutboxEvents("WalletBalanceChanged");

    await sendDuplicateDeliveries(message, wallet.id, runId);
    await waitForProcessedInbox(message.messageId);
    await waitForQueueToDrain("both LOSS deliveries to be acknowledged");

    const inboxRows = await query<InboxRow>(
      `SELECT message_id, processed_at
         FROM inbox_messages
         WHERE consumer_name = ?
           AND message_id = ?`,
      [CONSUMER_NAME, message.messageId],
    );

    expect(inboxRows).toHaveLength(1);
    expect(inboxRows[0].processed_at).not.toBeNull();

    const transactions = await query<TransactionRow>(
      `SELECT id, status, resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [message.data.providerId, message.data.externalTransactionId],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0].status).toBe("PROCESSED");

    const ledgerRows = await query<CountRow>(
      `SELECT COUNT(*)::text AS count
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [transactions[0].id],
    );

    // LOSS records the operation, but does not move money.
    expect(Number(ledgerRows[0].count)).toBe(0);

    expect(await countOutboxEvents("WagerTransactionProcessed")).toBe(
      processedEventsBefore + 1,
    );

    // LOSS must not emit a balance-changed event.
    expect(await countOutboxEvents("WalletBalanceChanged")).toBe(
      balanceEventsBefore,
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "100.00");
  }, 30_000);

  it("processes duplicate BET deliveries with only one debit", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const message = createMessage({
      runId,
      wallet,
      kind: "BET",
      amount: "25.00",
    });

    const processedEventsBefore = await countOutboxEvents(
      "WagerTransactionProcessed",
    );
    const balanceEventsBefore = await countOutboxEvents("WalletBalanceChanged");

    await sendDuplicateDeliveries(message, wallet.id, runId);
    await waitForProcessedInbox(message.messageId);
    await waitForQueueToDrain("both BET deliveries to be acknowledged");

    const inboxRows = await query<InboxRow>(
      `SELECT message_id, processed_at
         FROM inbox_messages
         WHERE consumer_name = ?
           AND message_id = ?`,
      [CONSUMER_NAME, message.messageId],
    );

    expect(inboxRows).toHaveLength(1);
    expect(inboxRows[0].processed_at).not.toBeNull();

    const transactions = await query<TransactionRow>(
      `SELECT id, status, resulting_balance_amount
         FROM wager_transactions
         WHERE provider_id = ?
           AND external_transaction_id = ?`,
      [message.data.providerId, message.data.externalTransactionId],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0].status).toBe("PROCESSED");
    expect(transactions[0].resulting_balance_amount).toBe("75.00");

    const ledgerRows = await query<LedgerRow>(
      `SELECT direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [transactions[0].id],
    );

    expect(ledgerRows).toHaveLength(1);
    expect(ledgerRows[0]).toEqual({
      direction: "DEBIT",
      amount: "25.00",
      balance_before: "100.00",
      balance_after: "75.00",
    });

    expect(await countOutboxEvents("WagerTransactionProcessed")).toBe(
      processedEventsBefore + 1,
    );

    expect(await countOutboxEvents("WalletBalanceChanged")).toBe(
      balanceEventsBefore + 1,
    );

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);
});
