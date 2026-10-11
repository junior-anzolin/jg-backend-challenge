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
import {
  ProcessWagerTransactionMessageUseCase,
  WagerTransactionRequestedMessage,
} from "../src/application/messaging/process-wager-transaction-message.use-case";
import {
  UNIT_OF_WORK,
  UnitOfWorkPort,
} from "../src/application/ports/unit-of-work.port";
import { ProcessWagerTransactionUseCase } from "../src/application/wagering/process-wager-transaction.use-case";
import { OutboxPublisherWorker } from "../src/infrastructure/messaging/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../src/infrastructure/messaging/pending-reference-retry.worker";

const SQS_ENDPOINT = "http://localhost:4566";
const INITIAL_BALANCE = "100.00";
const BET_AMOUNT = "25.00";
const VISIBILITY_TIMEOUT_SECONDS = 2;
const CONSUMER_NAME = "wager-transactions";
const TIMEOUT_MS = 20_000;

type WalletResponse = {
  id: string;
  playerId: string;
  balance: {
    amount: string;
    currency: string;
  };
};

type TransactionRow = {
  id: string;
  external_transaction_id: string;
  status: string;
  resulting_balance_amount: string | null;
  failure_code: string | null;
};

type InboxRow = {
  message_id: string;
  processed_at: Date | string | null;
};

type LedgerRow = {
  direction: string;
  amount: string;
  balance_before: string;
  balance_after: string;
};

type OutboxRow = {
  event_type: string;
  count: string;
};

type CountRow = {
  count: string;
};

type BalanceRow = {
  balance: string;
};

type QueueCounts = {
  visible: number;
  inFlight: number;
};

type RunningApplication = {
  app: INestApplication;
  entityManager: EntityManager;
  serverUrl: string;
};

type ProcessingMode = "crash-after-commit" | "observe-duplicate";

let runningApplication: RunningApplication | undefined;
let sqs: SQSClient | undefined;
let queueUrl: string | undefined;
let injectedCrashAfterCommit = false;
let duplicateDeliveryObserved = false;

const managedEnvironmentKeys = [
  "SQS_ENDPOINT",
  "AWS_REGION",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "SQS_WAGER_TRANSACTIONS_QUEUE_NAME",
  "SQS_WAGER_EVENTS_QUEUE_NAME",
  "SQS_VISIBILITY_TIMEOUT_SECONDS",
] as const;

const previousEnvironment = new Map<
  (typeof managedEnvironmentKeys)[number],
  string | undefined
>();

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

async function startApplication(
  mode: ProcessingMode,
): Promise<RunningApplication> {
  const moduleBuilder = Test.createTestingModule({
    imports: [AppModule],
  })
    .overrideProvider(OutboxPublisherWorker)
    .useValue({})
    .overrideProvider(PendingReferenceRetryWorker)
    .useValue({})
    .overrideProvider(ProcessWagerTransactionMessageUseCase)
    .useFactory({
      inject: [UNIT_OF_WORK, ProcessWagerTransactionUseCase],
      factory: (
        unitOfWork: UnitOfWorkPort,
        processWagerTransaction: ProcessWagerTransactionUseCase,
      ) => {
        const realUseCase = new ProcessWagerTransactionMessageUseCase(
          unitOfWork,
          processWagerTransaction,
        );

        let shouldInjectCrash = mode === "crash-after-commit";

        return {
          execute: async (message: WagerTransactionRequestedMessage) => {
            const result = await realUseCase.execute(message);

            if (shouldInjectCrash) {
              shouldInjectCrash = false;
              injectedCrashAfterCommit = true;

              throw new Error(
                "Injected interruption after database commit and before SQS acknowledgement",
              );
            }

            if (mode === "observe-duplicate" && result.duplicateMessage) {
              duplicateDeliveryObserved = true;
            }

            return result;
          },
        };
      },
    });

  const moduleFixture = await moduleBuilder.compile();
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

async function sendDelivery(
  message: WagerTransactionRequestedMessage,
  groupId: string,
  deduplicationId: string,
): Promise<void> {
  if (!sqs || !queueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(message),
      MessageGroupId: groupId,
      MessageDeduplicationId: deduplicationId,
    }),
  );
}

async function getQueueCounts(): Promise<QueueCounts> {
  if (!sqs || !queueUrl) {
    throw new Error("E2E SQS queue has not been initialized");
  }

  const response = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
      ],
    }),
  );

  return {
    visible: Number(response.Attributes?.ApproximateNumberOfMessages ?? "-1"),
    inFlight: Number(
      response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? "-1",
    ),
  };
}

async function findTransaction(
  providerId: string,
  externalTransactionId: string,
): Promise<TransactionRow[]> {
  return query<TransactionRow>(
    `SELECT id,
            external_transaction_id,
            status,
            resulting_balance_amount::text AS resulting_balance_amount,
            failure_code
       FROM wager_transactions
       WHERE provider_id = ?
         AND external_transaction_id = ?`,
    [providerId, externalTransactionId],
  );
}

async function findInboxMessage(messageId: string): Promise<InboxRow[]> {
  return query<InboxRow>(
    `SELECT message_id, processed_at
       FROM inbox_messages
       WHERE consumer_name = ?
         AND message_id = ?`,
    [CONSUMER_NAME, messageId],
  );
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

describe("SQS crash before acknowledgement E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_VISIBILITY_TIMEOUT_SECONDS = String(
      VISIBILITY_TIMEOUT_SECONDS,
    );

    const queueName = `wager-transactions-crash-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = queueName;

    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-crash-${randomUUID()}.fifo`;

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
            VisibilityTimeout: String(VISIBILITY_TIMEOUT_SECONDS),
          },
        }),
      );

      if (!queue.QueueUrl) {
        throw new Error("MiniStack did not return the E2E queue URL");
      }

      queueUrl = queue.QueueUrl;
    } catch (error) {
      sqs.destroy();
      sqs = undefined;
      restoreEnvironment();
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
        restoreEnvironment();
      }
    }
  });

  it("does not duplicate financial effects after a post-commit pre-ack interruption", async () => {
    injectedCrashAfterCommit = false;
    duplicateDeliveryObserved = false;

    runningApplication = await startApplication("crash-after-commit");

    const runId = randomUUID();
    const wallet = await createWallet(runId);

    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-bet-${runId}`;
    const messageId = `e2e-message-${runId}`;

    const message: WagerTransactionRequestedMessage = {
      messageId,
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId,
        externalTransactionId,
        idempotencyKey: `e2e-idempotency-${runId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: `e2e-round-${runId}`,
        gameId: "e2e-game",
        kind: "BET" as WagerTransactionRequestedMessage["data"]["kind"],
        money: {
          amount: BET_AMOUNT,
          currency: "BRL",
        },
      },
    };

    await sendDelivery(message, `e2e-group-${runId}`, `e2e-dedup-${runId}`);

    await waitUntil(
      async () => injectedCrashAfterCommit,
      "the injected post-commit interruption",
    );

    const committedTransaction = await findTransaction(
      providerId,
      externalTransactionId,
    );

    expect(committedTransaction).toHaveLength(1);
    expect(committedTransaction[0].status).toBe("PROCESSED");
    expect(committedTransaction[0].resulting_balance_amount).toBe("75.00");

    const inboxBeforeRestart = await findInboxMessage(messageId);

    expect(inboxBeforeRestart).toHaveLength(1);
    expect(inboxBeforeRestart[0].processed_at).not.toBeNull();

    const initialLedger = await query<LedgerRow>(
      `SELECT direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [committedTransaction[0].id],
    );

    expect(initialLedger).toHaveLength(1);
    expect(initialLedger[0]).toEqual({
      direction: "DEBIT",
      amount: BET_AMOUNT,
      balance_before: INITIAL_BALANCE,
      balance_after: "75.00",
    });

    const interruptedApplication = runningApplication;
    runningApplication = undefined;

    await interruptedApplication!.app.close();

    const countsWhileWaiting = await getQueueCounts();

    expect(countsWhileWaiting.inFlight).toBe(1);

    runningApplication = await startApplication("observe-duplicate");

    await waitUntil(
      async () => duplicateDeliveryObserved,
      "the restarted consumer to detect the redelivered Inbox message",
    );

    await waitUntil(async () => {
      const counts = await getQueueCounts();

      return counts.visible === 0 && counts.inFlight === 0;
    }, "the redelivered message to be acknowledged");

    const finalTransactions = await findTransaction(
      providerId,
      externalTransactionId,
    );

    expect(finalTransactions).toHaveLength(1);
    expect(finalTransactions[0].id).toBe(committedTransaction[0].id);
    expect(finalTransactions[0].status).toBe("PROCESSED");
    expect(finalTransactions[0].resulting_balance_amount).toBe("75.00");
    expect(finalTransactions[0].failure_code).toBeNull();

    const finalInbox = await findInboxMessage(messageId);

    expect(finalInbox).toHaveLength(1);
    expect(finalInbox[0].processed_at).not.toBeNull();

    const finalLedger = await query<LedgerRow>(
      `SELECT direction,
              money_amount::text AS amount,
              balance_before_amount::text AS balance_before,
              balance_after_amount::text AS balance_after
         FROM wallet_ledger_entries
         WHERE transaction_id = ?`,
      [finalTransactions[0].id],
    );

    expect(finalLedger).toHaveLength(1);
    expect(finalLedger[0]).toEqual({
      direction: "DEBIT",
      amount: BET_AMOUNT,
      balance_before: INITIAL_BALANCE,
      balance_after: "75.00",
    });

    const outboxRows = await query<OutboxRow>(
      `SELECT event_type, COUNT(*)::text AS count
         FROM outbox_messages
         WHERE payload->'data'->>'transactionId' = ?
           AND event_type IN (
             'WagerTransactionProcessed',
             'WalletBalanceChanged'
           )
         GROUP BY event_type`,
      [finalTransactions[0].id],
    );

    expect(outboxRows).toHaveLength(2);

    expect(
      outboxRows.find((row) => row.event_type === "WagerTransactionProcessed")
        ?.count,
    ).toBe("1");

    expect(
      outboxRows.find((row) => row.event_type === "WalletBalanceChanged")
        ?.count,
    ).toBe("1");

    await expectWalletBalanceMatchesLedger(wallet.id, "75.00");
  }, 30_000);
});
