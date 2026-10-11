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
};

type WagerResponse = {
  transactionId: string;
  status: string;
  failureCode?: string;
  idempotentReplay: boolean;
};

type TransactionDetails = {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: {
    amount: string;
    currency: string;
  };
  status: string;
  failureCode?: string;
  referenceExternalTransactionId?: string;
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

async function submitTransaction(input: {
  wallet: WalletResponse;
  providerId: string;
  externalTransactionId: string;
  roundId: string;
  kind: "BET" | "ROLLBACK";
  amount: string;
  referenceExternalTransactionId?: string;
}): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${input.externalTransactionId}`,
    },
    body: JSON.stringify({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.wallet.playerId,
      walletId: input.wallet.id,
      roundId: input.roundId,
      gameId: "e2e-game",
      kind: input.kind,
      money: {
        amount: input.amount,
        currency: "BRL",
      },
      ...(input.referenceExternalTransactionId !== undefined
        ? {
            referenceExternalTransactionId:
              input.referenceExternalTransactionId,
          }
        : {}),
    }),
  });
}

async function expectQueryableByBothIdentifiers(input: {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
}): Promise<TransactionDetails> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const byInternalIdResponse = await fetch(
    `${serverUrl}/wagering/transactions/${input.transactionId}`,
  );

  const byProviderResponse = await fetch(
    `${serverUrl}/providers/${input.providerId}/wagering/transactions/${input.externalTransactionId}`,
  );

  expect(byInternalIdResponse.status).toBe(200);
  expect(byProviderResponse.status).toBe(200);

  const byInternalId =
    (await byInternalIdResponse.json()) as TransactionDetails;

  const byProvider = (await byProviderResponse.json()) as TransactionDetails;

  expect(byInternalId).toEqual(byProvider);

  return byInternalId;
}

describe("Wagering query statuses E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-query-status-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-query-status-${randomUUID()}.fifo`;

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
    } catch (error) {
      await app?.close();
      app = undefined;
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
      serverUrl = undefined;
      restoreEnvironment();
    }
  });

  it("returns rejected transaction details consistently by internal and provider identifiers", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-rejected-${runId}`;

    const response = await submitTransaction({
      wallet,
      providerId,
      externalTransactionId,
      roundId: `e2e-round-${runId}`,
      kind: "BET",
      amount: "101.00",
    });

    expect(response.status).toBe(422);

    const submission = (await response.json()) as WagerResponse;

    expect(submission.status).toBe("REJECTED");
    expect(submission.failureCode).toBe("INSUFFICIENT_BALANCE");

    const details = await expectQueryableByBothIdentifiers({
      transactionId: submission.transactionId,
      providerId,
      externalTransactionId,
    });

    expect(details).toMatchObject({
      transactionId: submission.transactionId,
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      kind: "BET",
      status: "REJECTED",
      failureCode: "INSUFFICIENT_BALANCE",
      money: {
        amount: "101.00",
        currency: "BRL",
      },
    });
  }, 30_000);

  it("returns pending-reference transaction details consistently by both identifiers", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-rollback-${runId}`;
    const referenceExternalTransactionId = `e2e-missing-reference-${runId}`;

    const response = await submitTransaction({
      wallet,
      providerId,
      externalTransactionId,
      roundId: `e2e-round-${runId}`,
      kind: "ROLLBACK",
      amount: "25.00",
      referenceExternalTransactionId,
    });

    expect(response.status).toBe(202);

    const submission = (await response.json()) as WagerResponse;

    expect(submission.status).toBe("PENDING_REFERENCE");
    expect(submission.idempotentReplay).toBe(false);

    const details = await expectQueryableByBothIdentifiers({
      transactionId: submission.transactionId,
      providerId,
      externalTransactionId,
    });

    expect(details).toMatchObject({
      transactionId: submission.transactionId,
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      kind: "ROLLBACK",
      status: "PENDING_REFERENCE",
      referenceExternalTransactionId,
      money: {
        amount: "25.00",
        currency: "BRL",
      },
    });
  }, 30_000);
});
