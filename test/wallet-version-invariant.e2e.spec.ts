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

type WagerResponse = {
  transactionId: string;
  status: string;
  balance?: {
    amount: string;
    currency: string;
  };
  failureCode?: string;
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

async function getWallet(walletId: string): Promise<WalletResponse> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const response = await fetch(`${serverUrl}/wallets/${walletId}`);

  expect(response.status).toBe(200);

  return (await response.json()) as WalletResponse;
}

async function submitTransaction(input: {
  wallet: WalletResponse;
  runId: string;
  kind: "BET" | "LOSS";
  amount: string;
}): Promise<Response> {
  if (!serverUrl) {
    throw new Error("E2E HTTP server has not been initialized");
  }

  const externalTransactionId = `e2e-${input.kind.toLowerCase()}-${randomUUID()}`;

  return fetch(`${serverUrl}/wagering/transactions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `e2e-idempotency-${externalTransactionId}`,
    },
    body: JSON.stringify({
      providerId: `e2e-provider-${input.runId}`,
      externalTransactionId,
      playerId: input.wallet.playerId,
      walletId: input.wallet.id,
      roundId: `e2e-round-${externalTransactionId}`,
      gameId: "e2e-game",
      kind: input.kind,
      money: {
        amount: input.amount,
        currency: "BRL",
      },
    }),
  });
}

describe("Wallet version invariant E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";
    process.env.SQS_WAGER_TRANSACTIONS_QUEUE_NAME = `wager-version-${randomUUID()}.fifo`;
    process.env.SQS_WAGER_EVENTS_QUEUE_NAME = `wager-events-version-${randomUUID()}.fifo`;

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

  it("increments wallet version only when a transaction changes the balance", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);

    expect(wallet.version).toBe(1);

    const lossResponse = await submitTransaction({
      wallet,
      runId,
      kind: "LOSS",
      amount: "10.00",
    });

    expect(lossResponse.status).toBe(200);

    const lossResult = (await lossResponse.json()) as WagerResponse;

    expect(lossResult.status).toBe("PROCESSED");

    const walletAfterLoss = await getWallet(wallet.id);

    expect(walletAfterLoss.balance).toEqual({
      amount: "100.00",
      currency: "BRL",
    });
    expect(walletAfterLoss.version).toBe(1);

    const betResponse = await submitTransaction({
      wallet,
      runId,
      kind: "BET",
      amount: "20.00",
    });

    expect(betResponse.status).toBe(200);

    const betResult = (await betResponse.json()) as WagerResponse;

    expect(betResult.status).toBe("PROCESSED");
    expect(betResult.balance).toEqual({
      amount: "80.00",
      currency: "BRL",
    });

    const walletAfterBet = await getWallet(wallet.id);

    expect(walletAfterBet.balance).toEqual({
      amount: "80.00",
      currency: "BRL",
    });
    expect(walletAfterBet.version).toBe(2);

    const rejectedBetResponse = await submitTransaction({
      wallet,
      runId,
      kind: "BET",
      amount: "81.00",
    });

    expect(rejectedBetResponse.status).toBe(422);

    const rejectedBet = (await rejectedBetResponse.json()) as WagerResponse;

    expect(rejectedBet.status).toBe("REJECTED");
    expect(rejectedBet.failureCode).toBe("INSUFFICIENT_BALANCE");

    const walletAfterRejection = await getWallet(wallet.id);

    expect(walletAfterRejection.balance).toEqual({
      amount: "80.00",
      currency: "BRL",
    });
    expect(walletAfterRejection.version).toBe(2);
  }, 30_000);
});
