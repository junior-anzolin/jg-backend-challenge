import { INestApplication } from "@nestjs/common";
import { Test } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";

import { AppModule } from "../src/app.module";

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
  balance?: {
    amount: string;
    currency: string;
  };
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
  balance?: {
    amount: string;
    currency: string;
  };
};

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

describe("Wagering transaction query API E2E", () => {
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

  it("returns the same processed transaction by internal ID and provider external ID", async () => {
    const runId = randomUUID();
    const wallet = await createWallet(runId);
    const providerId = `e2e-provider-${runId}`;
    const externalTransactionId = `e2e-bet-${runId}`;
    const roundId = `e2e-round-${runId}`;

    const submission = await fetch(`${serverUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": `e2e-idempotency-${runId}`,
      },
      body: JSON.stringify({
        providerId,
        externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId: "e2e-game",
        kind: "BET",
        money: {
          amount: "25.00",
          currency: "BRL",
        },
      }),
    });

    expect(submission.status).toBe(200);

    const result = (await submission.json()) as WagerResponse;

    expect(result.status).toBe("PROCESSED");
    expect(result.balance).toEqual({
      amount: "75.00",
      currency: "BRL",
    });

    const byInternalIdResponse = await fetch(
      `${serverUrl}/wagering/transactions/${result.transactionId}`,
    );

    expect(byInternalIdResponse.status).toBe(200);

    const byInternalId =
      (await byInternalIdResponse.json()) as TransactionDetails;

    const byProviderResponse = await fetch(
      `${serverUrl}/providers/${providerId}/wagering/transactions/${externalTransactionId}`,
    );

    expect(byProviderResponse.status).toBe(200);

    const byProvider = (await byProviderResponse.json()) as TransactionDetails;

    expect(byInternalId).toEqual(byProvider);
    expect(byInternalId).toMatchObject({
      transactionId: result.transactionId,
      providerId,
      externalTransactionId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId,
      gameId: "e2e-game",
      kind: "BET",
      money: {
        amount: "25.00",
        currency: "BRL",
      },
      status: "PROCESSED",
      balance: {
        amount: "75.00",
        currency: "BRL",
      },
    });
  }, 30_000);

  it("returns 404 for transactions that do not exist by either identifier", async () => {
    const missingTransactionId = randomUUID();
    const missingProviderId = `e2e-provider-${randomUUID()}`;
    const missingExternalTransactionId = `e2e-transaction-${randomUUID()}`;

    const byInternalIdResponse = await fetch(
      `${serverUrl}/wagering/transactions/${missingTransactionId}`,
    );

    const byProviderResponse = await fetch(
      `${serverUrl}/providers/${missingProviderId}/wagering/transactions/${missingExternalTransactionId}`,
    );

    expect(byInternalIdResponse.status).toBe(404);
    expect(byProviderResponse.status).toBe(404);
  });
});
