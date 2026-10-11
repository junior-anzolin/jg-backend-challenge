import { INestApplication } from "@nestjs/common";
import { Test, TestingModule } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";

import { AppModule } from "../src/app.module";

const SQS_ENDPOINT = "http://localhost:4566";

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

describe("Health E2E", () => {
  beforeAll(async () => {
    for (const key of managedEnvironmentKeys) {
      previousEnvironment.set(key, process.env[key]);
    }

    process.env.SQS_ENDPOINT = SQS_ENDPOINT;
    process.env.AWS_REGION = "us-east-1";
    process.env.AWS_ACCESS_KEY_ID = "test";
    process.env.AWS_SECRET_ACCESS_KEY = "test";

    try {
      const moduleFixture: TestingModule = await Test.createTestingModule({
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

  it("/health/live (GET)", async () => {
    const response = await fetch(`${serverUrl}/health/live`);

    expect(response.status).toBe(200);

    const body = (await response.json()) as {
      status: string;
    };

    expect(body.status).toBe("UP");
  });

  it("/health/ready (GET)", async () => {
    const response = await fetch(`${serverUrl}/health/ready`);

    expect(response.status).toBe(200);

    const body = (await response.json()) as {
      status: string;
      database: string;
      sqs: string;
    };

    expect(body.status).toBe("UP");
    expect(body.database).toBe("CONNECTED");
    expect(body.sqs).toBe("CONNECTED");
  }, 15_000);

  it("/health/ready (GET) fails when SQS is unreachable", async () => {
    const previousSqsEndpoint = process.env.SQS_ENDPOINT;
    let unavailableApp: INestApplication | undefined;

    try {
      process.env.SQS_ENDPOINT = "http://127.0.0.1:1";

      const moduleFixture = await Test.createTestingModule({
        imports: [AppModule],
      }).compile();

      unavailableApp = moduleFixture.createNestApplication();
      await unavailableApp.listen(0);

      const response = await fetch(
        `${await unavailableApp.getUrl()}/health/ready`,
      );

      expect(response.status).toBe(503);

      const body = (await response.json()) as {
        status: string;
        database: string;
        sqs: string;
      };

      expect(body.status).toBe("DOWN");
      expect(body.database).toBe("CONNECTED");
      expect(body.sqs).toBe("DISCONNECTED");
    } finally {
      try {
        await unavailableApp?.close();
      } finally {
        if (previousSqsEndpoint === undefined) {
          delete process.env.SQS_ENDPOINT;
        } else {
          process.env.SQS_ENDPOINT = previousSqsEndpoint;
        }
      }
    }
  }, 15_000);
});
