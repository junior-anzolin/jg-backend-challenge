import { INestApplication } from "@nestjs/common";
import { Test, TestingModule } from "@nestjs/testing";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { AppModule } from "../src/app.module";

describe("Health E2E", () => {
  let app: INestApplication;
  let serverUrl: string;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.listen(0);
    serverUrl = await app.getUrl();
  });

  afterAll(async () => {
    await app.close();
  });

  it("/health/live (GET)", async () => {
    const res = await fetch(`${serverUrl}/health/live`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { status: string };
    expect(body.status).toBe("UP");
  });

  it("/health/ready (GET)", async () => {
    const res = await fetch(`${serverUrl}/health/ready`);

    expect(res.status).toBe(200);

    const body = (await res.json()) as {
      status: string;
      database: string;
      sqs: string;
    };

    expect(body.status).toBe("UP");
    expect(body.database).toBe("CONNECTED");
    expect(body.sqs).toBe("CONNECTED");
  });

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

      expect(response.ok).toBe(false);
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
  }, 30_000);
});
