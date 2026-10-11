import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AppModule } from '../src/app.module';
import { describe, beforeAll, afterAll, it, expect } from 'bun:test';

describe('Health E2E', () => {
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

  it('/health/live (GET)', async () => {
    const res = await fetch(`${serverUrl}/health/live`);
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string };
    expect(body.status).toBe('UP');
  });

  it('/health/ready (GET)', async () => {
    const res = await fetch(`${serverUrl}/health/ready`);
    expect(res.status).toBe(200);
    const body = await res.json() as { status: string; database: string };
    expect(body.status).toBe('UP');
    expect(body.database).toBe('CONNECTED');
  });
});
