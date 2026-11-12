import { expect, test } from "bun:test";
import { DomainError } from "@gateway/kernel";
import { Lifecycle, MemoryBudget, Metrics, encrypt } from "@gateway/runtime";
import { createApp } from "../app.ts";
import { loadConfig } from "../config.ts";
import type { Dependencies } from "../context.ts";

test("malformed resource duration returns 502 instead of internal_error and releases its request", async () => {
  const config = loadConfig({
    NODE_ENV: "test",
    DATABASE_URL: "postgres://unused/unused",
    REDIS_URL: "redis://127.0.0.1:1",
    QUEUE_REDIS_URL: "redis://127.0.0.1:2",
    ADMIN_KEY: "a".repeat(32),
    KEY_PEPPER: "p".repeat(32),
    ENCRYPTION_KEY: "22".repeat(32),
  });
  const project = crypto.randomUUID(),
    provider = crypto.randomUUID();
  const memory = new MemoryBudget(1024 * 1024),
    lifecycle = new Lifecycle(10);
  let usage: Record<string, string> = {};
  let storageFailure = false;
  const deps = {
    config,
    memory,
    lifecycle,
    metrics: new Metrics(),
    store: {
      authenticate: async () => ({
        credential: {
          id: crypto.randomUUID(),
          project_id: project,
          models: [],
          operations: ["videos"],
        },
        project: { id: project, rpm: 100, concurrency: 5 },
      }),
      resource: async () => {
        if (storageFailure) throw new DomainError("numeric_limit", "Synthetic storage error");
        return {
          id: "video_public",
          kind: "video",
          project_id: project,
          provider_id: provider,
          remote_id: "video_remote",
          model: "model",
          state: "ACTIVE",
          attempt_id: null,
          batch_manifest: null,
        };
      },
      provider: async () => ({
        provider_id: provider,
        kind: "openai",
        base_url: "https://fixture.invalid/v1",
        secret: encrypt("synthetic-secret", config.ENCRYPTION_KEY, provider),
        rpm: 100,
        concurrency: 5,
      }),
      updateResource: async () => {},
    },
    limits: { enter: async () => async () => {} },
    transport: {
      fetch: async () => Response.json({ id: "video_remote", status: "completed", usage }),
    },
  } as unknown as Dependencies;
  const app = createApp(deps);
  const request = () =>
    app.fetch(
      new Request("http://gateway.invalid/v1/videos/video_public", {
        headers: { authorization: `Bearer gw_${"a".repeat(43)}` },
      }),
    );
  for (const [field, value, code] of [
    ["audio_seconds", "invalid", "invalid_numeric_literal"],
    ["video_seconds", "1".repeat(201), "numeric_limit"],
  ] as const) {
    usage = { [field]: value };
    const response = await request();
    expect(response.status).toBe(502);
    expect(await response.json()).toMatchObject({ error: { code, type: code } });
    expect(memory.bufferedBytes).toBe(0);
    expect(lifecycle.size).toBe(0);
  }
  usage = { audio_seconds: "0.125" };
  const exact = await request();
  expect(exact.status).toBe(200);
  expect(await exact.json()).toMatchObject({ id: "video_public", usage });
  expect(memory.bufferedBytes).toBe(0);
  expect(lifecycle.size).toBe(0);
  storageFailure = true;
  const internal = await request();
  expect(internal.status).toBe(500);
  expect(await internal.json()).toMatchObject({ error: { code: "internal_error" } });
  expect(memory.bufferedBytes).toBe(0);
  expect(lifecycle.size).toBe(0);
});
