import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { loadGatewayConfig } from "../config.ts";

const base = {
  hostname: "127.0.0.1",
  port: 0,
  core: {
    origin: "https://127.0.0.1:9443",
    servername: "core.internal",
    audience: "core.internal/core-api",
    peer_sha256: "a".repeat(64),
    subject_altname: "URI:spiffe://synthetic/core-api/a",
    key_file: "/run/gateway/key.pem",
    cert_file: "/run/gateway/cert.pem",
    ca_file: "/run/gateway/ca.pem",
  },
  queue: { url: "redis://127.0.0.1:6380", namespace: "evidence", required_replicas: 1 },
  plugins: {
    helper_executable: "/usr/lib/gateway/plugin-host",
    trust_policy: "/etc/gateway/policy.json",
  },
};

async function load(config: unknown) {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-config-"));
  try {
    const path = resolve(directory, "gateway.json");
    await writeFile(path, JSON.stringify(config));
    return await loadGatewayConfig(path);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

test("a minimal production config loads with bounded defaults", async () => {
  const config = await load({ ...base, production: true });
  expect(config).toMatchObject({
    allow_private_upstream: false,
    private_upstream_cidrs: [],
    trusted_proxy_cidrs: [],
    max_body_bytes: 16 << 20,
    total_timeout_ms: 180000,
  });
});

test("production refuses private upstream bypass and evidence without an acknowledged replica", async () => {
  await expect(load({ ...base, production: true, allow_private_upstream: true })).rejects.toThrow(
    "private upstream",
  );
  await expect(
    load({ ...base, production: true, queue: { ...base.queue, required_replicas: 0 } }),
  ).rejects.toThrow("replica");
});

test("an audience the Core client would refuse at startup is rejected while loading", async () => {
  await expect(load({ ...base, core: { ...base.core, audience: "core-api" } })).rejects.toThrow();
  await expect(
    load({ ...base, core: { ...base.core, audience: "a/b/core-api" } }),
  ).rejects.toThrow();
});

test("database settings, unknown keys and malformed CIDRs are rejected", async () => {
  await expect(load({ ...base, database_url: "postgres://x" })).rejects.toThrow();
  await expect(load({ ...base, trusted_proxy_cidrs: ["10.0.0.0/33"] })).rejects.toThrow();
  await expect(loadGatewayConfig(undefined)).rejects.toThrow("GATEWAY_CONFIG_FILE");
});

test("the shipped production example loads and keeps the production safeguards", async () => {
  const config = await loadGatewayConfig(
    resolve(import.meta.dir, "../../../../infra/gateway.example.json"),
  );
  expect(config).toMatchObject({
    production: true,
    allow_private_upstream: false,
    private_upstream_cidrs: [],
  });
  expect(config.queue.required_replicas).toBeGreaterThanOrEqual(1);
  expect(config.queue.url.startsWith("rediss://")).toBe(true);
  expect(config.core.origin.startsWith("https://")).toBe(true);
});
