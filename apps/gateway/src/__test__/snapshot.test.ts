import { generateKeyPairSync } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "bun:test";
import type { ConfigSnapshot, PluginRecord } from "@gateway/ledger";
import { loadConfigFromEnv } from "../config.ts";
import { SnapshotManager } from "../snapshot.ts";
import { splitSseChunkText, transformRequestTexts } from "../upstream.ts";

function emptySnapshot(
  epoch: number,
  lineageToken: string,
  revoked: string[] = [],
  plugins: PluginRecord[] = [],
): ConfigSnapshot {
  return {
    epoch,
    lineageToken,
    billingAccounts: new Map(),
    keysByVerifier: new Map(),
    accounts: new Map(),
    channels: new Map(),
    models: new Map(),
    prices: new Map(),
    pricingOverridesByBillingAccount: new Map(),
    routesByModel: new Map(),
    plugins: new Map(plugins.map((p) => [p.id, p])),
    revokedPlugins: new Set(revoked),
    providerPluginsByName: new Map(),
  };
}

test("SnapshotManager detects PITR epoch rollback, same-epoch lineage divergence, and revocation regression while preserving revokedPlugins", async () => {
  let remoteStamp = { version: 10, lineageToken: "11111111-1111-4111-8111-111111111111" };
  let remoteSnap = emptySnapshot(10, remoteStamp.lineageToken, ["revoked-plugin-1"]);
  const divergences: number[] = [];
  const passedPrevious: (ConfigSnapshot | undefined)[] = [];

  const catalog = {
    async getEpoch() {
      return remoteStamp.version;
    },
    async getEpochStamp() {
      return remoteStamp;
    },
    async loadSnapshot(previous?: ConfigSnapshot) {
      passedPrevious.push(previous);
      return remoteSnap;
    },
  };
  const nodeRepo = {
    async report() {},
    async stop() {},
    async pruneStaleNodes() {
      return 0;
    },
  };

  const mgr = new SnapshotManager(
    catalog,
    "ab".repeat(32),
    0,
    { id: "node-1", repository: nodeRepo },
    {
      onDivergence(info) {
        divergences.push(info.remoteEpoch);
      },
    },
  );

  await mgr.load();
  expect(mgr.isFresh()).toBe(true);
  expect(mgr.isDiverged()).toBe(false);
  expect(passedPrevious[0]).toBeUndefined();

  remoteStamp = { version: 10, lineageToken: "22222222-2222-4222-8222-222222222222" };
  await mgr.pollEpoch();
  expect(mgr.isDiverged()).toBe(true);
  expect(mgr.isFresh()).toBe(false);
  expect(mgr.get().revokedPlugins.has("revoked-plugin-1")).toBe(true);
  expect(divergences).toEqual([10]);

  const mgr2 = new SnapshotManager(catalog, "ab".repeat(32), 0, {
    id: "node-2",
    repository: nodeRepo,
  });
  remoteStamp = { version: 10, lineageToken: "11111111-1111-4111-8111-111111111111" };
  remoteSnap = emptySnapshot(10, remoteStamp.lineageToken, ["revoked-plugin-1"]);
  await mgr2.load();

  remoteStamp = { version: 12, lineageToken: "33333333-3333-4333-8333-333333333333" };
  remoteSnap = emptySnapshot(12, remoteStamp.lineageToken, []);
  await expect(mgr2.load()).rejects.toThrow("revocation history regressed");
  expect(mgr2.isDiverged()).toBe(true);
  expect(mgr2.isFresh()).toBe(false);
  expect(mgr2.get().revokedPlugins.has("revoked-plugin-1")).toBe(true);
  expect(passedPrevious.at(-1)?.epoch).toBe(10);
});

test("SnapshotManager reports WASM plugin integrity mismatch without blocking unrelated reloads", async () => {
  const tampered: PluginRecord = {
    id: "wasm-tampered",
    name: "tampered",
    version: "1.0.0",
    capability: "CONTENT",
    kind: "WASM",
    digest: "0".repeat(64),
    wasm_bytes: new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]),
    webhook_config: null,
    provider_config: null,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
  };
  const failures: [string, string][] = [];
  const snap = emptySnapshot(1, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", [], [tampered]);
  const mgr = new SnapshotManager(
    {
      async getEpoch() {
        return 1;
      },
      async getEpochStamp() {
        return { version: 1, lineageToken: snap.lineageToken! };
      },
      async loadSnapshot() {
        return snap;
      },
    },
    "ab".repeat(32),
    0,
    {
      id: "node-3",
      repository: {
        async report() {},
        async stop() {},
        async pruneStaleNodes() {
          return 0;
        },
      },
    },
    {
      onIntegrityFailure(id, reason) {
        failures.push([id, reason]);
      },
    },
  );
  await mgr.load();
  expect(failures).toEqual([["wasm-tampered", "digest_mismatch"]]);
});

test("Production config requires valid PLUGIN_SIGNING_PUBLIC_KEY_PEM", () => {
  const dir = mkdtempSync("/tmp/stg-cfg-");
  try {
    const authFile = join(dir, "auth.json");
    writeFileSync(
      authFile,
      JSON.stringify([{ id: "ops", roles: ["ops"], token_sha256: "a".repeat(64) }]),
    );
    const baseEnv = {
      NODE_ENV: "production",
      GATEWAY_MODE: "gateway",
      DATABASE_URL: "postgres://data_user@127.0.0.1:5432/stg",
      INTERNAL_DATABASE_URL: "postgres://control_user@127.0.0.1:5432/stg",
      INTERNAL_AUTH_FILE: authFile,
      KEY_PEPPER: "1234567890abcdef",
      ENCRYPTION_KEY: "ab".repeat(32),
      REDIS_URL: "redis://127.0.0.1:6379",
    };
    expect(() => loadConfigFromEnv(baseEnv, [])).toThrow("PLUGIN_SIGNING_PUBLIC_KEY_PEM");
    expect(() =>
      loadConfigFromEnv({ ...baseEnv, PLUGIN_SIGNING_PUBLIC_KEY_PEM: "not-a-pem" }, []),
    ).toThrow("not a valid public key");
    const { publicKey: rsaKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const rsaPem = rsaKey.export({ type: "spki", format: "pem" }).toString();
    expect(() =>
      loadConfigFromEnv({ ...baseEnv, PLUGIN_SIGNING_PUBLIC_KEY_PEM: rsaPem }, []),
    ).toThrow("Ed25519");
    const { publicKey } = generateKeyPairSync("ed25519");
    const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
    const cfg = loadConfigFromEnv({ ...baseEnv, PLUGIN_SIGNING_PUBLIC_KEY_PEM: pubPem }, []);
    expect(cfg.pluginSigningPublicKeyPem).toBe(pubPem.trim());
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Request and SSE helpers scan Anthropic array system, Gemini systemInstruction, and preserve Gemini thought and terminal metadata", async () => {
  const seen: string[] = [];
  const antRes = await transformRequestTexts(
    {
      system: [{ type: "text", text: "secret system sk-proj-01234567890123456789" }],
      messages: [{ role: "user", content: "hi" }],
    },
    "messages",
    async (text) => {
      seen.push(text);
      return { blocked: false, text: text.replace("sk-proj-01234567890123456789", "[MASKED]") };
    },
  );
  expect(seen).toContain("secret system sk-proj-01234567890123456789");
  expect(JSON.stringify(antRes.body)).toContain("[MASKED]");

  const gemRes = await transformRequestTexts(
    {
      systemInstruction: { parts: [{ text: "gem sys SECRET" }] },
      contents: [{ role: "user", parts: [{ text: "hello" }] }],
    },
    "gemini",
    async (text) => ({ blocked: false, text: text.replace("SECRET", "[REDACTED]") }),
  );
  expect(JSON.stringify(gemRes.body)).toContain("[REDACTED]");

  const enc = new TextEncoder();
  const gemThoughtSse = enc.encode(
    `data: ${JSON.stringify({ candidates: [{ index: 0, content: { role: "model", parts: [{ text: "internal", thought: true }] } }] })}\n\n`,
  );
  expect(splitSseChunkText(gemThoughtSse, "gemini")).toEqual({
    text: null,
    remainder: null,
    hasOtherText: false,
  });

  const gemTerminalSse = enc.encode(
    `data: ${JSON.stringify({ candidates: [{ index: 0, content: { role: "model", parts: [{ text: "thought", thought: true }, { text: "Call 13812345678" }] }, finishReason: "STOP" }], usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 2, totalTokenCount: 3 } })}\n\n`,
  );
  const splitGemTerminal = splitSseChunkText(gemTerminalSse, "gemini");
  expect(splitGemTerminal.text).toBe("Call 13812345678");
  expect(splitGemTerminal.remainderHasText).toBe(false);
  expect(splitGemTerminal.remainder).not.toBeNull();
  const strippedGemText = new TextDecoder().decode(splitGemTerminal.remainder!);
  expect(strippedGemText).not.toContain("13812345678");
  expect(strippedGemText).toContain('"thought":true');
  expect(strippedGemText).toContain('"finishReason":"STOP"');

  const antStart = enc.encode(
    'event: content_block_start\ndata: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}\n\n',
  );
  const antStop = enc.encode(
    'event: content_block_stop\ndata: {"type":"content_block_stop","index":1}\n\n',
  );
  expect(splitSseChunkText(antStart, "messages").anthropicBlock).toEqual({
    kind: "text_start",
    index: 1,
  });
  expect(splitSseChunkText(antStop, "messages").anthropicBlock).toEqual({
    kind: "block_stop",
    index: 1,
  });
});
