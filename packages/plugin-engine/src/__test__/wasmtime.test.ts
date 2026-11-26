import { expect, test } from "bun:test";
import { generateKeyPairSync, sign } from "node:crypto";
import type { PluginRecord } from "@gateway/ledger";
import {
  createWasmtimeExecutor,
  createFfiWasmtimeExecutor,
  PluginPipelineEngine,
  resolveWasmtimeLibraryPath,
} from "../index.ts";
import {
  buildInfiniteLoopWasm,
  buildCrossTenantProbeWasm,
  buildSubstringWasm,
  buildMemoryBombWasm,
  buildWasiImportWasm,
} from "./wasm-fixtures.ts";

test("WASM invocations expire while waiting behind a busy worker and reject missing native library", async () => {
  expect(() =>
    createWasmtimeExecutor({ wasmtimeLibraryPath: "/nonexistent/libstg_plugin_wasmtime.so" }),
  ).toThrow("Native Wasmtime library not found");

  const maxMemoryBytes = 4 * 1024 * 1024;
  const executor = createWasmtimeExecutor({
    maxMemoryBytes,
    poolSize: 1,
    maxPending: 1,
    minTimeoutMs: 25,
    maxTimeoutMs: 150,
  });
  executor.compile("loop", "loop", buildInfiniteLoopWasm("on_response"));
  executor.compile("fast", "fast", buildCrossTenantProbeWasm());
  const input = new Uint8Array();
  try {
    await executor.selfCheck?.();
    await executor
      .invoke("loop", "on_response", input, { maxMemoryBytes, fuelBudget: 10_000 })
      .catch(() => undefined);
    const busy = executor
      .invoke("loop", "on_response", input, { maxMemoryBytes, fuelBudget: 250_000_000 })
      .catch((error: unknown) => error);
    const queued = executor.invoke("fast", "on_response", input, {
      maxMemoryBytes,
      fuelBudget: 250_000,
    });
    expect(executor.acceptsEgress?.()).toBe(false);
    const rejected = executor.invoke("fast", "on_response", input, {
      maxMemoryBytes,
      fuelBudget: 250_000,
    });
    await Promise.all([
      expect(rejected).rejects.toMatchObject({ code: "wasm_queue_full" }),
      expect(queued).rejects.toThrow("WASM queue timed out"),
    ]);
    expect(await busy).toBeInstanceOf(Error);
    const fastOut = await executor.invoke("fast", "on_response", input, {
      maxMemoryBytes,
      fuelBudget: 1_500_000,
    });
    expect(fastOut).toBeInstanceOf(Uint8Array);
    expect(executor.acceptsEgress?.()).toBe(true);
  } finally {
    executor.close?.();
  }
});

test("G3.1 Native Wasmtime Worker pool executes signed WASM, isolates cross-tenant memory, enforces fuel/memory/WASI limits, and preserves compiled modules after traps", async () => {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const signBytes = (bytes: Uint8Array) => {
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");
    return { digest, signature };
  };

  const executor = createWasmtimeExecutor({
    maxMemoryBytes: 4 * 1024 * 1024,
    poolSize: 1,
    minTimeoutMs: 40,
    maxTimeoutMs: 500,
  });
  const engine = new PluginPipelineEngine({
    wasmExecutor: executor,
    signingPublicKeyPem: pubPem,
  });

  try {
    const filterBytes = buildSubstringWasm({
      hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
      needle: "SECRET_42",
      matchReply: { action: "TRANSFORM", text: "[CLEANSED_BY_WASM]" },
    });
    const filterSig = signBytes(filterBytes);
    const filterPlugin: PluginRecord = {
      id: "wasm-real-filter",
      name: "real-filter",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: filterSig.digest,
      wasm_bytes: filterBytes,
      webhook_config: null,
      signature: filterSig.signature,
      signing_key_id: "k1",
      manifest: {
        hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
        window_bytes: 12,
      },
      enabled: true,
    };

    const okRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "clean text",
    );
    expect(okRes).toMatchObject({ blocked: false, text: "clean text" });

    const transformRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "has SECRET_42 inside",
    );
    expect(transformRes).toMatchObject({ blocked: false, text: "[CLEANSED_BY_WASM]" });

    const streamSession = engine.createStreamSession(
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-3", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      () => undefined,
    );
    let streamOut = "";
    streamOut += (await streamSession.push("part SEC")).emit;
    streamOut += (await streamSession.push("RET_42 end")).emit;
    const streamEnd = await streamSession.finalize();
    expect(streamEnd.blocked).toBe(false);
    streamOut += streamEnd.emit;
    expect(streamOut).toContain("[CLEANSED_BY_WASM]");
    expect(streamOut).not.toContain("SECRET_42");

    const probeBytes = buildCrossTenantProbeWasm();
    const probeSig = signBytes(probeBytes);
    const probePlugin: PluginRecord = {
      id: "wasm-tenant-probe",
      name: "tenant-probe",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: probeSig.digest,
      wasm_bytes: probeBytes,
      webhook_config: null,
      signature: probeSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"] },
      enabled: true,
    };
    const probeMap = new Map([[probePlugin.id, probePlugin]]);
    const probe1 = await engine.runUnary(
      "on_response",
      [{ plugin_id: probePlugin.id, priority: 1 }],
      probeMap,
      { request_id: "p1", billing_account_id: "tenant-a", model: "gpt-4o", operation: "chat" },
      "tenant a",
    );
    const probe2 = await engine.runUnary(
      "on_response",
      [{ plugin_id: probePlugin.id, priority: 1 }],
      probeMap,
      { request_id: "p2", billing_account_id: "tenant-b", model: "gpt-4o", operation: "chat" },
      "tenant b",
    );
    expect(probe1.blocked).toBe(false);
    expect(probe2.blocked).toBe(false);

    const loopBytes = buildInfiniteLoopWasm("on_response");
    const loopSig = signBytes(loopBytes);
    const loopPlugin: PluginRecord = {
      id: "wasm-inf-loop",
      name: "inf-loop",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: loopSig.digest,
      wasm_bytes: loopBytes,
      webhook_config: null,
      signature: loopSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"], fuel_budget: 500_000, failure_policy: "FAIL_CLOSED" },
      enabled: true,
    };
    const loopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: loopPlugin.id, priority: 1 }],
      new Map([[loopPlugin.id, loopPlugin]]),
      { request_id: "loop-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hang",
    );
    expect(loopRes).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_execution_failed",
      http_status: 500,
    });

    const compilesAfterFirstLoop = executor.stats!().compileTotal;
    const loopRes2 = await engine.runUnary(
      "on_response",
      [{ plugin_id: loopPlugin.id, priority: 1 }],
      new Map([[loopPlugin.id, loopPlugin]]),
      { request_id: "loop-2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hang again",
    );
    expect(loopRes2).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      rule_code: "wasm_execution_failed",
    });
    expect(executor.stats!().compileTotal).toBe(compilesAfterFirstLoop);

    const afterLoopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "rw-recover", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "still healthy",
    );
    expect(afterLoopRes).toMatchObject({ blocked: false, text: "still healthy" });

    const bigInitBytes = buildMemoryBombWasm(128, 0);
    const bigInitSig = signBytes(bigInitBytes);
    const bigInitPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-mem-init-bomb",
      digest: bigInitSig.digest,
      wasm_bytes: bigInitBytes,
      signature: bigInitSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 2 * 1024 * 1024 },
    };
    const bigInitRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: bigInitPlugin.id, priority: 1 }],
      new Map([[bigInitPlugin.id, bigInitPlugin]]),
      { request_id: "mem-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "mem",
    );
    expect(bigInitRes).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_memory_limit",
    });

    const growBombBytes = buildMemoryBombWasm(1, 40);
    const growBombSig = signBytes(growBombBytes);
    const growBombPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-mem-grow-bomb",
      digest: growBombSig.digest,
      wasm_bytes: growBombBytes,
      signature: growBombSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 1 * 1024 * 1024 },
    };
    const growBombRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: growBombPlugin.id, priority: 1 }],
      new Map([[growBombPlugin.id, growBombPlugin]]),
      { request_id: "mem-2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "mem-grow",
    );
    expect(growBombRes).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_memory_limit",
    });

    const wasiBytes = buildWasiImportWasm();
    const wasiSig = signBytes(wasiBytes);
    const wasiPlugin: PluginRecord = {
      ...loopPlugin,
      id: "wasm-wasi-escape",
      digest: wasiSig.digest,
      wasm_bytes: wasiBytes,
      signature: wasiSig.signature,
      manifest: { hooks: ["on_response"] },
    };
    const wasiRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: wasiPlugin.id, priority: 1 }],
      new Map([[wasiPlugin.id, wasiPlugin]]),
      { request_id: "wasi-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "escape",
    );
    expect(wasiRes).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_compile_failed",
    });

    const tamperedBytes = new Uint8Array(filterBytes);
    tamperedBytes[tamperedBytes.length - 1] = (tamperedBytes[tamperedBytes.length - 1]! + 1) & 0xff;
    const tamperedPlugin: PluginRecord = {
      ...filterPlugin,
      id: "wasm-tampered",
      wasm_bytes: tamperedBytes,
    };
    const tamperedRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: tamperedPlugin.id, priority: 1 }],
      new Map([[tamperedPlugin.id, tamperedPlugin]]),
      { request_id: "tamper-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hello",
    );
    expect(tamperedRes).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      rule_code: "plugin_digest_mismatch",
    });
  } finally {
    executor.close();
  }
});

test("G3.1 Native Rust wasmtime FFI sandbox executes signed WASM, isolates cross-tenant memory, enforces exact fuel & memory limits, and blocks WASI imports", async () => {
  const libPath = resolveWasmtimeLibraryPath();
  if (!libPath) return;

  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const pubPem = publicKey.export({ type: "spki", format: "pem" }).toString();
  const signBytes = (bytes: Uint8Array) => {
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    const signature = sign(null, Buffer.from(digest, "utf8"), privateKey).toString("base64");
    return { digest, signature };
  };

  const executor = createFfiWasmtimeExecutor({
    libraryPath: libPath,
    maxMemoryBytes: 4 * 1024 * 1024,
    defaultFuel: 2_000_000,
  });
  const engine = new PluginPipelineEngine({
    wasmExecutor: executor,
    signingPublicKeyPem: pubPem,
  });

  try {
    const filterBytes = buildSubstringWasm({
      hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
      needle: "SECRET_42",
      matchReply: { action: "TRANSFORM", text: "[CLEANSED_BY_WASMTIME]" },
    });
    const filterSig = signBytes(filterBytes);
    const filterPlugin: PluginRecord = {
      id: "ffi-real-filter",
      name: "ffi-filter",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: filterSig.digest,
      wasm_bytes: filterBytes,
      webhook_config: null,
      signature: filterSig.signature,
      signing_key_id: "k1",
      manifest: {
        hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
        window_bytes: 12,
      },
      enabled: true,
    };

    const passRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "clean text",
    );
    expect(passRes).toMatchObject({ blocked: false, text: "clean text" });

    const transformRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-2", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "payload SECRET_42 here",
    );
    expect(transformRes).toMatchObject({ blocked: false, text: "[CLEANSED_BY_WASMTIME]" });

    const streamSession = engine.createStreamSession(
      [{ plugin_id: filterPlugin.id, priority: 1 }],
      new Map([[filterPlugin.id, filterPlugin]]),
      { request_id: "ffi-3", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      () => undefined,
    );
    let streamOut = "";
    streamOut += (await streamSession.push("split SEC")).emit;
    streamOut += (await streamSession.push("RET_42 tail")).emit;
    const streamEnd = await streamSession.finalize();
    expect(streamEnd.blocked).toBe(false);
    streamOut += streamEnd.emit;
    expect(streamOut).toContain("[CLEANSED_BY_WASMTIME]");
    expect(streamOut).not.toContain("SECRET_42");

    const probeBytes = buildCrossTenantProbeWasm();
    const probeSig = signBytes(probeBytes);
    const probePlugin: PluginRecord = {
      id: "ffi-tenant-probe",
      name: "ffi-tenant-probe",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: probeSig.digest,
      wasm_bytes: probeBytes,
      webhook_config: null,
      signature: probeSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"] },
      enabled: true,
    };
    const probeMap = new Map([[probePlugin.id, probePlugin]]);
    expect(
      (
        await engine.runUnary(
          "on_response",
          [{ plugin_id: probePlugin.id, priority: 1 }],
          probeMap,
          { request_id: "fp-1", billing_account_id: "t1", model: "gpt-4o", operation: "chat" },
          "t1",
        )
      ).blocked,
    ).toBe(false);
    expect(
      (
        await engine.runUnary(
          "on_response",
          [{ plugin_id: probePlugin.id, priority: 1 }],
          probeMap,
          { request_id: "fp-2", billing_account_id: "t2", model: "gpt-4o", operation: "chat" },
          "t2",
        )
      ).blocked,
    ).toBe(false);

    const loopBytes = buildInfiniteLoopWasm("on_response");
    const loopSig = signBytes(loopBytes);
    const loopPlugin: PluginRecord = {
      id: "ffi-inf-loop",
      name: "ffi-inf-loop",
      version: "1.0.0",
      kind: "WASM",
      capability: "CONTENT",
      provider_config: null,
      digest: loopSig.digest,
      wasm_bytes: loopBytes,
      webhook_config: null,
      signature: loopSig.signature,
      signing_key_id: "k1",
      manifest: { hooks: ["on_response"], fuel_budget: 50_000, failure_policy: "FAIL_CLOSED" },
      enabled: true,
    };
    const t0 = performance.now();
    const loopRes = await engine.runUnary(
      "on_response",
      [{ plugin_id: loopPlugin.id, priority: 1 }],
      new Map([[loopPlugin.id, loopPlugin]]),
      { request_id: "floop-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
      "hang",
    );
    expect(loopRes).toMatchObject({ blocked: true, rule_code: "wasm_execution_failed" });
    expect(performance.now() - t0).toBeLessThan(100);

    const bigInitBytes = buildMemoryBombWasm(128, 0);
    const bigInitSig = signBytes(bigInitBytes);
    const bigInitPlugin: PluginRecord = {
      ...loopPlugin,
      id: "ffi-mem-bomb",
      digest: bigInitSig.digest,
      wasm_bytes: bigInitBytes,
      signature: bigInitSig.signature,
      manifest: { hooks: ["on_response"], max_memory_bytes: 2 * 1024 * 1024 },
    };
    expect(
      await engine.runUnary(
        "on_response",
        [{ plugin_id: bigInitPlugin.id, priority: 1 }],
        new Map([[bigInitPlugin.id, bigInitPlugin]]),
        { request_id: "fmem-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
        "mem",
      ),
    ).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_memory_limit",
    });

    const wasiBytes = buildWasiImportWasm();
    const wasiSig = signBytes(wasiBytes);
    const wasiPlugin: PluginRecord = {
      ...loopPlugin,
      id: "ffi-wasi-escape",
      digest: wasiSig.digest,
      wasm_bytes: wasiBytes,
      signature: wasiSig.signature,
      manifest: { hooks: ["on_response"] },
    };
    expect(
      await engine.runUnary(
        "on_response",
        [{ plugin_id: wasiPlugin.id, priority: 1 }],
        new Map([[wasiPlugin.id, wasiPlugin]]),
        { request_id: "fwasi-1", billing_account_id: "pr1", model: "gpt-4o", operation: "chat" },
        "escape",
      ),
    ).toMatchObject({
      blocked: true,
      outcome_kind: "plugin_fault",
      charge_policy: "PLATFORM_ABSORB",
      rule_code: "wasm_compile_failed",
    });
  } finally {
    executor.close();
  }
});
