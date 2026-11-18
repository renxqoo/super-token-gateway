import { expect, spyOn, test } from "bun:test";
import { ownPluginModule, pluginModuleInfo } from "@gateway/plugin-sdk";
import { createBuiltinWorkerExecutor } from "../builtin-worker.ts";

const module = ownPluginModule(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]));
const hash = pluginModuleInfo(module).hash;
const code = new TextEncoder().encode(`
let calls = 0;
self.onmessage = async ({ data }) => {
  calls++;
  const input = data.input;
  if (input[0] === 1) for (;;) Math.imul(17, 31);
  if (input[0] === 2) await Bun.sleep(150);
  if (input[0] === 3) { self.postMessage({ version: 1, id: "wrong" }); return; }
  const output = new Uint8Array([input[1], calls]);
  const state = new Uint8Array([(data.state?.[0] ?? 0) + 1]);
  self.postMessage({ version: 1, id: data.id, output, state });
};
self.postMessage({ version: 1, event: "ready" });
`);
const digest = new Bun.CryptoHasher("sha256").update(code).digest("hex");
const create = (options: Partial<Parameters<typeof createBuiltinWorkerExecutor>[0]> = {}) =>
  createBuiltinWorkerExecutor({ code, digest, modules: [hash], slots: 1, ...options });
const input = (mode = 0, value = 17) => new Uint8Array([mode, value]);
function program(source: string) {
  const bytes = new TextEncoder().encode(source);
  return { code: bytes, digest: new Bun.CryptoHasher("sha256").update(bytes).digest("hex") };
}

async function empty(executor: ReturnType<typeof create>) {
  await executor.close();
  expect(executor.snapshot()).toMatchObject({
    workers: 0,
    retiring: 0,
    active: 0,
    queued: 0,
    pending: 0,
    pendingBytes: 0,
    timers: 0,
    closed: true,
  });
}

test("builtin worker owns verified code and request bytes while preserving each caller's explicit state", async () => {
  const bytes = code.slice();
  const executor = create({ code: bytes });
  bytes.fill(0);
  try {
    const loaded = await executor.load(module);
    expect(loaded.hash).toBe(hash);
    const first = await executor.call(hash, input(), new Uint8Array([40]));
    expect([...first.output]).toEqual([17, 1]);
    expect([...first.state]).toEqual([41]);
    const holding = executor.call(hash, input(2), undefined);
    const body = input(0, 29),
      state = new Uint8Array([7]);
    const queued = executor.call(hash, body, state);
    body.fill(255);
    state.fill(255);
    await holding;
    expect(await queued).toEqual({ output: new Uint8Array([29, 3]), state: new Uint8Array([8]) });
  } finally {
    await empty(executor);
  }
});

test("builtin queued Buffer input and state retain the bytes captured before caller mutation", async () => {
  const executor = create();
  try {
    await executor.call(hash, input(), undefined);
    const holding = executor.call(hash, input(2), undefined);
    const body = Buffer.from([0, 29]),
      state = Buffer.from([7]);
    const queued = executor.call(hash, body, state);
    body.fill(255);
    state.fill(255);
    await holding;
    const actual = await queued;
    expect([...actual.output]).toEqual([29, 3]);
    expect([...actual.state]).toEqual([8]);
  } finally {
    await empty(executor);
  }
});

test("builtin worker rejects unverified code and modules and pre-cancelled or over-budget calls without starting a thread", async () => {
  expect(() => create({ digest: "0".repeat(64) })).toThrow("WORKER_ARTIFACT_INVALID");
  const executor = create({ maxPendingBytes: 1 });
  try {
    await expect(executor.call(hash, input(), undefined)).rejects.toMatchObject({
      code: "PENDING_BUDGET",
    });
    await expect(
      executor.call(hash, input(), undefined, { signal: AbortSignal.abort() }),
    ).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    await expect(executor.load(new Uint8Array([1]))).rejects.toMatchObject({
      code: "MODULE_MISSING",
    });
    await expect(
      executor.call(hash, new Uint8Array((256 << 10) + 1), undefined),
    ).rejects.toMatchObject({ code: "INPUT_BUDGET" });
    expect(executor.snapshot().workers).toBe(0);
  } finally {
    await empty(executor);
  }
});

test("builtin queued cancellation and queue deadlines never execute or cancel the active caller", async () => {
  const executor = create({ maxPending: 2 });
  try {
    await executor.call(hash, input(), undefined);
    const first = executor.call(hash, input(2), undefined);
    const abort = new AbortController();
    const cancelled = executor.call(hash, input(), undefined, { signal: abort.signal });
    await expect(executor.call(hash, input(), undefined)).rejects.toMatchObject({
      code: "PENDING_BUDGET",
    });
    abort.abort();
    await expect(cancelled).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    await expect(executor.call(hash, input(), undefined, { timeoutMs: 20 })).rejects.toMatchObject({
      code: "CALL_TIMEOUT",
    });
    expect([...(await first).output]).toEqual([17, 2]);
    expect([...(await executor.call(hash, input(), undefined)).output]).toEqual([17, 3]);
  } finally {
    await empty(executor);
  }
});

test("builtin active CPU cancellation reaps the spinning thread while the main loop stays responsive and fresh work succeeds", async () => {
  const executor = create();
  let ticks = 0;
  const timer = setInterval(() => ticks++, 5);
  try {
    await executor.call(hash, input(), undefined);
    const abort = new AbortController();
    const running = executor.call(hash, input(1), undefined, { signal: abort.signal });
    const observed = running.catch((error: unknown) => error);
    await Bun.sleep(80);
    const started = performance.now();
    abort.abort();
    expect(await observed).toMatchObject({ code: "CALL_CANCELLED" });
    expect([...(await executor.call(hash, input(0, 42), undefined)).output]).toEqual([42, 1]);
    expect(performance.now() - started).toBeLessThan(500);
    expect(ticks).toBeGreaterThan(3);
  } finally {
    clearInterval(timer);
    await empty(executor);
  }
});

test("builtin CPU timeout and invalid replies cannot replay a failed call or corrupt a fresh worker", async () => {
  const executor = create();
  try {
    await executor.call(hash, input(), undefined);
    await expect(executor.call(hash, input(1), undefined, { timeoutMs: 30 })).rejects.toMatchObject(
      { code: "CALL_TIMEOUT" },
    );
    expect([...(await executor.call(hash, input(0, 31), undefined)).output]).toEqual([31, 1]);
    expect(
      await executor.call(hash, input(3), undefined).catch((error: unknown) => error),
    ).toMatchObject({
      code: "PROTOCOL_INVALID",
    });
    expect([...(await executor.call(hash, input(0, 37), undefined)).output]).toEqual([37, 1]);
  } finally {
    await empty(executor);
  }
});

test("builtin close is shared and reentrant and awaits actual shutdown after cancelling active and queued calls", async () => {
  const executor = create();
  await executor.call(hash, input(), undefined);
  const first = executor.call(hash, input(1), undefined).catch((error: unknown) => error);
  const second = executor.call(hash, input(), undefined).catch((error: unknown) => error);
  let observed: Promise<void> | undefined;
  executor.onDisconnect(() => {
    observed = executor.close();
    throw new Error("observer failed");
  });
  const closing = executor.close();
  expect(observed).toBe(closing);
  expect(executor.close()).toBe(closing);
  expect(await first).toMatchObject({ code: "HOST_CLOSED" });
  expect(await second).toMatchObject({ code: "HOST_CLOSED" });
  await empty(executor);
  await expect(executor.call(hash, input(), undefined)).rejects.toMatchObject({
    code: "HOST_CLOSED",
  });
});

test("builtin startup CPU stalls are terminated under the original call deadline", async () => {
  const executor = create(program("for (;;) Math.imul(17,31);"));
  const started = performance.now();
  try {
    const error = await executor
      .call(hash, input(), undefined, { timeoutMs: 30 })
      .catch((failure: unknown) => failure);
    expect(error).toMatchObject({ code: "CALL_TIMEOUT" });
    await executor.close();
    expect(performance.now() - started).toBeLessThan(500);
  } finally {
    await empty(executor);
  }
});

test("builtin cancelling one active slot leaves another tenant's state and response intact", async () => {
  const executor = create({ slots: 2 });
  try {
    await executor.call(hash, input(), undefined);
    const abort = new AbortController();
    const running = executor
      .call(hash, input(1), undefined, { signal: abort.signal })
      .catch((error: unknown) => error);
    const healthy = executor.call(hash, input(2, 41), new Uint8Array([80]));
    await Bun.sleep(30);
    abort.abort();
    expect(await running).toMatchObject({ code: "CALL_CANCELLED" });
    const result = await healthy;
    expect([...result.output]).toEqual([41, 1]);
    expect([...result.state]).toEqual([81]);
    expect([...(await executor.call(hash, input(0, 43), result.state)).state]).toEqual([82]);
  } finally {
    await empty(executor);
  }
});

test("builtin repeated startup failures stop spawning within the rate window and recover after it", async () => {
  let now = 0;
  const clock = spyOn(performance, "now").mockImplementation(() => now);
  const executor = create(program('throw new Error("PRIVATE_WORKER_DIAGNOSTIC");'));
  try {
    for (let n = 0; n < 16; n++) {
      const error = await executor
        .call(hash, input(), undefined)
        .catch((failure: unknown) => failure);
      expect(error).toMatchObject({ code: "WORKER_FAILED", message: "WORKER_FAILED" });
    }
    expect(
      await executor.call(hash, input(), undefined).catch((error: unknown) => error),
    ).toMatchObject({ code: "RESTART_BUDGET" });
    now = 1001;
    expect(
      await executor.call(hash, input(), undefined).catch((error: unknown) => error),
    ).toMatchObject({ code: "WORKER_FAILED" });
  } finally {
    clock.mockRestore();
    await empty(executor);
  }
});

test("builtin oversized or shared-memory replies are rejected before becoming caller-owned output", async () => {
  for (const output of [
    "new Uint8Array(262145)",
    "new Uint8Array(new SharedArrayBuffer(1))",
    "new Uint8Array(new ArrayBuffer(262145),0,1)",
  ]) {
    const executor = create(
      program(
        `self.onmessage=({data})=>self.postMessage({version:1,id:data.id,output:${output},state:new Uint8Array()});self.postMessage({version:1,event:"ready"});`,
      ),
    );
    try {
      const error = await executor
        .call(hash, input(), undefined)
        .catch((failure: unknown) => failure);
      expect(error).toMatchObject({ code: "PROTOCOL_INVALID" });
    } finally {
      await empty(executor);
    }
  }
});

test("builtin replies delivered after a blocked main loop passes the deadline cannot become success or contaminate the next call", async () => {
  const executor = create();
  try {
    await executor.call(hash, input(), undefined);
    const expired = executor
      .call(hash, input(0, 91), undefined, { timeoutMs: 20 })
      .catch((error: unknown) => error);
    const until = performance.now() + 60;
    while (performance.now() < until) Math.imul(17, 31);
    expect(await expired).toMatchObject({ code: "CALL_TIMEOUT" });
    expect([...(await executor.call(hash, input(0, 97), undefined)).output]).toEqual([97, 1]);
  } finally {
    await empty(executor);
  }
});
