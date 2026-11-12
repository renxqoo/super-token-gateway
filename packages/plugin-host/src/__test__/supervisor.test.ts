import { expect, test } from "bun:test";
import { mkdtemp, chmod, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPluginSupervisor } from "../supervisor.ts";

const executable = new URL("../../../../.local/plugin-host", import.meta.url).pathname;
const fixture = await Bun.file(
  new URL("../../../../tests/support/plugins/isolation.wasm", import.meta.url),
).bytes();
const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
test("invalid native resource budgets fail before a process or oversized frame can be allocated", () => {
  for (const native of [
    { slots: 0 },
    { maxInputBytes: Number.MAX_SAFE_INTEGER },
    { maxModules: 0 },
    { maxStateBytes: 2 << 20 },
    { maxInputBytes: 16 << 20 },
    { maxCacheBytes: 8 },
    { maxWorkerRssBytes: 2 * 1024 ** 3 },
    { queue: 1 },
  ])
    expect(() => createPluginSupervisor({ executable, native })).toThrow("CONFIG_INVALID");
});
function input(value: unknown) {
  return encode({
    abi_version: 1,
    hook: "synthetic",
    session: { id: crypto.randomUUID(), sequence: 0 },
    config: {},
    payload: Buffer.from(JSON.stringify(value)).toString("base64"),
    output_budget: 100000,
  });
}
function decoded(output: Uint8Array) {
  return JSON.parse(new TextDecoder().decode(output)) as Record<string, unknown>;
}
async function eventually(check: () => boolean | Promise<boolean>) {
  const until = Date.now() + 2000;
  while (Date.now() < until) {
    if (await check()) return;
    await Bun.sleep(10);
  }
  expect(await check()).toBe(true);
}
function gone(pid: number) {
  try {
    process.kill(pid, 0);
    return false;
  } catch {
    return true;
  }
}

test("same helper, compiled module and slot cannot expose A to B, including after trap and cancellation", async () => {
  const host = createPluginSupervisor({ executable, native: { slots: 1, callMs: 500 } });
  try {
    const { hash } = await host.load(fixture),
      before = await host.stats();
    const a = await host.call(hash, input({ value: "canary-A" }), undefined);
    expect(decoded(a.output)).toMatchObject({ previous: null, value: "canary-A" });
    const b = await host.call(hash, input({ value: "canary-B" }), undefined);
    expect(decoded(b.output)).toMatchObject({ previous: null, value: "canary-B", state: null });
    await expect(host.call(hash, input({}), undefined, { function: "trap" })).rejects.toMatchObject(
      { code: "GUEST_TRAP" },
    );
    expect(
      decoded((await host.call(hash, input({ value: "after-trap" }), undefined)).output).previous,
    ).toBeNull();
    const abort = new AbortController();
    const pending = host.call(hash, input({}), undefined, {
      function: "loop",
      signal: abort.signal,
    });
    setTimeout(() => abort.abort(), 20);
    await expect(pending).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    expect(host.snapshot().pending).toBe(0);
    expect(Number((await host.stats())?.pending)).toBe(0);
    expect(
      decoded((await host.call(hash, input({ value: "after-cancel" }), undefined)).output).previous,
    ).toBeNull();
    expect((await host.stats())?.workers).toEqual(before?.workers);
    expect(host.snapshot().starts).toBe(1);
    expect(host.snapshot().pending).toBe(0);
  } finally {
    await host.close();
  }
}, 15000);
test("opaque state crosses Bun and the SDK guest without rounding large integers or decimal literals", async () => {
  const host = createPluginSupervisor({ executable });
  try {
    const { hash } = await host.load(fixture);
    const state = new TextEncoder().encode('{"n":9007199254740993,"d":0.10000000000000001}');
    const reply = await host.call(hash, input({ value: "synthetic" }), state);
    expect(new TextDecoder().decode(reply.state)).toBe(
      '{"n":9007199254740993,"d":0.10000000000000001}',
    );
  } finally {
    await host.close();
  }
});
test("an evicted compiled module can reload for a future transform without replaying an old call", async () => {
  const host = createPluginSupervisor({ executable, native: { maxModules: 1 } });
  try {
    const first = await host.load(fixture);
    expect(
      decoded((await host.call(first.hash, input({ value: "A" }), undefined)).output).value,
    ).toBe("A");
    await host.load(
      await Bun.file(
        new URL("../../../adapters/openai-chat/package/module.wasm", import.meta.url),
      ).bytes(),
    );
    const reloaded = await host.load(fixture);
    expect(
      decoded((await host.call(reloaded.hash, input({ value: "B" }), undefined)).output),
    ).toMatchObject({ previous: null, value: "B" });
  } finally {
    await host.close();
  }
});
test("a module worker crash cannot leave a stale Bun load entry that breaks the next transform", async () => {
  const host = createPluginSupervisor({ executable, native: { slots: 1 } });
  try {
    const first = await host.load(fixture),
      worker = ((await host.stats())!.workers as number[])[0]!;
    const active = host.call(first.hash, input({ value: "old" }), undefined, { function: "loop" }),
      settled = Promise.allSettled([active]);
    await eventually(async () => Number((await host.stats())!.active) === 1);
    await Bun.sleep(20);
    process.kill(worker, "SIGKILL");
    const outcome = (await settled)[0]!;
    expect(outcome.status).toBe("rejected");
    if (outcome.status === "rejected")
      expect(["WORKER_FAILED", "WORKER_MONITOR_FAILED"]).toContain(
        (outcome.reason as { code: string }).code,
      );
    const next = await host.load(fixture);
    expect(
      decoded((await host.call(next.hash, input({ value: "future" }), undefined)).output),
    ).toMatchObject({ previous: null, value: "future" });
  } finally {
    await host.close();
  }
});
test("SIGSTOP worker followed by helper SIGKILL reclaims the owned process group and leaves unrelated helper intact", async () => {
  const owned = createPluginSupervisor({ executable, native: { slots: 1 } }),
    other = createPluginSupervisor({ executable });
  try {
    await Promise.all([owned.load(fixture), other.load(fixture)]);
    const worker = ((await owned.stats())!.workers as number[])[0]!,
      pid = owned.snapshot().pid!;
    process.kill(worker, "SIGSTOP");
    process.kill(pid, "SIGKILL");
    await eventually(() => owned.snapshot().dead);
    await eventually(() => gone(worker));
    await eventually(() => gone(pid));
    expect((await other.stats())!.modules).toBe(1);
    const fresh = await owned.load(fixture);
    expect(
      decoded((await owned.call(fresh.hash, input({ value: "future-only" }), undefined)).output)
        .previous,
    ).toBeNull();
    expect(owned.snapshot().starts).toBe(2);
  } finally {
    await Promise.all([owned.close(), other.close()]);
  }
}, 15000);
test("helper death settles active work without replay, restart is finite and close settles every queued promise", async () => {
  const host = createPluginSupervisor({
    executable,
    native: { slots: 1, callMs: 1000 },
    maxRestarts: 1,
  });
  try {
    const { hash } = await host.load(fixture);
    const call = host.call(hash, input({ value: "old" }), undefined, { function: "loop" });
    process.kill(host.snapshot().pid!, "SIGKILL");
    await expect(call).rejects.toBeInstanceOf(Error);
    const reloaded = await host.load(fixture);
    expect(host.snapshot().starts).toBe(2);
    const first = host.call(reloaded.hash, input({}), undefined, { function: "loop" });
    const second = host.call(reloaded.hash, input({ value: "queued" }), undefined);
    const settled = Promise.allSettled([first, second]);
    await host.close();
    expect((await settled).every((r) => r.status === "rejected")).toBe(true);
    expect(host.snapshot().pending).toBe(0);
    expect(host.snapshot().pendingBytes).toBe(0);
    await expect(host.load(fixture)).rejects.toMatchObject({ code: "HOST_CLOSED" });
  } finally {
    await host.close();
  }
});
test("host pending count and bytes, queue deadline and native call deadline remain bounded", async () => {
  const host = createPluginSupervisor({
    executable,
    native: { slots: 1, callMs: 200 },
    maxPending: 2,
    queueMs: 20,
  });
  try {
    const { hash } = await host.load(fixture);
    const first = host.call(hash, input({}), undefined, { function: "loop" });
    const second = host.call(hash, input({ value: "queued" }), undefined);
    await expect(host.call(hash, input({}), undefined)).rejects.toMatchObject({
      code: "PENDING_BUDGET",
    });
    await expect(second).rejects.toMatchObject({ code: "QUEUE_TIMEOUT" });
    await expect(first).rejects.toMatchObject({ code: "CALL_TIMEOUT" });
    expect(host.snapshot().pendingBytes).toBe(0);
  } finally {
    await host.close();
  }
});
async function syntheticHelper(source: string) {
  const dir = await mkdtemp(join(tmpdir(), "gateway-synthetic-helper-")),
    path = join(dir, "helper.js");
  await Bun.write(path, `#!${process.execPath} --no-env-file\n${source}`);
  await chmod(path, 0o700);
  return { dir, path, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
test("an idle stopped helper loses heartbeat and its stopped worker is forcibly reaped", async () => {
  const host = createPluginSupervisor({ executable, heartbeatMs: 1200 });
  try {
    await host.load(fixture);
    const pid = host.snapshot().pid!,
      worker = ((await host.stats())!.workers as number[])[0]!;
    process.kill(worker, "SIGSTOP");
    process.kill(pid, "SIGSTOP");
    await eventually(() => host.snapshot().dead);
    await eventually(() => gone(worker));
    await eventually(() => gone(pid));
    expect(host.snapshot().pending).toBe(0);
  } finally {
    await host.close();
  }
}, 6000);
test("restart exhaustion cannot turn old commands into an automatic replay loop", async () => {
  const helper = await syntheticHelper("process.exit(1);"),
    host = createPluginSupervisor({ executable: helper.path, maxRestarts: 1 });
  try {
    await expect(host.stats()).rejects.toBeInstanceOf(Error);
    await expect(host.stats()).rejects.toBeInstanceOf(Error);
    await expect(host.stats()).rejects.toMatchObject({ code: "RESTART_BUDGET" });
    expect(host.snapshot().starts).toBe(2);
  } finally {
    await host.close();
    await helper.cleanup();
  }
});
test("pending and write byte budgets reject large frames and return their reservations", async () => {
  for (const limits of [{ maxPendingBytes: 4096 }, { maxWriteBytes: 4096 }]) {
    const host = createPluginSupervisor({ executable, ...limits });
    try {
      await expect(host.load(fixture)).rejects.toBeInstanceOf(Error);
      expect(host.snapshot().pendingBytes).toBe(0);
      expect(host.snapshot().writeBytes).toBe(0);
      expect((await host.stats())!.modules).toBe(0);
    } finally {
      await host.close();
    }
  }
});
test("unknown and duplicate replies cannot match new commands and cwd dotenv cannot pollute the helper environment", async () => {
  const helper = await syntheticHelper(
    'process.stdout.write(JSON.stringify({version:2,id:"",event:"ready"})+"\\n");let buffer="";process.stdin.on("data",chunk=>{buffer+=chunk;let i;while((i=buffer.indexOf("\\n"))>=0){const r=JSON.parse(buffer.slice(0,i));buffer=buffer.slice(i+1);const out={version:2,id:r.id,...(r.op==="stats"?{stats:{keys:Object.keys(process.env)}}:{})};process.stdout.write(JSON.stringify({...out,id:"late:unknown"})+"\\n"+JSON.stringify(out)+"\\n"+JSON.stringify(out)+"\\n");}});',
  );
  const host = createPluginSupervisor({ executable: helper.path });
  const previousCwd = process.cwd();
  try {
    await Bun.write(
      join(helper.dir, ".env.local"),
      "GATEWAY_SYNTHETIC_DOTENV_SENTINEL=synthetic-cwd-only\n",
    );
    process.chdir(helper.dir);
    await host.load(new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]));
    expect((await host.stats())!.keys).toEqual(["GOMEMLIMIT", "GOMAXPROCS"]);
    expect((await host.stats())!.keys).toEqual(["GOMEMLIMIT", "GOMAXPROCS"]);
    expect(host.snapshot().pending).toBe(0);
  } finally {
    process.chdir(previousCwd);
    await host.close();
    await helper.cleanup();
  }
});
test("malformed, oversized, EOF, stderr and stalled stdout fail all promises and are reaped", async () => {
  for (const source of [
    'process.stdout.write("bad\\n");setInterval(()=>{},1000);',
    'process.stdout.write("x".repeat(5000));setInterval(()=>{},1000);',
    "process.stdout.end();setInterval(()=>{},1000);",
    'process.stderr.write("synthetic");setInterval(()=>{},1000);',
    'process.stdout.write(JSON.stringify({version:2,id:"",event:"ready"})+"\\n");setInterval(()=>{},1000);',
  ]) {
    const helper = await syntheticHelper(source),
      host = createPluginSupervisor({
        executable: helper.path,
        maxFrameBytes: 4096,
        heartbeatMs: 1200,
      });
    try {
      const first = host.stats(),
        pid = host.snapshot().pid!;
      await expect(first).rejects.toBeInstanceOf(Error);
      await eventually(() => host.snapshot().dead);
      await eventually(() => gone(pid));
      expect(host.snapshot().pending).toBe(0);
    } finally {
      await host.close();
      await helper.cleanup();
    }
  }
}, 10000);

test("fresh native parents complete READY, cold and warm loads with isolated guest state and no remaining lifecycle resources", async () => {
  for (let cycle = 0; cycle < 3; cycle++) {
    const host = createPluginSupervisor({ executable });
    let owned: number[] = [];
    try {
      const first = await host.load(fixture);
      expect(host.snapshot()).toMatchObject({ ready: true, loaded: 1, starts: 1 });
      expect(host.snapshot().diagnostics!.startupMs).toBeGreaterThanOrEqual(0);
      const cached = await host.load(fixture);
      expect(cached).toEqual(first);
      for (const value of [`cold-${cycle}`, `warm-${cycle}`])
        expect(
          decoded((await host.call(first.hash, input({ value }), undefined)).output),
        ).toMatchObject({ previous: null, value });
      owned = [host.snapshot().pid!, ...((await host.stats())!.workers as number[])];
    } finally {
      await host.close();
    }
    for (const pid of owned) expect(gone(pid)).toBe(true);
    expect(host.snapshot()).toMatchObject({
      timers: 0,
      inflight: 0,
      pending: 0,
      queued: 0,
      active: 0,
      waiters: 0,
      loaded: 0,
      observers: 0,
    });
  }
}, 15000);
