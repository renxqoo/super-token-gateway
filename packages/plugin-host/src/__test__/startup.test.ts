import { expect, test } from "bun:test";
import { getEventListeners } from "node:events";
import { chmod, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { createPluginSupervisor } from "../supervisor.ts";

const module = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
const ready = 'process.stdout.write(JSON.stringify({version:2,id:"",event:"ready"})+"\\n");';
const reader = `let buffer="";process.stdin.on("data",chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf("\\n"))>=0){const r=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);process.stdout.write(JSON.stringify({version:2,id:r.id,...(r.op==="stats"?{stats:{ok:true}}:{})})+"\\n");}});`;
async function helper(source: string) {
  const dir = await mkdtemp(join(tmpdir(), "gateway-native-startup-"));
  const path = join(dir, "helper.js");
  await Bun.write(path, `#!${process.execPath} --no-env-file\n${source}`);
  await chmod(path, 0o700);
  return { path, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
async function eventually(check: () => boolean) {
  const until = performance.now() + 3000;
  while (!check() && performance.now() < until) await Bun.sleep(5);
  expect(check()).toBe(true);
}
function gone(pid: number) {
  try {
    process.kill(pid, 0);
    return false;
  } catch {
    return true;
  }
}
async function cleanup(host: ReturnType<typeof createPluginSupervisor>) {
  const pid = host.snapshot().pid;
  await host.close();
  expect(host.snapshot()).toMatchObject({
    pending: 0,
    pendingBytes: 0,
    writeBytes: 0,
    loaded: 0,
    observers: 0,
    dead: true,
  });
  if (pid) expect(gone(pid)).toBe(true);
}

test("a parent starting to read after 2200ms does not spend its frame write budget before READY", async () => {
  const h = await helper(`setTimeout(()=>{${reader}${ready}},2200);`);
  const host = createPluginSupervisor({ executable: h.path });
  try {
    const started = performance.now();
    const outcome = await host.load(new Uint8Array(1 << 20)).then(
      (value) => ({ value }),
      (error: { code: string }) => ({ error: error.code }),
    );
    expect(outcome).toMatchObject({ value: { connection: host.snapshot().connection } });
    expect(performance.now() - started).toBeLessThan(5250);
    expect(await host.stats()).toEqual({ ok: true });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
}, 8000);

for (const startupMs of [0, 30001, Number.NaN, 1.5])
  test(`invalid startup budget ${startupMs} fails before spawn`, () => {
    expect(() => createPluginSupervisor({ executable: "/unused", native: { startupMs } })).toThrow(
      "CONFIG_INVALID",
    );
  });

test("a parent without READY fails startup, reports only bounded technical fields and releases every timer", async () => {
  const h = await helper("setInterval(()=>{},1000);");
  const host = createPluginSupervisor({ executable: h.path, native: { startupMs: 80 } });
  try {
    const pending = host.load(module);
    const pid = host.snapshot().pid!;
    await expect(pending).rejects.toMatchObject({
      code: "STARTUP_TIMEOUT",
      diagnostics: { phase: "startup", op: "load", frameBytes: 0, writeMs: 0 },
    });
    await eventually(() => gone(pid));
    expect(host.snapshot()).toMatchObject({
      pending: 0,
      active: 0,
      queued: 0,
      loading: 0,
      waiters: 0,
      timers: 0,
      inflight: 0,
      loaded: 0,
      ready: false,
    });
    expect(Object.keys(host.snapshot().diagnostics!).sort()).toEqual([
      "connection",
      "frameBytes",
      "op",
      "phase",
      "startupMs",
      "writeMs",
    ]);
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

for (const [symptom, messages] of [
  ["wrong version", [{ version: 1, id: "", event: "ready" }]],
  ["extra READY fields", [{ version: 2, id: "", event: "ready", stats: {} }]],
  ["unexpected event", [{ version: 2, id: "", event: "started" }]],
  ["ordinary reply before READY", [{ version: 2, id: "unknown:reply", stats: {} }]],
  ["heartbeat before READY", [{ version: 2, id: "", event: "heartbeat" }]],
  [
    "duplicate READY",
    [
      { version: 2, id: "", event: "ready" },
      { version: 2, id: "", event: "ready" },
    ],
  ],
  ["stale identified READY", [{ version: 2, id: "old:connection", event: "ready" }]],
] as const)
  test(`${symptom} cannot release startup dispatch`, async () => {
    const h = await helper(
      `process.stdout.write(${JSON.stringify(messages.map((r) => JSON.stringify(r) + "\n").join(""))});setInterval(()=>{},1000);`,
    );
    const host = createPluginSupervisor({ executable: h.path });
    try {
      await expect(host.load(module)).rejects.toMatchObject({ code: "PROTOCOL_INVALID" });
      expect(host.snapshot()).toMatchObject({ pending: 0, loaded: 0, active: 0, timers: 0 });
    } finally {
      await cleanup(host);
      await h.cleanup();
    }
  });

test("startup shared waiters have independent aborts, use bounded reservations and load only after READY", async () => {
  const h = await helper(`setTimeout(()=>{${reader}${ready}},600);`);
  const host = createPluginSupervisor({ executable: h.path, queueMs: 10, maxPending: 1 });
  const a = new AbortController(),
    b = new AbortController();
  try {
    const first = host.load(module, a.signal);
    const second = host.load(module, b.signal);
    const results = Promise.allSettled([first, second]);
    expect(host.snapshot()).toMatchObject({
      loaded: 0,
      loading: 1,
      waiters: 2,
      pending: 1,
      active: 0,
      inflight: 0,
      writeBytes: 0,
    });
    await expect(host.load(new Uint8Array(9))).rejects.toMatchObject({ code: "PENDING_BUDGET" });
    a.abort();
    await expect(first).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    expect(getEventListeners(a.signal, "abort")).toHaveLength(0);
    expect(host.snapshot()).toMatchObject({
      waiters: 1,
      starts: 1,
      pending: 1,
      active: 0,
      loaded: 0,
    });
    await expect(second).resolves.toMatchObject({ connection: host.snapshot().connection });
    await results;
    expect(getEventListeners(b.signal, "abort")).toHaveLength(0);
    expect(host.snapshot()).toMatchObject({
      pending: 0,
      loaded: 1,
      active: 0,
      waiters: 0,
      timers: 1,
    });
    expect(await host.stats()).toEqual({ ok: true });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

test("last startup waiter abort retires its helper and a future load receives a fresh READY", async () => {
  const h = await helper(`setTimeout(()=>{${reader}${ready}},600);`);
  const host = createPluginSupervisor({ executable: h.path });
  const a = new AbortController(),
    b = new AbortController();
  try {
    const results = Promise.allSettled([host.load(module, a.signal), host.load(module, b.signal)]);
    const old = host.snapshot();
    a.abort();
    b.abort();
    for (const result of await results)
      expect(result).toMatchObject({ status: "rejected", reason: { code: "CALL_CANCELLED" } });
    await eventually(() => gone(old.pid!));
    expect(host.snapshot()).toMatchObject({
      dead: true,
      timers: 0,
      pending: 0,
      waiters: 0,
      active: 0,
    });
    expect(getEventListeners(a.signal, "abort")).toHaveLength(0);
    expect(getEventListeners(b.signal, "abort")).toHaveLength(0);
    const future = await host.load(module);
    expect(future.connection).not.toBe(old.connection);
    expect(host.snapshot()).toMatchObject({ starts: 2, ready: true, loaded: 1, waiters: 0 });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

test("cancelling one distinct startup load cannot retain the owner process after READY and close", async () => {
  const h = await helper(`setTimeout(()=>{${reader}${ready}},250);`);
  const supervisor = pathToFileURL(join(import.meta.dir, "../supervisor.ts")).href;
  const source = `
    import assert from "node:assert/strict";
    import { rm } from "node:fs/promises";
    import { dirname } from "node:path";
    import { createPluginSupervisor } from ${JSON.stringify(supervisor)};
    const host = createPluginSupervisor({
      executable: ${JSON.stringify(h.path)},
      native: { compileMs: 10000 },
      queueMs: 10000,
    });
    try {
      const controller = new AbortController();
      const first = host.load(new Uint8Array([0,97,115,109,1,0,0,0]), controller.signal);
      const cancelled = assert.rejects(first, { code: "CALL_CANCELLED" });
      const second = host.load(new Uint8Array([0,97,115,109,1,0,0,0,0,1,0]));
      controller.abort();
      await cancelled;
      const loaded = await second;
      assert.equal(loaded.connection, host.snapshot().connection);
      assert.equal(host.snapshot().loaded, 1);
      await host.close();
      assert.throws(() => process.kill(-host.snapshot().pid, 0));
      for (const key of ["timers", "pending", "pendingBytes", "writeBytes", "queued", "active", "inflight", "loading", "loaded", "waiters"])
        assert.equal(host.snapshot()[key], 0);
    } finally {
      await host.close();
      await rm(dirname(${JSON.stringify(h.path)}), { recursive: true, force: true });
    }
    console.log("closed");
  `;
  const child = Bun.spawn([process.execPath, "--no-env-file", "--eval", source], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const stderr = new Response(child.stderr).text();
  let budget: ReturnType<typeof setTimeout> | undefined;
  try {
    const output = child.stdout.getReader();
    let text = "";
    while (!text.includes("closed\n")) {
      const chunk = await output.read();
      if (chunk.done) break;
      text += new TextDecoder().decode(chunk.value);
    }
    output.releaseLock();
    expect(text).toBe("closed\n");
    const exited = await Promise.race([
      child.exited.then((code) => ({ code })),
      new Promise<{ retained: true }>((resolve) => {
        budget = setTimeout(() => resolve({ retained: true }), 1000);
      }),
    ]);
    expect(exited).toEqual({ code: 0 });
    expect(await stderr).toBe("");
  } finally {
    clearTimeout(budget);
    if (child.exitCode === null) child.kill("SIGKILL");
    await child.exited;
    await stderr;
    await h.cleanup();
  }
}, 15000);

for (const mode of ["close", "drain", "crash"] as const)
  test(`${mode} during startup promptly settles work and reaps the unready parent`, async () => {
    const h = await helper("setInterval(()=>{},1000);");
    const host = createPluginSupervisor({ executable: h.path });
    const controller = new AbortController();
    try {
      const pending = host.load(module, controller.signal);
      const outcome = Promise.allSettled([pending]);
      const pid = host.snapshot().pid!;
      const started = performance.now();
      if (mode === "crash") process.kill(pid, "SIGKILL");
      else await host.close(mode === "drain");
      expect((await outcome)[0]?.status).toBe("rejected");
      await eventually(() => gone(pid));
      expect(performance.now() - started).toBeLessThan(500);
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
      expect(host.snapshot()).toMatchObject({
        timers: 0,
        active: 0,
        pending: 0,
        waiters: 0,
        loaded: 0,
      });
    } finally {
      await cleanup(host);
      await h.cleanup();
    }
  });

test("READY does not extend the original load deadline and delayed startup timers cannot revive a connection", async () => {
  const h = await helper(`setTimeout(()=>{${reader}${ready}},600);`);
  const host = createPluginSupervisor({ executable: h.path, native: { compileMs: 50 } });
  try {
    const started = performance.now();
    const pending = host.load(module);
    const result = Promise.allSettled([pending]);
    await Bun.sleep(20);
    const blockedUntil = performance.now() + 320;
    while (performance.now() < blockedUntil) {}
    expect((await result)[0]).toMatchObject({
      status: "rejected",
      reason: { code: "STARTUP_TIMEOUT" },
    });
    expect(performance.now() - started).toBeLessThan(600);
    await Bun.sleep(400);
    expect(host.snapshot()).toMatchObject({
      dead: true,
      ready: false,
      timers: 0,
      active: 0,
      pending: 0,
    });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

test("READY followed by a parent that never reads still enforces the unchanged 2000ms frame write budget", async () => {
  const h = await helper(`${ready}setInterval(()=>{},1000);`);
  const host = createPluginSupervisor({ executable: h.path });
  try {
    const result = host.load(new Uint8Array(1 << 20));
    const outcome = Promise.allSettled([result]);
    await eventually(() => host.snapshot().inflight === 1);
    const started = performance.now();
    expect((await outcome)[0]).toMatchObject({
      status: "rejected",
      reason: { code: "WRITE_TIMEOUT", diagnostics: { phase: "writing", op: "load" } },
    });
    expect(performance.now() - started).toBeLessThan(2400);
    expect(host.snapshot().diagnostics!.frameBytes).toBeGreaterThan(1 << 20);
    expect(host.snapshot()).toMatchObject({ timers: 0, inflight: 0, active: 0, pending: 0 });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
}, 6000);

test("a retired in-flight write cannot retain timers or poison a replacement after its late callback", async () => {
  const h = await helper(
    `import {existsSync,writeFileSync} from "node:fs";const marker=new URL("started",import.meta.url);if(existsSync(marker)){${reader}${ready}}else{writeFileSync(marker,"");${ready}setInterval(()=>{},1000);}`,
  );
  const host = createPluginSupervisor({ executable: h.path });
  const controller = new AbortController();
  try {
    const pending = host.load(new Uint8Array(1 << 20), controller.signal);
    const result = Promise.allSettled([pending]);
    await eventually(() => host.snapshot().inflight === 1);
    const old = host.snapshot();
    controller.abort();
    expect((await result)[0]).toMatchObject({
      status: "rejected",
      reason: { code: "CALL_CANCELLED" },
    });
    expect(host.snapshot()).toMatchObject({ timers: 0, inflight: 0, pending: 0, active: 0 });
    await host.load(module);
    expect(host.snapshot().connection).not.toBe(old.connection);
    const blockedUntil = performance.now() + 2100;
    while (performance.now() < blockedUntil) {}
    await Bun.sleep(10);
    expect(await host.stats()).toEqual({ ok: true });
    expect(host.snapshot()).toMatchObject({
      starts: 2,
      dead: false,
      pending: 0,
      inflight: 0,
      active: 0,
      timers: 1,
    });
    await eventually(() => gone(old.pid!));
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
}, 7000);

test("READY and execution together cannot restart a load's absolute compile allowance", async () => {
  const h = await helper(
    `let pending;setTimeout(()=>{${ready}let buffer="";process.stdin.on("data",chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf("\\n"))>=0){const r=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);const reply=(id,fields={})=>process.stdout.write(JSON.stringify({version:2,id,...fields})+"\\n");if(r.op==="load"){pending=r;setTimeout(()=>reply(r.id),1600);}else if(r.op==="cancel"){reply(r.id);reply(pending.id,{error:"CALL_CANCELLED"});}else reply(r.id);}});},200);`,
  );
  const host = createPluginSupervisor({ executable: h.path, native: { compileMs: 1500 } });
  try {
    const started = performance.now();
    await expect(host.load(module)).rejects.toMatchObject({ code: "CALL_TIMEOUT" });
    expect(performance.now() - started).toBeLessThan(2100);
    expect(host.snapshot()).toMatchObject({
      pending: 0,
      active: 0,
      loaded: 0,
      ready: true,
      timers: 1,
    });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
}, 5000);

test("a duplicate READY after a load reply cannot promote a retired connection to a loaded result", async () => {
  const h = await helper(
    `${ready}let buffer="";process.stdin.on("data",chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf("\\n"))>=0){const r=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);process.stdout.write(JSON.stringify({version:2,id:r.id})+"\\n"+JSON.stringify({version:2,id:"",event:"ready"})+"\\n");}});`,
  );
  const host = createPluginSupervisor({ executable: h.path });
  try {
    await expect(host.load(module)).rejects.toMatchObject({ code: "PROTOCOL_INVALID" });
    expect(host.snapshot()).toMatchObject({
      dead: true,
      loaded: 0,
      loading: 0,
      waiters: 0,
      timers: 0,
      pending: 0,
    });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});
