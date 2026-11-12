import { expect, spyOn, test } from "bun:test";
import { getEventListeners } from "node:events";
import { chmod, mkdtemp, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createPluginAttempts, createPluginSupervisor } from "../index.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

const module = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
const reader = `let buffer="";process.stdin.on("data",chunk=>{buffer+=chunk;let end;while((end=buffer.indexOf("\\n"))>=0){const r=JSON.parse(buffer.slice(0,end));buffer=buffer.slice(end+1);handle(r);}});`;
async function helper(source: string, readyDelayMs = 0) {
  const dir = await mkdtemp(join(tmpdir(), "gateway-ipc-regression-"));
  const path = join(dir, "helper.js");
  await Bun.write(
    path,
    `#!${process.execPath} --no-env-file\n${source}\n${reader}\n${readyDelayMs ? `await Bun.sleep(${readyDelayMs});` : ""}process.stdout.write(JSON.stringify({version:2,id:"",event:"ready"})+"\\n");`,
  );
  await chmod(path, 0o700);
  return { dir, path, cleanup: () => rm(dir, { recursive: true, force: true }) };
}
async function eventually(check: () => boolean | Promise<boolean>) {
  const until = Date.now() + 1500;
  while (Date.now() < until) {
    if (await check()) return;
    await Bun.sleep(5);
  }
  expect(await check()).toBe(true);
}
async function within<T>(promise: Promise<T>) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("CANCEL_WAITED_FOR_SHARED_LOAD")), 300);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
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
    observers: 0,
    loaded: 0,
    dead: true,
  });
  if (pid) expect(gone(pid)).toBe(true);
}
async function heldLoads(
  capabilities: unknown = {},
  cancellation: "both" | "ack-only" | "target-only" = "both",
  readyDelayMs = 0,
) {
  const h = await helper(
    `import {existsSync,readFileSync,writeFileSync} from "node:fs";
const release=new URL("release.json",import.meta.url);
const pending=new Map();let loads=0,calls=0,cancels=0,polls=0;
const reply=(r,fields={})=>process.stdout.write(JSON.stringify({version:2,id:r.id,...fields})+"\\n");
function handle(r){
if(r.op==="load"){loads++;pending.set(r.id,r);writeFileSync(new URL("loads",import.meta.url),String(loads));}
else if(r.op==="stats")reply(r,{stats:{loads,calls,cancels,polls,pending:pending.size}});
else if(r.op==="cancel"){cancels++;const target=pending.get(r.target);pending.delete(r.target);if(${JSON.stringify(cancellation)}!=="target-only")reply(r);if(target&&${JSON.stringify(cancellation)}!=="ack-only")reply(target,{error:"CALL_CANCELLED"});}
else if(r.op==="call"){calls++;reply(r,{output:Buffer.from(JSON.stringify({abi_version:1,kind:"description",sdk_version:"1.0.0",capabilities:${JSON.stringify(capabilities)}})).toString("base64"),state:Buffer.from("null").toString("base64")});}
else reply(r);
}
setInterval(()=>{polls++;if(!existsSync(release))return;const fields=JSON.parse(readFileSync(release,"utf8"));for(const r of pending.values())reply(r,fields);pending.clear();},5);`,
    readyDelayMs,
  );
  return {
    ...h,
    loaded: async (host: ReturnType<typeof createPluginSupervisor>, count: number) => {
      const until = Date.now() + 5000;
      while (!host.snapshot().ready && !host.snapshot().dead && Date.now() < until)
        await Bun.sleep(5);
      expect(host.snapshot()).toMatchObject({ ready: true, dead: false });
      await eventually(
        async () =>
          (await Bun.file(join(h.dir, "loads")).exists()) &&
          (await Bun.file(join(h.dir, "loads")).text()) === String(count),
      );
    },
    release: async (fields: Record<string, unknown> = {}) => {
      const stage = join(h.dir, "release.tmp");
      try {
        await Bun.write(stage, JSON.stringify(fields));
        await rename(stage, join(h.dir, "release.json"));
      } finally {
        await rm(stage, { force: true });
      }
    },
  };
}

test("held load setup does not reject a legal READY after the loads observation window", async () => {
  const h = await heldLoads({}, "both", 2200);
  const host = createPluginSupervisor({ executable: h.path });
  const pending = host.load(module);
  const result = Promise.allSettled([pending]);
  try {
    await h.loaded(host, 1);
    expect(host.snapshot()).toMatchObject({ starts: 1, ready: true, dead: false });
    expect(await host.stats()).toMatchObject({ loads: 1, pending: 1 });
    await h.release();
    await expect(pending).resolves.toMatchObject({ connection: host.snapshot().connection });
  } finally {
    try {
      await cleanup(host);
      await result;
    } finally {
      await h.cleanup();
    }
  }
}, 10_000);

test("held load setup stops when a helper misses its startup deadline and is reaped", async () => {
  const h = await heldLoads({}, "both", 2200);
  const host = createPluginSupervisor({ executable: h.path, native: { startupMs: 100 } });
  const result = Promise.allSettled([host.load(module)]);
  const pid = host.snapshot().pid!;
  try {
    await expect(h.loaded(host, 1)).rejects.toThrow();
    expect(await result).toMatchObject([
      { status: "rejected", reason: { code: "STARTUP_TIMEOUT" } },
    ]);
    expect(host.snapshot()).toMatchObject({ ready: false, dead: true });
    await eventually(() => gone(pid));
  } finally {
    try {
      await cleanup(host);
    } finally {
      await h.cleanup();
    }
  }
});

for (const cancelFirst of [false, true])
  test(`shared cold load ${cancelFirst ? "creator" : "joining waiter"} cancellation does not cancel the other caller`, async () => {
    const h = await heldLoads();
    const host = createPluginSupervisor({ executable: h.path });
    const controller = new AbortController();
    try {
      const first = host.load(module, cancelFirst ? controller.signal : undefined);
      const firstResult = Promise.allSettled([first]);
      await h.loaded(host, 1);
      await eventually(async () => (await host.stats())?.loads === 1);
      const second = host.load(module, cancelFirst ? undefined : controller.signal);
      const allResults = Promise.allSettled([first, second]);
      const cancelled = cancelFirst ? first : second;
      const survivor = cancelFirst ? second : first;
      const outcome = cancelled.then(
        () => "RESOLVED",
        (error: { code: string }) => error.code,
      );
      controller.abort();
      expect(await within(outcome)).toBe("CALL_CANCELLED");
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
      expect(await host.stats()).toMatchObject({ loads: 1, cancels: 0, pending: 1 });
      await h.release();
      await expect(survivor).resolves.toMatchObject({ connection: host.snapshot().connection });
      await firstResult;
      await allResults;
      expect(host.snapshot()).toMatchObject({
        pending: 0,
        pendingBytes: 0,
        writeBytes: 0,
        starts: 1,
      });
    } finally {
      await cleanup(host);
      await h.cleanup();
    }
  });

test("a cancelled shared-load attempt returns its owner and reservation before another attempt compiles", async () => {
  const { pkg } = await syntheticChatPackage();
  const h = await heldLoads(pkg.manifest.capabilities);
  const supervisor = createPluginSupervisor({ executable: h.path });
  const registry = createPluginAttempts(supervisor, { maxStateBytes: 2048 });
  const create = (deadlineAt = Date.now() + 3000) =>
    registry.create(pkg, {
      tenant: "synthetic",
      attempt: crypto.randomUUID(),
      incarnation: crypto.randomUUID(),
      deadlineAt,
      maxStateBytes: 1024,
      config: {
        model_map: { logical: "upstream" },
        max_input_tokens: 10000,
        max_output_tokens: 10,
        auth: "none",
      },
    });
  try {
    const a = create();
    const first = a.invoke("describe");
    const firstResult = Promise.allSettled([first]);
    await h.loaded(supervisor, 1);
    await eventually(async () => (await supervisor.stats())?.loads === 1);
    for (const deadline of [false, true]) {
      const b = create(deadline ? Date.now() + 80 : undefined);
      const controller = new AbortController();
      const pending = b.invoke("describe", undefined, controller.signal);
      const outcome = pending.then(
        () => "RESOLVED",
        (error: { code: string }) => error.code,
      );
      if (!deadline) controller.abort();
      expect(await within(outcome)).toBe("CALL_CANCELLED");
      expect(b.snapshot()).toMatchObject({ disposed: true, busy: false });
      expect(registry.snapshot()).toMatchObject({
        attempts: 1,
        reservedStateBytes: 1024,
        stateBytes: 0,
      });
    }
    await h.release();
    expect(await first).toMatchObject({ kind: "description" });
    await firstResult;
    expect(a.snapshot()).toMatchObject({ disposed: false, sequence: 1 });
    a.dispose();
    expect(registry.snapshot()).toMatchObject({
      attempts: 0,
      stateBytes: 0,
      reservedStateBytes: 0,
      moduleBytes: 0,
    });
    expect(registry.snapshot().json.bytes).toBe(0);
  } finally {
    registry.dispose();
    await cleanup(supervisor);
    await h.cleanup();
  }
});

test("pre-aborted cold loads do not spawn and final waiter cancellation releases the underlying command", async () => {
  const h = await heldLoads();
  const host = createPluginSupervisor({ executable: h.path });
  const pre = new AbortController();
  pre.abort();
  try {
    await expect(host.load(module, pre.signal)).rejects.toMatchObject({ code: "CALL_CANCELLED" });
    expect(host.snapshot().starts).toBe(0);
    const a = new AbortController(),
      b = new AbortController();
    const first = host.load(module, a.signal),
      second = host.load(module, b.signal);
    const results = Promise.allSettled([first, second]);
    await h.loaded(host, 1);
    await eventually(async () => (await host.stats())?.loads === 1);
    a.abort();
    b.abort();
    expect((await results).every((result) => result.status === "rejected")).toBe(true);
    await eventually(() => host.snapshot().pending === 0);
    expect(await host.stats()).toMatchObject({ cancels: 1, pending: 0 });
    expect(host.snapshot()).toMatchObject({ loaded: 0, pendingBytes: 0, writeBytes: 0, starts: 1 });
    expect(getEventListeners(a.signal, "abort")).toHaveLength(0);
    expect(getEventListeners(b.signal, "abort")).toHaveLength(0);
    const future = host.load(module);
    await h.loaded(host, 2);
    await eventually(async () => (await host.stats())?.loads === 2);
    await h.release();
    await expect(future).resolves.toMatchObject({ connection: host.snapshot().connection });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

test("incomplete release publication cannot turn a shared compilation failure into helper stderr", async () => {
  const h = await heldLoads();
  const host = createPluginSupervisor({ executable: h.path });
  const a = new AbortController(),
    b = new AbortController();
  const writing = Promise.withResolvers<void>(),
    resume = Promise.withResolvers<void>();
  const write = Bun.write;
  let restoreWrite = () => {};
  let publication: ReturnType<typeof h.release> | undefined;
  try {
    const first = host.load(module, a.signal),
      second = host.load(module, b.signal);
    const results = Promise.allSettled([first, second]);
    await h.loaded(host, 1);
    await eventually(async () => (await host.stats())?.loads === 1);
    const interrupted = spyOn(Bun, "write").mockImplementation(async (...args) => {
      if (![join(h.dir, "release.tmp"), join(h.dir, "release.json")].includes(String(args[0])))
        return Reflect.apply(write, Bun, args) as ReturnType<typeof Bun.write>;
      await write(args[0], "");
      writing.resolve();
      await resume.promise;
      return Reflect.apply(write, Bun, args) as ReturnType<typeof Bun.write>;
    });
    restoreWrite = () => interrupted.mockRestore();
    publication = h.release({ error: "COMPILE_FAILED" });
    await Promise.race([
      writing.promise,
      publication.then(() => {
        throw new Error("RELEASE_NOT_PAUSED");
      }),
    ]);
    expect(host.snapshot().dead).toBe(false);
    const polls = (await host.stats())?.polls as number;
    await eventually(async () => {
      expect(host.snapshot().dead).toBe(false);
      return ((await host.stats())?.polls as number) > polls;
    });
    expect(await host.stats()).toMatchObject({ loads: 1, pending: 1 });
    expect(host.snapshot()).toMatchObject({ dead: false, loading: 1, waiters: 2 });
    resume.resolve();
    await publication;
    restoreWrite();
    for (const result of await results) {
      expect(result.status).toBe("rejected");
      if (result.status === "rejected")
        expect(result.reason).toMatchObject({ code: "COMPILE_FAILED" });
    }
    expect(host.snapshot()).toMatchObject({
      loaded: 0,
      pending: 0,
      pendingBytes: 0,
      writeBytes: 0,
    });
    expect(getEventListeners(a.signal, "abort")).toHaveLength(0);
    expect(getEventListeners(b.signal, "abort")).toHaveLength(0);
  } finally {
    resume.resolve();
    try {
      await publication;
    } finally {
      restoreWrite();
      try {
        await cleanup(host);
        expect(host.snapshot()).toMatchObject({
          timers: 0,
          loading: 0,
          waiters: 0,
          queued: 0,
          active: 0,
          inflight: 0,
        });
      } finally {
        await h.cleanup();
      }
    }
  }
});

for (const mode of ["together", "fragmented", "bytewise"])
  test(`two matching JSONL replies below their frame limit survive ${mode} transport chunks`, async () => {
    const h = await helper(`const pending=[];
function handle(r){
if(r.op!=="stats"){process.stdout.write(JSON.stringify({version:2,id:r.id})+"\\n");return;}
pending.push(r);if(pending.length!==2)return;
const bytes=Buffer.from(pending.map((r,i)=>JSON.stringify({version:2,id:r.id,stats:{index:i,padding:"é😀".repeat(480)}})+"\\r\\n").join(""));
if(${JSON.stringify(mode)}==="together")process.stdout.write(bytes);
else if(${JSON.stringify(mode)}==="fragmented"){let offset=0;const write=()=>{if(offset===bytes.length)return;const n=Math.min(bytes.length,offset+137);process.stdout.write(bytes.subarray(offset,n),()=>{offset=n;setImmediate(write);});};write();}
else {let offset=0;const write=()=>{if(offset===bytes.length)return;process.stdout.write(bytes.subarray(offset,++offset),()=>setImmediate(write));};write();}
}`);
    const host = createPluginSupervisor({ executable: h.path, maxFrameBytes: 4096 });
    try {
      await host.load(module);
      const values = await Promise.all([host.stats(), host.stats()]);
      expect(values.map((value) => value?.index)).toEqual([0, 1]);
      expect(values.map((value) => value?.padding)).toEqual(["é😀".repeat(480), "é😀".repeat(480)]);
      expect(host.snapshot()).toMatchObject({
        pending: 0,
        pendingBytes: 0,
        writeBytes: 0,
        starts: 1,
        dead: false,
      });
    } finally {
      await cleanup(host);
      await h.cleanup();
    }
  });

for (const mode of ["exact", "oversized-line", "oversized-suffix"])
  test(`JSONL ${mode} enforces each frame and incomplete suffix budget after complete replies`, async () => {
    const h = await helper(`let count=0;
function handle(r){if(r.op!=="stats"){process.stdout.write(JSON.stringify({version:2,id:r.id})+"\\n");return;}
const prefix=JSON.stringify({version:2,id:r.id,stats:{padding:""}});
const frame=JSON.stringify({version:2,id:r.id,stats:{padding:"x".repeat(4096-Buffer.byteLength(prefix)-1)}})+"\\n";
if(${JSON.stringify(mode)}==="exact")process.stdout.write(frame);
else if(++count===1)process.stdout.write(JSON.stringify({version:2,id:r.id,stats:{ok:true}})+"\\n"+(${JSON.stringify(mode)}==="oversized-line"?frame.slice(0,-1)+"x\\n":"x".repeat(4097)));
}`);
    const host = createPluginSupervisor({ executable: h.path, maxFrameBytes: 4096 });
    try {
      await host.load(module);
      if (mode === "exact") {
        expect((await host.stats())?.padding).toBeTypeOf("string");
        expect(host.snapshot().dead).toBe(false);
      } else {
        const first = host.stats(),
          second = host.stats();
        const results = Promise.allSettled([first, second]);
        const [a, b] = await results;
        expect(a?.status).toBe("fulfilled");
        expect(b?.status).toBe("rejected");
        if (b?.status === "rejected") expect(b.reason).toMatchObject({ code: "FRAME_BUDGET" });
        expect(host.snapshot()).toMatchObject({
          dead: true,
          pending: 0,
          pendingBytes: 0,
          writeBytes: 0,
        });
      }
    } finally {
      await cleanup(host);
      await h.cleanup();
    }
  });

test("successful shared and cached loads release every waiter signal listener", async () => {
  const h = await heldLoads();
  const host = createPluginSupervisor({ executable: h.path });
  const a = new AbortController(),
    b = new AbortController();
  try {
    const results = Promise.all([host.load(module, a.signal), host.load(module, b.signal)]);
    await h.loaded(host, 1);
    await eventually(async () => (await host.stats())?.loads === 1);
    await h.release();
    const [first, second] = await results;
    expect(first).toEqual(second);
    expect(getEventListeners(a.signal, "abort")).toHaveLength(0);
    expect(getEventListeners(b.signal, "abort")).toHaveLength(0);
    expect(await host.load(module, a.signal)).toEqual(first);
    a.abort();
    b.abort();
    expect(await host.stats()).toMatchObject({ loads: 1, cancels: 0, pending: 0 });
    expect(host.snapshot()).toMatchObject({
      pending: 0,
      pendingBytes: 0,
      writeBytes: 0,
      dead: false,
    });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

for (const cancellation of ["ack-only", "target-only"] as const)
  test(`a final load waiter cannot leave its helper alive with ${cancellation} cancellation completion`, async () => {
    const h = await heldLoads({}, cancellation);
    const host = createPluginSupervisor({ executable: h.path });
    const otherHelper = await helper(
      `function handle(r){process.stdout.write(JSON.stringify({version:2,id:r.id,...(r.op==="stats"?{stats:{intact:true}}:{})})+"\\n");}`,
    );
    const other = createPluginSupervisor({ executable: otherHelper.path });
    const controller = new AbortController();
    try {
      await other.load(module);
      const pending = host.load(module, controller.signal);
      const outcome = pending.then(
        () => "RESOLVED",
        (error: { code: string }) => error.code,
      );
      await h.loaded(host, 1);
      await eventually(async () => (await host.stats())?.loads === 1);
      const pid = host.snapshot().pid!;
      controller.abort();
      expect(await within(outcome)).toBe("CALL_CANCELLED");
      await eventually(() => host.snapshot().dead);
      await eventually(() => gone(pid));
      expect(host.snapshot()).toMatchObject({
        loaded: 0,
        pending: 0,
        pendingBytes: 0,
        writeBytes: 0,
      });
      expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
      expect(await other.stats()).toEqual({ intact: true });
      expect(other.snapshot().dead).toBe(false);
    } finally {
      await cleanup(host);
      await cleanup(other);
      await otherHelper.cleanup();
      await h.cleanup();
    }
  });

test("multiple complete JSONL replies settle before an oversized incomplete suffix retires the helper", async () => {
  const h = await helper(`const pending=[];
function handle(r){if(r.op!=="stats"){process.stdout.write(JSON.stringify({version:2,id:r.id})+"\\n");return;}pending.push(r);if(pending.length===3)process.stdout.write(pending.slice(0,2).map((r,i)=>JSON.stringify({version:2,id:r.id,stats:{index:i}})+"\\n").join("")+"x".repeat(4097));}`);
  const host = createPluginSupervisor({ executable: h.path, maxFrameBytes: 4096 });
  try {
    await host.load(module);
    const [first, second, third] = await Promise.allSettled([
      host.stats(),
      host.stats(),
      host.stats(),
    ]);
    expect(first).toMatchObject({ status: "fulfilled", value: { index: 0 } });
    expect(second).toMatchObject({ status: "fulfilled", value: { index: 1 } });
    expect(third?.status).toBe("rejected");
    if (third?.status === "rejected") expect(third.reason).toMatchObject({ code: "FRAME_BUDGET" });
    expect(host.snapshot()).toMatchObject({
      dead: true,
      pending: 0,
      pendingBytes: 0,
      writeBytes: 0,
    });
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});

test("unknown duplicate and retired connection replies cannot settle a new command", async () => {
  const h = await helper(`import {existsSync,readFileSync,writeFileSync} from "node:fs";
const file=new URL("last-id",import.meta.url);const old=existsSync(file)?readFileSync(file,"utf8"):"unknown:reply";let sequence=0;
function handle(r){const reply=fields=>JSON.stringify({version:2,id:r.id,...fields})+"\\n";
if(r.op!=="stats"){process.stdout.write(reply({}));return;}
sequence++;writeFileSync(file,r.id);process.stdout.write(JSON.stringify({version:2,id:old,stats:{stale:true}})+"\\n"+reply({stats:{sequence,stale:false}})+reply({stats:{stale:true}}));}`);
  const host = createPluginSupervisor({ executable: h.path, maxFrameBytes: 4096 });
  try {
    await host.load(module);
    expect(await host.stats()).toEqual({ sequence: 1, stale: false });
    const old = host.snapshot();
    expect(await host.stats()).toEqual({ sequence: 2, stale: false });
    process.kill(old.pid!, "SIGKILL");
    await eventually(() => host.snapshot().dead);
    await host.load(module);
    expect(await host.stats()).toEqual({ sequence: 1, stale: false });
    expect(host.snapshot().connection).not.toBe(old.connection);
    expect(host.snapshot()).toMatchObject({
      starts: 2,
      pending: 0,
      pendingBytes: 0,
      writeBytes: 0,
    });
    await eventually(() => gone(old.pid!));
  } finally {
    await cleanup(host);
    await h.cleanup();
  }
});
