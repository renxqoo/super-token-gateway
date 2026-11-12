import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { resolve } from "node:path";

type Reply = {
  version: number;
  id: string;
  event?: string;
  error?: string;
  output?: string;
  state?: string;
  stats?: {
    pending: number;
    active: number;
    modules: number;
    workers: number[];
    peakTotalRssBytes: number;
    rssBytes: number;
    workerRssBytes: number;
    pendingBytes: number;
    bufferedOutputBytes: number;
  };
};
const binary = resolve(process.argv[2] ?? ".local/plugin-host");
const moduleBytes = new Uint8Array(
  await Bun.file(resolve(import.meta.dir, "testdata/plugin.wasm")).arrayBuffer(),
);
const hash = createHash("sha256").update(moduleBytes).digest("hex");
const moduleBase64 = Buffer.from(moduleBytes).toString("base64");
function host() {
  const processHandle = Bun.spawn([binary], {
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
    env: {},
  });
  const pending = new Map<
    string,
    { resolve: (reply: Reply) => void; reject: (error: Error) => void }
  >();
  let counter = 0,
    readyCount = 0;
  let markReady!: () => void;
  const ready = new Promise<void>((resolve) => {
    markReady = resolve;
  });
  const reader = processHandle.stdout.getReader();
  const reading = (async () => {
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        buffer += decoder.decode(part.value, { stream: true });
        let index: number;
        while ((index = buffer.indexOf("\n")) >= 0) {
          const reply = JSON.parse(buffer.slice(0, index)) as Reply;
          buffer = buffer.slice(index + 1);
          assert.equal(reply.version, 2);
          if (reply.event === "ready") {
            assert.equal(reply.id, "");
            assert.equal(++readyCount, 1);
            markReady();
            continue;
          }
          assert.equal(readyCount, 1);
          if (reply.event === "heartbeat") continue;
          const task = pending.get(reply.id);
          assert(task, "unmatched command response");
          pending.delete(reply.id);
          task.resolve(reply);
        }
      }
    } finally {
      for (const task of pending.values()) task.reject(new Error("native host closed"));
      pending.clear();
    }
  })();
  const request = async (value: Record<string, unknown>) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        ready,
        processHandle.exited.then(() => {
          throw new Error("native host closed before READY");
        }),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("native host startup timeout")), 5000);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
    const id = `bun-${++counter}`;
    const response = new Promise<Reply>((resolve, reject) => pending.set(id, { resolve, reject }));
    void response.catch(() => undefined);
    try {
      processHandle.stdin.write(JSON.stringify({ version: 2, id, ...value }) + "\n");
      await processHandle.stdin.flush();
    } catch (error) {
      pending.delete(id);
      throw error;
    }
    return await response;
  };
  const call = async (input: unknown, state: string | undefined = undefined) => {
    const reply = await request({
      op: "call",
      hash,
      function: "invoke",
      input: Buffer.from(JSON.stringify(input)).toString("base64"),
      state,
    });
    assert.equal(reply.error, undefined);
    return reply;
  };
  const exit = async () => {
    const code = await processHandle.exited;
    await reading;
    const stderr = await new Response(processHandle.stderr).text();
    assert.equal(stderr, "");
    assert.equal(pending.size, 0);
    return code;
  };
  return { processHandle, reader, request, call, exit, reading };
}
async function workersGone(pids: number[]) {
  for (let n = 0; n < 300; n++) {
    const alive = pids.some((pid) => {
      try {
        process.kill(pid, 0);
        return true;
      } catch {
        return false;
      }
    });
    if (!alive) return;
    await Bun.sleep(10);
  }
  assert.fail("module worker survived supervisor exit");
}
for (const mode of ["eof", "kill", "stdout"] as const) {
  const p = host();
  assert.equal((await p.request({ op: "load", hash, module: moduleBase64 })).error, undefined);
  const stats = (await p.request({ op: "stats" })).stats!;
  if (mode === "eof") p.processHandle.stdin.end();
  else if (mode === "kill") p.processHandle.kill(9);
  else await p.reader.cancel();
  const code = await p.exit();
  if (mode === "eof") assert.equal(code, 0);
  else assert.notEqual(code, 0);
  await workersGone(stats.workers);
  console.log(
    JSON.stringify({ case: `bun-supervision-${mode}`, exit: code, workers: 0, pending: 0 }),
  );
}
const p = host();
try {
  assert.equal((await p.request({ op: "load", hash, module: moduleBase64 })).error, undefined);
  const before = (await p.request({ op: "stats" })).stats;
  const batch = Array.from(
    Buffer.from('data: {"choices":[{"delta":{"content":"x"}}]}\n\n'.repeat(32)),
  );
  const latencies: number[] = [];
  const started = performance.now();
  await Promise.all(
    Array.from({ length: 8 }, async (_, lane) => {
      let state = (await p.call({ op: "begin", id: `lane-${lane}` })).state;
      for (let i = 0; i < 64; i++) {
        const start = performance.now();
        const reply = await p.call({ op: "push", bytes: batch }, state);
        latencies.push(performance.now() - start);
        state = reply.state;
      }
      state = (
        await p.call({ op: "push", bytes: Array.from(Buffer.from("data: [DONE]\n\n")) }, state)
      ).state;
      const end = await p.call({ op: "end" }, state);
      const summary = JSON.parse(Buffer.from(end.output!, "base64").toString()) as {
        text: string;
        frames: number;
      };
      assert.equal(summary.text, "x".repeat(2048));
      assert.equal(summary.frames, 2049);
    }),
  );
  const elapsedMs = performance.now() - started;
  const after = (await p.request({ op: "stats" })).stats!;
  latencies.sort((a, b) => a - b);
  const close = await p.request({ op: "close" });
  assert.equal(close.stats!.pending, 0);
  assert.equal(close.stats!.modules, 0);
  assert.equal(close.stats!.bufferedOutputBytes, 0);
  assert.equal(await p.exit(), 0);
  await workersGone(after.workers);
  console.log(
    JSON.stringify({
      case: "bounded-observation",
      lanes: 8,
      batchesPerLane: 64,
      framesPerBatch: 32,
      frames: 16384,
      elapsedMs,
      framesPerSecond: 16384 / (elapsedMs / 1000),
      batchP95Ms: latencies[Math.floor(latencies.length * 0.95)],
      before,
      after,
      cleanup: close.stats,
    }),
  );
} finally {
  p.processHandle.stdin.end();
  if (
    (await Promise.race([
      p.processHandle.exited.then(() => true),
      Bun.sleep(2000).then(() => false),
    ])) === false
  )
    p.processHandle.kill(9);
  await p.processHandle.exited;
}
