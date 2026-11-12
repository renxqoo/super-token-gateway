import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { isAbsolute } from "node:path";
import { base64Schema, sha256 } from "@gateway/plugin-sdk";

type Operation = "load" | "call" | "stats" | "cancel" | "close" | "drain";
export type PluginHostDiagnostics = {
  phase: "startup" | "queued" | "writing" | "running" | "idle";
  op?: Operation;
  frameBytes: number;
  startupMs: number;
  writeMs: number;
  connection: string;
};
export class PluginHostError extends Error {
  constructor(
    readonly code: string,
    readonly diagnostics?: PluginHostDiagnostics,
  ) {
    super(code);
    this.name = "PluginHostError";
  }
}
export type NativeConfig = Partial<
  Record<
    | "slots"
    | "queue"
    | "maxBufferedOutputBytes"
    | "maxPendingBytes"
    | "maxFrameBytes"
    | "maxModuleBytes"
    | "maxCacheBytes"
    | "maxModules"
    | "maxInputBytes"
    | "maxOutputBytes"
    | "maxStateBytes"
    | "memoryPages"
    | "tableElements"
    | "compileMs"
    | "callMs"
    | "writeMs"
    | "startupMs"
    | "maxTotalRssBytes"
    | "maxWorkerRssBytes"
    | "goMemoryBytes",
    number
  >
>;
export type SupervisorOptions = {
  executable: string;
  native?: NativeConfig;
  maxPending?: number;
  maxPendingBytes?: number;
  maxWriteBytes?: number;
  maxFrameBytes?: number;
  queueMs?: number;
  heartbeatMs?: number;
  closeMs?: number;
  maxRestarts?: number;
};
type Reply = {
  version: 2;
  id: string;
  error?: string;
  output?: string;
  state?: string;
  stats?: Record<string, unknown>;
  event?: "heartbeat" | "ready";
};
type Command = {
  id: string;
  op: Operation;
  deadline: number;
  queuedAt?: number;
  frame: Buffer;
  bytes: number;
  phase: "startup" | "queued" | "writing" | "running";
  execution: boolean;
  timer: ReturnType<typeof setTimeout>;
  abort?: () => void;
  signal?: AbortSignal;
  resolve: (reply: Reply) => void;
  reject: (error: PluginHostError) => void;
  cancelCode?: string;
  cancelReply?: boolean;
  cancelAck?: boolean;
};
type ModuleLoad = {
  controller: AbortController;
  waiters: Set<(error?: PluginHostError) => void>;
  ready: boolean;
};
type Connection = {
  process: ChildProcessWithoutNullStreams;
  id: string;
  sequence: number;
  pending: Map<string, Command>;
  queue: Command[];
  writes: Command[];
  active: number;
  pendingBytes: number;
  writeBytes: number;
  inflight?: { cmd: Command; started: number; timer: ReturnType<typeof setTimeout> };
  started: number;
  readyAt?: number;
  startup?: ReturnType<typeof setTimeout>;
  failure?: PluginHostDiagnostics;
  failureCode?: string;
  buffer: Buffer;
  heartbeat: number;
  watch?: ReturnType<typeof setInterval>;
  dead: boolean;
  loaded: Map<string, ModuleLoad>;
  exit: Promise<void>;
  retire?: Promise<void>;
};
export function createPluginSupervisor(options: SupervisorOptions) {
  if (!isAbsolute(options.executable) || !["darwin", "linux"].includes(process.platform))
    throw new PluginHostError("CONFIG_INVALID");
  const native = { ...options.native, queue: 0 };
  const bounds: Record<string, readonly [number, number]> = {
    slots: [1, 64],
    queue: [0, 0],
    maxBufferedOutputBytes: [4096, 256 << 20],
    maxPendingBytes: [4096, 256 << 20],
    maxFrameBytes: [4096, 48 << 20],
    maxModuleBytes: [8, 32 << 20],
    maxCacheBytes: [8, 256 << 20],
    maxModules: [1, 32],
    maxInputBytes: [1, 16 << 20],
    maxOutputBytes: [1, 16 << 20],
    maxStateBytes: [1, 1 << 20],
    memoryPages: [64, 1024],
    tableElements: [1, 65536],
    compileMs: [1, 30000],
    callMs: [1, 30000],
    writeMs: [1, 30000],
    startupMs: [1, 30000],
    maxTotalRssBytes: [32 << 20, 8 * 1024 ** 3],
    maxWorkerRssBytes: [32 << 20, 4 * 1024 ** 3],
    goMemoryBytes: [16 << 20, 2 * 1024 ** 3],
  };
  for (const [key, value] of Object.entries(options.native ?? {})) {
    const range = bounds[key];
    if (!range || !Number.isSafeInteger(value) || value! < range[0] || value! > range[1])
      throw new PluginHostError("CONFIG_INVALID");
  }
  if (
    (native.maxCacheBytes ?? 32 << 20) < (native.maxModuleBytes ?? 8 << 20) ||
    (native.maxTotalRssBytes ?? 1024 ** 3) < (native.maxWorkerRssBytes ?? 512 << 20)
  )
    throw new PluginHostError("CONFIG_INVALID");
  for (const size of [
    native.maxModuleBytes ?? 8 << 20,
    (native.maxInputBytes ?? 256 << 10) + (native.maxStateBytes ?? 256 << 10),
    (native.maxOutputBytes ?? 256 << 10) + (native.maxStateBytes ?? 256 << 10),
  ])
    if ((native.maxFrameBytes ?? 12 << 20) < Math.ceil(size / 3) * 4 + 4096)
      throw new PluginHostError("CONFIG_INVALID");
  const limits = {
    maxPending: options.maxPending ?? 32,
    maxPendingBytes: options.maxPendingBytes ?? 64 << 20,
    maxWriteBytes: options.maxWriteBytes ?? 16 << 20,
    maxFrameBytes: options.maxFrameBytes ?? native.maxFrameBytes ?? 12 << 20,
    queueMs: options.queueMs ?? 1000,
    heartbeatMs: options.heartbeatMs ?? 3000,
    closeMs: options.closeMs ?? 2000,
    maxRestarts: options.maxRestarts ?? 2,
  };
  for (const [key, value] of Object.entries(limits))
    if (!Number.isSafeInteger(value) || value < (key === "maxRestarts" ? 0 : 1))
      throw new PluginHostError("CONFIG_INVALID");
  if (
    limits.maxFrameBytes > 48 << 20 ||
    limits.maxWriteBytes > 256 << 20 ||
    limits.maxPendingBytes > 256 << 20 ||
    limits.maxPending > 1024 ||
    limits.queueMs > 30000 ||
    limits.closeMs > 30000 ||
    limits.maxRestarts > 10 ||
    limits.heartbeatMs < 1200 ||
    limits.heartbeatMs > 30000
  )
    throw new PluginHostError("CONFIG_INVALID");
  let connection: Connection | undefined,
    starts = 0,
    closed = false;
  const retirements = new Set<Promise<void>>();
  const listeners = new Set<(connection: string) => void>();
  const slots = native.slots ?? 8;
  function killGroup(c: Connection) {
    if (c.process.pid)
      try {
        process.kill(-c.process.pid, "SIGKILL");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") c.process.kill("SIGKILL");
      }
  }
  function diagnostic(c: Connection, cmd?: Command): PluginHostDiagnostics {
    const elapsed = (start: number, end = performance.now()) =>
      Math.min(60000, Math.max(0, Math.round(end - start)));
    return {
      phase: c.readyAt === undefined ? "startup" : (cmd?.phase ?? "idle"),
      ...(cmd ? { op: cmd.op } : {}),
      frameBytes: c.inflight?.cmd.bytes ?? 0,
      startupMs: elapsed(c.started, c.readyAt),
      writeMs: c.inflight ? elapsed(c.inflight.started) : 0,
      connection: c.id,
    };
  }
  function settle(c: Connection, cmd: Command, reply?: Reply, error?: string) {
    if (!c.pending.delete(cmd.id)) return;
    clearTimeout(cmd.timer);
    if (cmd.abort && cmd.signal) cmd.signal.removeEventListener("abort", cmd.abort);
    c.pendingBytes -= cmd.bytes;
    const queued = c.queue.indexOf(cmd);
    if (queued >= 0) c.queue.splice(queued, 1);
    const writing = c.writes.indexOf(cmd);
    if (writing >= 0) {
      c.writes.splice(writing, 1);
      c.writeBytes -= cmd.frame.length;
    }
    if (c.inflight?.cmd !== cmd) cmd.frame = Buffer.alloc(0);
    if (!["startup", "queued"].includes(cmd.phase) && cmd.execution) c.active--;
    if (error) cmd.reject(new PluginHostError(error, c.failure ?? diagnostic(c, cmd)));
    else cmd.resolve(reply!);
    pump(c);
  }
  function fail(c: Connection, code: string): Promise<void> {
    if (c.retire) return c.retire;
    c.failure = diagnostic(c, c.inflight?.cmd ?? c.pending.values().next().value);
    c.failureCode = code;
    c.dead = true;
    clearTimeout(c.startup);
    delete c.startup;
    clearInterval(c.watch);
    delete c.watch;
    clearTimeout(c.inflight?.timer);
    delete c.inflight;
    for (const cmd of c.pending.values()) settle(c, cmd, undefined, code);
    c.queue.length = 0;
    c.writes.length = 0;
    c.buffer = Buffer.alloc(0);
    c.loaded.clear();
    c.writeBytes = 0;
    killGroup(c);
    c.process.stdin.destroy();
    c.process.stdout.destroy();
    c.process.stderr.destroy();
    c.retire = c.exit.then(() => {
      killGroup(c);
      c.process.removeAllListeners();
      c.process.stdin.removeAllListeners();
      c.process.stdout.removeAllListeners();
      c.process.stderr.removeAllListeners();
    });
    retirements.add(c.retire);
    void c.retire.finally(() => retirements.delete(c.retire!));
    for (const listener of listeners) {
      try {
        listener(c.id);
      } catch {}
    }
    return c.retire;
  }
  function read(c: Connection, chunk: Buffer) {
    if (c.dead) return;
    let offset = 0;
    while (offset < chunk.length) {
      const end = chunk.indexOf(10, offset);
      const length = (end < 0 ? chunk.length : end + 1) - offset;
      if (c.buffer.length + length > limits.maxFrameBytes) {
        void fail(c, "FRAME_BUDGET");
        return;
      }
      const part = chunk.subarray(offset, offset + length);
      const frame = c.buffer.length ? Buffer.concat([c.buffer, part]) : part;
      if (end < 0) {
        c.buffer = Buffer.from(frame);
        return;
      }
      const line = frame.subarray(0, frame.length - 1);
      c.buffer = Buffer.alloc(0);
      offset = end + 1;
      let r: Reply;
      try {
        r = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(line)) as Reply;
        if (
          !r ||
          typeof r !== "object" ||
          r.version !== 2 ||
          typeof r.id !== "string" ||
          Object.keys(r).some(
            (k) => !["version", "id", "error", "output", "state", "stats", "event"].includes(k),
          )
        )
          throw 0;
        if (r.event !== undefined) {
          if (r.id !== "" || Object.keys(r).length !== 3) throw 0;
          if (r.event === "ready" && c.readyAt === undefined) {
            if (
              performance.now() - c.started >= (native.startupMs ?? 5000) ||
              [...c.pending.values()].some((cmd) => performance.now() >= cmd.deadline)
            ) {
              void fail(c, "STARTUP_TIMEOUT");
              return;
            }
            c.readyAt = performance.now();
            clearTimeout(c.startup);
            delete c.startup;
            c.heartbeat = c.readyAt;
            c.watch = setInterval(
              () => {
                if (performance.now() - c.heartbeat > limits.heartbeatMs)
                  void fail(c, "HEARTBEAT_TIMEOUT");
              },
              Math.min(500, limits.heartbeatMs / 2),
            );
            for (const cmd of c.queue) {
              if (!c.pending.has(cmd.id)) continue;
              cmd.phase = "queued";
              cmd.queuedAt = c.readyAt;
              clearTimeout(cmd.timer);
              cmd.timer = setTimeout(
                () =>
                  timeout(
                    c,
                    cmd,
                    performance.now() >= cmd.deadline ? "CALL_TIMEOUT" : "QUEUE_TIMEOUT",
                  ),
                Math.min(limits.queueMs, Math.max(0, cmd.deadline - performance.now())),
              );
            }
            pump(c);
            continue;
          }
          if (r.event !== "heartbeat" || c.readyAt === undefined) throw 0;
          c.heartbeat = performance.now();
          continue;
        }
        if (
          c.readyAt === undefined ||
          !/^[A-Za-z0-9_.:-]{1,96}$/.test(r.id) ||
          (r.error !== undefined &&
            (typeof r.error !== "string" || !/^[A-Z_]{1,64}$/.test(r.error)))
        )
          throw 0;
        if (r.output !== undefined) base64Schema.parse(r.output);
        if (r.state !== undefined) base64Schema.parse(r.state);
        if (
          r.stats !== undefined &&
          (!r.stats || typeof r.stats !== "object" || Array.isArray(r.stats))
        )
          throw 0;
      } catch {
        void fail(c, "PROTOCOL_INVALID");
        return;
      }
      const cmd = c.pending.get(r.id);
      if (!cmd) continue;
      c.heartbeat = performance.now();
      if (cmd.phase === "startup" || cmd.phase === "queued") {
        void fail(c, "PROTOCOL_INVALID");
        return;
      }
      if (!cmd.cancelCode && performance.now() >= cmd.deadline) timeout(c, cmd, "CALL_TIMEOUT");
      if (!c.pending.has(cmd.id)) continue;
      if (cmd.cancelCode) {
        cmd.cancelReply = true;
        if (cmd.cancelAck) settle(c, cmd, undefined, cmd.cancelCode);
      } else settle(c, cmd, r, r.error);
    }
  }
  function write(c: Connection) {
    if (c.dead || c.readyAt === undefined || c.inflight) return;
    const cmd = c.writes.shift();
    if (!cmd) return;
    c.writeBytes -= cmd.frame.length;
    if (!c.pending.has(cmd.id)) {
      write(c);
      return;
    }
    if (performance.now() >= cmd.deadline) {
      timeout(c, cmd, "CALL_TIMEOUT");
      write(c);
      return;
    }
    cmd.phase = "writing";
    const inflight = {
      cmd,
      started: performance.now(),
      timer: setTimeout(() => {
        if (c.inflight === inflight) void fail(c, "WRITE_TIMEOUT");
      }, native.writeMs ?? 2000),
    };
    c.inflight = inflight;
    c.process.stdin.write(cmd.frame, (error) => {
      if (c.dead || c.inflight !== inflight) return;
      clearTimeout(inflight.timer);
      delete c.inflight;
      if (error) {
        void fail(c, "HELPER_FAILED");
        return;
      }
      if (c.pending.has(cmd.id)) cmd.phase = "running";
      write(c);
    });
  }
  function pump(c: Connection) {
    if (c.dead || c.readyAt === undefined) return;
    for (let i = 0; i < c.queue.length;) {
      const cmd = c.queue[i]!;
      if (!c.pending.has(cmd.id)) {
        c.queue.splice(i, 1);
        continue;
      }
      if (performance.now() >= cmd.deadline) {
        timeout(c, cmd, "CALL_TIMEOUT");
        if (c.dead) return;
        continue;
      }
      if (cmd.queuedAt !== undefined && performance.now() - cmd.queuedAt >= limits.queueMs) {
        settle(c, cmd, undefined, "QUEUE_TIMEOUT");
        continue;
      }
      if (cmd.execution && c.active >= slots) {
        i++;
        continue;
      }
      c.queue.splice(i, 1);
      if (c.writeBytes + cmd.frame.length > limits.maxWriteBytes) {
        settle(c, cmd, undefined, "WRITE_BUDGET");
        continue;
      }
      clearTimeout(cmd.timer);
      cmd.phase = "running";
      if (cmd.execution) c.active++;
      cmd.timer = setTimeout(
        () => timeout(c, cmd, "CALL_TIMEOUT"),
        Math.max(0, cmd.deadline - performance.now()),
      );
      c.writes.push(cmd);
      c.writeBytes += cmd.frame.length;
    }
    write(c);
  }
  function timeout(c: Connection, cmd: Command, code: string) {
    if (!c.pending.has(cmd.id) || cmd.cancelCode) return;
    if (cmd.phase === "startup") {
      if (code !== "CALL_CANCELLED") {
        void fail(c, "STARTUP_TIMEOUT");
        return;
      }
      settle(c, cmd, undefined, code);
      if (!c.pending.size) void fail(c, code);
      return;
    }
    if (cmd.phase === "writing" || !cmd.execution) {
      void fail(c, code);
      return;
    }
    if (cmd.phase === "queued") {
      settle(c, cmd, undefined, code);
      return;
    }
    cmd.cancelCode = code;
    clearTimeout(cmd.timer);
    cmd.timer = setTimeout(() => {
      void fail(c, "CANCEL_FAILED");
    }, 200);
    if (cmd.abort && cmd.signal) cmd.signal.removeEventListener("abort", cmd.abort);
    void send(c, "cancel", { target: cmd.id }, 200, undefined, false).then(
      () => {
        cmd.cancelAck = true;
        if (cmd.cancelReply) settle(c, cmd, undefined, code);
      },
      (error: unknown) => {
        if (error instanceof PluginHostError && error.code === "COMMAND_MISSING") {
          cmd.cancelAck = true;
          if (cmd.cancelReply) settle(c, cmd, undefined, code);
        } else void fail(c, "CANCEL_FAILED");
      },
    );
  }
  function start(): Connection {
    if (closed) throw new PluginHostError("HOST_CLOSED");
    if (connection && !connection.dead) return connection;
    if (starts > limits.maxRestarts) throw new PluginHostError("RESTART_BUDGET");
    starts++;
    const child = spawn(
      options.executable,
      ["--config", Buffer.from(JSON.stringify(native)).toString("base64")],
      {
        detached: true,
        stdio: ["pipe", "pipe", "pipe"],
        env: { GOMEMLIMIT: String(native.goMemoryBytes ?? 256 << 20), GOMAXPROCS: "2" },
      },
    );
    let finish!: () => void;
    const exit = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const c: Connection = {
      process: child,
      id: crypto.randomUUID(),
      sequence: 0,
      pending: new Map(),
      queue: [],
      writes: [],
      active: 0,
      pendingBytes: 0,
      writeBytes: 0,
      started: performance.now(),
      buffer: Buffer.alloc(0),
      heartbeat: performance.now(),
      dead: false,
      loaded: new Map(),
      exit,
    };
    child.on("error", () => {
      finish();
      void fail(c, "HELPER_FAILED");
    });
    child.on("exit", () => {
      finish();
      void fail(c, "HELPER_EXITED");
    });
    child.stdout.on("readable", () => {
      while (!c.dead) {
        const chunk = child.stdout.read(
          Math.min(child.stdout.readableLength, limits.maxFrameBytes),
        ) as Buffer | null;
        if (!chunk) break;
        read(c, chunk);
      }
    });
    child.stdout.on("error", () => {
      void fail(c, "HELPER_FAILED");
    });
    child.stdout.on("end", () => {
      void fail(c, "HELPER_EOF");
    });
    child.stdin.on("error", () => {
      void fail(c, "HELPER_FAILED");
    });
    child.stderr.on("data", () => {
      void fail(c, "HELPER_STDERR");
    });
    child.stderr.on("error", () => {
      void fail(c, "HELPER_FAILED");
    });
    c.startup = setTimeout(() => {
      void fail(c, "STARTUP_TIMEOUT");
    }, native.startupMs ?? 5000);
    connection = c;
    return c;
  }
  function send(
    c: Connection,
    op: Operation,
    fields: Record<string, unknown>,
    timeoutMs: number,
    signal?: AbortSignal,
    execution = true,
  ): Promise<Reply> {
    if (c.dead) return Promise.reject(new PluginHostError("HELPER_FAILED"));
    if (signal?.aborted) return Promise.reject(new PluginHostError("CALL_CANCELLED"));
    const id = `${c.id}:${++c.sequence}`;
    const frame = Buffer.from(JSON.stringify({ version: 2, id, op, ...fields }) + "\n");
    if (frame.length >= limits.maxFrameBytes)
      return Promise.reject(new PluginHostError("FRAME_BUDGET"));
    if (
      c.pending.size >= limits.maxPending + (execution ? 0 : 4) ||
      c.pendingBytes + frame.length > limits.maxPendingBytes
    )
      return Promise.reject(new PluginHostError("PENDING_BUDGET"));
    return new Promise((resolve, reject) => {
      const cmd: Command = {
        id,
        op,
        deadline: performance.now() + timeoutMs,
        ...(c.readyAt === undefined ? {} : { queuedAt: performance.now() }),
        frame,
        bytes: frame.length,
        phase: c.readyAt === undefined ? "startup" : "queued",
        execution,
        timer: setTimeout(
          () =>
            timeout(c, cmd, performance.now() >= cmd.deadline ? "CALL_TIMEOUT" : "QUEUE_TIMEOUT"),
          c.readyAt === undefined ? timeoutMs : Math.min(limits.queueMs, timeoutMs),
        ),
        resolve,
        reject,
      };
      if (signal) {
        cmd.signal = signal;
        cmd.abort = () => timeout(c, cmd, "CALL_CANCELLED");
        signal.addEventListener("abort", cmd.abort, { once: true });
      }
      c.pending.set(id, cmd);
      c.pendingBytes += cmd.bytes;
      c.queue.push(cmd);
      pump(c);
    });
  }
  return {
    onDisconnect(listener: (connection: string) => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    async load(module: Uint8Array, signal?: AbortSignal) {
      if (module.length > (native.maxModuleBytes ?? 8 << 20))
        throw new PluginHostError("MODULE_BUDGET");
      if (signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
      const c = start(),
        hash = sha256(module);
      let load = c.loaded.get(hash);
      if (!load) {
        load = { controller: new AbortController(), waiters: new Set(), ready: false };
        c.loaded.set(hash, load);
        const current = load;
        void send(
          c,
          "load",
          { hash, module: Buffer.from(module).toString("base64") },
          (native.compileMs ?? 5000) + 250,
          current.controller.signal,
        ).then(
          () => {
            if (c.dead) {
              for (const waiter of current.waiters)
                waiter(new PluginHostError(c.failureCode ?? "HELPER_FAILED", c.failure));
              return;
            }
            current.ready = true;
            for (const waiter of current.waiters) waiter();
          },
          (error: PluginHostError) => {
            if (c.loaded.get(hash) === current) c.loaded.delete(hash);
            for (const waiter of current.waiters) waiter(error);
          },
        );
        while (c.loaded.size > (native.maxModules ?? 4))
          c.loaded.delete(c.loaded.keys().next().value!);
      } else {
        c.loaded.delete(hash);
        c.loaded.set(hash, load);
      }
      const current = load;
      if (!current.ready)
        await new Promise<void>((resolve, reject) => {
          const finish = (error?: PluginHostError) => {
            if (!current.waiters.delete(finish)) return;
            signal?.removeEventListener("abort", abort);
            if (error) reject(error);
            else resolve();
          };
          const abort = () => {
            finish(new PluginHostError("CALL_CANCELLED"));
            if (!current.ready && !current.waiters.size) {
              if (c.loaded.get(hash) === current) c.loaded.delete(hash);
              current.controller.abort();
            }
          };
          current.waiters.add(finish);
          signal?.addEventListener("abort", abort, { once: true });
        });
      return { hash, connection: c.id };
    },
    async call(
      hash: string,
      input: Uint8Array,
      state: Uint8Array | undefined,
      options: { signal?: AbortSignal; timeoutMs?: number; function?: string } = {},
    ) {
      const c = start();
      if (!/^[a-f0-9]{64}$/.test(hash)) throw new PluginHostError("MODULE_HASH");
      const cached = c.loaded.get(hash);
      if (cached) {
        c.loaded.delete(hash);
        c.loaded.set(hash, cached);
      }
      if (
        input.length > (native.maxInputBytes ?? 256 << 10) ||
        (state?.length ?? 0) > (native.maxStateBytes ?? 256 << 10)
      )
        throw new PluginHostError("INPUT_BUDGET");
      const timeoutMs = Math.min(options.timeoutMs ?? native.callMs ?? 1000, native.callMs ?? 1000);
      if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new PluginHostError("DEADLINE");
      const r = await send(
        c,
        "call",
        {
          hash,
          function: options.function ?? "invoke",
          input: Buffer.from(input).toString("base64"),
          ...(state ? { state: Buffer.from(state).toString("base64") } : {}),
          timeoutMs,
        },
        timeoutMs + 200,
        options.signal,
      ).catch((error: unknown) => {
        c.loaded.delete(hash);
        throw error;
      });
      if (r.output === undefined || r.state === undefined) {
        await fail(c, "PROTOCOL_INVALID");
        throw new PluginHostError("PROTOCOL_INVALID");
      }
      const output = new Uint8Array(Buffer.from(r.output, "base64")),
        next = new Uint8Array(Buffer.from(r.state, "base64"));
      if (
        output.length > (native.maxOutputBytes ?? 256 << 10) ||
        next.length > (native.maxStateBytes ?? 256 << 10)
      ) {
        await fail(c, "OUTPUT_BUDGET");
        throw new PluginHostError("OUTPUT_BUDGET");
      }
      return { output, state: next };
    },
    async stats() {
      return (await send(start(), "stats", {}, 1000, undefined, false)).stats;
    },
    snapshot() {
      const c = connection;
      return {
        starts,
        observers: listeners.size,
        pid: c?.process.pid,
        connection: c?.id,
        pending: c?.pending.size ?? 0,
        pendingBytes: c?.pendingBytes ?? 0,
        writeBytes: c?.writeBytes ?? 0,
        dead: c?.dead ?? true,
        closed,
        loaded: c ? [...c.loaded.values()].filter((load) => load.ready).length : 0,
        loading: c ? [...c.loaded.values()].filter((load) => !load.ready).length : 0,
        waiters: c ? [...c.loaded.values()].reduce((n, load) => n + load.waiters.size, 0) : 0,
        queued: c?.queue.filter((cmd) => c.pending.has(cmd.id)).length ?? 0,
        active: c?.active ?? 0,
        inflight: c?.inflight ? 1 : 0,
        timers: c
          ? c.pending.size + Number(!!c.startup) + Number(!!c.watch) + Number(!!c.inflight)
          : 0,
        ready: c?.readyAt !== undefined && !c.dead,
        diagnostics: c
          ? (c.failure ?? diagnostic(c, c.inflight?.cmd ?? c.pending.values().next().value))
          : undefined,
      };
    },
    async close(drain = false) {
      if (closed) {
        await Promise.all(retirements);
        listeners.clear();
        return;
      }
      closed = true;
      const c = connection;
      if (c && !c.dead) {
        if (drain && c.readyAt !== undefined) {
          const deadline = performance.now() + limits.closeMs;
          while (c.pending.size && !c.dead && performance.now() < deadline)
            await new Promise((resolve) => setTimeout(resolve, 5));
          if (c.pending.size) {
            await fail(c, "DRAIN_TIMEOUT");
            await Promise.all(retirements);
            listeners.clear();
            return;
          }
        }
        if (c.readyAt !== undefined)
          try {
            await send(c, drain ? "drain" : "close", {}, limits.closeMs, undefined, false);
          } catch {}
        await fail(c, "HOST_CLOSED");
      }
      await Promise.all(retirements);
      listeners.clear();
    },
  };
}
export type PluginSupervisor = ReturnType<typeof createPluginSupervisor>;
