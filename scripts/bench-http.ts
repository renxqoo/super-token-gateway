import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { get as httpGet, request as httpRequest, type Agent } from "node:http";
import { arch, cpus, platform, release, totalmem } from "node:os";
import { join } from "node:path";
import type { SQL } from "bun";

export type BenchTarget = { host: string; port: number };

export function benchmarkIngressEnvironment(env = process.env): Record<string, string> {
  const settings: Record<string, string> = {};
  for (const [name, value] of Object.entries(env)) {
    if (
      value !== undefined &&
      /^BENCH_(?:INGRESS_[A-Z_]+|MAX_BODY_BYTES|BODY_READ_TIMEOUT_MS)$/.test(name)
    ) {
      settings[name.slice("BENCH_".length)] = value;
    }
  }
  return settings;
}

export function benchmarkConcurrency(raw: string | undefined, fallback: number[]): number[] {
  if (!raw) return fallback;
  const values = raw.split(",").map(Number);
  if (values.some((value) => !Number.isInteger(value) || value < 1 || value > 16384)) {
    throw new Error("BENCH_JSON_CONCURRENCY requires comma-separated integers from 1 to 16384");
  }
  return values;
}

export function postJsonKeepAlive(
  agent: Agent,
  port: number,
  apiKey: string,
  payload: Buffer,
  expectDone: boolean,
  expectedText?: string,
  host = "127.0.0.1",
): Promise<{ ok: boolean; status: number; errText?: string }> {
  return new Promise((resolveDone) => {
    const req = httpRequest(
      {
        hostname: host,
        port,
        path: "/v1/chat/completions",
        method: "POST",
        agent,
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          "content-length": payload.byteLength,
        },
      },
      (res) => {
        const status = res.statusCode ?? 0;
        let sawDone = !expectDone;
        let tail = "";
        let fixtureBody = "";
        const errChunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => {
          if (expectedText && fixtureBody.length < 16384) fixtureBody += chunk.toString("utf8");
          if (status !== 200 && errChunks.length < 4) errChunks.push(chunk);
          if (!sawDone && status === 200) {
            const next = tail + chunk.toString("utf8");
            const terminal = "data: [DONE]\n\n";
            sawDone = next.includes(terminal);
            tail = next.slice(1 - terminal.length);
          }
        });
        res.on("end", () =>
          resolveDone({
            ok:
              status === 200 &&
              sawDone &&
              (expectedText === undefined ||
                (fixtureBody.includes(expectedText) && !fixtureBody.includes("alice@example.com"))),
            status,
            ...(status !== 200
              ? { errText: Buffer.concat(errChunks).toString("utf8").slice(0, 160) }
              : {}),
          }),
        );
        res.on("error", (e) => resolveDone({ ok: false, status, errText: e.message }));
      },
    );
    req.on("error", (e) => resolveDone({ ok: false, status: 0, errText: e.message }));
    req.setTimeout(expectedText ? 15_000 : 360_000, () =>
      req.destroy(new Error("Benchmark HTTP request timed out")),
    );
    req.end(payload);
  });
}

export async function warmHttpConnections(
  agent: Agent,
  port: number,
  target: number,
  host = "127.0.0.1",
) {
  for (let warm = Math.min(64, target); ; warm = Math.min(warm + 64, target)) {
    await Promise.all(
      Array.from(
        { length: warm },
        () =>
          new Promise<void>((done, fail) => {
            const req = httpGet({ host, port, path: "/livez", agent }, (response) => {
              response.resume();
              response.on("error", fail);
              response.on("end", () => {
                if (response.statusCode === 200) done();
                else fail(new Error(`Warmup HTTP status ${response.statusCode}`));
              });
            });
            req.on("error", fail);
            req.setTimeout(5000, () => req.destroy(new Error("Warmup HTTP request timed out")));
          }),
      ),
    );
    if (warm === target) return;
    await Bun.sleep(10);
  }
}

export type BenchResponse = { ok: boolean; status: number; errText?: string };

const HDR_SEP = Buffer.from("\r\n\r\n");
const LINE_SEP = Buffer.from("\r\n");
const DONE_MARKER = "data: [DONE]\n\n";

type FastExchange = {
  expectDone: boolean;
  expectedText: string | undefined;
  sawDone: boolean;
  tail: string;
  fixtureBody: string;
  errText: string;
  headersParsed: boolean;
  headerBuf: Buffer | null;
  status: number;
  connectionClose: boolean;
  isChunked: boolean;
  remaining: number;
  chunkBuf: Buffer;
  chunkState: "size" | "data" | "crlf" | "trailers";
  chunkRemaining: number;
  pendingWrite: string | null;
  deadlineAt: number;
  done: boolean;
  resolve: (res: BenchResponse) => void;
};

type FastConn = {
  targetIdx: number;
  socket: Bun.Socket<FastConn> | null;
  closed: boolean;
  exchange: FastExchange | null;
  writeBuf: Buffer | null;
  writeOffset: number;
};

export function buildPostRequest(target: BenchTarget, apiKey: string, payload: Buffer): string {
  const body = payload.toString("utf8");
  return (
    `POST /v1/chat/completions HTTP/1.1\r\n` +
    `host: ${target.host}:${target.port}\r\n` +
    `authorization: Bearer ${apiKey}\r\n` +
    `content-type: application/json\r\n` +
    `content-length: ${payload.byteLength}\r\n` +
    `connection: keep-alive\r\n\r\n` +
    body
  );
}

export function createFastHttp1BenchClient(
  targets: readonly BenchTarget[],
  maxIdlePerTarget = 2048,
) {
  const idleByTarget: FastConn[][] = targets.map(() => []);
  const allConns = new Set<FastConn>();
  let closed = false;

  const terminate = (sock: Bun.Socket<FastConn>) => {
    try {
      sock.terminate();
    } catch {
      try {
        sock.end();
      } catch {
        void 0;
      }
    }
  };

  const destroyConn = (conn: FastConn) => {
    allConns.delete(conn);
    conn.closed = true;
    conn.writeBuf = null;
    conn.writeOffset = 0;
    const sock = conn.socket;
    conn.socket = null;
    if (sock) terminate(sock);
  };

  const finishExchange = (conn: FastConn, ex: FastExchange, reuse: boolean, errMsg?: string) => {
    if (ex.done) return;
    ex.done = true;
    conn.exchange = null;
    if (errMsg !== undefined || !reuse || ex.connectionClose || closed || conn.closed) {
      destroyConn(conn);
    } else {
      const pool = idleByTarget[conn.targetIdx]!;
      if (pool.length < maxIdlePerTarget) pool.push(conn);
      else destroyConn(conn);
    }
    if (errMsg !== undefined) {
      ex.resolve({ ok: false, status: ex.status, errText: errMsg });
      return;
    }
    const ok =
      ex.status === 200 &&
      ex.sawDone &&
      (ex.expectedText === undefined ||
        (ex.fixtureBody.includes(ex.expectedText) &&
          !ex.fixtureBody.includes("alice@example.com")));
    ex.resolve(
      ex.status !== 200
        ? { ok, status: ex.status, errText: ex.errText.slice(0, 160) }
        : { ok, status: ex.status },
    );
  };

  const writeRaw = (conn: FastConn, raw: string) => {
    const sock = conn.socket;
    if (!sock) return;
    const written = sock.write(raw);
    const total = Buffer.byteLength(raw, "utf8");
    if (written < 0) {
      const ex = conn.exchange;
      if (ex) finishExchange(conn, ex, false, "socket write failed");
      else destroyConn(conn);
      return;
    }
    if (written < total) {
      conn.writeBuf = Buffer.from(raw, "utf8");
      conn.writeOffset = written;
    }
  };

  const flushPending = (conn: FastConn) => {
    const sock = conn.socket;
    if (!sock || !conn.writeBuf) return;
    while (conn.writeOffset < conn.writeBuf.byteLength) {
      const slice =
        conn.writeOffset === 0 ? conn.writeBuf : conn.writeBuf.subarray(conn.writeOffset);
      const written = sock.write(slice);
      if (written < 0) {
        const ex = conn.exchange;
        if (ex) finishExchange(conn, ex, false, "socket write failed");
        else destroyConn(conn);
        return;
      }
      if (written === 0) return;
      conn.writeOffset += written;
    }
    conn.writeBuf = null;
    conn.writeOffset = 0;
  };

  const observeBody = (ex: FastExchange, slice: Buffer) => {
    if (slice.byteLength === 0) return;
    if (ex.status !== 200 && ex.errText.length < 160) {
      ex.errText += slice.toString("utf8");
    }
    if (ex.expectedText !== undefined && ex.fixtureBody.length < 16384) {
      ex.fixtureBody += slice.toString("utf8");
    }
    if (!ex.sawDone && ex.status === 200) {
      const next = ex.tail + slice.toString("utf8");
      ex.sawDone = next.includes(DONE_MARKER);
      ex.tail = next.slice(1 - DONE_MARKER.length);
    }
  };

  const feedChunked = (conn: FastConn, ex: FastExchange, data: Buffer) => {
    const buf = ex.chunkBuf.byteLength > 0 ? Buffer.concat([ex.chunkBuf, data]) : data;
    let offset = 0;
    while (offset < buf.byteLength && !ex.done) {
      if (ex.chunkState === "size") {
        const idx = buf.indexOf(LINE_SEP, offset);
        if (idx === -1) break;
        const hex = buf.subarray(offset, idx).toString("latin1").split(";")[0]!.trim();
        const size = Number.parseInt(hex, 16);
        if (!Number.isSafeInteger(size) || size < 0) {
          finishExchange(conn, ex, false, "invalid chunk size");
          return;
        }
        offset = idx + 2;
        if (size === 0) {
          ex.chunkState = "trailers";
        } else {
          ex.chunkState = "data";
          ex.chunkRemaining = size;
        }
      } else if (ex.chunkState === "data") {
        const take = Math.min(buf.byteLength - offset, ex.chunkRemaining);
        if (take > 0) {
          observeBody(ex, buf.subarray(offset, offset + take));
          offset += take;
          ex.chunkRemaining -= take;
        }
        if (ex.chunkRemaining === 0) ex.chunkState = "crlf";
      } else if (ex.chunkState === "crlf") {
        if (buf.byteLength - offset < 2) break;
        offset += 2;
        ex.chunkState = "size";
      } else {
        if (buf.byteLength - offset >= 2 && buf[offset] === 13 && buf[offset + 1] === 10) {
          offset += 2;
          ex.chunkBuf = Buffer.alloc(0);
          finishExchange(conn, ex, offset === buf.byteLength);
          return;
        }
        const end = buf.indexOf(HDR_SEP, offset);
        if (end === -1) break;
        offset = end + 4;
        ex.chunkBuf = Buffer.alloc(0);
        finishExchange(conn, ex, offset === buf.byteLength);
        return;
      }
    }
    if (!ex.done) {
      ex.chunkBuf = offset < buf.byteLength ? Buffer.from(buf.subarray(offset)) : Buffer.alloc(0);
    }
  };

  const feedBody = (conn: FastConn, ex: FastExchange, data: Buffer) => {
    if (ex.done || data.byteLength === 0) return;
    if (ex.isChunked) {
      feedChunked(conn, ex, data);
      return;
    }
    if (ex.remaining >= 0) {
      const take = Math.min(data.byteLength, ex.remaining);
      if (take > 0) {
        observeBody(ex, data.subarray(0, take));
        ex.remaining -= take;
      }
      if (ex.remaining === 0) {
        finishExchange(conn, ex, data.byteLength === take);
      }
      return;
    }
    observeBody(ex, data);
  };

  const handlers: Bun.SocketHandler<FastConn> = {
    open(socket) {
      const conn = socket.data;
      if (closed || conn.closed) {
        terminate(socket);
        return;
      }
      conn.socket = socket;
      const ex = conn.exchange;
      if (ex && ex.pendingWrite !== null) {
        const raw = ex.pendingWrite;
        ex.pendingWrite = null;
        writeRaw(conn, raw);
      }
    },
    data(socket, chunk) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.done) {
        destroyConn(conn);
        return;
      }
      if (!ex.headersParsed) {
        const buf = ex.headerBuf ? Buffer.concat([ex.headerBuf, chunk]) : chunk;
        const sep = buf.indexOf(HDR_SEP);
        if (sep === -1) {
          ex.headerBuf = ex.headerBuf ? buf : Buffer.from(buf);
          return;
        }
        ex.headerBuf = null;
        const head = buf.subarray(0, sep).toString("latin1");
        const firstEnd = head.indexOf("\r\n");
        const statusLine = firstEnd === -1 ? head : head.slice(0, firstEnd);
        ex.status = Number(statusLine.slice(9, 12)) || 0;
        let pos = firstEnd === -1 ? head.length : firstEnd + 2;
        while (pos < head.length) {
          let next = head.indexOf("\r\n", pos);
          if (next === -1) next = head.length;
          const colon = head.indexOf(":", pos);
          if (colon > pos && colon < next) {
            const name = head.slice(pos, colon).toLowerCase();
            const val = head.slice(colon + 1, next).trim();
            if (name === "content-length") {
              ex.remaining = Number(val);
            } else if (name === "transfer-encoding" && val.toLowerCase().includes("chunked")) {
              ex.isChunked = true;
            } else if (name === "connection" && val.toLowerCase().includes("close")) {
              ex.connectionClose = true;
            }
          }
          pos = next + 2;
        }
        ex.headersParsed = true;
        const rest = buf.subarray(sep + 4);
        if (!ex.isChunked && ex.remaining === 0) {
          finishExchange(conn, ex, rest.byteLength === 0);
          return;
        }
        if (rest.byteLength > 0) feedBody(conn, ex, rest);
        return;
      }
      feedBody(conn, ex, chunk);
    },
    drain(socket) {
      flushPending(socket.data);
    },
    end(socket) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.done) {
        destroyConn(conn);
        return;
      }
      if (ex.headersParsed && !ex.isChunked && ex.remaining < 0) {
        finishExchange(conn, ex, false);
        return;
      }
      finishExchange(conn, ex, false, "connection closed before response completed");
    },
    close(socket) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.done) {
        destroyConn(conn);
        return;
      }
      finishExchange(conn, ex, false, "connection closed");
    },
    error(socket, error) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.done) {
        destroyConn(conn);
        return;
      }
      finishExchange(conn, ex, false, error.message);
    },
    connectError(socket, error) {
      const conn = socket.data;
      const ex = conn.exchange;
      if (!ex || ex.done) {
        destroyConn(conn);
        return;
      }
      finishExchange(conn, ex, false, error.message);
    },
  };

  const sweepTimer = setInterval(() => {
    const now = performance.now();
    for (const conn of allConns) {
      const ex = conn.exchange;
      if (ex && !ex.done && now >= ex.deadlineAt) {
        finishExchange(conn, ex, false, "request timeout");
      }
    }
  }, 500);
  sweepTimer.unref();

  function sendRaw(
    targetIdx: number,
    rawRequest: string,
    expectDone: boolean,
    expectedText?: string,
    timeoutMs = 30_000,
  ): Promise<BenchResponse> {
    if (closed) return Promise.resolve({ ok: false, status: 0, errText: "client closed" });
    return new Promise((resolve) => {
      const pool = idleByTarget[targetIdx]!;
      let reused: FastConn | undefined;
      while (pool.length > 0) {
        const cand = pool.pop()!;
        if (!cand.closed && cand.socket) {
          reused = cand;
          break;
        }
      }
      const ex: FastExchange = {
        expectDone,
        expectedText,
        sawDone: !expectDone,
        tail: "",
        fixtureBody: "",
        errText: "",
        headersParsed: false,
        headerBuf: null,
        status: 0,
        connectionClose: false,
        isChunked: false,
        remaining: -1,
        chunkBuf: Buffer.alloc(0),
        chunkState: "size",
        chunkRemaining: 0,
        pendingWrite: null,
        deadlineAt: performance.now() + timeoutMs,
        done: false,
        resolve,
      };
      if (reused) {
        reused.exchange = ex;
        writeRaw(reused, rawRequest);
        return;
      }
      const target = targets[targetIdx]!;
      const conn: FastConn = {
        targetIdx,
        socket: null,
        closed: false,
        exchange: ex,
        writeBuf: null,
        writeOffset: 0,
      };
      ex.pendingWrite = rawRequest;
      allConns.add(conn);
      void Bun.connect<FastConn>({
        hostname: target.host,
        port: target.port,
        data: conn,
        socket: handlers,
      }).catch((err: unknown) => {
        finishExchange(conn, ex, false, err instanceof Error ? err.message : String(err));
      });
    });
  }

  return {
    sendRaw,
    async warm(perTarget: number): Promise<void> {
      for (let t = 0; t < targets.length; t++) {
        const target = targets[t]!;
        const pool = idleByTarget[t]!;
        const liveReq = `GET /livez HTTP/1.1\r\nhost: ${target.host}:${target.port}\r\nconnection: keep-alive\r\n\r\n`;
        while (pool.length < perTarget) {
          const need = Math.min(64, perTarget - pool.length);
          const results = await Promise.all(
            Array.from({ length: need }, () => {
              return new Promise<BenchResponse>((resolve) => {
                const ex: FastExchange = {
                  expectDone: false,
                  expectedText: undefined,
                  sawDone: true,
                  tail: "",
                  fixtureBody: "",
                  errText: "",
                  headersParsed: false,
                  headerBuf: null,
                  status: 0,
                  connectionClose: false,
                  isChunked: false,
                  remaining: -1,
                  chunkBuf: Buffer.alloc(0),
                  chunkState: "size",
                  chunkRemaining: 0,
                  pendingWrite: liveReq,
                  deadlineAt: performance.now() + 10_000,
                  done: false,
                  resolve,
                };
                const conn: FastConn = {
                  targetIdx: t,
                  socket: null,
                  closed: false,
                  exchange: ex,
                  writeBuf: null,
                  writeOffset: 0,
                };
                allConns.add(conn);
                void Bun.connect<FastConn>({
                  hostname: target.host,
                  port: target.port,
                  data: conn,
                  socket: handlers,
                }).catch((err: unknown) => {
                  finishExchange(conn, ex, false, err instanceof Error ? err.message : String(err));
                });
              });
            }),
          );
          const bad = results.find((r) => !r.ok);
          if (bad) {
            throw new Error(
              `Fast warmup failed on target ${t}: status=${bad.status} ${bad.errText ?? ""}`,
            );
          }
          await Bun.sleep(10);
        }
      }
    },
    close(): void {
      closed = true;
      clearInterval(sweepTimer);
      for (const conn of allConns) {
        const ex = conn.exchange;
        if (ex && !ex.done) finishExchange(conn, ex, false, "client closed");
        else destroyConn(conn);
      }
      for (const pool of idleByTarget) pool.length = 0;
      allConns.clear();
    },
  };
}

export async function readProcessStats(
  pids: Record<string, number>,
): Promise<Record<string, { rssMb: number; cpuPct: number }>> {
  const entries = Object.entries(pids).filter(([, pid]) => Number.isInteger(pid) && pid > 0);
  if (entries.length === 0) return {};
  const proc = Bun.spawn(
    ["ps", "-o", "pid=,rss=,%cpu=", "-p", entries.map(([, pid]) => String(pid)).join(",")],
    { stdout: "pipe", stderr: "ignore" },
  );
  const output = await new Response(proc.stdout).text();
  await proc.exited;
  const byPid = new Map<number, { rssMb: number; cpuPct: number }>();
  for (const line of output.trim().split("\n")) {
    if (!line.trim()) continue;
    const parts = line.trim().split(/\s+/);
    const pid = Number(parts[0]);
    const rssKb = Number(parts[1] ?? 0);
    const cpuPct = Number(parts[2] ?? 0);
    if (Number.isFinite(pid)) {
      byPid.set(pid, {
        rssMb: Number.isFinite(rssKb) ? Number((rssKb / 1024).toFixed(1)) : 0,
        cpuPct: Number.isFinite(cpuPct) ? Number(cpuPct.toFixed(1)) : 0,
      });
    }
  }
  const out: Record<string, { rssMb: number; cpuPct: number }> = {};
  for (const [name, pid] of entries) {
    out[name] = byPid.get(pid) ?? { rssMb: 0, cpuPct: 0 };
  }
  return out;
}

export type CheckpointerStats = {
  num_timed: number;
  num_requested: number;
  write_time_ms: number;
  sync_time_ms: number;
  buffers_written: number;
};

export async function readCheckpointerStats(db: SQL): Promise<CheckpointerStats | null> {
  try {
    const [row] = await db<
      {
        num_timed: string;
        num_requested: string;
        write_time: string;
        sync_time: string;
        buffers_written: string;
      }[]
    >`
      SELECT num_timed::text, num_requested::text, write_time::text, sync_time::text, buffers_written::text
      FROM pg_stat_checkpointer
    `;
    if (!row) return null;
    return {
      num_timed: Number(row.num_timed),
      num_requested: Number(row.num_requested),
      write_time_ms: Number(Number(row.write_time).toFixed(2)),
      sync_time_ms: Number(Number(row.sync_time).toFixed(2)),
      buffers_written: Number(row.buffers_written),
    };
  } catch {
    return null;
  }
}

export function diffCheckpointerStats(
  before: CheckpointerStats | null,
  after: CheckpointerStats | null,
): CheckpointerStats | null {
  if (!before || !after) return null;
  return {
    num_timed: Math.max(0, after.num_timed - before.num_timed),
    num_requested: Math.max(0, after.num_requested - before.num_requested),
    write_time_ms: Number(Math.max(0, after.write_time_ms - before.write_time_ms).toFixed(2)),
    sync_time_ms: Number(Math.max(0, after.sync_time_ms - before.sync_time_ms).toFixed(2)),
    buffers_written: Math.max(0, after.buffers_written - before.buffers_written),
  };
}

export async function collectProvenance(db: SQL, distDir: string) {
  let gitCommit = "unknown";
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--short", "HEAD"], {
      stdout: "pipe",
      stderr: "ignore",
    });
    const txt = (await new Response(proc.stdout).text()).trim();
    if ((await proc.exited) === 0 && txt) gitCommit = txt;
  } catch {
    void 0;
  }

  let distManifestSha256 = "unknown";
  try {
    const raw = await readFile(join(distDir, "manifest.json"));
    distManifestSha256 = createHash("sha256").update(raw).digest("hex");
  } catch {
    void 0;
  }

  const [verRow] = await db<{ version: string }[]>`SELECT version() AS version`;
  const pgSettingsRows = await db<{ name: string; setting: string }[]>`
    SELECT name, setting FROM pg_settings
    WHERE name IN (
      'fsync',
      'synchronous_commit',
      'full_page_writes',
      'max_wal_size',
      'checkpoint_timeout',
      'checkpoint_completion_target',
      'shared_buffers',
      'synchronous_standby_names'
    )
    ORDER BY name
  `;
  const cpuInfo = cpus();
  return {
    git_commit: gitCommit,
    dist_manifest_sha256: distManifestSha256,
    bun_version: Bun.version,
    host: {
      cpu_model: cpuInfo[0]?.model ?? "unknown",
      cpu_cores: cpuInfo.length,
      memory_gib: Number((totalmem() / (1024 * 1024 * 1024)).toFixed(1)),
      platform: platform(),
      arch: arch(),
      release: release(),
    },
    pg_version: verRow?.version ?? "unknown",
    pg_settings: Object.fromEntries(pgSettingsRows.map((r) => [r.name, r.setting])),
  };
}
