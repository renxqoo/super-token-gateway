import { spawnSync } from "node:child_process";
import { SQL, RedisClient } from "bun";

export type ProcessRow = {
  pid: number;
  ppid: number;
  rss_bytes: number;
  cpu_seconds: number;
  started: string;
  command: string;
};

export function parseProcessRows(output: string): ProcessRow[] {
  return output
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const match =
        /^\s*(\d+)\s+(\d+)\s+(\d+)\s+([\d:.-]+)\s+(\S+\s+\S+\s+\d+\s+[\d:]+\s+\d+)\s+(.*)$/.exec(
          line,
        );
      if (!match) throw new Error(`Malformed process sample: ${line}`);
      const clock = match[4]!.split("-");
      const seconds =
        clock
          .at(-1)!
          .split(":")
          .reduce((total, part) => total * 60 + Number(part), 0) +
        (clock.length > 1 ? Number(clock[0]) * 86400 : 0);
      if (!Number.isFinite(seconds) || seconds < 0) throw new Error("Invalid process CPU");
      return {
        pid: Number(match[1]),
        ppid: Number(match[2]),
        rss_bytes: Number(match[3]) * 1024,
        cpu_seconds: seconds,
        started: match[5]!,
        command: match[6]!,
      };
    });
}

export function readProcessRows() {
  const result = spawnSync("ps", ["-A", "-o", "pid=,ppid=,rss=,time=,lstart=,command="], {
    encoding: "utf8",
  });
  if (result.status !== 0) throw new Error(`Process sampling failed: ${result.stderr}`);
  return parseProcessRows(result.stdout);
}

export function createProcessAccumulator() {
  const pids = new Map<
    string,
    {
      pid: number;
      started: string;
      role: string;
      baseline_cpu_seconds: number;
      last_cpu_seconds: number;
      cpu_seconds: number;
      peak_rss_bytes: number;
      first_sample_ms: number;
      last_sample_ms: number;
    }
  >();
  const peaks = new Map<string, { peak_rss_bytes: number; peak_processes: number }>();
  let initialized = false;
  function sample(rows: ProcessRow[], roots: Record<string, number[]>, at: number) {
    const byPid = new Map(rows.map((row) => [row.pid, row]));
    const roles = new Map<number, string>();
    function visit(pid: number, role: string) {
      if (roles.has(pid)) return;
      const row = byPid.get(pid);
      if (!row) return;
      if (/plugin-host/.test(row.command)) role = "plugin_helper";
      roles.set(pid, role);
      for (const child of rows) if (child.ppid === pid) visit(child.pid, role);
    }
    for (const [role, ids] of Object.entries(roots)) for (const pid of ids) visit(pid, role);
    const usage: Record<string, { rss_bytes: number; processes: number }> = {};
    for (const [pid, role] of roles) {
      const row = byPid.get(pid)!;
      const key = `${pid}:${row.started}`;
      let entry = pids.get(key);
      if (!entry) {
        const baseline = initialized ? 0 : row.cpu_seconds;
        entry = {
          pid,
          started: row.started,
          role,
          baseline_cpu_seconds: baseline,
          last_cpu_seconds: baseline,
          cpu_seconds: 0,
          peak_rss_bytes: 0,
          first_sample_ms: at,
          last_sample_ms: at,
        };
        pids.set(key, entry);
      }
      entry.cpu_seconds += Math.max(0, row.cpu_seconds - entry.last_cpu_seconds);
      entry.last_cpu_seconds = Math.max(entry.last_cpu_seconds, row.cpu_seconds);
      entry.last_sample_ms = at;
      entry.peak_rss_bytes = Math.max(entry.peak_rss_bytes, row.rss_bytes);
      const total = (usage[role] ??= { rss_bytes: 0, processes: 0 });
      total.rss_bytes += row.rss_bytes;
      total.processes++;
    }
    for (const [role, total] of Object.entries(usage)) {
      const peak = peaks.get(role) ?? { peak_rss_bytes: 0, peak_processes: 0 };
      peak.peak_rss_bytes = Math.max(peak.peak_rss_bytes, total.rss_bytes);
      peak.peak_processes = Math.max(peak.peak_processes, total.processes);
      peaks.set(role, peak);
    }
    initialized = true;
    return usage;
  }
  function summary(requests: number) {
    if (!Number.isSafeInteger(requests) || requests < 1)
      throw new Error("CPU denominator must be positive");
    return Object.fromEntries(
      [...peaks].map(([role, peak]) => {
        const entries = [...pids.values()].filter((entry) => entry.role === role);
        const cpu_seconds = entries.reduce((total, entry) => total + entry.cpu_seconds, 0);
        return [
          role,
          {
            ...peak,
            cpu_seconds,
            cpu_ms_per_request: (cpu_seconds * 1000) / requests,
            pids: entries,
          },
        ];
      }),
    );
  }
  return { sample, summary };
}

export type StatementRow = {
  userid: string;
  queryid: string;
  toplevel: boolean;
  query: string;
  calls: number;
  total_exec_time: number;
  wal_bytes: number;
  wal_records: number;
};
export type StatementSnapshot = { stats_reset: string; dealloc: number; rows: StatementRow[] };

export function summarizeStatements(
  before: StatementSnapshot,
  after: StatementSnapshot,
  requests: number,
) {
  if (!Number.isSafeInteger(requests) || requests < 1)
    throw new Error("Statement denominator must be positive");
  const key = (row: StatementRow) => `${row.userid}:${row.queryid}:${row.toplevel}`;
  const initial = new Map(before.rows.map((row) => [key(row), row]));
  const final = new Set(after.rows.map(key));
  const errors: string[] = [];
  if (before.stats_reset !== after.stats_reset) errors.push("statistics_reset");
  if (before.dealloc !== after.dealloc) errors.push("statements_evicted");
  if (before.rows.some((row) => !final.has(key(row)))) errors.push("statements_missing");
  const rows = after.rows
    .map((row) => {
      const old = initial.get(key(row));
      const delta = (name: "calls" | "total_exec_time" | "wal_bytes" | "wal_records") => {
        const value = row[name] - (old?.[name] ?? 0);
        if (!Number.isFinite(value) || value < 0)
          errors.push(`counter_regressed:${key(row)}:${name}`);
        return Math.max(0, value);
      };
      return {
        ...row,
        calls: delta("calls"),
        total_exec_time: delta("total_exec_time"),
        wal_bytes: delta("wal_bytes"),
        wal_records: delta("wal_records"),
      };
    })
    .filter((row) => row.calls > 0);
  function totals(selected: StatementRow[]) {
    const sum = (name: "calls" | "total_exec_time" | "wal_bytes" | "wal_records") =>
      selected.reduce((total, row) => total + row[name], 0);
    return {
      calls: sum("calls"),
      exec_ms: sum("total_exec_time"),
      wal_bytes: sum("wal_bytes"),
      wal_records: sum("wal_records"),
      calls_per_request: sum("calls") / requests,
      exec_ms_per_request: sum("total_exec_time") / requests,
      wal_bytes_per_request: sum("wal_bytes") / requests,
      wal_records_per_request: sum("wal_records") / requests,
    };
  }
  return {
    valid: errors.length === 0,
    errors,
    requests,
    statistics: {
      before: { stats_reset: before.stats_reset, dealloc: before.dealloc },
      after: { stats_reset: after.stats_reset, dealloc: after.dealloc },
    },
    topLevel: totals(rows.filter((row) => row.toplevel)),
    nested: totals(rows.filter((row) => !row.toplevel)),
    top25: rows.sort((a, b) => b.total_exec_time - a.total_exec_time).slice(0, 25),
  };
}

export async function createDatabaseMetrics(databaseUrl: string) {
  const target = new URL(databaseUrl);
  const database = decodeURIComponent(target.pathname.slice(1));
  target.pathname = "/postgres";
  const observer = new SQL(target.toString(), { max: 1 });
  try {
    await observer`SELECT 1 FROM pg_stat_statements LIMIT 1`;
    const settings =
      await observer`SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','max_wal_size','checkpoint_timeout','checkpoint_completion_target','wal_compression','wal_sync_method','fsync','full_page_writes','synchronous_commit','max_connections','pg_stat_statements.track') ORDER BY name`;
    const oid = (await observer`SELECT oid::text FROM pg_database WHERE datname=${database}`)[0]
      ?.oid;
    if (!oid) throw new Error(`Benchmark database not found: ${database}`);
    async function snapshot(): Promise<StatementSnapshot> {
      const info = (
        await observer`SELECT stats_reset::text,dealloc::float8 FROM pg_stat_statements_info`
      )[0]!;
      const rows = await observer<
        StatementRow[]
      >`SELECT userid::text,queryid::text,toplevel,query,calls::float8,total_exec_time,wal_bytes::float8,wal_records::float8 FROM pg_stat_statements WHERE dbid=${oid}::oid`;
      return {
        stats_reset: String(info.stats_reset),
        dealloc: Number(info.dealloc),
        rows: [...rows],
      };
    }
    async function resetOwned() {
      if (!/^(newapi_p0_|gateway_test_foundation_)[a-f0-9]{32}$/.test(database))
        throw new Error("Refusing to reset statistics for an unowned database");
      await observer`SELECT pg_stat_statements_reset(0::oid,${oid}::oid,0::bigint,false)`;
    }
    async function roots() {
      const rows = await observer<
        { pid: number; backend_type: string }[]
      >`SELECT pid,backend_type FROM pg_stat_activity WHERE datname=${database} OR backend_type IN ('checkpointer','background writer','walwriter','autovacuum launcher')`;
      return {
        pg: rows
          .filter(
            (row) =>
              !["checkpointer", "background writer", "walwriter", "autovacuum launcher"].includes(
                row.backend_type,
              ),
          )
          .map((row) => row.pid),
        pg_shared: rows
          .filter((row) =>
            ["checkpointer", "background writer", "walwriter", "autovacuum launcher"].includes(
              row.backend_type,
            ),
          )
          .map((row) => row.pid),
      };
    }
    return {
      database,
      settings: [...settings],
      snapshot,
      resetOwned,
      roots,
      close: () => observer.close(),
    };
  } catch (error) {
    await observer.close();
    throw error;
  }
}

export function startProcessSampler(
  getRoots: () => Promise<Record<string, number[]>>,
  intervalMs = 500,
) {
  const accumulator = createProcessAccumulator();
  const samples: { at: number; usage: ReturnType<typeof accumulator.sample> }[] = [];
  const started = Date.now();
  let active: Promise<void> | undefined;
  let errors = 0;
  let timer: ReturnType<typeof setInterval> | undefined;
  async function collect() {
    const roots = await getRoots();
    const at = Date.now() - started;
    samples.push({ at, usage: accumulator.sample(readProcessRows(), roots, at) });
  }
  return {
    async start() {
      await collect();
      timer = setInterval(() => {
        if (!active)
          active = collect()
            .catch(() => {
              errors++;
            })
            .finally(() => {
              active = undefined;
            });
      }, intervalMs);
    },
    async stop(requests: number) {
      clearInterval(timer);
      await active;
      await collect();
      return {
        interval_ms: intervalMs,
        errors,
        samples,
        processes: accumulator.summary(requests),
        limitation:
          "CPU is a sampled lower bound: a process born and exited between samples, or CPU after its last sample, cannot be observed. pg_shared includes instance-wide background work.",
      };
    },
    async close() {
      clearInterval(timer);
      await active;
    },
  };
}

async function inspectRedis(url: string) {
  const redis = new RedisClient(url, { autoReconnect: false, connectionTimeout: 1000 });
  try {
    await redis.connect();
    const info = String(await redis.send("INFO", ["server"]));
    const pid = Number(/^process_id:(\d+)/m.exec(info)?.[1]);
    if (!Number.isSafeInteger(pid) || pid < 1) throw new Error("Redis PID unavailable");
    const replication = String(await redis.send("INFO", ["replication"]));
    const replicas = [...replication.matchAll(/^slave\d+:ip=([^,]+),port=(\d+),/gm)].map(
      (match) => {
        const replica = new URL(url);
        replica.hostname = match[1]!;
        replica.port = match[2]!;
        return replica.toString();
      },
    );
    return { pid, replicas };
  } finally {
    redis.close();
  }
}

export async function discoverRedisProcesses(urls: Record<string, string>, inspect = inspectRedis) {
  const roots: Record<string, number[]> = {};
  const seen = new Set<string>();
  async function visit(role: string, url: string) {
    if (seen.has(url)) return;
    seen.add(url);
    if (seen.size > 16) throw new Error("Redis topology exceeds sampling bound");
    if (!["localhost", "127.0.0.1"].includes(new URL(url).hostname))
      throw new Error("PID sampling requires local Redis");
    const info = await inspect(url);
    (roots[role] ??= []).push(info.pid);
    for (const replica of info.replicas) await visit(`${role}_replica`, replica);
  }
  for (const [role, url] of Object.entries(urls)) await visit(role, url);
  return roots;
}
