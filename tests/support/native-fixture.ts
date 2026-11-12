import { spawn } from "node:child_process";
import { createServer } from "node:net";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  statfs,
  writeFile,
} from "node:fs/promises";
import { isAbsolute, join } from "node:path";
import { RedisClient, SQL } from "bun";

export interface NativeFixtureOptions {
  postgresToolsDirectory: string;
  redisServer: string;
  readyTimeoutMs?: number;
  stopTimeoutMs?: number;
}

export function nativeFixtureOptions(): NativeFixtureOptions {
  const postgresToolsDirectory = process.env.PG_TOOLS_DIRECTORY;
  const redisServer = process.env.DURABILITY_REDIS_SERVER;
  if (!postgresToolsDirectory || !redisServer)
    throw new Error(
      "PG_TOOLS_DIRECTORY and DURABILITY_REDIS_SERVER are required absolute native tool paths",
    );
  return { postgresToolsDirectory, redisServer };
}

export function nativeChildEnvironment(
  values: Record<string, string> = {},
): Record<string, string> {
  return {
    PATH: "/usr/bin:/bin",
    LANG: "C",
    LC_ALL: "C",
    TZ: "UTC",
    ...values,
    MINIMAX_API_KEY: "",
  };
}

export async function within<T>(
  operation: Promise<T>,
  milliseconds: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function members(group: number): Promise<number[]> {
  const child = spawn("/bin/ps", ["-axo", "pid=,pgid="], {
    env: nativeChildEnvironment(),
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  let overflow = false;
  const exited = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", resolve);
  });
  child.stdout.on("data", (data: Buffer) => {
    if (output.length + data.length > 1048576) {
      overflow = true;
      child.kill("SIGKILL");
    } else output += data.toString();
  });
  child.stderr.resume();
  try {
    const code = await within(exited, 1000, "Process group inspection");
    if (code !== 0 || overflow) throw new Error("Process group inspection failed");
    return output.split("\n").flatMap((line) => {
      const match = /^\s*(\d+)\s+(\d+)\s*$/.exec(line);
      return match && Number(match[2]) === group ? [Number(match[1])] : [];
    });
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    try {
      await within(exited, 1000, "Process inspection reap");
    } finally {
      child.stdout.destroy();
      child.stderr.destroy();
    }
  }
}

async function unusedPort(): Promise<number> {
  const server = createServer();
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Native port allocation failed");
    return address.port;
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
}

export interface NativeProcess {
  readonly pid: number;
  readonly incarnation: string;
  readonly exited: Promise<number | null>;
  readonly running: boolean;
  readonly output: string;
  stop(signal?: "SIGTERM" | "SIGKILL"): Promise<void>;
}

async function startProcess(
  command: string[],
  env: Record<string, string>,
  name: string,
  stopTimeoutMs: number,
  onFailure: (error: Error) => void,
): Promise<NativeProcess> {
  const previousMask = process.umask(0o077);
  const child = (() => {
    try {
      return spawn(command[0]!, command.slice(1), {
        env: nativeChildEnvironment(env),
        detached: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } finally {
      process.umask(previousMask);
    }
  })();
  let bytes = 0;
  let output = "";
  let ended = false;
  let failure: Error | undefined;
  const exited = new Promise<number | null>((resolve) => {
    child.once("error", () => {
      ended = true;
      failure = new Error(`${name} could not start`);
      resolve(null);
    });
    child.once("exit", (code) => {
      ended = true;
      resolve(code);
    });
  });
  const capture = (data: Buffer) => {
    bytes += data.length;
    if (bytes <= 262144) output += data.toString();
    else if (!failure) {
      failure = new Error(`${name} output exceeded 256 KiB`);
      onFailure(failure);
    }
  };
  child.stdout.on("data", capture);
  child.stderr.on("data", capture);
  await within(
    new Promise<void>((resolve, reject) => {
      child.once("spawn", resolve);
      child.once("error", () => reject(new Error(`${name} could not start`)));
    }),
    1000,
    `${name} spawn`,
  );
  const pid = child.pid!;
  const incarnation = crypto.randomUUID();
  let readersClosed = false;
  child.once("close", () => {
    readersClosed = true;
  });
  let stopped: Promise<void> | undefined;
  async function stop(signal: "SIGTERM" | "SIGKILL" = "SIGTERM"): Promise<void> {
    if (stopped) return stopped;
    stopped = (async () => {
      const signalGroup = (value: NodeJS.Signals) => {
        try {
          process.kill(-pid, value);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
        }
      };
      if ((await members(pid)).length) signalGroup(signal);
      const deadline = Date.now() + (signal === "SIGKILL" ? 0 : stopTimeoutMs);
      while (Date.now() < deadline && (await members(pid)).length) await Bun.sleep(20);
      if ((await members(pid)).length) signalGroup("SIGKILL");
      await within(exited, 2000, `${name} reap`);
      const end = Date.now() + 2000;
      while ((await members(pid)).length) {
        if (Date.now() > end) throw new Error(`${name} process group survived SIGKILL`);
        await Bun.sleep(20);
      }
      await within(
        new Promise<void>((resolve) => {
          if (readersClosed) resolve();
          else child.once("close", () => resolve());
        }),
        1000,
        `${name} readers`,
      ).finally(() => {
        child.stdout.destroy();
        child.stderr.destroy();
      });
    })();
    return stopped;
  }
  return {
    pid,
    incarnation,
    exited,
    get running() {
      return !ended && !failure;
    },
    get output() {
      return output;
    },
    stop,
  };
}

async function tool(path: string, pattern: RegExp): Promise<{ path: string; version: string }> {
  if (!isAbsolute(path)) throw new Error("Native tools must use absolute paths");
  const resolved = await realpath(path);
  if (!(await lstat(resolved)).isFile()) throw new Error("Native tool is not a regular file");
  let failed: Error | undefined;
  const child = await startProcess(
    [resolved, "--version"],
    {},
    "Native version probe",
    100,
    (error) => {
      failed = error;
    },
  );
  try {
    const code = await within(child.exited, 2000, "Native tool version");
    if (failed || code !== 0 || !pattern.test(child.output))
      throw new Error("Native tool version is unsupported; require PostgreSQL 17 and Redis 7.4");
    return { path: resolved, version: child.output.trim() };
  } finally {
    await child.stop();
  }
}

async function directoryBytes(path: string): Promise<number> {
  let bytes = 0;
  for (const entry of await readdir(path, { withFileTypes: true })) {
    const file = join(path, entry.name);
    try {
      const state = await lstat(file);
      if (state.isSymbolicLink()) throw new Error("Native fixture contains an unexpected symlink");
      bytes += state.isDirectory() ? await directoryBytes(file) : state.size;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
  return bytes;
}

export async function createNativeFixture(options: NativeFixtureOptions = nativeFixtureOptions()) {
  const readyTimeoutMs = options.readyTimeoutMs ?? 15000;
  const stopTimeoutMs = options.stopTimeoutMs ?? 750;
  if (
    !Number.isInteger(readyTimeoutMs) ||
    readyTimeoutMs < 1 ||
    readyTimeoutMs > 30000 ||
    !Number.isInteger(stopTimeoutMs) ||
    stopTimeoutMs < 0 ||
    stopTimeoutMs > 2000
  )
    throw new Error("Native fixture deadlines must be finite and within the supported bounds");
  const initdb = await tool(join(options.postgresToolsDirectory, "initdb"), /\(PostgreSQL\) 17\./);
  const postgres = await tool(
    join(options.postgresToolsDirectory, "postgres"),
    /\(PostgreSQL\) 17\./,
  );
  const redis = await tool(options.redisServer, /v=7\.4\./);
  const temp = await realpath("/tmp");
  const disk = await statfs(temp);
  if (disk.bavail * disk.bsize < 1073741824)
    throw new Error("Native fixture requires 1 GiB free disk including cleanup reserve");
  const directory = await mkdtemp(join(temp, "gateway-native-"));
  await chmod(directory, 0o700);
  const processes: NativeProcess[] = [];
  const launches = new Set<Promise<NativeProcess>>();
  const ports: number[] = [];
  let failure: Error | undefined;
  let closing: Promise<void> | undefined;
  let removed = false;
  let budgetWork: Promise<void> | undefined;
  const fail = (error: Error) => {
    failure ??= error;
    void close().catch((cleanup: unknown) => {
      failure = new AggregateError([failure, cleanup], "Native fixture budget cleanup failed");
    });
  };
  const budgetTimer = setInterval(() => {
    if (budgetWork || closing) return;
    budgetWork = directoryBytes(directory)
      .then((bytes) => {
        if (bytes > 402653184) fail(new Error("Native fixture exceeded 384 MiB disk budget"));
      })
      .catch((error: unknown) =>
        fail(new Error("Native fixture disk inspection failed", { cause: error })),
      )
      .finally(() => {
        budgetWork = undefined;
      });
  }, 500);
  budgetTimer.unref();
  function healthy() {
    if (failure) throw failure;
    if (closing) throw new Error("Native fixture is closing");
  }
  async function waitFor(
    predicate: () => Promise<boolean>,
    timeoutMs = readyTimeoutMs,
  ): Promise<void> {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30000)
      throw new Error("Native readiness deadline must be 1 through 30000 ms");
    const end = Date.now() + timeoutMs;
    while (true) {
      healthy();
      if (await within(predicate(), Math.max(1, end - Date.now()), "Native readiness probe"))
        return;
      if (Date.now() >= end) throw new Error("Native readiness timed out");
      await Bun.sleep(25);
    }
  }
  async function launch(
    name: string,
    command: string[],
    env: Record<string, string> = {},
  ): Promise<NativeProcess> {
    healthy();
    const pending = startProcess(command, env, name, stopTimeoutMs, fail).then((child) => {
      processes.push(child);
      return child;
    });
    launches.add(pending);
    try {
      const child = await pending;
      if (closing) {
        await child.stop();
        throw new Error("Native fixture closed during process startup");
      }
      return child;
    } finally {
      launches.delete(pending);
    }
  }

  function ownedPath(name: string) {
    if (!/^[A-Za-z0-9_.-]+$/.test(name) || name === "." || name === "..")
      throw new Error("Native fixture filename is invalid");
    return join(directory, name);
  }
  async function write(name: string, value: string): Promise<string> {
    healthy();
    const path = ownedPath(name);
    await writeFile(path, value, { mode: 0o600, flag: "wx" });
    return path;
  }
  async function startBun(
    name: string,
    script: string,
    env: Record<string, string> = {},
    args: string[] = [],
  ) {
    return launch(name, [process.execPath, "--no-env-file", script, ...args], env);
  }
  async function startPostgres() {
    healthy();
    const data = ownedPath("postgres");
    const socket = ownedPath("socket");
    await mkdir(socket, { mode: 0o700 });
    const password = crypto.randomUUID().replaceAll("-", "");
    const username = `native_${crypto.randomUUID().replaceAll("-", "")}`;
    const passwordFile = await write("pg-password", password);
    try {
      const setup = await launch("initdb", [
        initdb.path,
        "-D",
        data,
        "-U",
        username,
        "--pwfile",
        passwordFile,
        "--auth-host=scram-sha-256",
        "--auth-local=scram-sha-256",
        "--encoding=UTF8",
        "--locale=C",
      ]);
      if ((await within(setup.exited, readyTimeoutMs, "initdb")) !== 0)
        throw new Error("Native initdb failed");
      await setup.stop();
    } finally {
      await rm(passwordFile, { force: true });
    }
    const port = await unusedPort();
    ports.push(port);
    const name = `native_${crypto.randomUUID().replaceAll("-", "")}`;
    await writeFile(
      join(data, "postgresql.conf"),
      `listen_addresses = '127.0.0.1'\nport = ${port}\nunix_socket_directories = '${socket}'\ncluster_name = '${name}'\nshared_buffers = '32MB'\nwork_mem = '2MB'\nmax_connections = 64\nmax_wal_size = '64MB'\nmin_wal_size = '32MB'\nfsync = on\nsynchronous_commit = on\nfull_page_writes = on\nlog_min_messages = warning\nlog_min_error_statement = panic\nlog_error_verbosity = terse\n`,
      { mode: 0o600 },
    );
    const child = await launch("postgres", [postgres.path, "-D", data]);
    const url = `postgres://${username}:${password}@127.0.0.1:${port}/postgres`;
    const db = new SQL(url, {
      max: 1,
      connectionTimeout: 1,
      connection: { statement_timeout: 1000 },
    });
    try {
      await waitFor(async () => {
        if (!child.running) throw new Error("Native PostgreSQL exited before readiness");
        try {
          const row = (
            await db`SELECT current_setting('data_directory') AS directory, current_setting('cluster_name') AS name, current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS commit, current_setting('full_page_writes') AS pages, current_user AS username`
          )[0];
          const pid = Number((await readFile(join(data, "postmaster.pid"), "utf8")).split("\n")[0]);
          if (
            row.directory !== data ||
            row.name !== name ||
            row.username !== username ||
            pid !== child.pid ||
            row.fsync !== "on" ||
            row.commit !== "on" ||
            row.pages !== "on"
          )
            throw new Error("Native PostgreSQL ownership or durability mismatch");
          return true;
        } catch (error) {
          if (error instanceof Error && error.message.includes("mismatch")) throw error;
          return false;
        }
      });
    } finally {
      await within(db.close({ timeout: 1 }), 2000, "PostgreSQL probe close");
    }
    return {
      url,
      port,
      pid: child.pid,
      directory: data,
      get output() {
        return child.output;
      },
    };
  }
  async function startRedis(name: string, replicaOf?: { port: number; password: string }) {
    healthy();
    const data = ownedPath(name);
    await mkdir(data, { mode: 0o700 });
    const password = replicaOf?.password ?? crypto.randomUUID().replaceAll("-", "");
    const port = await unusedPort();
    ports.push(port);
    const config = await write(
      `${name}.conf`,
      `bind 127.0.0.1\nport ${port}\ndir ${data}\nprotected-mode yes\nrequirepass ${password}\nmasterauth ${password}\nsave ""\nappendonly yes\nappendfsync always\naof-load-truncated no\nno-appendfsync-on-rewrite no\nmaxmemory 64mb\nmaxclients 128\nmaxmemory-policy noeviction\nrepl-diskless-sync-delay 0\nauto-aof-rewrite-percentage 0\n${replicaOf ? `replicaof 127.0.0.1 ${replicaOf.port}\n` : ""}`,
    );
    const url = `redis://:${password}@127.0.0.1:${port}`;
    let child: NativeProcess;
    let runId: string | undefined;
    async function command(...args: string[]): Promise<string> {
      if (!child.running) throw new Error("Native Redis is not running");
      const client = new RedisClient(url, {
        autoReconnect: false,
        maxRetries: 0,
        connectionTimeout: 250,
      });
      try {
        await within(client.connect(), 500, "Redis connection");
        const info = String(await within(client.send("INFO", ["server"]), 500, "Redis identity"));
        const current = /^run_id:(\w+)\r?$/m.exec(info)?.[1];
        if (
          Number(/^process_id:(\d+)\r?$/m.exec(info)?.[1]) !== child.pid ||
          !current ||
          (runId && current !== runId)
        )
          throw new Error("Native Redis ownership mismatch");
        const config = (await client.send("CONFIG", ["GET", "dir"])) as
          string[] | Record<string, string>;
        const ownDir = Array.isArray(config) ? config[1] : config.dir;
        if (ownDir !== data) throw new Error("Native Redis directory ownership mismatch");
        runId = current;
        return String(await within(client.send(args[0]!, args.slice(1)), 1000, "Redis command"));
      } finally {
        client.close();
      }
    }
    async function restart() {
      if (child?.running) throw new Error("Native Redis must stop before restart");
      runId = undefined;
      child = await launch(name, [redis.path, config]);
      await waitFor(async () => {
        if (!child.running) throw new Error("Native Redis exited before readiness");
        try {
          return (await command("PING")) === "PONG";
        } catch (error) {
          if (error instanceof Error && error.message.includes("mismatch")) throw error;
          return false;
        }
      });
    }
    await restart();
    return {
      url,
      port,
      password,
      directory: data,
      get pid() {
        return child.pid;
      },
      command,
      restart,
      stop: (signal?: "SIGTERM" | "SIGKILL") => child.stop(signal),
    };
  }
  async function resources() {
    const groups = await Promise.all(
      processes.map(async (child) => ({
        pid: child.pid,
        pgid: child.pid,
        incarnation: child.incarnation,
        members: await members(child.pid),
      })),
    );
    return {
      processes: groups.filter((group) => group.members.length),
      directories: removed ? [] : [directory],
      ports: groups.some((group) => group.members.length) ? [...ports] : [],
      timers: closing ? 0 : 1,
    };
  }
  async function close(): Promise<void> {
    if (closing) return closing;
    clearInterval(budgetTimer);
    closing = (async () => {
      await Promise.allSettled(launches);
      const results = await Promise.allSettled(processes.map((child) => child.stop()));
      await budgetWork;
      const errors = results.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      const alive = (await resources()).processes;
      if (alive.length) errors.push(new Error("Native fixture still owns live process groups"));
      if (!alive.length) {
        try {
          await rm(directory, { recursive: true });
          removed = true;
        } catch (error) {
          errors.push(error);
        }
      }
      if (errors.length) throw new AggregateError(errors, "Native fixture cleanup failed");
    })();
    return closing;
  }
  return {
    directory,
    tools: { initdb, postgres, redis },
    startPostgres,
    startRedis,
    startBun,
    write,
    waitFor,
    resources,
    assertHealthy() {
      if (failure) throw failure;
    },
    close,
  };
}
