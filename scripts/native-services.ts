import { spawn, spawnSync } from "node:child_process";
import { closeSync, openSync } from "node:fs";
import { chmod, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { connect } from "node:net";
import { isAbsolute, join, resolve } from "node:path";
import { RedisClient } from "bun";
import { createDatabase } from "@gateway/ledger";

const root = resolve(import.meta.dir, "../.local/native-services");
const command = Bun.argv[2];
if (!["start", "stop", "status"].includes(command ?? ""))
  throw new Error("Usage: native-services.ts start|stop|status");

function tool(variable: string, name: string) {
  const value = process.env[variable];
  if (!value || !isAbsolute(value)) throw new Error(`${variable} must be an absolute path`);
  return variable === "PG_TOOLS_DIRECTORY" ? join(value, name) : value;
}
function endpoint(variable: string, protocol: string) {
  const value = process.env[variable];
  if (!value) throw new Error(`${variable} is required`);
  const url = new URL(value);
  if (url.protocol !== protocol || url.hostname !== "127.0.0.1" || !url.port)
    throw new Error(`${variable} must use ${protocol}//127.0.0.1 with an explicit port`);
  return url;
}

const database = endpoint("DATABASE_URL", "postgres:");
const services = [
  {
    name: "postgres",
    port: Number(database.port),
    directory: join(root, "postgres"),
  },
  {
    name: "admission-redis",
    port: Number(endpoint("REDIS_URL", "redis:").port),
    directory: join(root, "admission-redis"),
  },
];
type Service = (typeof services)[number];

function owned(service: Service, pid: number) {
  const result = spawnSync("ps", ["-o", "command=", "-p", String(pid)], { encoding: "utf8" });
  const marker =
    service.name === "postgres"
      ? join(service.directory, "data")
      : `redis-server 127.0.0.1:${service.port}`;
  return result.status === 0 && result.stdout.includes(marker);
}
async function redisDirectory(port: number) {
  const redis = new RedisClient(`redis://127.0.0.1:${port}`, {
    connectionTimeout: 500,
    autoReconnect: false,
  });
  try {
    return ((await redis.send("CONFIG", ["GET", "dir"])) as { dir?: string }).dir;
  } finally {
    redis.close();
  }
}
async function running(service: Service) {
  const text = await readFile(join(root, `${service.name}.pid`), "utf8").catch(() => "");
  const pid = Number(text.trim());
  if (!Number.isSafeInteger(pid) || pid <= 0 || !owned(service, pid)) return undefined;
  if (service.name !== "postgres" && (await redisDirectory(service.port)) !== service.directory)
    return undefined;
  return pid;
}
function listening(port: number) {
  return new Promise<boolean>((done) => {
    const socket = connect({ host: "127.0.0.1", port });
    socket.once("connect", () => {
      socket.destroy();
      done(true);
    });
    socket.once("error", () => done(false));
  });
}
async function until(check: () => Promise<boolean>, label: string) {
  const end = Date.now() + 30000;
  while (!(await check().catch(() => false))) {
    if (Date.now() > end) throw new Error(`${label} did not become ready`);
    await Bun.sleep(100);
  }
}
function launch(service: Service, executable: string, args: string[]) {
  const log = openSync(join(root, `${service.name}.log`), "a", 0o600);
  try {
    const child = spawn(executable, args, {
      detached: true,
      stdio: ["ignore", log, log],
      env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C", TZ: "UTC" },
    });
    child.unref();
    if (!child.pid) throw new Error(`${service.name} failed to start`);
    return child.pid;
  } finally {
    closeSync(log);
  }
}

async function startPostgres(service: Service) {
  const data = join(service.directory, "data"),
    socket = join(service.directory, "socket");
  if (!(await Bun.file(join(data, "PG_VERSION")).exists())) {
    await mkdir(service.directory, { recursive: true, mode: 0o700 });
    const passwordFile = join(service.directory, "initdb-password");
    await writeFile(passwordFile, decodeURIComponent(database.password), { mode: 0o600 });
    try {
      const init = spawnSync(
        tool("PG_TOOLS_DIRECTORY", "initdb"),
        [
          "-D",
          data,
          "-U",
          decodeURIComponent(database.username),
          `--pwfile=${passwordFile}`,
          "--auth=scram-sha-256",
          "--encoding=UTF8",
          "--locale=C",
        ],
        { encoding: "utf8", env: { PATH: "/usr/bin:/bin", LANG: "C", LC_ALL: "C" } },
      );
      if (init.status !== 0) throw new Error("initdb failed; see its output above");
    } finally {
      await rm(passwordFile, { force: true });
    }
  }
  await mkdir(socket, { recursive: true, mode: 0o700 });
  const pid = launch(service, tool("PG_TOOLS_DIRECTORY", "postgres"), [
    "-D",
    data,
    "-c",
    "listen_addresses=127.0.0.1",
    "-c",
    `port=${service.port}`,
    "-c",
    `unix_socket_directories=${socket}`,
    "-c",
    "max_connections=200",
    "-c",
    "shared_buffers=256MB",
    "-c",
    "fsync=on",
    "-c",
    "synchronous_commit=on",
    "-c",
    "full_page_writes=on",
    "-c",
    "log_min_error_statement=panic",
  ]);
  await writeFile(join(root, `${service.name}.pid`), `${pid}\n`, { mode: 0o600 });
  const maintenance = new URL(database);
  maintenance.pathname = "/postgres";
  const db = createDatabase(maintenance.toString(), 1);
  try {
    await until(async () => (await db`SELECT 1 AS ok`)[0].ok === 1, service.name);
    const name = decodeURIComponent(database.pathname.slice(1));
    if (!/^[a-zA-Z0-9_]{1,63}$/.test(name)) throw new Error("Invalid DATABASE_URL database name");
    if (!(await db`SELECT 1 FROM pg_database WHERE datname=${name}`).length)
      await db.unsafe(`CREATE DATABASE ${name}`);
  } finally {
    await db.close();
  }
}

async function startRedis(service: Service) {
  await mkdir(service.directory, { recursive: true, mode: 0o700 });
  const pid = launch(service, tool("DURABILITY_REDIS_SERVER", "redis-server"), [
    "--bind",
    "127.0.0.1",
    "--port",
    String(service.port),
    "--protected-mode",
    "yes",
    "--dir",
    service.directory,
    "--appendonly",
    "yes",
    "--maxmemory",
    "256mb",
    "--maxmemory-policy",
    "noeviction",
  ]);
  await writeFile(join(root, `${service.name}.pid`), `${pid}\n`, { mode: 0o600 });
  await until(async () => {
    const redis = new RedisClient(`redis://127.0.0.1:${service.port}`, {
      connectionTimeout: 500,
      autoReconnect: false,
    });
    try {
      return (await redis.send("PING", [])) === "PONG";
    } finally {
      redis.close();
    }
  }, service.name);
}

async function stop(service: Service) {
  const pid = await running(service);
  if (!pid) return "stopped";
  process.kill(pid, service.name === "postgres" ? "SIGINT" : "SIGTERM");
  const end = Date.now() + 30000;
  while (owned(service, pid)) {
    if (Date.now() > end)
      throw new Error(`${service.name} (${pid}) did not stop within 30 seconds; left running`);
    await Bun.sleep(100);
  }
  await rm(join(root, `${service.name}.pid`), { force: true });
  return "stopped";
}

await mkdir(root, { recursive: true, mode: 0o700 });
await chmod(root, 0o700);
const report: Record<string, string> = {};
if (command === "start") {
  for (const service of services) {
    if (await running(service)) {
      report[service.name] = `running on ${service.port}`;
      continue;
    }
    if (await listening(service.port))
      throw new Error(
        `Port ${service.port} for ${service.name} is used by a process these scripts do not own; choose another port`,
      );
    if (service.name === "postgres") await startPostgres(service);
    else await startRedis(service);
    report[service.name] = `started on ${service.port}`;
  }
} else if (command === "stop") {
  for (const service of [...services].reverse()) report[service.name] = await stop(service);
} else
  for (const service of services) {
    const pid = await running(service);
    report[service.name] = pid ? `running on ${service.port}` : "stopped";
  }
console.log(JSON.stringify({ directory: root, services: report }));
