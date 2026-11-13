import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import type { SQL } from "bun";
import { RedisClient } from "bun";
import { sha256 } from "@gateway/runtime/crypto";
import type { withFinancial } from "./financial-execution.ts";
import { identityAudience } from "./internal-identity.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];

export async function startTlsUpstream(fetch: (request: Request) => Response | Promise<Response>) {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-upstream-"));
  execFileSync(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-days",
      "1",
      "-subj",
      "/CN=127.0.0.1",
      "-addext",
      "subjectAltName=IP:127.0.0.1",
      "-keyout",
      "upstream.key",
      "-out",
      "upstream.pem",
    ],
    { cwd: directory, stdio: "pipe" },
  );
  const certificate = resolve(directory, "upstream.pem");
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: {
      key: await readFile(resolve(directory, "upstream.key"), "utf8"),
      cert: await readFile(certificate, "utf8"),
    },
    fetch,
  });
  return {
    baseUrl: `https://127.0.0.1:${server.port}/v1`,
    certificate,
    async stop() {
      await server.stop(true);
      await rm(directory, { recursive: true, force: true });
    },
  };
}

async function readyLine(
  child: Bun.Subprocess<"ignore", "pipe", "pipe">,
  label: string,
  stderr: Promise<string>,
) {
  const reader = child.stdout.getReader();
  let output = "";
  const timer = setTimeout(() => child.kill("SIGKILL"), 20000);
  try {
    while (!output.includes("\n")) {
      const next = await reader.read();
      if (next.done) throw new Error(`${label} exited: ${await stderr} ${output}`);
      output += new TextDecoder().decode(next.value);
    }
  } finally {
    clearTimeout(timer);
  }
  const ready = JSON.parse(output.slice(0, output.indexOf("\n"))) as {
    event: string;
    port: number;
  };
  if (ready.event !== "ready") throw new Error(`${label} not ready: ${output}`);
  let rest = output.slice(output.indexOf("\n") + 1);
  return {
    port: ready.port,
    async remaining() {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        rest += new TextDecoder().decode(next.value);
      }
      return rest;
    },
  };
}

function owned(child: Bun.Subprocess<"ignore", "pipe", "pipe">, label: string) {
  const stderr = new Response(child.stderr).text();
  return {
    pid: child.pid,
    kill(signal: NodeJS.Signals = "SIGKILL") {
      if (child.exitCode === null) child.kill(signal);
    },
    async stop(lines: { remaining(): Promise<string> }) {
      if (child.exitCode === null) child.kill("SIGTERM");
      const timer = setTimeout(() => child.kill("SIGKILL"), 30000);
      const code = await child.exited;
      clearTimeout(timer);
      const stdout = await lines.remaining();
      if (code !== 0) throw new Error(`${label} exited ${code}: ${await stderr}`);
      return JSON.parse(stdout.trim().split("\n").at(-1)!) as Record<string, unknown>;
    },
    exited: () => child.exited,
    stderr: () => stderr,
  };
}

export async function startCoreWorker(db: SQL, namespace: string, entry?: string, cwd?: string) {
  const url = new URL(process.env.DATABASE_URL!);
  url.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
  const child = Bun.spawn(["bun", resolve(entry ?? "apps/core-worker/src/main.ts")], {
    ...(cwd ? { cwd } : {}),
    env: {
      PATH: process.env.PATH,
      DATABASE_URL: url.toString(),
      QUEUE_REDIS_URL: process.env.QUEUE_REDIS_URL,
      REDIS_NAMESPACE: namespace,
      WORKER_PORT: "0",
      SHUTDOWN_GRACE_MS: "5000",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const process_ = owned(child, "core worker");
  const lines = await readyLine(child, "core worker", process_.stderr());
  return { ...process_, port: lines.port, stop: () => process_.stop(lines) };
}

export async function startGateway(
  f: Fixture,
  options: {
    namespace: string;
    upstreamCertificate: string;
    core?: "coreA" | "coreB";
    entry?: string;
    instance?: "gateway" | "gatewayB";
    overrides?: Record<string, unknown>;
    cwd?: string;
  },
) {
  const name = options.instance ?? "gateway",
    core = options.core ?? "coreA",
    identity = f.identity,
    certificate = identity.certificates.get(core)!,
    port = core === "coreA" ? identity.a.port : identity.b.port;
  const configFile = resolve(identity.directory, `${name}-${crypto.randomUUID()}.json`);
  await writeFile(
    configFile,
    JSON.stringify({
      hostname: "127.0.0.1",
      port: 0,
      total_timeout_ms: 20000,
      shutdown_grace_ms: 3000,
      allow_private_upstream: true,
      core: {
        origin: `https://127.0.0.1:${port}`,
        servername: "localhost",
        audience: identityAudience,
        peer_sha256: sha256(certificate.x509.raw),
        subject_altname: certificate.x509.subjectAltName,
        key_file: resolve(identity.directory, `${name}.key`),
        cert_file: resolve(identity.directory, `${name}.pem`),
        ca_file: resolve(identity.directory, "ca.pem"),
      },
      queue: {
        url: process.env.QUEUE_REDIS_URL,
        namespace: options.namespace,
        required_replicas: 0,
      },
      plugins: {
        helper_executable: resolve(".local/plugin-host"),
        trust_policy: f.catalog.policyPath,
      },
      ...options.overrides,
    }),
    { mode: 0o600 },
  );
  const child = Bun.spawn(["bun", resolve(options.entry ?? "apps/gateway/src/main.ts")], {
    ...(options.cwd ? { cwd: options.cwd } : {}),
    env: {
      PATH: process.env.PATH,
      GATEWAY_CONFIG_FILE: configFile,
      NODE_EXTRA_CA_CERTS: options.upstreamCertificate,
      MINIMAX_API_KEY: "",
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const process_ = owned(child, "gateway");
  const lines = await readyLine(child, "gateway", process_.stderr());
  return {
    ...process_,
    origin: `http://127.0.0.1:${lines.port}`,
    stop: () => process_.stop(lines),
  };
}

export async function clearQueueNamespace(namespace: string) {
  const redis = new RedisClient(process.env.QUEUE_REDIS_URL!);
  try {
    const keys = (await redis.send("KEYS", [`*${namespace}*`])) as string[];
    if (keys.length) await redis.send("DEL", keys);
  } finally {
    redis.close();
  }
}

export async function eventually(check: () => Promise<boolean>, label: string, ms = 15000) {
  const end = Date.now() + ms;
  while (!(await check())) {
    if (Date.now() > end) throw new Error(`Timed out waiting for ${label}`);
    await Bun.sleep(25);
  }
}
