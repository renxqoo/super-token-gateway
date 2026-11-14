import { execFileSync } from "node:child_process";
import { connect, createServer, type Socket } from "node:net";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import type { SQL } from "bun";
import { RedisClient } from "bun";
import { sha256 } from "@gateway/runtime/crypto";
import { withFinancial } from "./financial-execution.ts";
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
    alive: () => child.exitCode === null && child.signalCode === null,
    kill(signal: NodeJS.Signals = "SIGKILL") {
      if (child.exitCode === null && child.signalCode === null) child.kill(signal);
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

export async function withSettlementRole<T>(db: SQL, run: (databaseUrl: string) => Promise<T>) {
  const role = `core_settlement_${crypto.randomUUID().replaceAll("-", "")}`,
    password = crypto.randomUUID();
  await db.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
  try {
    await db.unsafe(
      (await Bun.file(resolve("infra/core-settlement-role.sql")).text()).replaceAll(
        "core_settlement_worker",
        role,
      ),
    );
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
    url.username = role;
    url.password = password;
    return await run(url.toString());
  } finally {
    await db.unsafe(`DROP OWNED BY ${role}`);
    await db.unsafe(`DROP ROLE ${role}`);
  }
}

export async function startCoreWorker(
  namespace: string,
  options: { databaseUrl: string; entry?: string; cwd?: string; env?: Record<string, string> },
) {
  const child = Bun.spawn(["bun", resolve(options.entry ?? "apps/core-worker/src/main.ts")], {
    ...(options.cwd ? { cwd: options.cwd } : {}),
    env: {
      PATH: process.env.PATH,
      DATABASE_URL: options.databaseUrl,
      QUEUE_REDIS_URL: process.env.QUEUE_REDIS_URL,
      REDIS_NAMESPACE: namespace,
      WORKER_PORT: "0",
      SHUTDOWN_GRACE_MS: "5000",
      ...options.env,
    },
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
  });
  const process_ = owned(child, "core worker");
  const lines = await readyLine(child, "core worker", process_.stderr());
  return {
    ...process_,
    port: lines.port,
    stop: () => process_.stop(lines),
    output: async () => (await lines.remaining()) + (await process_.stderr()),
  };
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
    output: async () => (await lines.remaining()) + (await process_.stderr()),
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

export type UpstreamCall = {
  index: number;
  path: string;
  apiKey: string | null;
  authorization: string | null;
  body: Record<string, unknown>;
  signal: AbortSignal;
};
type Process = Awaited<ReturnType<typeof startGateway>>;
const boundedSupplier = {
  rates: {
    input_tokens: { numerator: "1", denominator: "3" },
    output_tokens: { numerator: "1", denominator: "3" },
  },
  ceilings: { input_tokens: "1000", output_tokens: "50" },
};

export function withGatewayStack(
  options: {
    reply: (call: UpstreamCall) => Response | Promise<Response>;
    channels?: number;
    gateway?: Record<string, unknown>;
    databasePort?: number;
    supplierCredit?: string;
  },
  run: (stack: {
    f: Fixture;
    db: SQL;
    namespace: string;
    setup: Awaited<ReturnType<Fixture["bootstrap"]>>;
    received: UpstreamCall[];
    gateway: Process;
    worker: Awaited<ReturnType<typeof startCoreWorker>>;
    workerDatabaseUrl: string;
    startGateway: (
      overrides?: Record<string, unknown>,
      instance?: "gateway" | "gatewayB",
    ) => Promise<Process>;
    startWorker: () => Promise<Awaited<ReturnType<typeof startCoreWorker>>>;
    send: (input?: {
      stream?: boolean;
      via?: Process;
      headers?: Record<string, string>;
      signal?: AbortSignal;
      content?: string;
    }) => Promise<Response>;
  }) => Promise<void>,
) {
  return withFinancial(
    (f, db) =>
      withSettlementRole(db, async (workerDatabaseUrl) => {
        const received: UpstreamCall[] = [];
        const upstream = await startTlsUpstream(async (request) => {
          const call = {
            index: received.length,
            path: new URL(request.url).pathname,
            apiKey: request.headers.get("x-api-key"),
            authorization: request.headers.get("authorization"),
            body: (await request.json()) as Record<string, unknown>,
            signal: request.signal,
          };
          received.push(call);
          return options.reply(call);
        });
        const namespace = `gateway-stack_${crypto.randomUUID().replaceAll("-", "")}`;
        const processes: { alive(): boolean; kill(): void; stop(): Promise<unknown> }[] = [];
        try {
          const setup = await f.bootstrap(
            undefined,
            options.supplierCredit,
            undefined,
            undefined,
            boundedSupplier,
            {
              baseUrl: upstream.baseUrl,
              maxInputTokens: 1000,
            },
          );
          for (let channel = 2; channel <= (options.channels ?? 1); channel++)
            await f.addChannel(setup, {
              baseUrl: upstream.baseUrl,
              apiKey: `synthetic-upstream-secret-${channel}`,
              maxInputTokens: 1000,
              ceilings: boundedSupplier.ceilings,
            });
          const startWorker = async () => {
            const worker = await startCoreWorker(namespace, { databaseUrl: workerDatabaseUrl });
            processes.push(worker);
            return worker;
          };
          const launch = async (
            overrides: Record<string, unknown> = {},
            instance: "gateway" | "gatewayB" = "gateway",
          ) => {
            const gateway = await startGateway(f, {
              namespace,
              upstreamCertificate: upstream.certificate,
              instance,
              overrides: { ...options.gateway, ...overrides },
            });
            processes.push(gateway);
            return gateway;
          };
          const worker = await startWorker();
          const gateway = await launch();
          await run({
            f,
            db,
            namespace,
            setup,
            received,
            gateway,
            worker,
            workerDatabaseUrl,
            startGateway: launch,
            startWorker,
            send: (input = {}) =>
              fetch(`${(input.via ?? gateway).origin}/v1/chat/completions`, {
                method: "POST",
                headers: {
                  authorization: `Bearer ${setup.key.key}`,
                  "content-type": "application/json",
                  ...input.headers,
                },
                body: JSON.stringify({
                  model: setup.model,
                  messages: [{ role: "user", content: input.content ?? "合成" }],
                  max_completion_tokens: 30,
                  ...(input.stream ? { stream: true } : {}),
                }),
                ...(input.signal ? { signal: input.signal } : {}),
              }),
          });
          for (const process_ of processes.reverse()) if (process_.alive()) await process_.stop();
        } finally {
          for (const process_ of processes) process_.kill();
          await upstream.stop();
          await clearQueueNamespace(namespace);
        }
      }),
    options.databasePort ? { databasePort: options.databasePort } : {},
  );
}

export const chatReplies = {
  json: (content = "ok") =>
    Response.json({
      id: "chain",
      model: "synthetic",
      choices: [{ message: { role: "assistant", content }, finish_reason: "stop" }],
      usage: { prompt_tokens: 30, completion_tokens: 15 },
    }),
  sse: (frames: unknown[] = chatFrames(), done = true) =>
    new Response(
      frames
        .map((frame) => `data: ${JSON.stringify(frame)}\n\n`)
        .concat(done ? ["data: [DONE]\n\n"] : [])
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    ),
};

export function chatFrames() {
  return [
    { id: "chain", model: "synthetic", choices: [{ index: 0, delta: { content: "o" } }] },
    {
      id: "chain",
      model: "synthetic",
      choices: [{ index: 0, delta: { content: "k" }, finish_reason: "stop" }],
    },
    {
      id: "chain",
      model: "synthetic",
      choices: [],
      usage: { prompt_tokens: 30, completion_tokens: 15 },
    },
  ];
}

export async function settledCharge(db: SQL, requestId: string) {
  await eventually(
    async () =>
      (await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${requestId}`)[0]
        .n === 1,
    "customer charge",
  );
  return (
    await db`SELECT c.amount_atoms::text AS amount,r.state,r.billing_outcome FROM billing_charges c JOIN gateway_requests r ON r.id=c.request_id WHERE c.request_id=${requestId}`
  )[0] as { amount: string; state: string; billing_outcome: string };
}

export function stall() {
  let release: () => void = () => undefined;
  const released = new Promise<void>((resolve) => (release = resolve));
  return { released, release };
}

export function slowStream(frames: unknown[], delayMs: number, onCancel: () => void) {
  let index = 0;
  return new Response(
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        await Bun.sleep(delayMs);
        controller.enqueue(
          new TextEncoder().encode(`data: ${JSON.stringify(frames[index % frames.length])}\n\n`),
        );
        index++;
      },
      cancel() {
        onCancel();
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  );
}

export async function requestOutcome(db: SQL, requestId: string) {
  await eventually(
    async () =>
      (
        await db`SELECT count(*)::integer n FROM gateway_requests WHERE id=${requestId} AND billing_outcome<>'PENDING' AND execution_closed_at IS NOT NULL`
      )[0].n === 1,
    "request outcome",
  );
  return (
    await db`SELECT r.state,r.billing_outcome,coalesce((SELECT sum(c.amount_atoms) FROM billing_charges c WHERE c.request_id=r.id),0)::text AS charged FROM gateway_requests r WHERE r.id=${requestId}`
  )[0] as { state: string; billing_outcome: string; charged: string };
}

export async function tcpProxy(target: URL) {
  const sockets = new Set<Socket>();
  let open = true;
  const server = createServer((client) => {
    if (!open) return client.destroy();
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    for (const socket of [client, upstream]) {
      sockets.add(socket);
      socket.on("error", () => undefined);
      socket.on("close", () => {
        sockets.delete(socket);
        client.destroy();
        upstream.destroy();
      });
    }
    client.pipe(upstream);
    upstream.pipe(client);
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Proxy did not bind");
  const url = new URL(target);
  url.hostname = "127.0.0.1";
  url.port = String(address.port);
  return {
    url: url.toString(),
    cut() {
      open = false;
      for (const socket of sockets) socket.destroy();
    },
    resume() {
      open = true;
    },
    async close() {
      open = false;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
