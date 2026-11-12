import { SQL } from "bun";
import { X509Certificate, generateKeyPairSync, randomBytes, randomUUID, sign } from "node:crypto";
import { execFileSync } from "node:child_process";
import { appendFile, chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { migrate } from "@gateway/storage";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { createBoundedH2Client } from "@gateway/runtime/h2";
import {
  startupChallengeSchema,
  startupRegistrationResponseSchema,
  commandProofFields,
  registrationProofFields,
  type StartupReceipt,
  type InternalService,
} from "@gateway/contracts";
import { withDatabase } from "./database.ts";
export const identityRegistry = ".local/identity-resources.jsonl";
export const identityAudience = "synthetic-identity/core-api";
async function record(event: string, details: Record<string, unknown>) {
  await appendFile(
    identityRegistry,
    JSON.stringify({ event, ...details, at: new Date().toISOString() }) + "\n",
    { mode: 0o600 },
  );
}
export async function withIdentity(
  run: (f: Awaited<ReturnType<typeof prepareIdentity>>, db: SQL) => Promise<void>,
  options: {
    databasePort?: number;
    execution?: import("../../apps/core-api/src/execution/handler.ts").ExecutionOptions;
    entry?: string;
    challengeTtl?: number;
    sessionTtl?: number;
    limits?: Partial<import("@gateway/runtime/h2").H2Limits>;
  } = {},
) {
  await record("before", {
    databasePrefix: "gateway_test_foundation_",
    rolePrefix: "gateway_identity_",
    tempPrefix: "gateway-identity-",
    command: "internal-identity test",
    minimaxDisabled: process.env.MINIMAX_API_KEY === "",
  });
  await withDatabase(
    async (db) => {
      await migrate(db);
      const f = await prepareIdentity(db, options);
      try {
        await run(f, db);
      } finally {
        await f.close();
      }
    },
    { max: 8, registry: identityRegistry },
  );
}
async function prepareIdentity(
  db: SQL,
  options: {
    databasePort?: number;
    execution?: import("../../apps/core-api/src/execution/handler.ts").ExecutionOptions;
    entry?: string;
    challengeTtl?: number;
    sessionTtl?: number;
    limits?: Partial<import("@gateway/runtime/h2").H2Limits>;
  },
) {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-identity-"));
  await record("temp_created", { directory });
  const suffix = randomUUID().replaceAll("-", ""),
    api = `gateway_identity_api_${suffix}`,
    deployer = `gateway_identity_deployer_${suffix}`,
    verifier = `gateway_identity_verifier_${suffix}`,
    password = randomUUID(),
    roles = [api, deployer, verifier];
  const processes: Bun.Subprocess[] = [];
  const connections: Awaited<ReturnType<typeof createBoundedH2Client>>[] = [];
  const certificates = new Map<
    string,
    { cert: string; key: string; x509: X509Certificate; service: InternalService; instance: string }
  >();
  let roleCount = 0;
  const openssl = (args: string[]) =>
    execFileSync("openssl", args, { cwd: directory, stdio: "pipe" });
  const identity = (await db`SELECT current_database() AS name`)[0].name;
  const base = new URL(process.env.DATABASE_URL!);
  base.pathname = `/${identity}`;
  try {
    openssl([
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-nodes",
      "-keyout",
      "ca.key",
      "-out",
      "ca.pem",
      "-days",
      "2",
      "-subj",
      "/CN=Synthetic Identity Test CA",
    ]);
    const ca = await Bun.file(resolve(directory, "ca.pem")).text();
    for (const [name, service, instance, uri] of [
      ["coreA", "core-api", "coreA", "spiffe://synthetic/core-api/coreA"],
      ["coreB", "core-api", "coreB", "spiffe://synthetic/core-api/coreB"],
      ["gateway", "gateway", "gatewayA", "spiffe://synthetic/gateway/gatewayA"],
      ["gatewayB", "gateway", "gatewayB", "spiffe://synthetic/gateway/gatewayB"],
      ["worker", "core-worker", "workerA", "spiffe://synthetic/core-worker/workerA"],
      ["extra", "gateway", "extra", "spiffe://synthetic/gateway/extra"],
    ] as const) {
      openssl([
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        `${name}.key`,
        "-out",
        `${name}.csr`,
        "-subj",
        `/CN=${name}`,
      ]);
      await writeFile(
        resolve(directory, "extensions"),
        `subjectAltName=DNS:localhost,URI:${uri}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n`,
        { mode: 0o600 },
      );
      openssl([
        "x509",
        "-req",
        "-in",
        `${name}.csr`,
        "-CA",
        "ca.pem",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        `${name}.pem`,
        "-days",
        "1",
        "-extfile",
        "extensions",
      ]);
      await chmod(resolve(directory, `${name}.key`), 0o600);
      const cert = await Bun.file(resolve(directory, `${name}.pem`)).text(),
        key = await Bun.file(resolve(directory, `${name}.key`)).text(),
        x509 = new X509Certificate(cert);
      certificates.set(name, { cert, key, x509, service, instance });
    }
    await chmod(resolve(directory, "ca.key"), 0o600);
    for (const role of roles) {
      await db.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
      roleCount++;
      await record("role_created", { role });
    }
    await db.unsafe(
      (await Bun.file("infra/core-identity-role.sql").text())
        .replaceAll("core_identity_api", api)
        .replaceAll("core_identity_deployer", deployer)
        .replaceAll("core_identity_verifier", verifier),
    );
    if (options.execution)
      await db.unsafe(
        (await Bun.file("infra/core-execution-role.sql").text()).replaceAll(
          "core_execution_api",
          api,
        ),
      );
    function roleUrl(role: string) {
      const url = new URL(base);
      url.username = role;
      url.password = password;
      if (options.databasePort) {
        url.port = String(options.databasePort);
        url.searchParams.set("sslmode", "disable");
      }
      return url.toString();
    }
    for (const [name, c] of certificates) {
      if (name === "extra") continue;
      await db`SELECT internal_identity_provision_v1(${c.service},${c.instance},${sha256(c.x509.raw)},${`spiffe://synthetic/${c.service}/${c.instance}`},${c.x509.subjectAltName},${new Date(c.x509.validFrom).toISOString()}::timestamptz,${new Date(c.x509.validTo).toISOString()}::timestamptz)`;
    }
    async function start(name: "coreA" | "coreB") {
      const config = resolve(directory, `${name}.json`);
      await writeFile(
        config,
        JSON.stringify({
          database_url: roleUrl(api),
          ...(options.execution ? { execution: options.execution } : {}),
          audience: identityAudience,
          hostname: "127.0.0.1",
          servername: "localhost",
          port: 0,
          key_file: resolve(directory, `${name}.key`),
          cert_file: resolve(directory, `${name}.pem`),
          ca_file: resolve(directory, "ca.pem"),
          client_key_file: resolve(directory, `${name}.key`),
          client_cert_file: resolve(directory, `${name}.pem`),
          ...(options.limits ? { limits: options.limits } : {}),
          ...(options.challengeTtl ? { challenge_ttl_seconds: options.challengeTtl } : {}),
          ...(options.sessionTtl ? { session_ttl_seconds: options.sessionTtl } : {}),
        }),
        { mode: 0o600 },
      );
      const child = Bun.spawn(["bun", options.entry ?? "apps/core-api/src/identity-main.ts"], {
        env: { ...process.env, MINIMAX_API_KEY: "", INTERNAL_IDENTITY_CONFIG_FILE: config },
        stdout: "pipe",
        stderr: "pipe",
      });
      processes.push(child);
      await record("process_started", { pid: child.pid, name });
      const reader = child.stdout.getReader();
      let output = "";
      const read = async () => {
        while (!output.includes('"event":"ready"')) {
          const next = await reader.read();
          if (next.done)
            throw new Error(
              `Core startup failed: ${await new Response(child.stderr).text()} ${output}`,
            );
          output += new TextDecoder().decode(next.value);
        }
        return JSON.parse(output.split("\n").find((line) => line.includes('"event":"ready"'))!);
      };
      const timeout = setTimeout(() => child.kill("SIGKILL"), 10000);
      let ready: { port: number; session: StartupReceipt };
      try {
        ready = await read();
      } finally {
        clearTimeout(timeout);
      }
      const stopped = async () => {
        if (child.exitCode !== null) return;
        child.kill("SIGTERM");
        const kill = setTimeout(() => child.kill("SIGKILL"), 6000);
        const code = await child.exited;
        clearTimeout(kill);
        let rest = "";
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          rest += new TextDecoder().decode(next.value);
        }
        reader.releaseLock();
        const stderr = await new Response(child.stderr).text();
        await record("process_stopped", { pid: child.pid, code, stdout: rest, stderr });
        if (code !== 0) throw new Error(`Core stop failed ${code}: ${stderr}`);
        const resources = JSON.parse(rest.trim().split("\n").at(-1)!).resources;
        for (const count of Object.values(resources))
          if (count !== 0) throw new Error(`Core resources retained: ${JSON.stringify(resources)}`);
      };
      return { ...ready, pid: child.pid, stop: stopped };
    }
    const a = await start("coreA"),
      b = await start("coreB");
    async function connection(
      name = "gateway",
      port = a.port,
      extra: Partial<Parameters<typeof createBoundedH2Client>[0]> = {},
    ) {
      const c = certificates.get(name)!;
      const client = await createBoundedH2Client({
        origin: `https://127.0.0.1:${port}`,
        servername: "localhost",
        key: c.key,
        cert: c.cert,
        ca,
        ...extra,
      });
      connections.push(client);
      return client;
    }
    async function close() {
      for (const c of connections) await c.close();
      const errors = await Promise.allSettled([a.stop(), b.stop()]);
      for (const role of roles) {
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
        await record("role_dropped", { role });
      }
      await rm(directory, { recursive: true, force: true });
      await record("temp_removed", { directory });
      for (const result of errors) if (result.status === "rejected") throw result.reason;
    }
    return {
      a,
      b,
      ca,
      directory,
      certificates,
      connection,
      apiUrl: roleUrl(api),
      deployerUrl: roleUrl(deployer),
      verifierUrl: roleUrl(verifier),
      async close() {
        try {
          await close();
        } catch (error) {
          for (const c of processes) if (c.exitCode === null) c.kill("SIGKILL");
          throw error;
        }
      },
    };
  } catch (error) {
    for (const c of connections) await c.close();
    for (const c of processes) if (c.exitCode === null) c.kill("SIGKILL");
    await Promise.all(processes.map((c) => c.exited));
    for (const role of roles.slice(0, roleCount)) {
      await db.unsafe(`DROP OWNED BY ${role}`);
      await db.unsafe(`DROP ROLE ${role}`);
      await record("role_dropped", { role });
    }
    await rm(directory, { recursive: true, force: true });
    await record("temp_removed", { directory });
    throw error;
  }
}
export function startupIdentity() {
  const keys = generateKeyPairSync("ed25519");
  return {
    keys,
    registrationId: randomUUID(),
    boot: randomBytes(32).toString("base64url"),
    spki: keys.publicKey.export({ format: "der", type: "spki" }).toString("base64url"),
  };
}
export async function identityRequest(
  client: Awaited<ReturnType<typeof createBoundedH2Client>>,
  port: number,
  path: string,
  body: unknown,
  proof?: string,
  raw?: Buffer,
) {
  const bytes = raw ?? Buffer.from(JSON.stringify(body));
  const response = await client.request(
    {
      ":authority": `localhost:${port}`,
      ":method": "POST",
      ":path": path,
      "content-type": "application/json",
      "content-length": String(bytes.length),
      ...(proof ? { "x-stg-proof": proof } : {}),
    },
    bytes,
  );
  return {
    status: response.status,
    value: JSON.parse(response.body.toString()) as Record<string, unknown>,
    raw: response.body.toString(),
  };
}
export async function challengeIdentity(
  client: Awaited<ReturnType<typeof createBoundedH2Client>>,
  port: number,
  identity: ReturnType<typeof startupIdentity>,
) {
  return identityRequest(client, port, "/_internal/core/v1/identity/challenges", {
    registration_id: identity.registrationId,
    public_key_spki: identity.spki,
    boot_nonce: identity.boot,
    contract_version: 1,
  });
}
export async function registerIdentity(
  client: Awaited<ReturnType<typeof createBoundedH2Client>>,
  port: number,
  identity: ReturnType<typeof startupIdentity>,
  challenge: unknown,
) {
  const c = startupChallengeSchema.parse(challenge),
    signature = sign(
      null,
      Buffer.from(JSON.stringify(registrationProofFields(c))),
      identity.keys.privateKey,
    ).toString("base64url");
  const response = await identityRequest(
    client,
    port,
    "/_internal/core/v1/identity/registrations",
    {
      challenge_id: c.challenge_id,
      registration_id: identity.registrationId,
      signature,
      contract_version: 1,
    },
  );
  return response;
}
export async function registeredIdentity(
  client: Awaited<ReturnType<typeof createBoundedH2Client>>,
  port: number,
) {
  const identity = startupIdentity(),
    challenge = await challengeIdentity(client, port, identity);
  const result = await registerIdentity(client, port, identity, challenge.value);
  return {
    ...identity,
    challenge: challenge.value,
    receipt: startupRegistrationResponseSchema.parse(result.value).receipt,
  };
}
export function currentCommand(
  identity: ReturnType<typeof startupIdentity> & { receipt: StartupReceipt },
  overrides: Record<string, unknown> = {},
) {
  const owner = identity.receipt,
    deadline = new Date(Date.now() + 10000).toISOString(),
    command = randomUUID(),
    trace = randomUUID();
  const semantic = {
    kind: "identity_current_v1",
    contract_version: 1,
    owner: {
      service: owner.service,
      instance_id: owner.instance_id,
      session_id: owner.session_id,
      incarnation_id: owner.incarnation_id,
    },
    deadline_at: deadline,
    data: {},
  };
  const body = {
    envelope: {
      command_id: command,
      body_digest: sha256(canonical(semantic)),
      caller_incarnation: owner.incarnation_id,
      deadline_at: deadline,
      trace_id: trace,
      contract_version: 1,
    },
    data: {},
  };
  const raw = Buffer.from(JSON.stringify(body));
  const proof = {
    session_id: owner.session_id,
    incarnation_id: owner.incarnation_id,
    command_type: "identity_current_v1" as const,
    command_id: command,
    deadline_at: deadline,
    trace_id: trace,
    contract_version: 1 as const,
    raw_body_sha256: sha256(raw),
    ...overrides,
  };
  const signature = sign(
    null,
    Buffer.from(
      JSON.stringify(
        commandProofFields(
          identityAudience,
          owner,
          proof,
          "POST",
          "/_internal/core/v1/identity/current-session",
        ),
      ),
    ),
    identity.keys.privateKey,
  ).toString("base64url");
  return {
    body,
    raw,
    proof: { ...proof, signature },
    header: Buffer.from(JSON.stringify({ ...proof, signature })).toString("base64url"),
  };
}
