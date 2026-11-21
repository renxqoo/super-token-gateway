import { spawn, spawnSync } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase, createLedgerEngine } from "@gateway/ledger";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const pgToolsDir =
  process.env.PG_TOOLS_DIRECTORY ??
  new URL("../.local/toolchain/pg17/bin", import.meta.url).pathname;

async function findFreePort(): Promise<number> {
  return new Promise((resolvePort, rejectPort) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolvePort(port));
    });
    srv.on("error", rejectPort);
  });
}

async function waitForJson(url: string): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return (await res.json()) as Record<string, unknown>;
    } catch {
      void 0;
    }
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

function runSync(cwd: string, args: string[], env: Record<string, string>): Promise<void> {
  return new Promise((done, fail) => {
    const proc = spawn(process.execPath, args, { cwd, env, stdio: "inherit" });
    proc.on("exit", (code) => {
      if (code === 0) done();
      else fail(new Error(`Exited ${code}: ${args.join(" ")}`));
    });
  });
}

const tempDir = await mkdtemp(join(tmpdir(), "stg-durability-"));
const dbName = `gateway_v4_dur_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const restoredDbName = `${dbName}_restored`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;
const restoredUrl = new URL(baseDbUrl);
restoredUrl.pathname = `/${restoredDbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
const procs: ReturnType<typeof spawn>[] = [];

try {
  await cp(resolve("dist"), join(tempDir, "dist"), { recursive: true });
  await adminDb.unsafe(`CREATE DATABASE ${dbName}`);
  await adminDb.unsafe(`CREATE DATABASE ${restoredDbName}`);

  const [mockPort, gwPort, internalPort, workerPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
    findFreePort(),
    findFreePort(),
  ]);

  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    DATABASE_URL: targetUrl.toString(),
    KEY_PEPPER: "durability-pepper-0123456789abcde",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ADMIN_TOKEN: "durability-admin-0123456789abcdef",
    ALLOW_PRIVATE_UPSTREAMS: "true",
    ACCESS_LOG: "false",
    HOLD_TTL_MS: "1500",
    WORKER_SWEEP_MS: "300",
    GATEWAY_HOST: "127.0.0.1",
    GATEWAY_PORT: String(gwPort),
    INTERNAL_HOST: "127.0.0.1",
    INTERNAL_PORT: String(internalPort),
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: String(mockPort),
    MOCK_CONTROL_TOKEN: "dur-mock-ctrl",
    WORKER_HEALTH_HOST: "127.0.0.1",
    WORKER_HEALTH_PORT: String(workerPort),
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], env);

  const mockProc = spawn(process.execPath, ["dist/mock-providers/main.js"], {
    cwd: tempDir,
    env,
    stdio: "ignore",
  });
  const workerProc = spawn(process.execPath, ["dist/worker/main.js"], {
    cwd: tempDir,
    env,
    stdio: "ignore",
  });
  let gwProc = spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
    cwd: tempDir,
    env,
    stdio: "ignore",
  });
  procs.push(mockProc, workerProc, gwProc);

  await Promise.all([
    waitForJson(`http://127.0.0.1:${mockPort}/healthz`),
    waitForJson(`http://127.0.0.1:${gwPort}/readyz`),
    waitForJson(`http://127.0.0.1:${workerPort}/readyz`),
  ]);

  const adminHeaders = {
    authorization: `Bearer ${env.ADMIN_TOKEN}`,
    "content-type": "application/json",
  };
  const callInternal = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:${internalPort}${path}`, {
      method,
      headers: adminHeaders,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!res.ok) throw new Error(`Internal ${path} failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as Record<string, unknown>;
  };

  const project = await callInternal("PUT", "/internal/v1/projects", {
    name: "dur-proj",
    currency: "USD",
  });
  const key = await callInternal("PUT", "/internal/v1/api-keys", {
    project_id: project.id,
    name: "dur-key",
  });
  await callInternal("POST", `/internal/v1/wallets/${project.id}/credits`, {
    currency: "USD",
    external_ref: "dur-credit-1",
    amount_atoms: "1000000",
    kind: "CASH",
  });
  const acc = await callInternal("PUT", "/internal/v1/upstream-accounts", { name: "dur-acc" });
  await callInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "dur-sup-1",
    amount_atoms: "1000000",
  });
  const ch = await callInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "dur-ch",
    kind: "openai",
    base_url: `http://127.0.0.1:${mockPort}/openai`,
    api_key: "sk-dur",
  });
  await callInternal("PUT", "/internal/v1/models", {
    name: "dur-gpt",
    operations: ["chat"],
    max_output_tokens: 64,
  });
  const custPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  const supPrice = await callInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "64" },
  });
  await callInternal("PUT", "/internal/v1/routes", {
    model_name: "dur-gpt",
    channel_id: ch.id,
    upstream_model: "dur-slow",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  await fetch(`http://127.0.0.1:${mockPort}/__control/scenarios`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({ model: "dur-slow", scenario: { headerDelayMs: 6000 } }),
  });

  const IN_FLIGHT = 16;
  const inflightPromises = Array.from({ length: IN_FLIGHT }, () =>
    fetch(`http://127.0.0.1:${gwPort}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "dur-gpt",
        messages: [{ role: "user", content: "crash recovery" }],
        max_tokens: 32,
      }),
    }).catch(() => null),
  );

  const checkDb = createDatabase(targetUrl.toString(), 4);
  try {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const rows =
        await checkDb`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'HELD'`;
      if (Number(rows[0]!.n) === IN_FLIGHT) break;
      await Bun.sleep(50);
    }
    const heldCount = Number(
      (await checkDb`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'HELD'`)[0]!
        .n,
    );
    if (heldCount !== IN_FLIGHT) {
      throw new Error(`Expected ${IN_FLIGHT} active HELD holds before SIGKILL, got ${heldCount}`);
    }

    gwProc.kill("SIGKILL");
    await Promise.allSettled(inflightPromises);

    const sweepDeadline = Date.now() + 8000;
    while (Date.now() < sweepDeadline) {
      const rows =
        await checkDb`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'UNKNOWN'`;
      if (Number(rows[0]!.n) === IN_FLIGHT) break;
      await Bun.sleep(100);
    }
    const unknownCount = Number(
      (
        await checkDb`SELECT count(*)::int AS n FROM public.billing_holds WHERE state = 'UNKNOWN'`
      )[0]!.n,
    );
    if (unknownCount !== IN_FLIGHT) {
      throw new Error(
        `Worker failed to sweep all ${IN_FLIGHT} holds to UNKNOWN; got ${unknownCount}`,
      );
    }

    gwProc = spawn(process.execPath, ["dist/gateway/main.js", "--mode=gateway"], {
      cwd: tempDir,
      env,
      stdio: "ignore",
    });
    procs.push(gwProc);
    await waitForJson(`http://127.0.0.1:${gwPort}/readyz`);

    const unknownHolds = await callInternal("GET", "/internal/v1/holds?state=UNKNOWN&limit=100");
    const items = unknownHolds.items as { requestId: string }[];
    if (items.length !== IN_FLIGHT) {
      throw new Error(`Expected ${IN_FLIGHT} UNKNOWN holds via API, got ${items.length}`);
    }

    for (let i = 0; i < items.length; i++) {
      const reqId = items[i]!.requestId;
      if (i % 2 === 0) {
        await callInternal("POST", `/internal/v1/requests/${reqId}/resolve`, {
          action: "RELEASE",
          reason: "sigkill_verified_unbilled",
          actor: "durability-bot",
        });
      } else {
        await callInternal("POST", `/internal/v1/requests/${reqId}/resolve`, {
          action: "SETTLE",
          billing_price_id: custPrice.id,
          usage: { input_tokens: "10", output_tokens: "5" },
          supplier_cost_atoms: "40",
          reason: "sigkill_verified_billed",
          actor: "durability-bot",
        });
      }
    }

    const auditBeforeDump = await callInternal("GET", "/internal/v1/audit/conservation");
    if (
      !auditBeforeDump.ok ||
      auditBeforeDump.open_holds !== 0 ||
      auditBeforeDump.unknown_holds !== 0
    ) {
      throw new Error(`Post-SIGKILL conservation failed: ${JSON.stringify(auditBeforeDump)}`);
    }

    const walletOrig = await callInternal("GET", `/internal/v1/wallets/${project.id}?currency=USD`);
    if (
      walletOrig.reserved_atoms !== "0" ||
      walletOrig.spent_atoms !== String(8 * (10 * 10 + 5 * 20))
    ) {
      throw new Error(
        `Unexpected wallet state after SIGKILL recovery: ${JSON.stringify(walletOrig)}`,
      );
    }

    const dumpFile = join(tempDir, "backup.sql");
    const dumpRes = spawnSync(
      join(pgToolsDir, "pg_dump"),
      ["--dbname", targetUrl.toString(), "--file", dumpFile, "--no-owner", "--no-privileges"],
      { encoding: "utf8" },
    );
    if (dumpRes.status !== 0) {
      throw new Error(`pg_dump failed: ${dumpRes.stderr}`);
    }

    const restoreRes = spawnSync(
      join(pgToolsDir, "psql"),
      ["--dbname", restoredUrl.toString(), "--set", "ON_ERROR_STOP=1", "--file", dumpFile],
      { encoding: "utf8" },
    );
    if (restoreRes.status !== 0) {
      throw new Error(`psql restore failed: ${restoreRes.stderr}`);
    }

    const restoredDb = createDatabase(restoredUrl.toString(), 4);
    try {
      const restoredLedger = createLedgerEngine(restoredDb);
      const restoredAudit = await restoredLedger.auditConservation();
      const restoredWallet = await restoredLedger.getWallet(String(project.id), "USD");
      if (
        !restoredAudit.ok ||
        restoredAudit.open_holds !== 0 ||
        restoredAudit.unknown_holds !== 0
      ) {
        throw new Error(`Restored DB conservation failed: ${JSON.stringify(restoredAudit)}`);
      }
      if (
        restoredWallet?.available_atoms !== walletOrig.available_atoms ||
        restoredWallet?.spent_atoms !== walletOrig.spent_atoms
      ) {
        throw new Error(`Restored wallet mismatch: ${JSON.stringify(restoredWallet)}`);
      }
      console.log(
        JSON.stringify({
          durability_verified: true,
          sigkill_recovered_holds: IN_FLIGHT,
          resolved_released: 8,
          resolved_settled: 8,
          pg_dump_restore_verified: true,
          conservation: restoredAudit,
        }),
      );
    } finally {
      await restoredDb.close();
    }
  } finally {
    await checkDb.close();
  }
} finally {
  for (const p of procs) {
    try {
      p.kill("SIGTERM");
    } catch {
      void 0;
    }
  }
  await Bun.sleep(300);
  await adminDb.unsafe(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname IN ('${dbName}', '${restoredDbName}') AND pid <> pg_backend_pid()`,
  );
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${restoredDbName}`);
  await adminDb.close();
  await rm(tempDir, { recursive: true, force: true });
}
