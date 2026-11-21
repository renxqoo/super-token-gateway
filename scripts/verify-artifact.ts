import { spawn } from "node:child_process";
import { cp, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createDatabase } from "@gateway/ledger";

const baseDbUrl = process.env.DATABASE_URL;
if (!baseDbUrl) throw new Error("DATABASE_URL is required");

const distSource = resolve("dist");
const tempDir = await mkdtemp(join(tmpdir(), "stg-artifact-"));
const dbName = `gateway_v4_art_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;

const adminDb = createDatabase(adminUrl.toString(), 2);
const children: ReturnType<typeof spawn>[] = [];

async function waitForJson(
  url: string,
  headers?: Record<string, string>,
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url, headers ? { headers } : undefined);
      if (res.ok) return (await res.json()) as Record<string, unknown>;
    } catch {
      void 0;
    }
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

function runSync(cwd: string, args: string[], env: Record<string, string>): Promise<void> {
  return new Promise((resolveDone, rejectDone) => {
    const proc = spawn(process.execPath, args, { cwd, env, stdio: "inherit" });
    proc.on("exit", (code) => {
      if (code === 0) resolveDone();
      else rejectDone(new Error(`Process exited with code ${code}: ${args.join(" ")}`));
    });
  });
}

try {
  await cp(distSource, join(tempDir, "dist"), { recursive: true });
  await adminDb.unsafe(`CREATE DATABASE ${dbName}`);

  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    DATABASE_URL: targetUrl.toString(),
    KEY_PEPPER: "artifact-pepper-0123456789abcdef",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ADMIN_TOKEN: "artifact-admin-token-0123456789ab",
    ALLOW_PRIVATE_UPSTREAMS: "true",
    GATEWAY_HOST: "127.0.0.1",
    GATEWAY_PORT: "18080",
    INTERNAL_HOST: "127.0.0.1",
    INTERNAL_PORT: "18081",
    MOCK_HOST: "127.0.0.1",
    MOCK_PORT: "19088",
    MOCK_CONTROL_TOKEN: "artifact-mock-ctrl",
    WORKER_HEALTH_HOST: "127.0.0.1",
    WORKER_HEALTH_PORT: "18090",
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], env);

  for (const script of [
    ["dist/mock-providers/main.js"],
    ["dist/gateway/main.js", "--mode=gateway"],
    ["dist/worker/main.js"],
  ]) {
    const child = spawn(process.execPath, script, { cwd: tempDir, env, stdio: "inherit" });
    children.push(child);
  }

  await waitForJson("http://127.0.0.1:19088/healthz");
  await waitForJson("http://127.0.0.1:18080/readyz");
  await waitForJson("http://127.0.0.1:18090/readyz");

  const adminHeaders = {
    authorization: `Bearer ${env.ADMIN_TOKEN}`,
    "content-type": "application/json",
  };
  const postInternal = async (method: string, path: string, body?: unknown) => {
    const res = await fetch(`http://127.0.0.1:18081${path}`, {
      method,
      headers: adminHeaders,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!res.ok) throw new Error(`Internal ${path} failed: ${res.status} ${await res.text()}`);
    return (await res.json()) as Record<string, unknown>;
  };

  const project = await postInternal("PUT", "/internal/v1/projects", {
    name: "artifact-proj",
    currency: "USD",
  });
  const key = await postInternal("PUT", "/internal/v1/api-keys", {
    project_id: project.id,
    name: "artifact-key",
  });
  await postInternal("POST", `/internal/v1/wallets/${project.id}/credits`, {
    currency: "USD",
    external_ref: "art-credit-1",
    amount_atoms: "100000",
    kind: "CASH",
  });
  const acc = await postInternal("PUT", "/internal/v1/upstream-accounts", { name: "art-acc" });
  await postInternal("POST", `/internal/v1/upstream-accounts/${acc.id}/recharges`, {
    currency: "USD",
    external_ref: "art-sup-1",
    amount_atoms: "100000",
  });
  const ch = await postInternal("PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "art-ch",
    kind: "openai",
    base_url: "http://127.0.0.1:19088/openai",
    api_key: "sk-art",
  });
  await postInternal("PUT", "/internal/v1/models", {
    name: "art-gpt",
    operations: ["chat"],
    max_output_tokens: 100,
  });
  const custPrice = await postInternal("POST", "/internal/v1/prices", {
    side: "CUSTOMER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
  });
  const supPrice = await postInternal("POST", "/internal/v1/prices", {
    side: "SUPPLIER",
    currency: "USD",
    rates: {
      input_tokens: { numerator: "2", denominator: "1" },
      cached_input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "4", denominator: "1" },
    },
    ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "100" },
  });
  await postInternal("PUT", "/internal/v1/routes", {
    model_name: "art-gpt",
    channel_id: ch.id,
    upstream_model: "art-gpt-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });

  const chatRes = await fetch("http://127.0.0.1:18080/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
    body: JSON.stringify({
      model: "art-gpt",
      messages: [{ role: "user", content: "artifact check" }],
      max_tokens: 50,
    }),
  });
  if (chatRes.status !== 200) throw new Error(`Artifact chat failed: ${chatRes.status}`);
  const chatJson = (await chatRes.json()) as { choices: { message: { content: string } }[] };
  if (!chatJson.choices[0]?.message?.content) throw new Error("Empty artifact completion");

  const audit = await postInternal("GET", "/internal/v1/audit/conservation");
  if (!audit.ok || audit.open_holds !== 0) {
    throw new Error(`Conservation check failed: ${JSON.stringify(audit)}`);
  }

  const archiveDir = join(tempDir, "archive");
  const month = new Date().toISOString().slice(0, 7);
  await runSync(tempDir, ["dist/tools/archive.js", "export", month, archiveDir], env);
  await runSync(
    tempDir,
    ["dist/tools/archive.js", "verify", join(archiveDir, "manifest.json")],
    env,
  );

  console.log(JSON.stringify({ artifact_verified: true, conservation: audit }));
} finally {
  for (const child of children) child.kill("SIGTERM");
  await Bun.sleep(300);
  await adminDb.unsafe(
    `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${dbName}' AND pid <> pg_backend_pid()`,
  );
  await adminDb.unsafe(`DROP DATABASE IF EXISTS ${dbName}`);
  await adminDb.close();
  await rm(tempDir, { recursive: true, force: true });
}
