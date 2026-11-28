import { localConsoleTokenEnvironment, writeLocalInternalAuth } from "./local-internal-auth.ts";
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
const consoleDbName = `console_art_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const adminUrl = new URL(baseDbUrl);
adminUrl.pathname = "/postgres";
const targetUrl = new URL(baseDbUrl);
targetUrl.pathname = `/${dbName}`;
const consoleTargetUrl = new URL(baseDbUrl);
consoleTargetUrl.pathname = `/${consoleDbName}`;

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
  await adminDb.unsafe(`CREATE DATABASE ${consoleDbName}`);

  const operatorToken = crypto.randomUUID() + crypto.randomUUID();
  const internalAuthFile = join(tempDir, "internal-auth.json");
  await writeLocalInternalAuth(internalAuthFile, operatorToken);
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    DATABASE_URL: targetUrl.toString(),
    KEY_PEPPER: "artifact-pepper-0123456789abcdef",
    ENCRYPTION_KEY: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    INTERNAL_AUTH_FILE: internalAuthFile,
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

  const consoleEnv: Record<string, string> = {
    PATH: process.env.PATH ?? "/usr/bin:/bin",
    CONSOLE_DATABASE_URL: consoleTargetUrl.toString(),
    CONSOLE_HOST: "127.0.0.1",
    CONSOLE_PORT: "18092",
    CONSOLE_ADMIN_HOST: "127.0.0.1",
    CONSOLE_ADMIN_PORT: "18093",
    GATEWAY_INTERNAL_URL: "http://127.0.0.1:18081",
    GATEWAY_PUBLIC_URL: "http://127.0.0.1:18080",
    ...localConsoleTokenEnvironment(operatorToken),
    OUTBOX_WEBHOOK_TOKEN: "artifact-outbox-webhook-token-1234",
    BOOTSTRAP_ADMIN_EMAIL: "root@artifact.test",
    BOOTSTRAP_ADMIN_TOKEN: "artifact-bootstrap-token-1234",
    CONSOLE_REGISTRATION_SIGNING_SECRET: Buffer.from(
      crypto.getRandomValues(new Uint8Array(32)),
    ).toString("hex"),
  };

  await runSync(tempDir, ["dist/tools/migrate.js"], env);
  await runSync(tempDir, ["dist/tools/console-migrate.js"], consoleEnv);

  for (const script of [
    ["dist/mock-providers/main.js"],
    ["dist/gateway/main.js", "--mode=gateway"],
    ["dist/worker/main.js"],
  ]) {
    const child = spawn(process.execPath, script, { cwd: tempDir, env, stdio: "inherit" });
    children.push(child);
  }
  children.push(
    spawn(process.execPath, ["dist/console-api/main.js"], {
      cwd: tempDir,
      env: consoleEnv,
      stdio: "inherit",
    }),
  );

  await waitForJson("http://127.0.0.1:19088/healthz");
  await waitForJson("http://127.0.0.1:18080/readyz");
  await waitForJson("http://127.0.0.1:18090/readyz");
  await waitForJson("http://127.0.0.1:18092/readyz");
  await waitForJson("http://127.0.0.1:18093/readyz");

  const bootstrapRes = await fetch("http://127.0.0.1:18093/admin/v1/auth/bootstrap", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      email: "root@artifact.test",
      password: "ArtifactPassword123!",
      display_name: "Artifact Root",
      bootstrap_token: consoleEnv.BOOTSTRAP_ADMIN_TOKEN,
    }),
  });
  if (bootstrapRes.status !== 201) {
    throw new Error(
      `Console artifact bootstrap failed: ${bootstrapRes.status} ${await bootstrapRes.text()}`,
    );
  }
  const bootstrapJson = (await bootstrapRes.json()) as { access_token: string };
  await runSync(tempDir, ["dist/tools/verify-console-deployment.js"], {
    PATH: env.PATH!,
    CONSOLE_ACCEPTANCE_PUBLIC_URL: "http://127.0.0.1:18092",
    CONSOLE_ACCEPTANCE_ADMIN_URL: "http://127.0.0.1:18093",
    CONSOLE_ACCEPTANCE_ADMIN_TOKEN: bootstrapJson.access_token,
    CONSOLE_ACCEPTANCE_ALLOW_PUBLIC_LOOPBACK_HTTP: "1",
  });
  const crossPortRes = await fetch("http://127.0.0.1:18092/api/v1/auth/me", {
    headers: { authorization: `Bearer ${bootstrapJson.access_token}` },
  });
  if (crossPortRes.status !== 401) {
    throw new Error(
      `Expected admin token to be rejected on client listener, got ${crossPortRes.status}`,
    );
  }

  const enrollment = await fetch("http://127.0.0.1:18093/admin/v1/auth/registration-token", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${bootstrapJson.access_token}`,
    },
    body: JSON.stringify({ email: "artifact-tenant@example.test" }),
  });
  if (enrollment.status !== 201)
    throw new Error(`Console artifact enrollment returned ${enrollment.status}`);
  const { registration_token } = (await enrollment.json()) as { registration_token: string };

  const consoleRegistration = await fetch("http://127.0.0.1:18092/api/v1/auth/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      email: "artifact-tenant@example.test",
      registration_token,
      password: crypto.randomUUID(),
      display_name: "Artifact Tenant",
    }),
  });
  if (consoleRegistration.status !== 201)
    throw new Error(`Console artifact registration returned ${consoleRegistration.status}`);
  const tenant = (await consoleRegistration.json()) as {
    access_token: string;
    default_org_id: string;
    default_billing_account_id: string;
  };
  const issueKey = await fetch(
    `http://127.0.0.1:18092/api/v1/billing-accounts/${tenant.default_billing_account_id}/api-keys`,
    {
      method: "POST",
      headers: {
        authorization: `Bearer ${tenant.access_token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ name: "Artifact Console Key" }),
    },
  );
  if (issueKey.status !== 201)
    throw new Error(`Console artifact API key creation returned ${issueKey.status}`);
  const issued = (await issueKey.json()) as { id: string };
  const rotated = await fetch(
    `http://127.0.0.1:18092/api/v1/billing-accounts/${tenant.default_billing_account_id}/api-keys/${issued.id}/rotate`,
    { method: "POST", headers: { authorization: `Bearer ${tenant.access_token}` } },
  );
  if (rotated.status !== 201)
    throw new Error(`Console artifact Key rotation returned ${rotated.status}`);
  const rotatedKey = (await rotated.json()) as { id: string; key: string };
  const consoleKey = rotatedKey.key;
  const gatewayDb = createDatabase(targetUrl.toString(), 2);
  try {
    const [bound] =
      await gatewayDb`SELECT external_ref FROM public.billing_accounts WHERE id = ${tenant.default_billing_account_id}`;
    if (
      bound?.external_ref !==
      `console:org:${tenant.default_org_id}:billing-account:${tenant.default_billing_account_id}`
    ) {
      throw new Error("Console artifact did not bind the billing account at signup");
    }
    const [scope] =
      await gatewayDb`SELECT resource_scope_id FROM public.api_keys WHERE id = ${rotatedKey.id}`;
    if (scope?.resource_scope_id !== issued.id) {
      throw new Error("Console artifact did not record the resource scope at rotation");
    }
  } finally {
    await gatewayDb.close();
  }
  const suspendTenant = await fetch(
    `http://127.0.0.1:18093/admin/v1/orgs/${tenant.default_org_id}`,
    {
      method: "PATCH",
      headers: {
        authorization: `Bearer ${bootstrapJson.access_token}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ status: "SUSPENDED" }),
    },
  );
  if (suspendTenant.status !== 200)
    throw new Error(`Console artifact tenant suspension returned ${suspendTenant.status}`);
  const modelsAfterSuspend = await fetch("http://127.0.0.1:18080/v1/models", {
    headers: { authorization: `Bearer ${consoleKey}` },
  });
  const denial = (await modelsAfterSuspend.json()) as { error?: { code?: string } };
  if (modelsAfterSuspend.status !== 403 || denial.error?.code !== "org_suspended") {
    throw new Error("Gateway artifact did not reject the suspended Console API key");
  }

  const adminHeaders = {
    authorization: `Bearer ${operatorToken}`,
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

  const billingAccount = await postInternal("PUT", "/internal/v1/billing-accounts", {
    name: "artifact-proj",
    currency: "USD",
  });
  const key = await postInternal("PUT", "/internal/v1/api-keys", {
    billing_account_id: billingAccount.id,
    name: "artifact-key",
  });
  await postInternal("POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
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
  const builtins = (await postInternal("GET", "/internal/v1/plugins/builtins")) as {
    items: { name: string; version: string }[];
  };
  if (!builtins.items.some((b) => b.name === "pii-guard")) {
    throw new Error(`Built-in pii-guard missing from artifact: ${JSON.stringify(builtins)}`);
  }
  const artPlugin = await postInternal(
    "POST",
    "/internal/v1/plugins/builtins/pii-guard/install",
    {},
  );
  await postInternal("POST", "/internal/v1/plugin-releases", {
    id: crypto.randomUUID(),
    expected_epoch: artPlugin.config_epoch,
    targets: [{ plugin_id: artPlugin.id }],
    samples: { [String(artPlugin.id)]: { text: "Contact 13812345678 or alice@example.com" } },
  });
  const dryTest = (await postInternal("POST", `/internal/v1/plugins/${artPlugin.id}/test`, {
    hook: "on_response",
    text: "Contact 13812345678 or alice@example.com",
  })) as { blocked: boolean; text: string };
  if (dryTest.blocked || dryTest.text !== "Contact [REDACTED_PHONE] or [REDACTED_EMAIL]") {
    throw new Error(`Artifact built-in plugin dry-run failed: ${JSON.stringify(dryTest)}`);
  }

  await postInternal("PUT", "/internal/v1/routes", {
    model_name: "art-gpt",
    channel_id: ch.id,
    upstream_model: "art-gpt-up",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    plugin_pipeline: [{ plugin_id: String(artPlugin.id), priority: 1 }],
  });

  const scenarioRes = await fetch("http://127.0.0.1:19088/__control/scenarios", {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.MOCK_CONTROL_TOKEN}`,
      "content-type": "application/json",
    },
    body: JSON.stringify({
      model: "art-gpt-up",
      scenario: {
        output: "Support phone 13812345678 key sk_A1b2C3d4E5-f6G7_H8I9J0K1L2M3N4O5P6Q7R8S9xyA",
      },
    }),
  });
  if (!scenarioRes.ok) {
    throw new Error(`Failed to configure artifact mock scenario: ${scenarioRes.status}`);
  }

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
  if (
    chatJson.choices[0]?.message?.content !==
    "Support phone [REDACTED_PHONE] key [REDACTED_API_KEY]"
  ) {
    throw new Error(
      `Expected pii-guard redacted artifact completion, got: ${JSON.stringify(chatJson)}`,
    );
  }

  const audit = await postInternal("GET", "/internal/v1/audit/conservation");
  if (!audit.ok || audit.open_holds !== 0) {
    throw new Error(`Conservation check failed: ${JSON.stringify(audit)}`);
  }

  const consoleAuditRes = await fetch("http://127.0.0.1:18093/admin/v1/finance/conservation", {
    headers: { authorization: `Bearer ${bootstrapJson.access_token}` },
  });
  if (consoleAuditRes.status !== 200) {
    throw new Error(`Console artifact conservation failed: ${consoleAuditRes.status}`);
  }
  const consoleAudit = (await consoleAuditRes.json()) as { ok?: boolean };
  if (!consoleAudit.ok) {
    throw new Error(`Console artifact conservation not ok: ${JSON.stringify(consoleAudit)}`);
  }

  const archiveDir = join(tempDir, "archive");
  const month = new Date().toISOString().slice(0, 7);
  await runSync(tempDir, ["dist/tools/archive.js", "export", month, archiveDir], env);
  await runSync(
    tempDir,
    ["dist/tools/archive.js", "verify", join(archiveDir, "manifest.json")],
    env,
  );

  console.log(
    JSON.stringify({ artifact_verified: true, console_verified: true, conservation: audit }),
  );
} finally {
  for (const child of children) child.kill("SIGTERM");
  await Bun.sleep(300);
  for (const targetDb of [dbName, consoleDbName]) {
    try {
      await adminDb.unsafe(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${targetDb}' AND pid <> pg_backend_pid()`,
      );
      await adminDb.unsafe(`DROP DATABASE IF EXISTS ${targetDb}`);
    } catch {
      void 0;
    }
  }
  await adminDb.close().catch(() => undefined);
  await rm(tempDir, { recursive: true, force: true });
}
