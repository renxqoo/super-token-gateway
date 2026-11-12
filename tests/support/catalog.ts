import { generateKeyPairSync } from "node:crypto";
import { appendFile, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import type { SQL } from "bun";
import { createDatabase, migrate, Store } from "@gateway/storage";
import { createCatalogApplication } from "../../apps/core-api/src/plugins/application.ts";
import { catalogRoutes } from "../../apps/core-api/src/plugins/http.ts";
import { bundledReleaseId, type ReleasePolicy } from "../../apps/core-api/src/plugins/releases.ts";
import { withDatabase } from "./database.ts";
const registry = ".local/T04b1-resources.jsonl";
export const syntheticAdminKey = "synthetic-platform-admin-key-0123456789";
export const syntheticPepper = "synthetic-platform-pepper-0123456789";
export const catalogHelper = resolve(".local/plugin-host");
export async function withCatalog(
  run: (fixture: Awaited<ReturnType<typeof prepareCatalog>>, db: SQL) => Promise<void>,
) {
  await withDatabase(
    async (db) => {
      await migrate(db);
      const fixture = await prepareCatalog(db);
      try {
        await run(fixture, db);
      } finally {
        await fixture.close();
      }
    },
    { max: 8, registry },
  );
}
export async function prepareCatalog(db: SQL) {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-t04b1-")),
    role = `gateway_t04b1_${crypto.randomUUID().replaceAll("-", "")}`;
  const record = async (event: string, details: Record<string, unknown>) =>
    appendFile(registry, JSON.stringify({ event, ...details }) + "\n", { mode: 0o600 });
  await record("temp_created", { directory });
  let roleCreated = false,
    appDb: SQL | undefined;
  try {
    const keys = generateKeyPairSync("ed25519"),
      privatePath = resolve(directory, "ephemeral-key.pem"),
      credentialPath = resolve(directory, "credential");
    await writeFile(privatePath, keys.privateKey.export({ type: "pkcs8", format: "pem" }), {
      mode: 0o600,
    });
    await writeFile(credentialPath, syntheticAdminKey, { mode: 0o600 });
    const signer = Bun.spawn(
      [
        "bun",
        "scripts/sign-bundled-release.ts",
        "--private-key-file",
        privatePath,
        "--key-id",
        "synthetic-catalog-test",
        "--output-directory",
        directory,
        "--helper-executable",
        catalogHelper,
      ],
      { env: { ...process.env, MINIMAX_API_KEY: "" }, stdout: "pipe", stderr: "pipe" },
    );
    const [stdout, stderr] = await Promise.all([
      new Response(signer.stdout).text(),
      new Response(signer.stderr).text(),
    ]);
    if (await signer.exited) throw new Error(`Synthetic release signing failed: ${stderr}`);
    const descriptor = JSON.parse(stdout) as Record<string, unknown>,
      release = await Bun.file(resolve(directory, bundledReleaseId, "release.json")).json();
    const d = release.descriptor;
    const policy: ReleasePolicy = {
      format: 1,
      expires_at: "2099-01-01T00:00:00.000Z",
      revoked: false,
      keys: [
        {
          id: "synthetic-catalog-test",
          public_key: keys.publicKey.export({ format: "pem", type: "spki" }).toString(),
          not_before: "2020-01-01T00:00:00.000Z",
          expires_at: "2099-01-01T00:00:00.000Z",
          revoked: false,
          purposes: ["package", "release"],
          release_ids: [bundledReleaseId],
        },
      ],
      releases: [
        {
          release_id: bundledReleaseId,
          descriptor_digest: String(descriptor.descriptor_digest),
          package_digest: d.package_digest,
          abi_version: 1,
          config_schema_digest: d.config_schema_digest,
          fixture_digest: d.fixture_digest,
          validation_policy_digest: d.validation_policy_digest,
          runtime_digest: d.runtime_digest,
          revoked: false,
        },
      ],
    };
    const policyPath = resolve(directory, "policy.json");
    await writeFile(policyPath, JSON.stringify(policy), { mode: 0o600 });
    const password = crypto.randomUUID();
    await db.unsafe(`CREATE ROLE ${role} LOGIN PASSWORD '${password}'`);
    roleCreated = true;
    await record("role_created", { role });
    const grants = (await Bun.file("infra/core-catalog-role.sql").text()).replaceAll(
      "core_catalog",
      role,
    );
    await db.unsafe(grants);
    const database = (await db`SELECT current_database() AS name`)[0].name;
    const url = new URL(process.env.DATABASE_URL!);
    url.username = role;
    url.password = password;
    url.pathname = `/${database}`;
    appDb = createDatabase(url.toString(), 4);
    const applicationOptions = {
      db: appDb,
      adminKey: syntheticAdminKey,
      pepper: syntheticPepper,
      policyFile: policyPath,
      releaseDirectory: directory,
      helperExecutable: catalogHelper,
    };
    let application = createCatalogApplication(applicationOptions),
      http = catalogRoutes(application);
    const request = async (
      path: string,
      body?: unknown,
      token: string | undefined = syntheticAdminKey,
      method = body ? "POST" : "GET",
    ) => {
      const response = await http.request(`http://catalog.test${path}`, {
        method,
        headers: {
          ...(token ? { authorization: `Bearer ${token}` } : {}),
          ...(body ? { "content-type": "application/json" } : {}),
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      return { status: response.status, value: (await response.json()) as Record<string, unknown> };
    };
    const actor = () => application.authenticate(`Bearer ${syntheticAdminKey}`);
    const providerId = crypto.randomUUID();
    const provider = await new Store(db).createProvider(
      providerId,
      {
        name: `catalog-${providerId}`,
        kind: "openai",
        base_url: "http://127.0.0.1:1/v1",
        concurrency: 10,
        rpm: 100,
      },
      "synthetic-encrypted-secret",
    );
    const binding = (versionId: string, changes: Record<string, unknown> = {}) => ({
      command_id: crypto.randomUUID(),
      version_id: versionId,
      expected_revision: "0",
      account_id: provider.account_id,
      brand_id: provider.brand_id,
      secret_reference: `channel:${providerId}`,
      config: {
        model_map: { demo: "synthetic" },
        max_output_tokens: 100,
        max_input_tokens: 10000,
        auth: "bearer",
      },
      reason: "Synthetic catalog binding",
      ...changes,
    });
    return {
      directory,
      policyPath,
      credentialPath,
      policy,
      release,
      keys,
      appDb,
      get application() {
        return application;
      },
      get http() {
        return http;
      },
      async reload() {
        await application.close();
        application = createCatalogApplication(applicationOptions);
        http = catalogRoutes(application);
      },
      request,
      actor,
      provider,
      binding,
      async close() {
        await application.close();
        await appDb?.close();
        await db.unsafe(`DROP OWNED BY ${role}`);
        await db.unsafe(`DROP ROLE ${role}`);
        await record("role_dropped", { role });
        await rm(directory, { recursive: true, force: true });
        await record("temp_removed", { directory });
      },
    };
  } catch (error) {
    await appDb?.close();
    if (roleCreated) {
      await db.unsafe(`DROP OWNED BY ${role}`);
      await db.unsafe(`DROP ROLE ${role}`);
      await record("role_dropped", { role });
    }
    await rm(directory, { recursive: true, force: true });
    await record("temp_removed", { directory });
    throw error;
  }
}
