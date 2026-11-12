import assert from "node:assert/strict";
import { appendFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { RedisClient } from "bun";
import { withCatalog, syntheticAdminKey, syntheticPepper } from "../tests/support/catalog.ts";
import { bundledReleaseId } from "../apps/core-api/src/plugins/releases.ts";
const registry = ".local/T04b1-resources.jsonl";
const record = (event: string, details: Record<string, unknown>) =>
  appendFile(registry, JSON.stringify({ event, ...details }) + "\n", { mode: 0o600 });
await withCatalog(async (f, db) => {
  const namespace = `catalog-artifact:${crypto.randomUUID().replaceAll("-", "")}`,
    children = new Set<ReturnType<typeof Bun.spawn>>(),
    logs: Promise<string>[] = [];
  const temporary = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response() });
  const port = temporary.port;
  await temporary.stop(true);
  const origin = `http://127.0.0.1:${port}`,
    helper = resolve("dist/native/plugin-host"),
    signedDirectory = resolve(f.directory, "artifact-signed");
  const start = (
    path: string,
    args: string[] = [],
    env: Record<string, string | undefined> = { ...process.env, MINIMAX_API_KEY: "" },
  ) => {
    const child = Bun.spawn(["bun", resolve(path), ...args], {
      cwd: f.directory,
      env,
      stdout: "pipe",
      stderr: "pipe",
    });
    children.add(child);
    const stdout = new Response(child.stdout).text(),
      stderr = new Response(child.stderr).text();
    logs.push(stderr);
    return { child, stdout, stderr };
  };
  await record("redis_prefix_created", { namespace });
  try {
    const signed = start("dist/tools/sign-bundled-release.js", [
      "--private-key-file",
      resolve(f.directory, "ephemeral-key.pem"),
      "--key-id",
      "synthetic-catalog-test",
      "--output-directory",
      signedDirectory,
      "--helper-executable",
      helper,
    ]);
    const output = await signed.stdout;
    assert.equal(await signed.child.exited, 0);
    assert.equal(await signed.stderr, "");
    const descriptor = JSON.parse(output);
    assert.equal(descriptor.package_digest, f.release.descriptor.package_digest);
    await writeFile(
      f.policyPath,
      JSON.stringify({
        ...f.policy,
        releases: [{ ...f.policy.releases[0]!, descriptor_digest: descriptor.descriptor_digest }],
      }),
    );
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${(await db`SELECT current_database() AS name`)[0].name}`;
    const server = start("dist/gateway/main.js", [], {
      ...process.env,
      MINIMAX_API_KEY: "",
      DATABASE_URL: url.toString(),
      NODE_ENV: "test",
      ADMIN_KEY: syntheticAdminKey,
      KEY_PEPPER: syntheticPepper,
      ENCRYPTION_KEY: "44".repeat(32),
      HOST: "127.0.0.1",
      PORT: String(port),
      REDIS_NAMESPACE: namespace,
      QUEUE_REQUIRED_REPLICAS: "0",
      ALLOW_PRIVATE_UPSTREAM: "true",
      PRIVATE_UPSTREAM_CIDRS: "127.0.0.0/8",
      PLUGIN_RELEASE_POLICY: f.policyPath,
      PLUGIN_RELEASE_DIRECTORY: signedDirectory,
      PLUGIN_HELPER_EXECUTABLE: helper,
      SHUTDOWN_GRACE_MS: "1000",
    });
    logs.push(server.stdout);
    await record("process_started", { pid: server.child.pid, purpose: "compiled-catalog-gateway" });
    const deadline = Date.now() + 5000;
    while (true) {
      try {
        const ready = await fetch(origin + "/readyz", { signal: AbortSignal.timeout(200) });
        if (ready.ok) break;
      } catch {}
      if (Date.now() > deadline) throw new Error("Compiled catalog readiness timeout");
      await Bun.sleep(20);
    }
    const request = async (path: string, body?: unknown, method = body ? "POST" : "GET") => {
      const response = await fetch(origin + path, {
        method,
        headers: {
          authorization: `Bearer ${syntheticAdminKey}`,
          "content-type": "application/json",
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      assert(response.ok, `Compiled catalog ${path}: ${response.status}`);
      return (await response.json()) as Record<string, unknown>;
    };
    const provider = await request("/admin/providers", {
      name: `artifact-provider-${port}`,
      kind: "openai",
      base_url: "http://127.0.0.1:1/v1",
      api_key: "synthetic-no-real-provider-key",
      concurrency: 1,
      rpm: 1,
    });
    assert.equal(provider.config_revision, "0");
    assert.equal(provider.binding_id, null);
    assert.equal(provider.funded, false);
    assert.equal(
      (await request(`/admin/providers/${provider.id}/plugin-binding`)).executable,
      false,
    );
    const bindingPath = resolve(f.directory, "binding.json"),
      binding = f.binding("00000000-0000-4000-8000-000000000001", {
        account_id: provider.account_id,
        brand_id: provider.brand_id,
        secret_reference: `channel:${provider.id}`,
      });
    const bindingInput: Record<string, unknown> = { ...binding };
    delete bindingInput.version_id;
    await writeFile(bindingPath, JSON.stringify(bindingInput), { mode: 0o600 });
    const install = start("dist/tools/install-bundled-release.js", [
      "--admin-url",
      origin + "/admin",
      "--credential-file",
      f.credentialPath,
      "--command-id",
      crypto.randomUUID(),
      "--release-id",
      bundledReleaseId,
      "--reason",
      "Synthetic compiled catalog acceptance",
      "--provider-id",
      String(provider.id),
      "--binding-file",
      bindingPath,
    ]);
    const installed = JSON.parse(await install.stdout);
    assert.equal(await install.child.exited, 0);
    assert.equal(await install.stderr, "");
    assert.equal(installed.publication.state, "PUBLISHED");
    assert.equal(installed.publication.revision, "3");
    assert.equal(installed.binding.config_revision, "1");
    const truth = await request(`/admin/providers/${provider.id}/plugin-binding`);
    assert.equal(truth.executable, true);
    assert.equal(truth.trust_state, "TRUSTED");
    const result = (await db`SELECT state,result FROM plugin_validation_runs`)[0];
    assert.equal(result.state, "SUCCEEDED");
    assert.deepEqual(result.result, {
      cases: 15,
      steps: 24,
      cleanup_confirmed: true,
      error_code: null,
    });
    assert.equal((await db`SELECT count(*)::int AS n FROM plugin_publication_receipts`)[0].n, 1);
    assert.equal((await db`SELECT count(*)::int AS n FROM channel_plugin_bindings`)[0].n, 1);
    server.child.kill("SIGTERM");
    await server.child.exited;
    await record("process_reaped", { pid: server.child.pid, purpose: "compiled-catalog-gateway" });
    console.log(
      JSON.stringify({
        catalog_artifact: "pass",
        checks: [
          "standalone compiled signer resolves immutable builtin assets",
          "compiled gateway loads explicit external policy snapshot",
          "compiled provider API returns real zero-funded IDs",
          "unbound provider is not executable",
          "compiled installer runs 15 cases and 24 steps through native helper",
          "atomic publication and revisioned binding use same public application",
          "SIGTERM closes catalog and owned helper before exit",
        ],
        publication_revision: "3",
        binding_revision: "1",
        cases: 15,
        steps: 24,
      }),
    );
  } finally {
    for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
    await Promise.allSettled([...children].map((c) => c.exited));
    await Promise.all(logs);
    for (const endpoint of new Set(
      [
        process.env.REDIS_URL,
        process.env.QUEUE_REDIS_URL,
        ...(process.env.REDIS_EXTRA_URLS ?? "").split(","),
      ].filter(Boolean),
    )) {
      const redis = new RedisClient(endpoint!);
      try {
        const keys = (await redis.send("KEYS", [`*${namespace}*`])) as string[];
        if (keys.length) await redis.send("DEL", keys);
        assert.equal(((await redis.send("KEYS", [`*${namespace}*`])) as string[]).length, 0);
      } finally {
        redis.close();
      }
    }
    await record("redis_prefix_removed", { namespace });
  }
});
