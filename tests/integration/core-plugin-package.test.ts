import { expect, test } from "bun:test";
import { executionContextResponseSchema } from "@gateway/contracts";
import { CoreRpcError } from "@gateway/core-client";
import { packageTrust, verifyPackage } from "@gateway/plugin-sdk";
import { withFinancial } from "../support/financial-execution.ts";

async function candidateDigest(
  setup: { key: { key: string }; model: string },
  client: ReturnType<Parameters<Parameters<typeof withFinancial>[0]>[0]["client"]>,
) {
  const context = executionContextResponseSchema.parse(
    await client.execution(
      "execution_context_v1",
      {
        customer_token: setup.key.key,
        ip: "127.0.0.1",
        operation: "chat",
        model: setup.model,
        fingerprint: "0".repeat(64),
      },
      { deadlineAt: new Date(Date.now() + 10000).toISOString() },
    ),
  );
  return context.candidates[0]!.ref.package_digest;
}

async function refused(task: Promise<unknown>) {
  const error = await task.then(
    () => undefined,
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(CoreRpcError);
  return { code: (error as CoreRpcError).detail.code, status: (error as CoreRpcError).status };
}

test("a Gateway session downloads the published package that verifies against its own trust anchor as the candidate digest", () =>
  withFinancial(async (f) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const digest = await candidateDigest(setup, client);
    const bytes = await client.pluginPackage(digest);
    const trust = packageTrust(f.catalog.policy);
    expect(verifyPackage(bytes, trust(digest)).digest).toBe(digest);
    expect(() => verifyPackage(bytes, trust("0".repeat(64)))).toThrow();
  }));

test("unknown and quarantined packages are unavailable and a worker identity cannot download packages", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const digest = await candidateDigest(setup, client);
    expect(await refused(client.pluginPackage("f".repeat(64)))).toEqual({
      code: "plugin_package_unavailable",
      status: 404,
    });
    const worker = f.client(undefined, "worker");
    await worker.startup();
    expect(await refused(worker.pluginPackage(digest))).toEqual({
      code: "internal_peer_forbidden",
      status: 403,
    });
    await db.begin(async (tx) => {
      await tx`SELECT set_config('gateway.catalog_mutation','on',true)`;
      await tx`UPDATE plugin_versions SET quarantined=true WHERE package_digest=${digest}`;
    });
    expect(await refused(client.pluginPackage(digest))).toEqual({
      code: "plugin_package_unavailable",
      status: 404,
    });
  }));
