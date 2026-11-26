import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { makeConfig, db, internalCall, webhookSample } from "./gateway-fixture.ts";
import { registerGatewayFixture } from "./gateway-fixture.ts";
import { registerGatewayRoutingCases } from "./gateway-routing-cases.ts";
import { registerGatewayModerationCases } from "./gateway-moderation-cases.ts";
import { registerGatewayWorkerCases } from "./gateway-worker-cases.ts";
import { registerGatewayHardeningCases } from "./gateway-hardening-cases.ts";
import { registerGatewayWasmCases } from "./gateway-wasm-cases.ts";
import { registerGatewayPiiCases } from "./gateway-pii-cases.ts";
import { registerGatewayProviderCases } from "./gateway-provider-cases.ts";
import { registerGatewayBuiltinProviderCases } from "./gateway-builtin-provider-cases.ts";
import { registerGatewayPolicyAccountingCases } from "./gateway-policy-accounting-cases.ts";

registerGatewayFixture();

test("G7 Plugin uploads and builtin installs reject enabled:true", async () => {
  const runtime = createGatewayRuntime({ config: makeConfig(), db });
  try {
    await runtime.start();
    const draft = {
      name: "reject-legacy-activation",
      version: "1",
      kind: "WEBHOOK",
      webhook_config: { url: "http://127.0.0.1:1234/hook" },
    };
    expect(
      (
        await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
          ...draft,
          enabled: true,
        })
      ).status,
    ).toBe(400);
    expect(
      (
        await internalCall(
          runtime.internalApp,
          "POST",
          "/internal/v1/plugins/builtins/pii-guard/install",
          { enabled: true },
        )
      ).status,
    ).toBe(400);
    const created = await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", draft);
    expect(created.status).toBe(200);
    expect(created.data.enabled).toBe(false);
  } finally {
    await runtime.close();
  }
});

test("G7 Cross-node publication confirms applied snapshots and revokes pending and dispatched plugin work", async () => {
  const policyEntered = Promise.withResolvers<void>();
  const policyCancelled = Promise.withResolvers<void>();
  const upstreamCancelled = Promise.withResolvers<void>();
  const unlockAck = Promise.withResolvers<void>();
  const returnSettlement = Promise.withResolvers<void>();
  let ackLock: Promise<unknown> | undefined;
  let upstreamCalls = 0;
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      if (new URL(request.url).pathname === "/policy") {
        return new Response(
          new ReadableStream({
            start() {
              policyEntered.resolve();
            },
            cancel() {
              policyCancelled.resolve();
            },
          }),
          { headers: { "content-type": "application/json" } },
        );
      }
      upstreamCalls++;
      const body = (await request.json()) as { stream?: boolean };
      if (!body.stream) {
        return Response.json({
          id: "result",
          model: "upstream",
          object: "chat.completion",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 3, completion_tokens: 2 },
        });
      }
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(
              new TextEncoder().encode(
                'data: {"id":"running","model":"upstream","choices":[{"index":0,"delta":{"content":"partial"}}]}\n\n',
              ),
            );
          },
          cancel() {
            upstreamCancelled.resolve();
          },
        }),
        { headers: { "content-type": "text/event-stream" } },
      );
    },
  });

  const admin = createGatewayRuntime({ config: makeConfig(), db });
  const peer = createGatewayRuntime({ config: makeConfig({ snapshotPollMs: 250 }), db });
  const waitFor = async (condition: () => Promise<boolean>) => {
    const deadline = Date.now() + 3000;
    while (!(await condition())) {
      if (Date.now() >= deadline) throw new Error("Configuration was not applied within 3 seconds");
      await Bun.sleep(20);
    }
  };
  const call = async (method: string, path: string, body?: unknown) => {
    const result = await internalCall(admin.internalApp, method, path, body);
    expect(result.status, JSON.stringify(result.data)).toBe(
      path === "/internal/v1/prices" ? 201 : 200,
    );
    return result.data;
  };

  try {
    await admin.start();
    await peer.start();
    const seenNodes = [admin.cluster.nodeId, peer.cluster.nodeId].sort();
    const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
      name: "revoke-billingAccount",
      currency: "USD",
      rpm: 500,
      tpm: 100_000,
      concurrency: 20,
    });
    await call("POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
      currency: "USD",
      external_ref: "revoke-credit",
      amount_atoms: "1000000",
      kind: "CASH",
    });
    const key = await call("PUT", "/internal/v1/api-keys", {
      billing_account_id: billingAccount.id,
      name: "revoke-test",
    });
    const account = await call("PUT", "/internal/v1/upstream-accounts", {
      name: "revoke-upstream",
    });
    await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
      currency: "USD",
      external_ref: "revoke-supplier-credit",
      amount_atoms: "1000000",
    });
    const channel = await call("PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai",
      name: "revoke-primary",
      base_url: upstream.url.origin,
      api_key: "mock-revoke",
    });
    const healthyChannel = await call("PUT", "/internal/v1/channels", {
      account_id: account.id,
      kind: "openai_compatible",
      name: "revoke-unrelated",
      base_url: upstream.url.origin,
      api_key: "mock-revoke",
    });
    const customer = await call("POST", "/internal/v1/prices", {
      side: "CUSTOMER",
      currency: "USD",
      rates: { input_tokens: { numerator: "1", denominator: "1" } },
      ceilings: { input_tokens: "50" },
    });
    const supplier = await call("POST", "/internal/v1/prices", {
      side: "SUPPLIER",
      currency: "USD",
      rates: { input_tokens: { numerator: "1", denominator: "1" } },
      ceilings: { input_tokens: "50" },
    });
    const policy = await call("PUT", "/internal/v1/plugins", {
      name: "revoke-policy",
      version: "1",
      kind: "WEBHOOK",
      webhook_config: {
        url: `${upstream.url.origin}/policy`,
        timeout_ms: 5000,
        failure_policy: "FAIL_OPEN",
      },
      manifest: { hooks: ["on_request"] },
    });
    const providerDefinition = {
      name: "revoke-provider",
      version: "1",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: { scheme: "bearer" },
        endpoints: { chat: { path: "/v1/chat/completions" } },
      },
    };
    const provider = await call("PUT", "/internal/v1/plugins", providerDefinition);

    const routes = new Map<string, Record<string, unknown>>();
    for (const model of ["revoke-pre", "revoke-after", "revoke-unrelated"]) {
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 20,
      });
      routes.set(
        model,
        await call("PUT", "/internal/v1/routes", {
          model_name: model,
          channel_id: model === "revoke-after" ? channel.id : healthyChannel.id,
          upstream_model: "upstream",
          operations: ["chat"],
          priority: model === "revoke-after" ? 10 : 0,
          billing_price_id: customer.id,
          cost_version_id: supplier.id,
        }),
      );
    }
    const release = await call("POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: admin.snapshot.get().epoch,
      targets: [
        { channel_id: channel.id, provider_plugin_id: provider.id },
        {
          route_id: routes.get("revoke-pre")!.id,
          plugin_pipeline: [{ plugin_id: policy.id, priority: 1 }],
        },
      ],
      samples: { [String(policy.id)]: webhookSample },
    });
    expect(release.expected_nodes).toEqual(seenNodes);
    const releasePath = `/internal/v1/plugin-releases/${release.id}`;
    await waitFor(async () => {
      const value = await call("GET", releasePath);
      const confirmations = value.nodes as {
        node_id: string;
        applied_epoch: number;
        status: string;
        applied_at: string;
      }[];
      return (
        confirmations.length === 2 &&
        confirmations.every(
          (node) =>
            node.status === "APPLIED" &&
            Boolean(node.applied_at) &&
            Number(node.applied_epoch) === release.config_epoch,
        )
      );
    });

    const request = (model: string, stream = false) =>
      peer.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
          body: JSON.stringify({ model, stream, messages: [{ role: "user", content: "test" }] }),
        }),
      );
    const pending = request("revoke-pre");
    await policyEntered.promise;
    const ackLocked = Promise.withResolvers<void>();
    ackLock = db.begin(async (tx) => {
      await tx`SELECT id FROM public.gateway_config_nodes WHERE id = ${peer.cluster.nodeId} FOR UPDATE`;
      ackLocked.resolve();
      await unlockAck.promise;
    });
    await ackLocked.promise;
    await waitFor(async () => {
      const [blocked] = await db`SELECT EXISTS (
        SELECT 1 FROM pg_stat_activity WHERE datname = current_database()
          AND wait_event_type = 'Lock' AND query LIKE '%INSERT INTO public.gateway_config_nodes%'
      ) AS waiting`;
      return blocked?.waiting === true;
    });
    const revokedPolicy = await call("POST", `/internal/v1/plugins/${policy.id}/revoke`, {
      reason: "policy incident",
    });
    const cancelledBefore = await Promise.race([pending, Bun.sleep(1500).then(() => null)]);
    unlockAck.resolve();
    await ackLock;
    if (!cancelledBefore)
      throw new Error("Node waited for its write lock before applying revocation");
    expect(cancelledBefore.status).toBe(503);
    expect(await cancelledBefore.json()).toMatchObject({ error: { code: "plugin_revoked" } });
    await policyCancelled.promise;
    expect(upstreamCalls).toBe(0);
    expect(
      (
        await internalCall(
          admin.internalApp,
          "GET",
          `/internal/v1/requests/${cancelledBefore.headers.get("x-request-id")}`,
        )
      ).status,
    ).toBe(404);

    const running = await request("revoke-after", true);
    expect(running.status).toBe(200);
    const reader = running.body!.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toContain("partial");
    const revokedProvider = await call("POST", `/internal/v1/plugins/${provider.id}/revoke`, {
      reason: "provider incident",
    });
    expect(revokedProvider.config_epoch).toBe(Number(revokedPolicy.config_epoch) + 1);
    const interruptedFrames = await new Response(
      new ReadableStream({
        async pull(controller) {
          const item = await reader.read();
          if (item.done) controller.close();
          else controller.enqueue(item.value);
        },
      }),
    ).text();
    expect(interruptedFrames).toContain("plugin_revoked");
    expect(interruptedFrames).not.toContain("[DONE]");
    await upstreamCancelled.promise;
    const requestPath = `/internal/v1/requests/${running.headers.get("x-request-id")}`;
    await waitFor(
      async () =>
        ((await call("GET", requestPath)).hold as { state?: string } | null)?.state === "UNKNOWN",
    );
    const outcome = await call("GET", requestPath);
    expect(outcome.hold).toMatchObject({ state: "UNKNOWN" });
    expect(outcome.charge).toBeNull();
    await call("POST", `${requestPath}/resolve`, {
      action: "RELEASE",
      reason: "synthetic upstream stopped",
    });
    expect((await request("revoke-after", true)).status).toBe(503);
    expect((await request("revoke-unrelated")).status).toBe(200);
    expect(upstreamCalls).toBe(2);

    const rePublish = await internalCall(
      admin.internalApp,
      "POST",
      "/internal/v1/plugin-releases",
      {
        id: crypto.randomUUID(),
        expected_epoch: admin.snapshot.get().epoch,
        targets: [{ plugin_id: provider.id }],
      },
    );
    expect(rePublish.status).toBe(422);
    await expect(
      db`UPDATE public.plugins SET enabled = true WHERE id = ${provider.id}`,
    ).rejects.toThrow("Revoked plugin revisions cannot be enabled");

    await call("PUT", "/internal/v1/routes", {
      model_name: "revoke-after",
      channel_id: healthyChannel.id,
      upstream_model: "upstream",
      operations: ["chat"],
      billing_price_id: customer.id,
      cost_version_id: supplier.id,
      priority: 1,
    });
    await waitFor(async () => peer.snapshot.get().epoch === admin.snapshot.get().epoch);
    const fallback = await request("revoke-after");
    expect(fallback.status).toBe(200);
    expect(fallback.headers.get("x-gateway-channel")).toBe("revoke-unrelated");

    const replacement = await call("PUT", "/internal/v1/plugins", {
      ...providerDefinition,
      version: "2",
    });
    const recovered = await call("POST", "/internal/v1/plugin-releases", {
      id: crypto.randomUUID(),
      expected_epoch: admin.snapshot.get().epoch,
      targets: [{ channel_id: channel.id, provider_plugin_id: replacement.id }],
    });
    const forbiddenRollback = await internalCall(
      admin.internalApp,
      "POST",
      `/internal/v1/plugin-releases/${recovered.id}/rollback`,
      { id: crypto.randomUUID(), expected_epoch: recovered.config_epoch },
    );
    expect(forbiddenRollback.status).toBe(422);
    await waitFor(async () => peer.snapshot.get().epoch === recovered.config_epoch);
    const originalCommit = peer.ledger.commitTerminal;
    const settlementCommitted = Promise.withResolvers<void>();
    peer.ledger.commitTerminal = async (input) => {
      const result = await originalCommit(input);
      settlementCommitted.resolve();
      await returnSettlement.promise;
      return result;
    };
    const untilSettlement = request("revoke-after");
    await settlementCommitted.promise;
    const lateRevocation = await call("POST", `/internal/v1/plugins/${replacement.id}/revoke`, {
      reason: "revoke while returning a committed response",
    });
    await waitFor(async () => peer.snapshot.get().epoch === lateRevocation.config_epoch);
    returnSettlement.resolve();
    const suppressed = await untilSettlement;
    peer.ledger.commitTerminal = originalCommit;
    expect(suppressed.status).toBe(503);
    expect(await suppressed.json()).toMatchObject({ error: { code: "plugin_revoked" } });
    const settled = await call(
      "GET",
      `/internal/v1/requests/${suppressed.headers.get("x-request-id")}`,
    );
    expect(settled.charge).toMatchObject({
      outcome: "SETTLED",
      channelId: channel.id,
      customerAtoms: "3",
    });
    const retry = await call("POST", `/internal/v1/plugins/${provider.id}/revoke`, {
      reason: "repeat",
    });
    expect(retry).toMatchObject({
      reason: "provider incident",
      config_epoch: revokedProvider.config_epoch,
    });
  } finally {
    unlockAck.resolve();
    returnSettlement.resolve();
    await ackLock;
    await upstream.stop(true);
    await Promise.all([peer.close(), admin.close()]);
  }
}, 12_000);

registerGatewayRoutingCases();
registerGatewayModerationCases();
registerGatewayWorkerCases();
registerGatewayHardeningCases();
registerGatewayWasmCases();
registerGatewayPiiCases();
registerGatewayProviderCases();
registerGatewayBuiltinProviderCases();
registerGatewayPolicyAccountingCases();
