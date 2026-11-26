import { internalCall, publishPlugins } from "./gateway-fixture.ts";
import { expect } from "bun:test";
import { getBuiltinPlugin } from "@gateway/plugin-engine";
import { createGatewayRuntime } from "@gateway/gateway";

export type ProviderCapture = {
  path: string;
  query: string;
  headers: Record<string, string>;
  body: Record<string, unknown>;
};

export async function exerciseProviderPluginReleases({
  runtime,
  upstream,
  captures,
}: {
  runtime: ReturnType<typeof createGatewayRuntime>;
  upstream: Bun.Server<undefined>;
  captures: ProviderCapture[];
}) {
  const { status: bStat, data: bProv } = await internalCall(
    runtime.internalApp,
    "GET",
    "/internal/v1/plugins/builtins?capability=PROVIDER",
  );
  expect(bStat).toBe(200);
  const provNames = (bProv.items as { name: string }[]).map((p) => p.name);
  expect(provNames.length).toBe(15);
  expect(provNames).toContain("qwen");
  expect(provNames).toContain("minimax");
  expect(provNames).toContain("volcengine");
  expect(provNames).toContain("zhipu");
  expect(provNames).toContain("moonshot");
  expect(provNames).toContain("openrouter");

  const { data: billingAccount } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/billing-accounts",
    {
      name: "g7-proj",
      currency: "USD",
    },
  );
  const billingAccountId = String(billingAccount.id);
  const { data: keyRes } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
    billing_account_id: billingAccountId,
    name: "g7-key",
  });
  const apiKey = String(keyRes.key);
  await internalCall(
    runtime.internalApp,
    "POST",
    `/internal/v1/wallets/${billingAccountId}/credits`,
    {
      currency: "USD",
      external_ref: "g7-credit-1",
      amount_atoms: "10000000",
      kind: "CASH",
    },
  );
  const { data: acc } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/upstream-accounts",
    { name: "g7-acc" },
  );
  await internalCall(
    runtime.internalApp,
    "POST",
    `/internal/v1/upstream-accounts/${acc.id}/recharges`,
    {
      currency: "USD",
      external_ref: "g7-sup-1",
      amount_atoms: "10000000",
    },
  );
  const { data: custPrice } = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/prices",
    {
      side: "CUSTOMER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "10", denominator: "1" },
        cached_input_tokens: { numerator: "2", denominator: "1" },
        output_tokens: { numerator: "20", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "1000" },
    },
  );
  const { data: supPrice } = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/prices",
    {
      side: "SUPPLIER",
      currency: "USD",
      rates: {
        input_tokens: { numerator: "2", denominator: "1" },
        cached_input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "4", denominator: "1" },
      },
      ceilings: { input_tokens: "1000", cached_input_tokens: "1000", output_tokens: "1000" },
    },
  );

  const { status: unknownChStatus, data: unknownChData } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/channels",
    {
      account_id: acc.id,
      name: "bad-ch",
      kind: "not-installed-vendor",
      base_url: `http://127.0.0.1:${upstream.port}`,
      api_key: "sk-x",
    },
  );
  expect(unknownChStatus).toBe(400);
  expect((unknownChData.error as { code: string }).code).toBe("unknown_provider");

  const { status: protoBadStatus } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/plugins",
    {
      name: "bad-proto",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: { scheme: "bearer" },
        endpoints: { default: { path: "/v1/chat" } },
        request_transforms: [{ op: "drop", path: "__proto__.polluted" }],
      },
    },
  );
  expect(protoBadStatus).toBe(400);

  const { status: acmePluginStatus, data: acmePlugin } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/plugins",
    {
      name: "acme-llm",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: { scheme: "header", header_name: "X-Acme-Key", prefix: "Key " },
        endpoints: {
          chat: { path: "/v2/infer/chat", query: { api_ver: "2026" } },
        },
        extra_headers: { "X-Client-App": "super-token-gateway" },
        request_transforms: [
          { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
          { op: "drop", path: "stream_options" },
          { op: "clamp", path: "temperature", min: 0, max: 1 },
          { op: "set_default", path: "thinking_mode", value: "auto" },
        ],
        usage_aliases: [{ target: "cached_input_tokens", source_path: "usage.cache_hit_tokens" }],
      },
    },
  );
  expect(acmePluginStatus).toBe(200);
  await publishPlugins(runtime, [[acmePlugin.id]]);
  const immutableRevision = await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
    name: "acme-llm",
    version: "1.0.0",
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      auth: { scheme: "bearer" },
      endpoints: { chat: { path: "/does-not-replace-existing-revision" } },
    },
  });
  expect(immutableRevision.status).toBe(409);
  expect(immutableRevision.data.error).toMatchObject({ code: "plugin_revision_conflict" });

  const { status: acmeChStatus, data: chAcme } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/channels",
    {
      account_id: acc.id,
      name: "acme-ch",
      kind: "acme-llm",
      base_url: `http://127.0.0.1:${upstream.port}`,
      api_key: "sk-acme-secret-1",
    },
  );
  expect(acmeChStatus).toBe(200);
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
    name: "acme-model",
    operations: ["chat"],
    max_output_tokens: 200,
  });
  const invalidBinding = await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
    model_name: "acme-model",
    channel_id: chAcme.id,
    upstream_model: "acme-v2-pro",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
    plugin_pipeline: [{ plugin_id: crypto.randomUUID(), priority: 1 }],
  });
  expect(invalidBinding.status).toBe(400);
  expect(invalidBinding.data.error).toMatchObject({ code: "invalid_plugin_binding" });
  const { data: acmeRoute } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/routes",
    {
      model_name: "acme-model",
      channel_id: chAcme.id,
      upstream_model: "acme-v2-pro",
      operations: ["chat"],
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    },
  );

  const acmeResp = await runtime.publicApp.fetch(
    new Request("http://gw.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "acme-model",
        messages: [{ role: "user", content: "hi acme" }],
        max_completion_tokens: 80,
        temperature: 1.9,
      }),
    }),
  );
  expect(acmeResp.status).toBe(200);
  const acmeBody = (await acmeResp.json()) as { choices: { message: { content: string } }[] };
  expect(acmeBody.choices[0]!.message.content).toBe("from-acme-declarative");
  const acmeReqId = acmeResp.headers.get("x-request-id")!;
  const acmeCap = captures[captures.length - 1]!;
  expect(acmeCap.body.max_tokens).toBe(80);
  expect(acmeCap.body.max_completion_tokens).toBeUndefined();
  expect(acmeCap.body.temperature).toBe(1);
  expect(acmeCap.body.thinking_mode).toBe("auto");
  const { data: acmeBilling } = await internalCall(
    runtime.internalApp,
    "GET",
    `/internal/v1/requests/${acmeReqId}`,
  );
  expect((acmeBilling.charge as { usage: Record<string, string> }).usage.cached_input_tokens).toBe(
    "4",
  );

  const draftRevision = await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
    name: "acme-llm",
    version: "2.0.0",
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    provider_config: {
      base_dialect: "openai_chat",
      usage_dialect: "openai",
      auth: { scheme: "header", header_name: "X-Acme-Key", prefix: "Key " },
      endpoints: { chat: { path: "/v2/infer/chat-next" } },
    },
  });
  expect(draftRevision.status).toBe(200);
  expect(draftRevision.data.enabled).toBe(false);
  const releaseId = crypto.randomUUID();
  const publish = {
    id: releaseId,
    expected_epoch: runtime.snapshot.get().epoch,
    targets: [{ channel_id: chAcme.id, provider_plugin_id: draftRevision.data.id }],
  };
  const dispatchAcme = async (content = "release check") => {
    const response = await runtime.publicApp.fetch(
      new Request("http://gw.local/v1/chat/completions", {
        method: "POST",
        headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
        body: JSON.stringify({
          model: "acme-model",
          messages: [{ role: "user", content }],
        }),
      }),
    );
    expect(response.status, await response.text()).toBe(200);
    return captures.at(-1)?.path;
  };
  expect(await dispatchAcme()).toBe("/v2/infer/chat");
  const validated = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/plugin-releases/validate",
    publish,
  );
  expect(validated.status).toBe(200);
  expect(await dispatchAcme()).toBe("/v2/infer/chat");
  const published = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/plugin-releases",
    publish,
  );
  expect(published.status, JSON.stringify(published.data)).toBe(200);
  expect(published.data.config_epoch).toBe(publish.expected_epoch + 1);
  expect(await dispatchAcme()).toBe("/v2/infer/chat-next");
  const replay = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/plugin-releases",
    publish,
  );
  expect(replay.data.config_epoch).toBe(published.data.config_epoch);
  const stale = await internalCall(runtime.internalApp, "POST", "/internal/v1/plugin-releases", {
    ...publish,
    id: crypto.randomUUID(),
  });
  expect(stale.status).toBe(409);
  const rollbackPath = `/internal/v1/plugin-releases/${releaseId}/rollback`;
  const rollback = await internalCall(runtime.internalApp, "POST", rollbackPath, {
    id: crypto.randomUUID(),
    expected_epoch: published.data.config_epoch,
  });
  expect(rollback.status, JSON.stringify(rollback.data)).toBe(200);
  expect(rollback.data.rollback_of).toBe(releaseId);
  expect(await dispatchAcme()).toBe("/v2/infer/chat");
  const lateRollback = await internalCall(runtime.internalApp, "POST", rollbackPath, {
    id: crypto.randomUUID(),
    expected_epoch: rollback.data.config_epoch,
  });
  expect(lateRollback.status).toBe(409);

  const guard = getBuiltinPlugin("pii-guard");
  if (guard?.capability !== "CONTENT") throw new Error("Missing content plugin fixture");
  const contentDraft = await internalCall(runtime.internalApp, "PUT", "/internal/v1/plugins", {
    name: "acme-contact-filter",
    version: "1.0.0",
    kind: "WASM",
    wasm_bytes_base64: Buffer.from(guard.wasm_bytes).toString("base64"),
    manifest: { hooks: ["on_request"], failure_policy: "FAIL_CLOSED" },
  });
  expect(contentDraft.status).toBe(200);
  const contactText = "Contact alice@example.com";
  const contentRelease = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/plugin-releases",
    {
      id: crypto.randomUUID(),
      expected_epoch: runtime.snapshot.get().epoch,
      targets: [
        {
          route_id: acmeRoute.id,
          plugin_pipeline: [{ plugin_id: contentDraft.data.id, priority: 1 }],
        },
      ],
      samples: { [String(contentDraft.data.id)]: { text: contactText } },
    },
  );
  expect(contentRelease.status, JSON.stringify(contentRelease.data)).toBe(200);
  await dispatchAcme(contactText);
  expect(captures.at(-1)?.body.messages).toEqual([
    { role: "user", content: "Contact [REDACTED_EMAIL]" },
  ]);
  const contentRollback = await internalCall(
    runtime.internalApp,
    "POST",
    `/internal/v1/plugin-releases/${contentRelease.data.id}/rollback`,
    {
      id: crypto.randomUUID(),
      expected_epoch: runtime.snapshot.get().epoch,
    },
  );
  expect(contentRollback.status).toBe(200);
  await dispatchAcme(contactText);
  expect(captures.at(-1)?.body.messages).toEqual([{ role: "user", content: contactText }]);

  const { status: dsInstallStatus, data: deepseekPlugin } = await internalCall(
    runtime.internalApp,
    "POST",
    "/internal/v1/plugins/builtins/deepseek/install",
    {
      provider_config: {
        extra_headers: { "X-Hot-Override": "enabled-v2" },
        request_transforms: [
          { op: "rename", from: "max_completion_tokens", to: "max_tokens", overwrite: false },
          { op: "set_default", path: "custom_hot_flag", value: "live" },
        ],
      },
    },
  );
  expect(dsInstallStatus).toBe(200);
  await publishPlugins(runtime, [[deepseekPlugin.id]]);
  const { data: chDs } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "ds-hot-ch",
    kind: "deepseek",
    base_url: `http://127.0.0.1:${upstream.port}`,
    api_key: "sk-ds-hot",
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
    name: "ds-hot-model",
    operations: ["chat"],
    max_output_tokens: 200,
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
    model_name: "ds-hot-model",
    channel_id: chDs.id,
    upstream_model: "deepseek-chat",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  const dsHotResp = await runtime.publicApp.fetch(
    new Request("http://gw.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "ds-hot-model",
        messages: [{ role: "user", content: "hot override" }],
        max_completion_tokens: 40,
      }),
    }),
  );
  expect(dsHotResp.status).toBe(200);
  const dsHotCap = captures[captures.length - 1]!;
  expect(dsHotCap.headers["x-hot-override"]).toBe("enabled-v2");
  expect(dsHotCap.body.custom_hot_flag).toBe("live");
  expect(dsHotCap.body.max_tokens).toBe(40);

  const { data: hmacPlugin } = await internalCall(
    runtime.internalApp,
    "PUT",
    "/internal/v1/plugins",
    {
      name: "hmac-vendor",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: {
          scheme: "hmac_sha256",
          signature_header: "X-Hmac-Sig",
          timestamp_header: "X-Hmac-Ts",
          key_id_header: "X-Hmac-Key-Id",
          signature_prefix: "v1=",
          encoding: "hex",
        },
        endpoints: { default: { path: "/v1/hmac/chat" } },
      },
    },
  );
  await publishPlugins(runtime, [[hmacPlugin.id]]);
  const { data: chHmac } = await internalCall(runtime.internalApp, "PUT", "/internal/v1/channels", {
    account_id: acc.id,
    name: "hmac-ch",
    kind: "hmac-vendor",
    base_url: `http://127.0.0.1:${upstream.port}`,
    api_key: JSON.stringify({ keyId: "ak-99", secretKey: "hmac-secret-xyz" }),
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/models", {
    name: "hmac-model",
    operations: ["chat"],
    max_output_tokens: 100,
  });
  await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
    model_name: "hmac-model",
    channel_id: chHmac.id,
    upstream_model: "hmac-m",
    operations: ["chat"],
    billing_price_id: custPrice.id,
    cost_version_id: supPrice.id,
  });
  const hmacResp = await runtime.publicApp.fetch(
    new Request("http://gw.local/v1/chat/completions", {
      method: "POST",
      headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "hmac-model",
        messages: [{ role: "user", content: "verify hmac" }],
      }),
    }),
  );
  expect(hmacResp.status).toBe(200);
  expect(
    ((await hmacResp.json()) as { choices: { message: { content: string } }[] }).choices[0]!.message
      .content,
  ).toBe("hmac-verified");

  return { acc, custPrice, supPrice, apiKey };
}
