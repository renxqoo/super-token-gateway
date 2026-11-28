import { expect, test } from "bun:test";
import { createHash, createHmac } from "node:crypto";
import {
  accountId,
  adminAccess,
  billingAccountId,
  customerAccess,
  h,
  registerSyncFixture,
} from "./console-sync-fixture.ts";
import { draft, onboarding, waitForCommit } from "./console-model-onboarding-fixture.ts";

registerSyncFixture();

async function declarativeChannel(
  name: string,
  config: Record<string, unknown>,
  connection = { base_url: h.mockUrl, api_key: "" },
) {
  const plugin = await h.gatewayInternalApi<{ id: string }>("PUT", "/internal/v1/plugins", {
    name,
    version: "1",
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    provider_config: {
      base_dialect: "passthrough",
      usage_dialect: "openai",
      auth: { scheme: "none" },
      ...config,
    },
  });
  expect(plugin.status, JSON.stringify(plugin.data)).toBe(200);
  const active = await h.gatewayInternalApi("POST", "/internal/v1/plugin-releases", {
    id: crypto.randomUUID(),
    expected_epoch: h.gateway.snapshot.get().epoch,
    targets: [{ plugin_id: plugin.data.id }],
  });
  expect(active.status, JSON.stringify(active.data)).toBe(200);
  const channel = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
    account_id: accountId,
    name,
    kind: name,
    provider_plugin_id: plugin.data.id,
    ...connection,
  });
  expect(channel.status, JSON.stringify(channel.data)).toBe(201);
  return channel.data.id;
}

test("video onboarding rejects a create-only adapter whose tasks cannot be polled", async () => {
  const endpoints = { videos: { path: "/v2/video_generation" } };
  const createOnly = await declarativeChannel("onboarding-video-create", { endpoints });
  const nonSpecific = await declarativeChannel("onboarding-video-static", {
    endpoints,
    async_task_config: { query_path: "/v2/query/video_generation" },
  });
  const pollable = await declarativeChannel("onboarding-video-poll", {
    endpoints,
    async_task_config: { query_path: "/v2/query/video_generation", query_param: "task_id" },
  });
  const videoDraft = async (name: string, channelId: string) => {
    const input = await draft(name);
    return {
      ...input,
      model: { ...input.model, operations: ["videos"] },
      deployments: [{ ...input.deployments[0]!, channel_id: channelId }],
    };
  };
  const valid = await videoDraft("onboarding/video-pollable", pollable);
  expect((await h.adminApi("POST", onboarding + "/validate", adminAccess, valid)).status).toBe(200);
  for (const [name, channel] of [
    ["onboarding/video-create-only", createOnly],
    ["onboarding/video-without-task-id", nonSpecific],
  ] as const) {
    const invalid = await videoDraft(name, channel);
    const rejected = await h.adminApi("POST", onboarding, adminAccess, invalid);
    expect(rejected.status, name).toBe(400);
    expect(rejected.data.error).toMatchObject({ code: "invalid_model_onboarding" });
    expect(h.gateway.snapshot.get().models.has(name)).toBe(false);
  }
});

test("signed video task polling authenticates the GET request sent to the provider", async () => {
  const secret = "local-video-fixture-key";
  const upstreamTask = "provider-video-1";
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const url = new URL(request.url);
      const bodyHash = createHash("sha256")
        .update(await request.text())
        .digest("hex");
      const signature = createHmac("sha256", secret)
        .update(
          `${request.method}\n${url.pathname}${url.search}\n${request.headers.get("x-task-time")}\n${bodyHash}`,
        )
        .digest("hex");
      if (request.headers.get("x-task-signature") !== signature)
        return new Response(null, { status: 401 });
      if (request.method === "POST" && url.pathname === "/video") {
        return Response.json({ task_id: upstreamTask, status: "processing" });
      }
      if (
        request.method === "GET" &&
        url.pathname === "/video/status" &&
        url.searchParams.get("task_id") === upstreamTask
      ) {
        return Response.json({ task_id: upstreamTask, status: "completed" });
      }
      return new Response(null, { status: 404 });
    },
  });
  try {
    const channel = await declarativeChannel(
      "onboarding-video-signed",
      {
        auth: {
          scheme: "hmac_sha256",
          signature_header: "x-task-signature",
          timestamp_header: "x-task-time",
        },
        endpoints: { videos: { path: "/video" } },
        async_task_config: { query_path: "/video/status", query_param: "task_id" },
      },
      { base_url: upstream.url.toString(), api_key: secret },
    );
    const prices = [];
    for (const side of ["CUSTOMER", "SUPPLIER"]) {
      const price = await h.gatewayInternalApi<{ id: string }>("POST", "/internal/v1/prices", {
        side,
        currency: "USD",
        basis: "REQUEST_COMPLETION",
        rates: { requests: { numerator: "1", denominator: "1" } },
        ceilings: { requests: "1" },
      });
      expect(price.status).toBe(201);
      prices.push(price.data.id);
    }
    const draftInput = await draft("onboarding/video-signed");
    const input = {
      ...draftInput,
      model: { ...draftInput.model, operations: ["videos"] },
      billing_price_id: prices[0]!,
      deployments: [
        { ...draftInput.deployments[0]!, channel_id: channel, cost_version_id: prices[1]! },
      ],
    };
    expect([200, 202]).toContain((await h.adminApi("POST", onboarding, adminAccess, input)).status);
    await waitForCommit(input);
    const key = await h.clientApi<{ key: string }>(
      "POST",
      `/api/v1/billing-accounts/${billingAccountId}/api-keys`,
      customerAccess,
      { name: "video-poll-test" },
    );
    expect(key.status).toBe(201);
    const headers = { authorization: `Bearer ${key.data.key}`, "content-type": "application/json" };
    const submitted = await fetch(`${h.gatewayPublicUrl}/v1/videos`, {
      method: "POST",
      headers,
      body: JSON.stringify({ model: input.model.name, prompt: "local test video" }),
    });
    const task = (await submitted.json()) as { gateway_task_id?: string };
    expect(submitted.status, JSON.stringify(task)).toBe(200);
    expect(task.gateway_task_id).toMatch(/^vid_/);
    const polled = await fetch(`${h.gatewayPublicUrl}/v1/videos/${task.gateway_task_id}`, {
      headers,
    });
    const result = await polled.json();
    expect(polled.status, JSON.stringify(result)).toBe(200);
    expect(result).toMatchObject({ upstream_task_id: upstreamTask, status: "SUCCEEDED" });
  } finally {
    await upstream.stop(true);
  }
});

test("multipart onboarding accepts the same default endpoint as live transcription routing", async () => {
  const channel = await declarativeChannel("onboarding-multipart-default", {
    endpoints: { default: { path: "/v1/audio/transcriptions" } },
  });
  const input = await draft("onboarding/multipart-default");
  const transcription = {
    ...input,
    model: { ...input.model, operations: ["transcription"] },
    deployments: [{ ...input.deployments[0]!, channel_id: channel }],
  };
  const published = await h.adminApi("POST", onboarding, adminAccess, transcription);
  expect([200, 202]).toContain(published.status);
  expect(await waitForCommit(transcription)).toMatchObject({ current_configuration: "ACTIVE" });
});
