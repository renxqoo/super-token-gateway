import type { ConsoleConfig } from "@gateway/console-api";
import { afterAll, beforeAll, expect } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

export let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;

export let adminAccess = "";

export let customerAccess = "";

export let usdBillingAccountId = "";

export let eurBillingAccountId = "";

let accountId = "";

let channelId = "";

export let videoChannelId = "";

let customerPriceId = "";

let supplierPriceId = "";

export async function createTeamBillingAccount(name: string, slug: string, currency = "USD") {
  const team = await h.clientApi<{ default_billing_account_id: string }>(
    "POST",
    "/api/v1/orgs",
    customerAccess,
    { name, slug, billing_currency: currency },
  );
  expect(team.status).toBe(201);
  return team.data.default_billing_account_id;
}

export function registerBillingFixture(usageMonitor?: ConsoleConfig["usageMonitor"]) {
  beforeAll(async () => {
    h = await createConsoleTestHarness({
      consoleConfigOverrides: {
        allowPrivateAlertWebhooks: true,
        ...(usageMonitor ? { usageMonitor } : {}),
      },
      enableWorker: true,
    });
    const boot = await h.bootstrapAdmin();
    adminAccess = boot.accessToken;

    const user = await h.registerUser("billing-owner@example.com");
    customerAccess = user.accessToken;
    usdBillingAccountId = user.defaultBillingAccountId;

    eurBillingAccountId = await createTeamBillingAccount("EU Billing", "eu-billing", "EUR");

    const acc = await h.adminApi<{ id: string }>(
      "POST",
      "/admin/v1/upstream-accounts",
      adminAccess,
      {
        name: "bill-acc",
      },
    );
    expect(acc.status).toBe(201);
    accountId = acc.data.id;
    await h.adminApi("POST", `/admin/v1/upstream-accounts/${accountId}/recharges`, adminAccess, {
      currency: "USD",
      external_ref: "bill-sup-1",
      amount_atoms: "5000000",
    });

    const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
      account_id: accountId,
      name: "bill-openai",
      kind: "openai",
      base_url: h.mockUrl,
      api_key: "sk-bill-openai",
    });
    expect(ch.status).toBe(201);
    channelId = ch.data.id;

    const videoPlugin = await h.adminApi<{ id: string }>("PUT", "/admin/v1/plugins", adminAccess, {
      name: "bill-video-provider",
      version: "1.0.0",
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      provider_config: {
        base_dialect: "openai_chat",
        usage_dialect: "openai",
        auth: { scheme: "bearer" },
        endpoints: {
          chat: { path: "/v1/chat/completions" },
          videos: { path: "/v2/video_generation" },
        },
        async_task_config: {
          query_path: "/v2/query/video_generation",
          query_param: "task_id",
          task_id_path: "task_id",
          status_path: "status",
          succeeded_values: ["Success"],
          failed_values: ["Fail"],
          error_code_path: "error_code",
          refundable_system_error_codes: [5001],
          non_refundable_error_codes: [1026],
        },
      },
    });
    expect(videoPlugin.status).toBe(200);
    const vch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
      account_id: accountId,
      name: "bill-video",
      kind: "minimax",
      base_url: h.mockUrl,
      api_key: "sk-bill-video",
    });
    expect(vch.status).toBe(201);
    videoChannelId = vch.data.id;
    const videoRel = await h.adminApi("POST", "/admin/v1/plugin-releases", adminAccess, {
      id: crypto.randomUUID(),
      expected_epoch: h.gateway.snapshot.get().epoch,
      targets: [{ channel_id: videoChannelId, provider_plugin_id: videoPlugin.data.id }],
    });
    expect(videoRel.status).toBe(200);

    const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "bill-cust",
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "50" },
      },
    );
    expect(cp.status).toBe(201);
    customerPriceId = cp.data.currentPriceVersionId;

    const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "bill-sup",
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "50" },
      },
    );
    expect(sp.status).toBe(201);
    supplierPriceId = sp.data.currentPriceVersionId;

    const vcp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "bill-video-cust",
        side: "CUSTOMER",
        currency: "USD",
        basis: "REQUEST_COMPLETION",
        rates: { requests: { numerator: "5000", denominator: "1" } },
        ceilings: { requests: "1" },
      },
    );
    const vsp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "bill-video-sup",
        side: "SUPPLIER",
        currency: "USD",
        basis: "REQUEST_COMPLETION",
        rates: { requests: { numerator: "1000", denominator: "1" } },
        ceilings: { requests: "1" },
      },
    );

    expect(
      (
        await h.adminApi("POST", "/admin/v1/models", adminAccess, {
          name: "bill-gpt",
          display_name: "Bill GPT",
          operations: ["chat"],
          max_output_tokens: 100,
          list_price_template_id: cp.data.id,
        })
      ).status,
    ).toBe(201);
    expect(
      (
        await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
          model_name: "bill-gpt",
          channel_id: channelId,
          upstream_model: "bill-gpt-up",
          operations: ["chat"],
          billing_price_id: customerPriceId,
          cost_version_id: supplierPriceId,
        })
      ).status,
    ).toBe(201);

    expect(
      (
        await h.adminApi("POST", "/admin/v1/models", adminAccess, {
          name: "bill-video",
          display_name: "Bill Video",
          operations: ["videos"],
          max_output_tokens: 1,
          list_price_template_id: vcp.data.id,
        })
      ).status,
    ).toBe(201);
    expect(
      (
        await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
          model_name: "bill-video",
          channel_id: videoChannelId,
          upstream_model: "MiniMax-H3",
          operations: ["videos"],
          billing_price_id: vcp.data.currentPriceVersionId,
          cost_version_id: vsp.data.currentPriceVersionId,
        })
      ).status,
    ).toBe(201);
  });

  afterAll(async () => {
    await h?.close();
  });
}
