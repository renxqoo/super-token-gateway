import { afterAll, beforeAll, expect } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

export let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;

export let adminAccess = "";

export let customerAccess = "";

export let customerUserId = "";

export let orgId = "";

export let billingAccountId = "";

export let extraBillingAccountId = "";

export let accountId = "";

export let channelId = "";

export let apiKeyRaw = "";

export function registerFinanceFixture() {
  beforeAll(async () => {
    h = await createConsoleTestHarness({ enableWorker: false });
    const boot = await h.bootstrapAdmin();
    adminAccess = boot.accessToken;

    const user = await h.registerUser("finance-audit@example.com");
    customerAccess = user.accessToken;
    customerUserId = user.user.id;
    orgId = user.defaultOrgId;
    billingAccountId = user.defaultBillingAccountId;

    extraBillingAccountId = await h.seedLegacyBillingAccount(orgId, "Legacy separate account");

    const acc = await h.adminApi<{ id: string }>(
      "POST",
      "/admin/v1/upstream-accounts",
      adminAccess,
      {
        name: "audit-sup-acc",
      },
    );
    expect(acc.status).toBe(201);
    accountId = acc.data.id;

    const supRecharge = await h.adminApi<{ total_balance_decimal: string }>(
      "POST",
      `/admin/v1/upstream-accounts/${accountId}/recharges`,
      adminAccess,
      {
        currency: "USD",
        external_ref: "sup-init-1",
        amount_decimal: "100",
        reason: "Initial supplier funding",
      },
    );
    expect(supRecharge.status).toBe(200);
    expect(supRecharge.data.total_balance_decimal).toBe("100.000000000000000000");

    const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
      account_id: accountId,
      name: "audit-openai-ch",
      kind: "openai",
      base_url: h.mockUrl,
      api_key: "sk-audit-upstream",
    });
    expect(ch.status).toBe(201);
    channelId = ch.data.id;

    const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "audit-cust-18dec",
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1000000000000000", denominator: "1" },
          output_tokens: { numerator: "2000000000000000", denominator: "1" },
        },
        ceilings: { input_tokens: "200", output_tokens: "100" },
      },
    );
    expect(cp.status).toBe(201);

    const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "audit-sup-18dec",
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "300000000000000", denominator: "1" },
          output_tokens: { numerator: "600000000000000", denominator: "1" },
        },
        ceilings: { input_tokens: "200", output_tokens: "100" },
      },
    );
    expect(sp.status).toBe(201);

    expect(
      (
        await h.adminApi("POST", "/admin/v1/models", adminAccess, {
          name: "audit-gpt",
          display_name: "Audit GPT",
          operations: ["chat"],
          max_output_tokens: 100,
          list_price_template_id: cp.data.id,
        })
      ).status,
    ).toBe(201);

    expect(
      (
        await h.adminApi("POST", "/admin/v1/routes", adminAccess, {
          model_name: "audit-gpt",
          channel_id: channelId,
          upstream_model: "audit-gpt-up",
          operations: ["chat"],
          billing_price_id: cp.data.currentPriceVersionId,
          cost_version_id: sp.data.currentPriceVersionId,
        })
      ).status,
    ).toBe(201);

    const keyRes = await h.clientApi<{ key: string }>(
      "POST",
      `/api/v1/billing-accounts/${billingAccountId}/api-keys`,
      customerAccess,
      { name: "audit-key" },
    );
    expect(keyRes.status).toBe(201);
    apiKeyRaw = keyRes.data.key;
  });

  afterAll(async () => {
    await h?.close();
  });
}
