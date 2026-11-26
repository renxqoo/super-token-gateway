import { afterAll, beforeAll, expect } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

export let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;

export let adminAccess = "";

export let customerAccess = "";

export let billingAccountId = "";

export let accountId = "";

export let channelId = "";

export let customerPriceId = "";

export let supplierPriceId = "";

export function registerSyncFixture() {
  beforeAll(async () => {
    h = await createConsoleTestHarness({ enableWorker: false });
    const boot = await h.bootstrapAdmin();
    adminAccess = boot.accessToken;
    const user = await h.registerUser("sync-owner@example.com");
    customerAccess = user.accessToken;
    billingAccountId = user.defaultBillingAccountId;

    const topup = await h.adminApi(
      "POST",
      `/admin/v1/finance/billing-accounts/${billingAccountId}/topups`,
      adminAccess,
      {
        external_ref: "sync-topup-1",
        amount_atoms: "500000",
      },
    );
    expect(topup.status).toBe(200);

    const acc = await h.adminApi<{ id: string }>(
      "POST",
      "/admin/v1/upstream-accounts",
      adminAccess,
      {
        name: "sync-acc",
      },
    );
    expect(acc.status).toBe(201);
    accountId = acc.data.id;
    await h.adminApi("POST", `/admin/v1/upstream-accounts/${accountId}/recharges`, adminAccess, {
      currency: "USD",
      external_ref: "sync-sup-1",
      amount_atoms: "500000",
    });

    const ch = await h.adminApi<{ id: string }>("POST", "/admin/v1/channels", adminAccess, {
      account_id: accountId,
      name: "sync-ch",
      kind: "openai",
      base_url: h.mockUrl,
      api_key: "sk-sync-initial",
    });
    expect(ch.status).toBe(201);
    channelId = ch.data.id;

    const cp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "sync-cust-price",
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      },
    );
    expect(cp.status).toBe(201);
    customerPriceId = cp.data.currentPriceVersionId;

    const sp = await h.adminApi<{ id: string; currentPriceVersionId: string }>(
      "POST",
      "/admin/v1/pricing-templates",
      adminAccess,
      {
        name: "sync-sup-price",
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "2", denominator: "1" },
        },
        ceilings: { input_tokens: "10000", output_tokens: "1000" },
      },
    );
    expect(sp.status).toBe(201);
    supplierPriceId = sp.data.currentPriceVersionId;

    const model = await h.adminApi("POST", "/admin/v1/models", adminAccess, {
      name: "sync-gpt",
      display_name: "Sync GPT",
      operations: ["chat"],
      max_output_tokens: 100,
      list_price_template_id: cp.data.id,
    });
    expect(model.status).toBe(201);
  });

  afterAll(async () => {
    await h?.close();
  });
}
