import { afterAll, beforeAll, expect, test } from "bun:test";
import { createConsoleTestHarness } from "./helpers/console-harness.ts";

let h: Awaited<ReturnType<typeof createConsoleTestHarness>>;
let adminAccess = "";
let customerAccess = "";
let customerUserId = "";
let orgId = "";
let billingAccountId = "";
let extraBillingAccountId = "";
let accountId = "";
let channelId = "";
let apiKeyRaw = "";

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

  const acc = await h.adminApi<{ id: string }>("POST", "/admin/v1/upstream-accounts", adminAccess, {
    name: "audit-sup-acc",
  });
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

test("18-decimal wallet adjustments obey zero-overdraft credit rules, force_debt freeze, automatic debt collection, and manual freeze", async () => {
  const badMismatch = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/topups`,
    adminAccess,
    {
      external_ref: "bad-mismatch",
      amount_atoms: "100",
      amount_decimal: "1",
    },
  );
  expect(badMismatch.status).toBe(400);

  const bad19Dec = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/topups`,
    adminAccess,
    {
      external_ref: "bad-19dec",
      amount_decimal: "0.0000000000000000001",
    },
  );
  expect(bad19Dec.status).toBe(400);

  const topup = await h.adminApi<{
    available_atoms: string;
    available_decimal: string;
    scale: number;
  }>("POST", `/admin/v1/finance/billing-accounts/${billingAccountId}/topups`, adminAccess, {
    external_ref: "topup-18dec-1",
    amount_decimal: "12.345",
    reason: "Initial wire transfer",
  });
  expect(topup.status).toBe(200);
  expect(topup.data.scale).toBe(18);
  expect(topup.data.available_atoms).toBe("12345000000000000000");
  expect(topup.data.available_decimal).toBe("12.345000000000000000");

  const posAdj = await h.adminApi<{
    replayed: boolean;
    available_decimal: string;
    delta_decimal: string;
  }>("POST", `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`, adminAccess, {
    external_ref: "wadj-pos-1",
    delta_decimal: "1.5",
    reason: "Manual credit bonus",
  });
  expect(posAdj.status).toBe(200);
  expect(posAdj.data.replayed).toBe(false);
  expect(posAdj.data.available_decimal).toBe("13.845000000000000000");

  const posAdjReplay = await h.adminApi<{ replayed: boolean }>(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`,
    adminAccess,
    {
      external_ref: "wadj-pos-1",
      delta_decimal: "1.5",
      reason: "Manual credit bonus",
    },
  );
  expect(posAdjReplay.status).toBe(200);
  expect(posAdjReplay.data.replayed).toBe(true);

  const posAdjConflict = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`,
    adminAccess,
    {
      external_ref: "wadj-pos-1",
      delta_decimal: "2.0",
      reason: "Different payload",
    },
  );
  expect(posAdjConflict.status).toBe(409);

  const negWithin = await h.adminApi<{ available_decimal: string; debt_decimal: string }>(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`,
    adminAccess,
    {
      external_ref: "wadj-neg-1",
      delta_decimal: "-3.845",
      reason: "Correction of duplicate bonus",
    },
  );
  expect(negWithin.status).toBe(200);
  expect(negWithin.data.available_decimal).toBe("10.000000000000000000");
  expect(negWithin.data.debt_decimal).toBe("0.000000000000000000");

  const negOverZeroCredit = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`,
    adminAccess,
    {
      external_ref: "wadj-over-zero",
      delta_decimal: "-15",
      reason: "Should fail because credit_limit=0",
    },
  );
  expect(negOverZeroCredit.status).toBe(409);

  const adminWallet1 = await h.adminApi<{
    credit_policy_version: string;
    available_decimal: string;
    lots: { id: string; available_decimal: string }[];
  }>("GET", `/admin/v1/finance/billing-accounts/${billingAccountId}/wallet`, adminAccess);
  expect(adminWallet1.status).toBe(200);
  expect(adminWallet1.data.available_decimal).toBe("10.000000000000000000");
  expect(adminWallet1.data.lots.length).toBe(2);

  const setCredit = await h.adminApi<{ credit_limit_decimal: string }>(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/credit-policy`,
    adminAccess,
    {
      expected_version: adminWallet1.data.credit_policy_version,
      credit_limit_decimal: "5",
    },
  );
  expect(setCredit.status).toBe(200);
  expect(setCredit.data.credit_limit_decimal).toBe("5.000000000000000000");

  const negWithinCredit = await h.adminApi<{
    available_decimal: string;
    debt_decimal: string;
    freeze_reason: string | null;
  }>("POST", `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`, adminAccess, {
    external_ref: "wadj-within-credit",
    delta_decimal: "-13",
    reason: "Debit using 10 available + 3 credit",
  });
  expect(negWithinCredit.status).toBe(200);
  expect(negWithinCredit.data.available_decimal).toBe("0.000000000000000000");
  expect(negWithinCredit.data.debt_decimal).toBe("3.000000000000000000");
  expect(negWithinCredit.data.freeze_reason).toBeNull();

  const negExceedCredit = await h.adminApi(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`,
    adminAccess,
    {
      external_ref: "wadj-exceed-credit",
      delta_decimal: "-3",
      reason: "Exceeds remaining credit limit",
    },
  );
  expect(negExceedCredit.status).toBe(409);

  const forcedDebt = await h.adminApi<{
    debt_decimal: string;
    freeze_reason: string | null;
  }>("POST", `/admin/v1/finance/billing-accounts/${billingAccountId}/adjustments`, adminAccess, {
    external_ref: "wadj-chargeback-force",
    delta_decimal: "-4",
    force_debt: true,
    reason: "Bank chargeback recovery",
  });
  expect(forcedDebt.status).toBe(200);
  expect(forcedDebt.data.debt_decimal).toBe("7.000000000000000000");
  expect(forcedDebt.data.freeze_reason).toBe("DEBT_EXCEEDS_CREDIT_LIMIT");

  const callChat = () =>
    fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${apiKeyRaw}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "audit-gpt",
        messages: [{ role: "user", content: "audit check" }],
        max_tokens: 50,
      }),
    });

  expect((await callChat()).status).toBe(403);

  const repayTopup = await h.adminApi<{
    available_decimal: string;
    debt_collected_charges: number;
  }>("POST", `/admin/v1/finance/billing-accounts/${billingAccountId}/topups`, adminAccess, {
    external_ref: "topup-repay-debt",
    amount_decimal: "10",
    reason: "Customer repays chargeback",
  });
  expect(repayTopup.status).toBe(200);
  expect(repayTopup.data.debt_collected_charges).toBeGreaterThanOrEqual(1);
  expect(repayTopup.data.available_decimal).toBe("3.000000000000000000");

  const walletAfterRepay = await h.adminApi<{
    debt_decimal: string;
    freeze_reason: string | null;
  }>("GET", `/admin/v1/finance/billing-accounts/${billingAccountId}/wallet`, adminAccess);
  expect(walletAfterRepay.data.debt_decimal).toBe("0.000000000000000000");
  expect(walletAfterRepay.data.freeze_reason).toBeNull();

  const freezeRes = await h.adminApi<{ manual_frozen: boolean; freeze_reason: string | null }>(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/freeze`,
    adminAccess,
    { frozen: true, reason: "Temporary fraud hold" },
  );
  expect(freezeRes.status).toBe(200);
  expect(freezeRes.data.manual_frozen).toBe(true);
  expect(freezeRes.data.freeze_reason).toBe("MANUAL");
  expect((await callChat()).status).toBe(403);

  const unfreezeRes = await h.adminApi<{ manual_frozen: boolean; freeze_reason: string | null }>(
    "POST",
    `/admin/v1/finance/billing-accounts/${billingAccountId}/freeze`,
    adminAccess,
    { frozen: false, reason: "Verified clean" },
  );
  expect(unfreezeRes.status).toBe(200);
  expect(unfreezeRes.data.manual_frozen).toBe(false);
  expect(unfreezeRes.data.freeze_reason).toBeNull();
  expect((await callChat()).status).toBe(200);
});

test("platform default credit policy and batch unified credit updates work with dry_run and scope isolation", async () => {
  const setDef = await h.adminApi<{
    default_credit_limit_decimal: string;
    default_wallet_policy: string;
  }>("PUT", "/admin/v1/finance/credit-policy/default", adminAccess, {
    default_credit_limit_decimal: "2",
    default_wallet_policy: "CASH_FIRST",
  });
  expect(setDef.status).toBe(200);
  expect(setDef.data.default_credit_limit_decimal).toBe("2.000000000000000000");
  expect(setDef.data.default_wallet_policy).toBe("CASH_FIRST");

  const inheritedId = await h.seedLegacyBillingAccount(orgId, "Inherited credit limits");
  await h.consoleRuntime.sync.ensureBillingAccountSynced(inheritedId);

  const inheritedWallet = await h.adminApi<{
    credit_limit_decimal: string;
    policy: string;
  }>("GET", `/admin/v1/finance/billing-accounts/${inheritedId}/wallet`, adminAccess);
  expect(inheritedWallet.status).toBe(200);
  expect(inheritedWallet.data.credit_limit_decimal).toBe("2.000000000000000000");
  expect(inheritedWallet.data.policy).toBe("CASH_FIRST");

  await h.adminApi("PUT", "/admin/v1/finance/credit-policy/default", adminAccess, {
    default_credit_limit_decimal: "0",
    default_wallet_policy: "GIFT_FIRST",
  });

  const dryBatch = await h.adminApi<{
    dry_run: boolean;
    matched_count: number;
    updated_count: number;
  }>("POST", "/admin/v1/finance/credit-policy/batch", adminAccess, {
    scope: "ORGANIZATION",
    org_id: orgId,
    credit_limit_decimal: "0",
    dry_run: true,
    reason: "Preview org credit reset",
  });
  expect(dryBatch.status).toBe(200);
  expect(dryBatch.data.dry_run).toBe(true);
  expect(dryBatch.data.matched_count).toBe(3);
  expect(dryBatch.data.updated_count).toBe(0);

  const userDefaultBatch = await h.adminApi<{
    matched_count: number;
    updated_count: number;
    items: { billing_account_id: string; credit_limit_decimal: string }[];
  }>("POST", "/admin/v1/finance/credit-policy/batch", adminAccess, {
    scope: "USER",
    user_id: customerUserId,
    only_default_billing_account: true,
    credit_limit_decimal: "1.25",
    policy: "EARLIEST_EXPIRY",
    dry_run: false,
    reason: "Enterprise default billingAccount credit line",
  });
  expect(userDefaultBatch.status).toBe(200);
  expect(userDefaultBatch.data.matched_count).toBe(1);
  expect(userDefaultBatch.data.updated_count).toBe(1);
  expect(userDefaultBatch.data.items[0]?.billing_account_id).toBe(billingAccountId);
  expect(userDefaultBatch.data.items[0]?.credit_limit_decimal).toBe("1.250000000000000000");

  const extraWallet = await h.adminApi<{ credit_limit_decimal: string }>(
    "GET",
    `/admin/v1/finance/billing-accounts/${extraBillingAccountId}/wallet`,
    adminAccess,
  );
  expect(extraWallet.data.credit_limit_decimal).toBe("0.000000000000000000");
});

test("a lost default-credit response leaves billingAccount initialization retryable and applies the policy once", async () => {
  const defaults = await h.adminApi("PUT", "/admin/v1/finance/credit-policy/default", adminAccess, {
    default_credit_limit_decimal: "3",
    default_wallet_policy: "CASH_FIRST",
  });
  expect(defaults.status).toBe(200);
  const team = await h.clientApi<{ default_billing_account_id: string }>(
    "POST",
    "/api/v1/orgs",
    customerAccess,
    { name: "Retried Default Credit", slug: "retry-default-credit" },
  );
  expect(team.status).toBe(201);
  const id = team.data.default_billing_account_id;
  const path = `/internal/v1/wallets/${id}/credit-policy`;
  let committedStatus = 0;
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (init?.method === "POST" && new URL(url).pathname === path) {
      const response = await forward();
      committedStatus = response.status;
      await response.body?.cancel();
      return new Response("temporarily unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    const first = await h.consoleRuntime.sync
      .ensureBillingAccountSynced(id)
      .catch((err: unknown) => err);
    expect(committedStatus).toBe(200);
    expect(first).toMatchObject({ status: 503 });
  } finally {
    h.setGatewayInterceptor(null);
  }
  expect(await h.consoleRuntime.sync.ensureBillingAccountSynced(id)).toMatchObject({
    gatewaySynced: true,
  });
  const wallet = await h.adminApi(
    "GET",
    `/admin/v1/finance/billing-accounts/${id}/wallet`,
    adminAccess,
  );
  expect(wallet.data).toMatchObject({
    credit_limit_decimal: "3.000000000000000000",
    credit_policy_version: "1",
    policy: "CASH_FIRST",
  });
  await h.adminApi("PUT", "/admin/v1/finance/credit-policy/default", adminAccess, {
    default_credit_limit_decimal: "0",
    default_wallet_policy: "GIFT_FIRST",
  });
});

test("credit policy preview reports an unavailable wallet instead of assuming zero debt", async () => {
  h.setGatewayInterceptor(async (url, init, forward) => {
    if (
      init?.method === "GET" &&
      new URL(url).pathname === `/internal/v1/wallets/${billingAccountId}`
    ) {
      return new Response("unavailable", { status: 503 });
    }
    return forward();
  });
  try {
    const preview = await h.adminApi("POST", "/admin/v1/finance/credit-policy/batch", adminAccess, {
      scope: "BILLING_ACCOUNTS",
      billing_account_ids: [billingAccountId],
      credit_limit_decimal: "0",
      dry_run: true,
      reason: "Preview billingAccount credit policy",
    });
    expect(preview.status).toBe(200);
    expect(preview.data).toMatchObject({
      failed_count: 1,
      items: [{ billing_account_id: billingAccountId, ok: false }],
    });
  } finally {
    h.setGatewayInterceptor(null);
  }
});

test("batch credit updates reject a scope larger than the supported batch instead of silently truncating", async () => {
  const user = await h.registerUser("wide-credit-scope@example.com");
  try {
    await h.conDb`
      INSERT INTO public.billing_accounts (id, gateway_billing_account_id, org_id, name)
      SELECT id, id, ${user.defaultOrgId}, 'Credit scope ' || n::text
      FROM (SELECT n, gen_random_uuid() AS id FROM generate_series(1, 500) AS n) fixtures
    `;
    const preview = await h.adminApi("POST", "/admin/v1/finance/credit-policy/batch", adminAccess, {
      scope: "ORGANIZATION",
      org_id: user.defaultOrgId,
      credit_limit_atoms: "0",
      dry_run: true,
      reason: "Oversized credit scope",
    });
    expect(preview.status).toBe(400);
  } finally {
    await h.conDb`DELETE FROM public.billing_accounts WHERE org_id = ${user.defaultOrgId}`;
  }
});

test("supplier adjustments, admin/client audit views, request drilldown, P&L summary, and 4-way conservation work end-to-end", async () => {
  const supPos = await h.adminApi<{
    delta_decimal: string;
    total_balance_decimal: string;
    account_state: string;
  }>("POST", `/admin/v1/upstream-accounts/${accountId}/adjustments`, adminAccess, {
    currency: "USD",
    external_ref: "sup-adj-pos-1",
    delta_decimal: "5.5",
    reason: "Supplier rebate credit",
  });
  expect(supPos.status).toBe(200);
  expect(supPos.data.delta_decimal).toBe("5.500000000000000000");
  expect(supPos.data.account_state).toBe("ACTIVE");

  const supNegDenied = await h.adminApi(
    "POST",
    `/admin/v1/upstream-accounts/${accountId}/adjustments`,
    adminAccess,
    {
      currency: "USD",
      external_ref: "sup-adj-neg-deny",
      delta_decimal: "-500",
      reason: "Too large without allow_negative",
    },
  );
  expect(supNegDenied.status).toBe(409);

  const supNegForce = await h.adminApi<{
    total_balance_atoms: string;
    account_state: string;
  }>("POST", `/admin/v1/upstream-accounts/${accountId}/adjustments`, adminAccess, {
    currency: "USD",
    external_ref: "sup-adj-neg-force",
    delta_decimal: "-120",
    allow_negative: true,
    reason: "Reconcile upstream off-gateway usage",
  });
  expect(supNegForce.status).toBe(200);
  expect(BigInt(supNegForce.data.total_balance_atoms) < 0n).toBe(true);
  expect(supNegForce.data.account_state).toBe("RECONCILE_REQUIRED");

  const supRestore = await h.adminApi<{ total_balance_atoms: string }>(
    "POST",
    `/admin/v1/upstream-accounts/${accountId}/adjustments`,
    adminAccess,
    {
      currency: "USD",
      external_ref: "sup-adj-restore",
      delta_decimal: "50",
      reason: "Replenish after reconciliation",
    },
  );
  expect(supRestore.status).toBe(200);
  expect(BigInt(supRestore.data.total_balance_atoms) > 0n).toBe(true);

  const supBal = await h.adminApi<{ balance_decimal: string; scale: number }>(
    "GET",
    `/admin/v1/upstream-accounts/${accountId}/balance?currency=USD`,
    adminAccess,
  );
  expect(supBal.status).toBe(200);
  expect(supBal.data.scale).toBe(18);
  expect(supBal.data.balance_decimal).toMatch(/^\d+\.\d{18}$/);

  const adminCharges = await h.adminApi<{
    scale: number;
    items: {
      request_id: string;
      customer_atoms: string;
      customer_decimal: string;
      supplier_cost_atoms: string;
      supplier_cost_decimal: string;
      gross_margin_atoms: string;
      gross_margin_decimal: string;
    }[];
  }>("GET", `/admin/v1/finance/charges?billing_account_id=${billingAccountId}`, adminAccess);
  expect(adminCharges.status).toBe(200);
  expect(adminCharges.data.items.length).toBe(1);
  const charge = adminCharges.data.items[0]!;
  expect(charge.customer_decimal).toBe("0.200000000000000000");
  expect(charge.supplier_cost_decimal).toBe("0.060000000000000000");
  expect(charge.gross_margin_decimal).toBe("0.140000000000000000");

  const drilldown = await h.adminApi<{
    hold: Record<string, unknown> | null;
    charge: { customer_decimal: string } | null;
    entries: { account: string; atoms_decimal: string }[];
  }>("GET", `/admin/v1/finance/requests/${charge.request_id}`, adminAccess);
  expect(drilldown.status).toBe(200);
  expect(drilldown.data.hold).not.toBeNull();
  expect(drilldown.data.charge?.customer_decimal).toBe("0.200000000000000000");
  expect(drilldown.data.entries.length).toBeGreaterThanOrEqual(4);

  const ledgerEntriesRes = await h.adminApi<{
    items: { kind: string; account: string; atoms_decimal: string }[];
  }>("GET", "/admin/v1/finance/ledger-entries?limit=100", adminAccess);
  expect(ledgerEntriesRes.status).toBe(200);
  expect(ledgerEntriesRes.data.items.some((e) => e.account === "system:adjustment")).toBe(true);
  expect(ledgerEntriesRes.data.items.some((e) => e.account === "upstream:adjustment")).toBe(true);

  const adminTxns = await h.adminApi<{
    items: { side: string; kind: string; amount_decimal: string }[];
  }>("GET", "/admin/v1/finance/transactions?limit=100", adminAccess);
  expect(adminTxns.status).toBe(200);
  expect(adminTxns.data.items.some((t) => t.kind === "SUPPLIER_ADJUST")).toBe(true);
  expect(adminTxns.data.items.some((t) => t.side === "CUSTOMER" && t.kind === "ADJUST")).toBe(true);

  const summary = await h.adminApi<{
    scale: number;
    by_currency: {
      currency: string;
      customer_billed_decimal: string;
      supplier_cost_decimal: string;
      gross_margin_decimal: string;
      cash_topup_decimal: string;
    }[];
    by_model: { key: string; gross_margin_decimal: string }[];
  }>("GET", "/admin/v1/finance/summary?currency=USD", adminAccess);
  expect(summary.status).toBe(200);
  expect(summary.data.scale).toBe(18);
  expect(summary.data.by_currency[0]?.customer_billed_decimal).toBe("0.200000000000000000");
  expect(summary.data.by_currency[0]?.supplier_cost_decimal).toBe("0.060000000000000000");
  expect(summary.data.by_currency[0]?.gross_margin_decimal).toBe("0.140000000000000000");
  expect(summary.data.by_currency[0]?.cash_topup_decimal).toBe("22.345000000000000000");
  expect(summary.data.by_model[0]?.key).toBe("audit-gpt");

  const clientWallet = await h.clientApi<{
    scale: number;
    available_decimal: string;
    credit_limit_decimal: string;
    lots: { issued_decimal: string; available_decimal: string }[];
  }>("GET", `/api/v1/billing-accounts/${billingAccountId}/wallet`, customerAccess);
  expect(clientWallet.status).toBe(200);
  expect(clientWallet.data.scale).toBe(18);
  expect(clientWallet.data.available_decimal).toBe("2.800000000000000000");
  expect(clientWallet.data.credit_limit_decimal).toBe("1.250000000000000000");
  expect(clientWallet.data.lots.length).toBeGreaterThanOrEqual(3);

  const clientTxns = await h.clientApi<{
    scale: number;
    items: Record<string, unknown>[];
  }>("GET", `/api/v1/billing-accounts/${billingAccountId}/transactions`, customerAccess);
  expect(clientTxns.status).toBe(200);
  expect(clientTxns.data.items.length).toBeGreaterThanOrEqual(5);
  for (const item of clientTxns.data.items) {
    expect(item.side).toBeUndefined();
    expect(item.account_id).toBeUndefined();
    expect(item.actor).toBeUndefined();
    expect(typeof item.amount_decimal).toBe("string");
  }

  const conservation = await h.adminApi<{
    ok: boolean;
    unbalanced_journals: number;
    broken_lots: number;
    wallet_mismatches: number;
    reservation_mismatches: number;
    customer_ledger_mismatches: number;
    supplier_ledger_mismatches: number;
  }>("GET", "/admin/v1/finance/conservation", adminAccess);
  expect(conservation.status).toBe(200);
  expect(conservation.data).toMatchObject({
    ok: true,
    unbalanced_journals: 0,
    broken_lots: 0,
    wallet_mismatches: 0,
    reservation_mismatches: 0,
    customer_ledger_mismatches: 0,
    supplier_ledger_mismatches: 0,
  });

  await h.gwDb`
    UPDATE public.supplier_cells
    SET balance_atoms = balance_atoms + 1
    WHERE account_id = ${accountId}::uuid AND currency = 'USD' AND cell = 0
  `;
  const tamperedConservation = await h.adminApi<{
    ok: boolean;
    supplier_ledger_mismatches: number;
  }>("GET", "/admin/v1/finance/conservation", adminAccess);
  await h.gwDb`
    UPDATE public.supplier_cells
    SET balance_atoms = balance_atoms - 1
    WHERE account_id = ${accountId}::uuid AND currency = 'USD' AND cell = 0
  `;
  expect(tamperedConservation.status).toBe(500);
  expect(tamperedConservation.data.ok).toBe(false);
  expect(tamperedConservation.data.supplier_ledger_mismatches).toBe(1);
  expect(
    (
      await h.adminApi("PATCH", `/admin/v1/upstream-accounts/${accountId}`, adminAccess, {
        state: "ACTIVE",
        expected_state: "RECONCILE_REQUIRED",
      })
    ).status,
  ).toBe(200);
});

test("a personal Key and two team Keys debit the accounts that issued them", async () => {
  const user = await h.registerUser(
    "billing-account-owner@example.com",
    undefined,
    "A".repeat(120),
  );
  const team = await h.clientApi<{ id: string; default_billing_account_id: string }>(
    "POST",
    "/api/v1/orgs",
    user.accessToken,
    { name: "Shared Billing Team", slug: "shared-billing-team" },
  );
  expect(team.status).toBe(201);
  const personalId = user.defaultBillingAccountId;
  const teamId = team.data.default_billing_account_id;
  const extraAccount = await h.clientApi(
    "POST",
    `/api/v1/orgs/${team.data.id}/billing-accounts`,
    user.accessToken,
    { name: "Another account in the same team" },
  );
  expect(extraAccount.status).toBe(404);

  const teamGatewayId = crypto.randomUUID();
  await h.conDb`UPDATE public.billing_accounts SET gateway_billing_account_id = ${teamGatewayId} WHERE id = ${teamId}`;
  const me = await h.clientApi("GET", "/api/v1/auth/me", user.accessToken);
  expect(me.status).toBe(200);
  expect(me.data.organizations).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ kind: "PERSONAL", default_billing_account_id: personalId }),
      expect.objectContaining({ kind: "ENTERPRISE", default_billing_account_id: teamId }),
    ]),
  );

  const issueKey = async (id: string, name: string) => {
    const result = await h.clientApi<{ key: string }>(
      "POST",
      `/api/v1/billing-accounts/${id}/api-keys`,
      user.accessToken,
      { name },
    );
    expect(result.status).toBe(201);
    return result.data.key;
  };
  const personalKey = await issueKey(personalId, "Personal");
  const teamKey = await issueKey(teamId, "Team application");
  const secondTeamKey = await issueKey(teamId, "Team background jobs");
  const teamSummary = () =>
    h.adminApi("GET", `/admin/v1/finance/summary?billing_account_id=${teamId}`, adminAccess);
  const emptySummary = await teamSummary();

  for (const [id, amount] of [
    [personalId, "1"],
    [teamId, "2"],
  ] as const) {
    const credit = await h.adminApi(
      "POST",
      `/admin/v1/finance/billing-accounts/${id}/topups`,
      adminAccess,
      { external_ref: "account-test-topup", amount_decimal: amount },
    );
    expect(credit.status).toBe(200);
  }

  for (const key of [personalKey, teamKey, secondTeamKey]) {
    const response = await fetch(`${h.gatewayPublicUrl}/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({
        model: "audit-gpt",
        messages: [{ role: "user", content: "Confirm account attribution" }],
        max_tokens: 75,
      }),
    });
    const body = await response.json();
    expect({ status: response.status, body }).toMatchObject({
      status: 200,
      body: { choices: [{ message: { content: "hello from upstream" } }] },
    });
  }

  const personal = await h.clientApi<{ billing_account_id: string; available_decimal: string }>(
    "GET",
    `/api/v1/billing-accounts/${personalId}/wallet`,
    user.accessToken,
  );
  const shared = await h.clientApi<typeof personal.data>(
    "GET",
    `/api/v1/billing-accounts/${teamId}/wallet`,
    user.accessToken,
  );
  expect(personal.data).toMatchObject({
    billing_account_id: personalId,
    available_decimal: "0.800000000000000000",
  });
  expect(shared.data).toMatchObject({
    billing_account_id: teamId,
    available_decimal: "1.600000000000000000",
  });
  const charges = await h.adminApi<{ items: { billing_account_id: string }[] }>(
    "GET",
    `/admin/v1/finance/charges?billing_account_id=${teamId}`,
    adminAccess,
  );
  expect(charges.status).toBe(200);
  expect(charges.data.items.map((item) => item.billing_account_id)).toEqual([teamId, teamId]);
  expect([emptySummary, await teamSummary()]).toMatchObject([
    { status: 200, data: { billing_account_id: teamId, by_billing_account: [] } },
    { status: 200, data: { billing_account_id: teamId, by_billing_account: [{ key: teamId }] } },
  ]);
  const retiredFilter = await h.adminApi(
    "GET",
    `/admin/v1/finance/charges?project_id=${teamId}`,
    adminAccess,
  );
  expect(retiredFilter.status).toBe(400);
  const ledgerEntries = await h.adminApi(
    "GET",
    `/admin/v1/finance/ledger-entries?billing_account_id=${teamId}`,
    adminAccess,
  );
  expect(ledgerEntries.status).toBe(400);
  const gatewayEntries = await h.gatewayInternalApi(
    "GET",
    `/internal/v1/ledger-entries?billing_account_id=${teamGatewayId}`,
  );
  expect(gatewayEntries.status).toBe(400);
});

test("finance can filter transactions by a standalone Gateway billing account", async () => {
  const id = crypto.randomUUID();
  const account = await h.gatewayInternalApi("PUT", "/internal/v1/billing-accounts", {
    id,
    name: "Standalone account",
    currency: "USD",
    rpm: 100,
    tpm: 1000,
    concurrency: 1,
    enabled: true,
  });
  expect(account.status).toBe(200);
  const credit = await h.gatewayInternalApi("POST", `/internal/v1/wallets/${id}/credits`, {
    currency: "USD",
    external_ref: "standalone-topup",
    amount_atoms: "5000",
    kind: "CASH",
  });
  expect(credit.status).toBe(200);
  const transactions = await h.adminApi<{ items: Record<string, unknown>[] }>(
    "GET",
    `/admin/v1/finance/transactions?billing_account_id=${id}`,
    adminAccess,
  );
  expect(transactions.status).toBe(200);
  expect(transactions.data.items).toMatchObject([{ billing_account_id: id, amount_atoms: "5000" }]);
});
