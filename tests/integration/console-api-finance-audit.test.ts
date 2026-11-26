import { expect, test } from "bun:test";
import {
  h,
  billingAccountId,
  adminAccess,
  apiKeyRaw,
  orgId,
  customerUserId,
  extraBillingAccountId,
  customerAccess,
} from "./console-finance-fixture.ts";
import { registerFinanceFixture } from "./console-finance-fixture.ts";
import { registerFinanceAuditCases } from "./console-finance-audit-cases.ts";

registerFinanceFixture();

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
  await h.consoleRuntime.sync.admission.ensureBillingAccountSynced(inheritedId);

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
    const first = await h.consoleRuntime.sync.admission
      .ensureBillingAccountSynced(id)
      .catch((err: unknown) => err);
    expect(committedStatus).toBe(200);
    expect(first).toMatchObject({ status: 503 });
  } finally {
    h.setGatewayInterceptor(null);
  }
  expect(await h.consoleRuntime.sync.admission.ensureBillingAccountSynced(id)).toMatchObject({
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

registerFinanceAuditCases();
