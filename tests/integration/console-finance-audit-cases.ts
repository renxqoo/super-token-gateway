import { expect, test } from "bun:test";
import {
  h,
  accountId,
  adminAccess,
  billingAccountId,
  channelId,
  customerAccess,
} from "./console-finance-fixture.ts";

export function registerFinanceAuditCases() {
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
    expect(adminTxns.data.items.some((t) => t.side === "CUSTOMER" && t.kind === "ADJUST")).toBe(
      true,
    );

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
    expect(transactions.data.items).toMatchObject([
      { billing_account_id: id, amount_atoms: "5000" },
    ]);
  });

  test("reconciliation budget, default/account policies with unsynced account auto-sync, and case listing work end-to-end", async () => {
    const forbidden = await h.adminApi(
      "GET",
      "/admin/v1/finance/reconciliation/cases",
      customerAccess,
    );
    expect(forbidden.status).toBe(401);

    const badCurrency = await h.adminApi(
      "PUT",
      "/admin/v1/finance/reconciliation/budgets/invalid",
      adminAccess,
      { limit_decimal: "10" },
    );
    expect(badCurrency.status).toBe(400);

    const budget = await h.adminApi<{
      scale: number;
      currency: string;
      limit_atoms: string;
      limit_decimal: string;
      used_decimal: string;
    }>("PUT", "/admin/v1/finance/reconciliation/budgets/USD", adminAccess, {
      limit_decimal: "25",
    });
    expect(budget.status).toBe(200);
    expect(budget.data).toMatchObject({
      scale: 18,
      currency: "USD",
      limit_decimal: "25.000000000000000000",
      used_decimal: "0.000000000000000000",
    });

    const defaultPolicy = await h.adminApi<{
      scale: number;
      billing_account_id: string | null;
      channel_id: string;
      enabled: boolean;
      max_customer_decimal: string;
      limit_decimal: string;
    }>("PUT", "/admin/v1/finance/reconciliation/policies", adminAccess, {
      billing_account_id: null,
      channel_id: channelId,
      supplier_currency: "USD",
      enabled: true,
      include_historical: true,
      age_seconds: 300,
      max_customer_decimal: "2",
      limit_decimal: "10",
    });
    expect(defaultPolicy.status).toBe(200);
    expect(defaultPolicy.data).toMatchObject({
      scale: 18,
      billing_account_id: null,
      channel_id: channelId,
      enabled: true,
      max_customer_decimal: "2.000000000000000000",
      limit_decimal: "10.000000000000000000",
    });

    const freshUser = await h.registerUser("reconciliation-unsynced@example.com");
    const unsyncedAccountId = freshUser.defaultBillingAccountId;
    const beforeSync = await h.conDb<{ gateway_synced: boolean }[]>`
      SELECT gateway_synced FROM public.billing_accounts WHERE id = ${unsyncedAccountId}::uuid
    `;
    expect(beforeSync[0]?.gateway_synced).toBe(false);

    const invalidScopedPolicy = await h.adminApi(
      "PUT",
      "/admin/v1/finance/reconciliation/policies",
      adminAccess,
      {
        billing_account_id: unsyncedAccountId,
        channel_id: channelId,
        supplier_currency: "USD",
        enabled: false,
        include_historical: false,
        age_seconds: 600,
      },
    );
    expect(invalidScopedPolicy.status).toBe(400);
    const stillUnsynced = await h.conDb<{ gateway_synced: boolean }[]>`
      SELECT gateway_synced FROM public.billing_accounts WHERE id = ${unsyncedAccountId}::uuid
    `;
    expect(stillUnsynced[0]?.gateway_synced).toBe(false);

    const scopedPolicy = await h.adminApi<{
      billing_account_id: string;
      enabled: boolean;
      max_customer_decimal: string;
      limit_decimal: string;
    }>("PUT", "/admin/v1/finance/reconciliation/policies", adminAccess, {
      billing_account_id: unsyncedAccountId,
      channel_id: channelId,
      supplier_currency: "USD",
      enabled: false,
      include_historical: false,
      age_seconds: 600,
      max_customer_decimal: "1",
      limit_decimal: "5",
    });
    expect(scopedPolicy.status, JSON.stringify(scopedPolicy.data)).toBe(200);
    expect(scopedPolicy.data).toMatchObject({
      billing_account_id: unsyncedAccountId,
      enabled: false,
      max_customer_decimal: "1.000000000000000000",
      limit_decimal: "5.000000000000000000",
    });
    const afterSync = await h.conDb<{ gateway_synced: boolean }[]>`
      SELECT gateway_synced FROM public.billing_accounts WHERE id = ${unsyncedAccountId}::uuid
    `;
    expect(afterSync[0]?.gateway_synced).toBe(true);

    const auditRows = await h.conDb<{ org_id: string | null; action: string; target_id: string }[]>`
      SELECT org_id::text, action, target_id
      FROM public.console_audit_logs
      WHERE action IN ('admin.finance.put_reconciliation_budget', 'admin.finance.put_reconciliation_policy')
      ORDER BY created_at ASC
    `;
    expect(auditRows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          action: "admin.finance.put_reconciliation_budget",
          target_id: "USD",
        }),
        expect.objectContaining({
          org_id: freshUser.defaultOrgId,
          action: "admin.finance.put_reconciliation_policy",
          target_id: `${unsyncedAccountId}:${channelId}:USD`,
        }),
      ]),
    );

    const badQuery = await h.adminApi(
      "GET",
      "/admin/v1/finance/reconciliation/cases?unknown=1",
      adminAccess,
    );
    expect(badQuery.status).toBe(400);

    const cases = await h.adminApi<{ scale: number; data: Record<string, unknown>[] }>(
      "GET",
      "/admin/v1/finance/reconciliation/cases?limit=10&min_age_days=0",
      adminAccess,
    );
    expect(cases.status).toBe(200);
    expect(cases.data.scale).toBe(18);
    expect(Array.isArray(cases.data.data)).toBe(true);
  });
}
