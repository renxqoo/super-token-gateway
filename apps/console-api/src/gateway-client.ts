import { GatewayError } from "@gateway/contracts";
import type { AdaptiveRoutingSetting, Pricing } from "@gateway/kernel";
import type { GatewayRoleTokens } from "./config.ts";

export type GatewayInternalRole = "catalog" | "security" | "finance" | "auditor" | "ops" | "events";

export type GatewayClientOptions = {
  baseUrl: string;
  tokens: GatewayRoleTokens;
  fetchImpl?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  timeoutMs?: number;
};

export type GatewayUpstreamAccountView = {
  id: string;
  name: string;
  state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
  revision: string;
  source_revision: number;
};

export type GatewayChannelView = {
  id: string;
  account_id: string;
  name: string;
  kind: string;
  provider_plugin_id: string | null;
  base_url: string;
  rpm: number;
  concurrency: number;
  enabled: boolean;
  source_revision: number;
};

export type GatewayPriceVersionView = {
  id: string;
  side: "CUSTOMER" | "SUPPLIER";
  currency: string;
  basis: "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS";
  rates: Pricing["rates"];
  ceilings: Record<string, string>;
  source_digest: string;
};

export class GatewayClient {
  private readonly baseUrl: string;
  private readonly tokens: GatewayRoleTokens;
  private readonly fetchImpl: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
  private readonly timeoutMs: number;

  constructor(options: GatewayClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.tokens = options.tokens;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.timeoutMs = options.timeoutMs ?? 15_000;
  }

  private tokenFor(role: GatewayInternalRole): string {
    const token = this.tokens[role];
    if (!token) {
      throw new GatewayError(
        502,
        "gateway_credential_missing",
        `Missing gateway token for ${role}`,
      );
    }
    return token;
  }

  private async sendRaw(
    role: GatewayInternalRole,
    method: "GET" | "POST" | "PUT",
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<{ status: number; ok: boolean; text: string }> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = {
      authorization: `Bearer ${this.tokenFor(role)}`,
      accept: "application/json",
    };
    if (body !== undefined) {
      headers["content-type"] = "application/json";
    }
    try {
      const timeout = AbortSignal.timeout(this.timeoutMs);
      const resp = await this.fetchImpl(url, {
        method,
        headers,
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
        signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
        redirect: "manual",
      });
      const text = await resp.text();
      return { status: resp.status, ok: resp.ok, text };
    } catch (err) {
      if (err instanceof Error && (err.name === "TimeoutError" || err.name === "AbortError")) {
        throw new GatewayError(504, "gateway_timeout", "Gateway internal API timed out");
      }
      throw new GatewayError(502, "gateway_unavailable", "Gateway internal API unavailable");
    }
  }

  async request<T = Record<string, unknown>>(
    role: GatewayInternalRole,
    method: "GET" | "POST" | "PUT",
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<T> {
    const { status, ok, text } = await this.sendRaw(role, method, path, body, signal);
    let parsed: unknown = null;
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        if (ok) {
          throw new GatewayError(
            502,
            "gateway_invalid_response",
            "Gateway internal API returned non-JSON response",
          );
        }
        parsed = null;
      }
    }
    if (!ok) {
      if (status === 401 || status === 403) {
        throw new GatewayError(
          502,
          "gateway_auth_error",
          "Gateway internal API rejected service credential",
        );
      }
      const errObj =
        parsed && typeof parsed === "object" && "error" in parsed
          ? (parsed as { error?: { code?: string; message?: string } }).error
          : undefined;
      throw new GatewayError(
        status >= 400 && status < 600 ? status : 502,
        errObj?.code ?? "gateway_upstream_error",
        errObj?.message ?? `Gateway internal API returned HTTP ${status}`,
      );
    }
    return parsed as T;
  }

  getHealth() {
    return this.request<Record<string, unknown>>("ops", "GET", "/internal/v1/health");
  }

  setOrganizationAdmission(organizationId: string, suspended: boolean, revision: number) {
    return this.request<{ source_revision: number }>(
      "security",
      "PUT",
      `/internal/v1/console/organizations/${organizationId}/admission`,
      { suspended, source_revision: revision },
    );
  }

  getChannelHealth() {
    return this.request<{ channels: unknown[] }>("ops", "GET", "/internal/v1/health/channels");
  }

  upsertBillingAccount(input: {
    id: string;
    external_ref?: string | null;
    name: string;
    currency: string;
    rpm: number;
    tpm: number;
    concurrency: number;
    enabled: boolean;
    source_revision?: number;
  }) {
    return this.request<{ id: string; config_epoch: number; source_revision: number }>(
      "catalog",
      "PUT",
      "/internal/v1/billing-accounts",
      input,
    );
  }

  upsertApiKey(input: {
    id: string;
    billing_account_id: string;
    rotated_from_key_id?: string;
    name: string;
    role: "ADMIN" | "BILLING" | "INVOKER" | "VIEWER";
    allowed_cidrs: string[];
    models: string[];
    operations: string[];
    enabled: boolean;
    expires_at: string | null;
    spend_limit_atoms: string | null;
    spend_limit_window: "LIFETIME" | "DAILY" | "MONTHLY";
    source_revision?: number;
  }) {
    return this.request<{
      id: string;
      billing_account_id: string;
      prefix: string;
      key?: string;
      config_epoch: number;
      source_revision: number;
    }>("security", "PUT", "/internal/v1/api-keys", input);
  }

  getApiKeySpend(keyId: string) {
    return this.request<{
      id: string;
      billing_account_id: string;
      spend_limit_atoms: string | null;
      spend_limit_window: "LIFETIME" | "DAILY" | "MONTHLY";
      spend_window_start: string;
      spent_atoms: string;
      reserved_atoms: string;
      remaining_atoms: string | null;
    }>("auditor", "GET", `/internal/v1/api-keys/${keyId}/spend`);
  }

  resetApiKeySpend(keyId: string, reason?: string) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      `/internal/v1/api-keys/${keyId}/reset-spend`,
      reason ? { reason } : {},
    );
  }

  listUpstreamAccounts() {
    return this.request<{ items: GatewayUpstreamAccountView[] }>(
      "auditor",
      "GET",
      "/internal/v1/upstream-accounts",
    );
  }

  upsertUpstreamAccount(input: {
    id: string;
    name: string;
    state?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
    expected_state?: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
    source_revision?: number;
  }) {
    return this.request<{
      id: string;
      state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
      config_epoch: number;
      source_revision: number;
    }>("catalog", "PUT", "/internal/v1/upstream-accounts", input);
  }

  rechargeSupplier(
    accountId: string,
    input: { currency: string; external_ref: string; amount_atoms: string; reason?: string },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/upstream-accounts/${accountId}/recharges`,
      input,
    );
  }

  adjustSupplierBalance(
    accountId: string,
    input: {
      currency: string;
      external_ref: string;
      delta_atoms: string;
      allow_negative?: boolean;
      reason: string;
    },
  ) {
    return this.request<{
      operation_id: string;
      replayed: boolean;
      delta_atoms: string;
      total_balance_atoms: string;
      reserved_atoms: string;
      account_state: "ACTIVE" | "DISABLED" | "RECONCILE_REQUIRED";
      config_epoch?: number;
    }>("finance", "POST", `/internal/v1/upstream-accounts/${accountId}/adjustments`, input);
  }

  getSupplierBalance(accountId: string, currency = "USD") {
    return this.request<{
      balance_atoms: string;
      reserved_atoms: string;
      spent_atoms: string;
    }>(
      "auditor",
      "GET",
      `/internal/v1/upstream-accounts/${accountId}/balance?currency=${encodeURIComponent(currency)}`,
    );
  }

  listChannels() {
    return this.request<{ items: GatewayChannelView[] }>("auditor", "GET", "/internal/v1/channels");
  }

  upsertChannel(input: {
    id: string;
    account_id: string;
    name: string;
    kind: string;
    provider_plugin_id?: string | null;
    base_url: string;
    api_key?: string;
    rpm: number;
    concurrency: number;
    enabled: boolean;
    routing_policy: Record<string, unknown>;
    adapter_config: Record<string, unknown>;
    source_revision?: number;
  }) {
    return this.request<{
      id: string;
      provider_plugin_id: string | null;
      config_epoch: number;
      source_revision: number;
    }>("catalog", "PUT", "/internal/v1/channels", input);
  }

  upsertModel(input: {
    name: string;
    operations: string[];
    routing_strategy: "WEIGHTED" | "ROUND_ROBIN" | "LEAST_LATENCY";
    adaptive_routing?: AdaptiveRoutingSetting;
    max_output_tokens: number;
    enabled: boolean;
    source_revision?: number;
    expected_source_revision?: number;
  }) {
    return this.request<{ name: string; config_epoch: number; source_revision: number }>(
      "catalog",
      "PUT",
      "/internal/v1/models",
      input,
    );
  }

  getPriceVersion(priceId: string, signal?: AbortSignal) {
    return this.request<GatewayPriceVersionView>(
      "auditor",
      "GET",
      `/internal/v1/prices/${priceId}`,
      undefined,
      signal,
    );
  }

  createPrice(input: {
    id?: string;
    side: "CUSTOMER" | "SUPPLIER";
    currency: string;
    basis: "PROVIDER_USAGE" | "REQUEST_COMPLETION" | "INPUT_CHARACTERS";
    rates: Pricing["rates"];
    ceilings: Record<string, string>;
  }) {
    return this.request<{ id: string; config_epoch: number; replayed?: boolean }>(
      "finance",
      "POST",
      "/internal/v1/prices",
      input,
    );
  }

  upsertRoute(input: {
    id: string;
    model_name: string;
    channel_id: string;
    upstream_model: string;
    operations: string[];
    priority: number;
    weight: number;
    billing_price_id: string;
    cost_version_id: string;
    request_policy: Record<string, unknown>;
    adapter_overrides: Record<string, unknown>;
    retry_policy: Record<string, unknown>;
    enabled: boolean;
    expected_source_revision?: number;
    source_revision?: number;
  }) {
    return this.request<{ id: string; config_epoch: number; source_revision: number }>(
      "catalog",
      "PUT",
      "/internal/v1/routes",
      input,
    );
  }

  upsertPricingOverride(input: {
    id: string;
    billing_account_id: string;
    model_name: string;
    operation: string;
    override_kind: "EXACT_PRICE" | "MULTIPLIER";
    billing_price_id?: string | null;
    multiplier_numerator?: string | null;
    multiplier_denominator?: string | null;
    enabled: boolean;
    source_revision?: number;
  }) {
    return this.request<{ id: string; config_epoch: number; source_revision: number }>(
      "finance",
      "PUT",
      "/internal/v1/pricing-overrides",
      input,
    );
  }

  listPricingOverrides(billingAccountId?: string) {
    const qs = billingAccountId
      ? `?billing_account_id=${encodeURIComponent(billingAccountId)}`
      : "";
    return this.request<{
      items: {
        id: string;
        model_name: string;
        operation: string;
        override_kind: "EXACT_PRICE" | "MULTIPLIER";
        billing_price_id: string | null;
        multiplier_numerator: string | null;
        multiplier_denominator: string | null;
        enabled: boolean;
      }[];
    }>("auditor", "GET", `/internal/v1/pricing-overrides${qs}`);
  }

  creditWallet(
    billingAccountId: string,
    input: {
      currency: string;
      external_ref: string;
      amount_atoms: string;
      kind: "CASH" | "GRANT";
      priority?: number;
      expires_at?: string | null;
      models?: string[];
      operations?: string[];
      reason?: string;
    },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/wallets/${billingAccountId}/credits`,
      input,
    );
  }

  configureCreditPolicy(
    billingAccountId: string,
    input: {
      currency: string;
      expected_version: string;
      credit_limit_atoms: string;
      policy?: "GIFT_FIRST" | "CASH_FIRST" | "EARLIEST_EXPIRY";
    },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/wallets/${billingAccountId}/credit-policy`,
      input,
    );
  }

  adjustWallet(
    billingAccountId: string,
    input: {
      currency: string;
      external_ref: string;
      delta_atoms: string;
      lot_kind?: "CASH" | "GRANT";
      force_debt?: boolean;
      reason: string;
    },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/wallets/${billingAccountId}/adjustments`,
      input,
    );
  }

  setWalletFreeze(
    billingAccountId: string,
    input: {
      currency: string;
      frozen: boolean;
      reason: string;
    },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/wallets/${billingAccountId}/freeze`,
      input,
    );
  }

  getWallet(billingAccountId: string, currency = "USD", includeLots = false) {
    const qs = new URLSearchParams({ currency });
    if (includeLots) qs.set("include_lots", "true");
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/wallets/${billingAccountId}?${qs.toString()}`,
    );
  }

  refundCharge(
    chargeId: string,
    input: { external_ref: string; amount_atoms: string; reason: string },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/charges/${chargeId}/refunds`,
      input,
    );
  }

  adjustCharge(
    chargeId: string,
    input: { external_ref: string; delta_atoms: string; reason: string },
  ) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/charges/${chargeId}/adjustments`,
      input,
    );
  }

  resolveRequest(requestId: string, body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/requests/${requestId}/resolve`,
      body,
    );
  }

  resolveSupplierAttempt(requestId: string, attemptId: string, body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "finance",
      "POST",
      `/internal/v1/requests/${requestId}/attempts/${attemptId}/resolve`,
      body,
    );
  }

  putReconciliationBudget(currency: string, limitAtoms: string) {
    return this.request<Record<string, unknown>>(
      "finance",
      "PUT",
      `/internal/v1/reconciliation/budgets/${encodeURIComponent(currency)}`,
      { limit_atoms: limitAtoms },
    );
  }

  putReconciliationPolicy(input: {
    billing_account_id?: string | null;
    channel_id: string;
    supplier_currency: string;
    enabled: boolean;
    include_historical: boolean;
    age_seconds: number;
    max_customer_atoms: string;
    limit_atoms: string;
  }) {
    return this.request<Record<string, unknown>>(
      "finance",
      "PUT",
      "/internal/v1/reconciliation/policies",
      input,
    );
  }

  private queryString(query: Record<string, string | undefined>): string {
    const sp = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== "") sp.set(k, v);
    }
    return sp.toString() ? `?${sp.toString()}` : "";
  }

  listReconciliationCases(query: {
    limit?: string | undefined;
    min_age_days?: string | undefined;
  }) {
    return this.request<{ data: Record<string, unknown>[] }>(
      "auditor",
      "GET",
      `/internal/v1/reconciliation/cases${this.queryString(query)}`,
    );
  }

  listCharges(query: Record<string, string | undefined>) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/charges${this.queryString(query)}`,
    );
  }

  listFundingOperations(query: Record<string, string | undefined>) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/funding-operations${this.queryString(query)}`,
    );
  }

  listLedgerEntries(query: Record<string, string | undefined>) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/ledger-entries${this.queryString(query)}`,
    );
  }

  getRequestBilling(requestId: string) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/requests/${requestId}`,
    );
  }

  getFinanceSummary(query: Record<string, string | undefined>) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/finance/summary${this.queryString(query)}`,
    );
  }

  listHolds(query: Record<string, string | undefined>) {
    return this.request<Record<string, unknown>>(
      "auditor",
      "GET",
      `/internal/v1/holds${this.queryString(query)}`,
    );
  }

  listAsyncTasks(query: Record<string, string | undefined>) {
    const sp = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== "") sp.set(k, v);
    }
    const qs = sp.toString() ? `?${sp.toString()}` : "";
    return this.request<Record<string, unknown>>("auditor", "GET", `/internal/v1/async-tasks${qs}`);
  }

  async getConservationAudit(): Promise<{ status: number; data: Record<string, unknown> }> {
    const { status, ok, text } = await this.sendRaw(
      "auditor",
      "GET",
      "/internal/v1/audit/conservation",
    );
    let parsed: unknown = null;
    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        throw new GatewayError(
          502,
          "gateway_invalid_response",
          "Gateway conservation endpoint returned non-JSON response",
        );
      }
    }
    if (status === 401 || status === 403) {
      throw new GatewayError(
        502,
        "gateway_auth_error",
        "Gateway internal API rejected service credential",
      );
    }
    if (
      parsed &&
      typeof parsed === "object" &&
      "ok" in parsed &&
      typeof (parsed as { ok?: unknown }).ok === "boolean"
    ) {
      return { status: ok ? 200 : 500, data: parsed as Record<string, unknown> };
    }
    throw new GatewayError(
      status >= 400 && status < 600 ? status : 502,
      "gateway_upstream_error",
      `Gateway internal API returned HTTP ${status}`,
    );
  }

  listPlugins(capability?: string) {
    const qs = capability ? `?capability=${encodeURIComponent(capability)}` : "";
    return this.request<Record<string, unknown>>("security", "GET", `/internal/v1/plugins${qs}`);
  }

  listBuiltinPlugins(capability?: string) {
    const qs = capability ? `?capability=${encodeURIComponent(capability)}` : "";
    return this.request<Record<string, unknown>>(
      "security",
      "GET",
      `/internal/v1/plugins/builtins${qs}`,
    );
  }

  installBuiltinPlugin(name: string, body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      `/internal/v1/plugins/builtins/${encodeURIComponent(name)}/install`,
      body,
    );
  }

  upsertPlugin(body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>("security", "PUT", "/internal/v1/plugins", body);
  }

  validatePluginRelease(body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      "/internal/v1/plugin-releases/validate",
      body,
    );
  }

  publishPluginRelease(body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      "/internal/v1/plugin-releases",
      body,
    );
  }

  rollbackPluginRelease(releaseId: string, body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      `/internal/v1/plugin-releases/${releaseId}/rollback`,
      body,
    );
  }

  revokePlugin(pluginId: string, reason: string) {
    return this.request<Record<string, unknown>>(
      "security",
      "POST",
      `/internal/v1/plugins/${pluginId}/revoke`,
      { reason },
    );
  }

  previewProvider(body: Record<string, unknown>) {
    return this.request<Record<string, unknown>>(
      "catalog",
      "POST",
      "/internal/v1/providers/preview",
      body,
    );
  }

  claimOutboxEvents(consumer: string, limit = 200) {
    return this.request<{
      lease_token: string;
      events: {
        seq: string;
        event_id: string;
        kind: string;
        request_id: string | null;
        billing_account_id: string | null;
        payload: Record<string, unknown>;
        created_at: string;
      }[];
    }>("events", "POST", "/internal/v1/events/claim", { consumer, limit });
  }

  ackOutboxEvents(consumer: string, leaseToken: string, throughSeq: string) {
    return this.request<{ ok: boolean }>("events", "POST", "/internal/v1/events/ack", {
      consumer,
      lease_token: leaseToken,
      through_seq: throughSeq,
    });
  }
}
