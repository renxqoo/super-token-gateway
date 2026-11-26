import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import type { ApiKeyRecord, ConfigSnapshot, BillingAccountRecord } from "@gateway/ledger";
import {
  clientAddress,
  inCidrs,
  BILLING_ACCOUNT_API_KEY_PATTERN,
  verifier,
} from "@gateway/runtime";
import type { GatewayConfig } from "../config.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { InferenceContext } from "./types.ts";

function extractApiKey(c: InferenceContext, operation?: Operation): string | null {
  const headers = c.req.raw.headers;
  const auth = headers.get("authorization");
  if (auth && (auth.startsWith("Bearer ") || auth.toLowerCase().startsWith("bearer "))) {
    const token = auth.slice(7).trim();
    if (token) return token;
  }
  if (operation === "messages" || operation === undefined) {
    const xKey = headers.get("x-api-key")?.trim();
    if (xKey) return xKey;
  }
  if (operation === "gemini" || operation === undefined) {
    const gKey = headers.get("x-goog-api-key")?.trim() || c.req.query("key")?.trim();
    if (gKey) return gKey;
  }
  return null;
}

export function billingAccountQuotaScope(id: string): string {
  return `billing-account:${id}`;
}

export function authenticateRequest(
  c: InferenceContext,
  snap: ConfigSnapshot,
  config: GatewayConfig,
  operation?: Operation,
  peerAddress?: string | (() => string | undefined),
  state?: ProxyRuntimeState,
): { key: ApiKeyRecord; billingAccount: BillingAccountRecord } {
  const rawKey = extractApiKey(c, operation);
  if (!rawKey) throw new GatewayError(401, "unauthorized", "Missing API key");
  if (!BILLING_ACCOUNT_API_KEY_PATTERN.test(rawKey)) {
    throw new GatewayError(401, "unauthorized", "Invalid API key");
  }
  const key = state
    ? state.lookupVerifiedKey(rawKey, snap, config.keyPepper)
    : snap.keysByVerifier.get(verifier(rawKey, config.keyPepper));
  if (!key || !key.enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
  if (key.expires_at && Date.parse(key.expires_at) <= Date.now()) {
    throw new GatewayError(401, "key_expired", "API key has expired");
  }
  if (operation !== undefined && key.role === "VIEWER") {
    throw new GatewayError(403, "forbidden", "API key role cannot invoke models");
  }
  assertKeyNetworkAndOperation(c, key, config, operation, peerAddress);
  const billingAccount = snap.billingAccounts.get(key.billing_account_id);
  if (!billingAccount || !billingAccount.enabled) {
    throw new GatewayError(403, "billing_account_disabled", "Billing account is disabled");
  }
  return { key, billingAccount };
}

export function assertKeyNetworkAndOperation(
  c: InferenceContext,
  key: Pick<ApiKeyRecord, "allowed_cidrs" | "operations">,
  config: GatewayConfig,
  operation?: string,
  peerAddress?: string | (() => string | undefined),
): void {
  if (key.allowed_cidrs.length > 0) {
    const resolvedPeer = typeof peerAddress === "function" ? peerAddress() : peerAddress;
    const addr = clientAddress(
      resolvedPeer,
      c.req.raw.headers.get("x-forwarded-for"),
      config.trustedProxyCidrs,
    );
    if (!addr || !inCidrs(addr, key.allowed_cidrs)) {
      throw new GatewayError(403, "ip_not_allowed", "Client IP address is not permitted");
    }
  }
  if (operation !== undefined && key.operations.length > 0 && !key.operations.includes(operation)) {
    throw new GatewayError(403, "operation_not_allowed", "API key cannot invoke this operation");
  }
}
