import type { ConfigSnapshot, ApiKeyRecord, BillingAccountRecord } from "@gateway/ledger";
import {
  ClusterCoordinator,
  createTransport,
  Lifecycle,
  SmartRouter,
  verifier,
  VertexTokens,
} from "@gateway/runtime";
import { PluginPipelineEngine } from "@gateway/plugin-engine";
import type { GatewayConfig } from "../config.ts";
import { GatewayMetrics } from "../observability.ts";
import type { ProxyDependencies } from "../inference/types.ts";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { SnapshotManager } from "../snapshot.ts";

export const TEST_PEPPER = "test-pepper-0123456789abcdef";

export function makeConfig(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
  return {
    mode: "gateway",
    host: "127.0.0.1",
    port: 0,
    internalHost: "127.0.0.1",
    internalPort: 0,
    databaseUrl: "postgres://u:p@127.0.0.1:5432/db",
    dbMaxConnections: 4,
    keyPepper: TEST_PEPPER,
    encryptionKey: "ab".repeat(32),
    allowPrivateUpstreams: true,
    privateUpstreamCidrs: [],
    trustedProxyCidrs: [],
    maxInFlight: 8,
    maxBodyBytes: 1024 * 1024,
    holdTtlMs: 60_000,
    attemptTimeoutMs: 5_000,
    snapshotPollMs: 0,
    microBatchWindowMs: 2,
    microBatchMaxSize: 64,
    ...overrides,
  };
}

export function makeBillingAccount(id: string, concurrency: number): BillingAccountRecord {
  return {
    id,
    name: id,
    external_ref: null,
    currency: "USD",
    enabled: true,
    rpm: 10_000,
    tpm: 10_000_000,
    concurrency,
  };
}

export function makeApiKey(
  rawKey: string,
  billingAccountId: string,
  pepper = TEST_PEPPER,
): ApiKeyRecord {
  return {
    id: `key-${rawKey}`,
    billing_account_id: billingAccountId,
    name: rawKey,
    prefix: rawKey.slice(0, 8),
    verifier: verifier(rawKey, pepper),
    role: "INVOKER",
    models: [],
    operations: [],
    allowed_cidrs: [],
    enabled: true,
    expires_at: null,
    spend_limit_atoms: null,
    spend_limit_window: "LIFETIME",
  };
}

export function makeSnapshot(
  epoch: number,
  billingAccounts: BillingAccountRecord[],
  keys: ApiKeyRecord[],
): ConfigSnapshot {
  return {
    epoch,
    lineageToken: "11111111-1111-4111-8111-111111111111",
    billingAccounts: new Map(billingAccounts.map((p) => [p.id, p])),
    keysByVerifier: new Map(keys.map((k) => [k.verifier, k])),
    accounts: new Map(),
    channels: new Map(),
    models: new Map(),
    prices: new Map(),
    pricingOverridesByBillingAccount: new Map(),
    routesByModel: new Map(),
    plugins: new Map(),
    revokedPlugins: new Set(),
    providerPluginsByName: new Map(),
  };
}

export function makeFakeDeps(snap: ConfigSnapshot, config: GatewayConfig): ProxyDependencies {
  const noopSignal = new AbortController().signal;
  const snapshot = {
    isDiverged: () => false,
    isFresh: () => true,
    get: () => snap,
    isNoopPluginSignal: (s: AbortSignal) => s === noopSignal,
    pluginSignal: () => noopSignal,
    decryptChannelSecret: () => "",
  } as unknown as SnapshotManager;
  const transport = createTransport(16);
  const cluster = new ClusterCoordinator();
  const router = new SmartRouter();
  const lifecycle = new Lifecycle(config.maxInFlight);
  const metrics = new GatewayMetrics(false);
  const pluginEngine = new PluginPipelineEngine({ wasmExecutor: null });
  const vertexTokens = new VertexTokens();
  const ledger = {} as ProxyDependencies["ledger"];
  const state = new ProxyRuntimeState();
  return {
    config,
    checkConsoleAdmission: () => undefined,
    snapshot,
    ledger,
    router,
    cluster,
    transport,
    vertexTokens,
    pluginEngine,
    wasmExecutor: null,
    lifecycle,
    metrics,
    state,
  };
}
