import type { Operation } from "@gateway/kernel";
import type {
  ApiKeyRecord,
  ConfigSnapshot,
  createAsyncTasksRepository,
  createLedgerEngine,
  ModelRecord,
  BillingAccountRecord,
} from "@gateway/ledger";
import type { PluginPipelineEngine, WasmRuntimeExecutor } from "@gateway/plugin-engine";
import type {
  Lifecycle,
  SmartRouter,
  VertexTokens,
  ClusterCoordinator,
  createTransport,
} from "@gateway/runtime";
import type { ContentfulStatusCode } from "hono/utils/http-status";
import type { GatewayConfig } from "../config.ts";
import type { ConsoleAdmissionCheck } from "../console-admission.ts";
import type { ProxyRuntimeState } from "../proxy-state.ts";
import type { GatewayMetrics, TraceContext } from "../observability.ts";
import type { createTraceQueue } from "../trace-queue.ts";
import type { SnapshotManager } from "../snapshot.ts";
import type { DeferredIngressIp } from "../public-ingress.ts";

export type InferenceFastHandoff = {
  preAuth?: {
    snap: ConfigSnapshot;
    key: ApiKeyRecord;
    billingAccount: BillingAccountRecord;
  };
  declaredSize?: number | undefined;
  deferredIngress?: DeferredIngressIp;
};

export interface InferenceContext {
  req: {
    raw: Request;
    query(name: string): string | undefined;
  };
  json(data: unknown, status?: ContentfulStatusCode, headers?: Record<string, string>): Response;
}

type LedgerEngine = ReturnType<typeof createLedgerEngine>;
type AsyncTasksRepo = ReturnType<typeof createAsyncTasksRepository>;
type Transport = ReturnType<typeof createTransport>;

export type ProxyDependencies = {
  config: GatewayConfig;
  checkConsoleAdmission: ConsoleAdmissionCheck;
  snapshot: SnapshotManager;
  ledger: LedgerEngine;
  asyncTasks?: AsyncTasksRepo;
  router: SmartRouter;
  cluster: ClusterCoordinator;
  transport: Transport;
  vertexTokens: VertexTokens;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
  lifecycle: Lifecycle;
  metrics: GatewayMetrics;
  traceQueue?: ReturnType<typeof createTraceQueue>;
  state?: ProxyRuntimeState;
  bodyBufferBudget?: { reserve(bytes: number): () => void };
};
export type InferenceRequest = {
  c: InferenceContext;
  deps: ProxyDependencies;
  state: ProxyRuntimeState;
  requestId: string;
  operation: Operation;
  trace: TraceContext;
  requestStart: number;
  snap: ConfigSnapshot;
  key: ApiKeyRecord;
  billingAccount: BillingAccountRecord;
  model: ModelRecord;
  clientBody: Record<string, unknown>;
  multipartEntries: [string, string | File][] | undefined;
  isMultipartOp: boolean;
  isStream: boolean;
  outputBound: number;
  initialChars: number | undefined;
  firstByteDeadlineMs: number | null;
  maxStreamDurationMs: number;
};
