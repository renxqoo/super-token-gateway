import type { SQL } from "bun";
import type { Handler, MiddlewareHandler } from "hono";
import type { z } from "zod";
import type {
  createAsyncTasksRepository,
  createCatalogRepository,
  createLedgerEngine,
  createLedgerQueries,
} from "@gateway/ledger";
import type { PluginPipelineEngine, WasmRuntimeExecutor } from "@gateway/plugin-engine";
import type { ClusterCoordinator, SmartRouter } from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { InternalRole } from "./internal-auth.ts";
import type { SnapshotManager } from "./snapshot.ts";
import type { AdminEnv, AdminContext } from "./internal-request.ts";

type CatalogRepo = ReturnType<typeof createCatalogRepository>;

type LedgerEngine = ReturnType<typeof createLedgerEngine>;

type LedgerQueries = ReturnType<typeof createLedgerQueries>;

type AsyncTasksRepo = ReturnType<typeof createAsyncTasksRepository>;

export type InternalApiDeps = {
  db: SQL;
  onAuditFailure: () => void;
  config: GatewayConfig;
  catalog: CatalogRepo;
  ledger: LedgerEngine;
  queries: LedgerQueries;
  asyncTasks: AsyncTasksRepo;
  snapshot: SnapshotManager;
  router: SmartRouter;
  cluster: ClusterCoordinator;
  pluginEngine: PluginPipelineEngine;
  wasmExecutor: WasmRuntimeExecutor | null;
};
export type InternalAuthorize = (
  method: string,
  path: string,
  role: InternalRole,
) => MiddlewareHandler<AdminEnv>;

export type InternalRoute = (
  method: "GET" | "POST",
  path: string,
  role: InternalRole,
  handle: Handler<AdminEnv>,
) => void;

export type InternalJsonRoute = <T>(
  method: "POST" | "PUT",
  path: string,
  role: InternalRole,
  schema: z.ZodType<T>,
  handle: (c: AdminContext, body: T) => ReturnType<Handler<AdminEnv>>,
) => void;
