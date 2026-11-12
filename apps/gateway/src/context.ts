import type { CatalogApplication } from "@gateway/core-api/catalog";
import type { Credential, Project } from "@gateway/contracts";
import type { Store, FundsStore } from "@gateway/storage";
import type { Lifecycle, Limits, Metrics, MemoryBudget, createTransport } from "@gateway/runtime";
import type { Metering } from "./usage.ts";
import type { Config } from "./config.ts";
export interface Auth {
  credential: Credential;
  project: Project;
}
export type Env = {
  Bindings: { peerAddress?: string };
  Variables: { requestId: string; auth: Auth; reserveMemory: (bytes: number) => void };
};
export interface Dependencies {
  config: Config;
  catalog?: CatalogApplication;
  usage: Metering;
  store: Omit<Store, "db">;
  funds: Omit<FundsStore, "db">;
  database: { ping(): Promise<void>; close(timeout?: number): Promise<void> };
  transport: ReturnType<typeof createTransport>;
  limits: Limits;
  lifecycle: Lifecycle;
  metrics: Metrics;
  memory: MemoryBudget;
}
