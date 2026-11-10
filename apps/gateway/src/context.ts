import type { Credential, Project } from "@gateway/kernel";
import type { Store, FundsStore } from "@gateway/storage";
import type { Lifecycle, Limits, Metrics, MemoryBudget } from "@gateway/runtime";
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
  usage: Metering;
  store: Store;
  funds: FundsStore;
  limits: Limits;
  lifecycle: Lifecycle;
  metrics: Metrics;
  memory: MemoryBudget;
}
