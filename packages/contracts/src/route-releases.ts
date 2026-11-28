import { z } from "zod";

const releaseId = z.uuid().transform((id) => id.toLowerCase());
const expectedEpoch = z.number().int().nonnegative();
const target = z
  .object({
    route_id: releaseId,
    expected_revision: z.number().int().min(0).max(2_147_483_646),
    priority: z.number().int().min(0).max(1000),
    weight: z.number().positive().max(1000),
  })
  .strict();

export const routeReleaseSchema = z
  .object({
    id: releaseId,
    expected_epoch: expectedEpoch,
    targets: z.tuple([target, target]),
  })
  .strict()
  .refine(({ targets }) => targets[0].route_id !== targets[1].route_id, {
    path: ["targets"],
    message: "Two distinct routes are required",
  });

export const routeRollbackSchema = z
  .object({ id: releaseId, expected_epoch: expectedEpoch })
  .strict();

export type RouteReleaseInput = z.output<typeof routeReleaseSchema>;
export type RouteRollbackInput = z.output<typeof routeRollbackSchema>;
export type RouteReleaseIntent = RouteReleaseInput | (RouteRollbackInput & { rollback_of: string });

export type RoutingDeployment = {
  id: string;
  channel_id: string;
  upstream_model: string;
  operations: string[];
  billing_price_id: string;
  cost_version_id: string;
  enabled: boolean;
  priority: number;
  weight: number;
  source_revision: number;
};

export type ModelRoutingView = {
  model_name: string;
  config_epoch: number;
  routes: RoutingDeployment[];
};

export type ReleasedRoute = Pick<
  RoutingDeployment,
  "id" | "priority" | "weight" | "source_revision"
>;
export type RouteReleaseChange = {
  before: ReleasedRoute;
  after: ReleasedRoute;
  definition_digest: string;
};

export type ConfigReleaseNode = {
  node_id: string;
  applied_epoch: number;
  last_seen_at: string;
  stopped_at: string | null;
  applied_at: string | null;
  status: "APPLIED" | "SUPERSEDED" | "OFFLINE" | "PENDING";
};

export type RouteReleaseResult = {
  id: string;
  model_name: string;
  config_epoch: number;
  expected_nodes: string[];
  local_applied_epoch: number;
  nodes: ConfigReleaseNode[];
  changes: RouteReleaseChange[];
  rollback_of: string | null;
};

export type GatewayRouteReleaseResult = RouteReleaseResult & { request_digest: string };
