import { orderRouteCandidates } from "@gateway/runtime";
import type { Operation } from "@gateway/kernel";
import type { Route } from "@gateway/contracts";
import type { Dependencies } from "./context.ts";

export async function chooseRoutes(
  deps: Dependencies,
  model: string,
  operation: Operation,
): Promise<Route[]> {
  const [routes, policy] = await Promise.all([
    deps.store.routes(model, operation),
    deps.store.model(model),
  ]);
  return orderRouteCandidates(routes, policy.routing_strategy, model, deps.limits);
}
