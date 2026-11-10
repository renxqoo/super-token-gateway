import { GatewayError } from "@gateway/kernel";
import type { Operation, Route } from "@gateway/kernel";
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
  const health = await Promise.all(
    routes.map(async (route) => ({ route, health: await deps.limits.health(route.provider_id) })),
  );
  const candidates = health.filter((r) => !r.health.open);
  if (!candidates.length)
    throw new GatewayError(
      503,
      "no_route",
      "No healthy provider supports this model and operation",
    );
  const result: Route[] = [];
  const priorities = [...new Set(candidates.map((c) => c.route.priority))].sort((a, b) => b - a);
  for (const priority of priorities) {
    const tier = candidates
      .filter((c) => c.route.priority === priority)
      .sort((a, b) => a.route.id.localeCompare(b.route.id));
    if (policy.routing_strategy === "ROUND_ROBIN" && tier.length > 1) {
      const offset = await deps.limits.roundRobin(`${model}:${priority}`, tier.length);
      result.push(
        ...tier.slice(offset).map((c) => c.route),
        ...tier.slice(0, offset).map((c) => c.route),
      );
    } else if (policy.routing_strategy === "LEAST_LATENCY")
      result.push(
        ...tier
          .sort((a, b) => a.health.latency - b.health.latency || b.route.weight - a.route.weight)
          .map((c) => c.route),
      );
    else
      result.push(
        ...tier
          .map((c) => ({
            route: c.route,
            score: -Math.log(Math.max(Number.MIN_VALUE, Math.random())) / c.route.weight,
          }))
          .sort((a, b) => a.score - b.score)
          .map((c) => c.route),
      );
  }
  return result;
}
