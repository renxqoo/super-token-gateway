import { GatewayError } from "@gateway/contracts";
import type { Limits } from "./limits.ts";
export async function orderRouteCandidates<
  T extends { id: string; provider_id: string; priority: number; weight: number },
>(
  routes: readonly T[],
  strategy: string,
  model: string,
  limits: Pick<Limits, "health" | "roundRobin">,
): Promise<T[]> {
  const health = await Promise.all(
      routes.map(async (route) => ({ route, health: await limits.health(route.provider_id) })),
    ),
    candidates = health.filter((row) => !row.health.open);
  if (!candidates.length)
    throw new GatewayError(
      503,
      "no_route",
      "No healthy provider supports this model and operation",
    );
  const result: T[] = [];
  for (const priority of [...new Set(candidates.map((c) => c.route.priority))].sort(
    (a, b) => b - a,
  )) {
    const tier = candidates
      .filter((c) => c.route.priority === priority)
      .sort((a, b) => a.route.id.localeCompare(b.route.id));
    if (strategy === "ROUND_ROBIN" && tier.length > 1) {
      const offset = await limits.roundRobin(`${model}:${priority}`, tier.length);
      result.push(
        ...tier.slice(offset).map((c) => c.route),
        ...tier.slice(0, offset).map((c) => c.route),
      );
    } else if (strategy === "LEAST_LATENCY")
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
