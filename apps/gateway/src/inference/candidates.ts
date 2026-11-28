import { GatewayError } from "@gateway/contracts";
import { canRouteMatchCurrency } from "../pricing.ts";
import type { HydratedCandidate } from "../proxy-state.ts";
import { resolveRouteUpstreamProtocol } from "../upstream.ts";
import type { InferenceRequest } from "./types.ts";
import { adaptiveOrdering } from "./adaptive-routing.ts";

export function selectInferenceRoutes(request: InferenceRequest) {
  const { deps, state, snap, model, operation, billingAccount } = request;
  let acquiredProbes: Map<string, symbol> | undefined;
  const baseHydrated = state.getHydratedCandidates(
    snap,
    model.name,
    operation,
    billingAccount.currency,
    () => {
      const rawRoutes = snap.routesByModel.get(model.name) ?? [];
      const list: HydratedCandidate[] = [];
      for (const r of rawRoutes) {
        const channel = snap.channels.get(r.channel_id);
        if (!channel) continue;
        const account = snap.accounts.get(channel.account_id);
        if (!account) continue;
        const billingPrice = snap.prices.get(r.billing_price_id);
        const costPrice = snap.prices.get(r.cost_version_id);
        if (!billingPrice || !costPrice) continue;
        if (
          !canRouteMatchCurrency(
            snap,
            billingPrice.currency,
            billingAccount.currency,
            model.name,
            operation,
          )
        )
          continue;
        const candidateUpstreamProto = resolveRouteUpstreamProtocol(snap, channel, r, operation);
        list.push({
          id: r.id,
          channel_id: channel.id,
          account_id: account.id,
          priority: r.priority,
          weight: r.weight,
          operations: r.operations,
          enabled: r.enabled,
          channel_enabled: channel.enabled,
          account_active: account.state === "ACTIVE",
          same_protocol: candidateUpstreamProto !== null && candidateUpstreamProto === operation,
          route: r,
          channel,
          account,
          billingPrice,
          costPrice,
        });
      }
      return list;
    },
  );
  let hydrated: readonly HydratedCandidate[] = baseHydrated;
  let revokedRoute: AbortSignal | undefined;
  if (snap.revokedPlugins.size > 0) {
    const filtered: HydratedCandidate[] = [];
    for (const item of baseHydrated) {
      if (item.enabled && item.channel_enabled && item.operations.includes(operation)) {
        const signal = deps.snapshot.pluginSignal(item.route, item.channel);
        if (signal.aborted) {
          revokedRoute = signal;
          continue;
        }
      }
      filtered.push(item);
    }
    hydrated = filtered;
  }

  const ordering = request.routing.ordering;
  const adaptive = ordering
    ? adaptiveOrdering(
        {
          policy: ordering,
          snapshot: snap,
          model,
          operation,
          isStream: request.isStream,
          outputBound: request.outputBound,
          clientBody: request.clientBody,
          defaultAttemptTimeoutMs: deps.config.attemptTimeoutMs,
          stats: state.adaptiveRouting,
          observe: (decision) =>
            deps.metrics.routing.observeDecision(model.name, ordering.mode, decision),
        },
        hydrated,
      )
    : undefined;
  const selected = deps.router.selectCandidates(
    model.name,
    hydrated,
    model.routing_strategy,
    operation,
    adaptive?.ordering,
  );
  if (selected.length === 0) {
    revokedRoute?.throwIfAborted();
    throw new GatewayError(503, "no_route", "No healthy channel available for this model");
  }
  for (let i = 0; i < selected.length; i++) {
    const entry = selected[i]!;
    if (entry.probe) (acquiredProbes ??= new Map()).set(entry.route.channel_id, entry.probe);
  }

  const releaseAllPendingProbes = () => {
    if (!acquiredProbes || acquiredProbes.size === 0) return;
    for (const [chId, probe] of acquiredProbes) {
      deps.router.releaseProbe(chId, probe);
    }
    acquiredProbes.clear();
  };

  const releaseProbeFor = (channelId: string) => {
    if (!acquiredProbes) return;
    const probe = acquiredProbes.get(channelId);
    if (probe && acquiredProbes.delete(channelId)) deps.router.releaseProbe(channelId, probe);
  };

  const markProbeCompleted = (channelId: string) => {
    acquiredProbes?.delete(channelId);
  };

  const releaseUnusedProbes = (fromIndex: number) => {
    if (!acquiredProbes || acquiredProbes.size === 0) return;
    for (let i = fromIndex; i < selected.length; i++) {
      if (selected[i]!.probe) {
        releaseProbeFor(selected[i]!.route.channel_id);
      }
    }
  };
  return {
    selected,
    adaptive,
    releaseAllPendingProbes,
    releaseProbeFor,
    markProbeCompleted,
    releaseUnusedProbes,
    takeProbe(channelId: string) {
      if (!acquiredProbes) return undefined;
      const probe = acquiredProbes.get(channelId);
      acquiredProbes.delete(channelId);
      return probe;
    },
  };
}

export type InferenceRoutes = ReturnType<typeof selectInferenceRoutes>;
