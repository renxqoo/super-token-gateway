import {
  bumpLabeled,
  escapeLabel,
  Histogram,
  STREAM_BUCKETS_SECONDS,
  type LabeledCounter,
} from "./prometheus.ts";

export type RoutingDispatch = "not_sent" | "possible" | "sent";
export type RoutingOutcome =
  "complete" | "failed" | "rate_limited" | "rejected" | "cancelled" | "local";
type Mode = "unary" | "stream";
type Timing = { labels: string; histogram: Histogram };
type AdaptiveMode = "observe" | "active";
export type AdaptiveDecision =
  "same" | "different" | "applied" | "incompatible" | "insufficient" | "incomparable";

export function createRoutingMetrics() {
  const attempts = new Map<
    string,
    LabeledCounter<readonly [string, string, Mode, RoutingDispatch, RoutingOutcome]>
  >();
  const deliveries = new Map<string, LabeledCounter<readonly [string, Mode, RoutingOutcome]>>();
  const firstContent = new Map<string, Timing>();
  const completionTimes = new Map<string, Timing>();
  const adaptiveDecisions = new Map<
    string,
    LabeledCounter<readonly [string, AdaptiveMode, AdaptiveDecision]>
  >();
  let overflow = 0;

  function observeTiming(
    map: Map<string, Timing>,
    key: string,
    labels: () => string,
    seconds: number,
  ): void {
    let entry = map.get(key);
    if (!entry) {
      if (map.size >= 256) {
        overflow++;
        return;
      }
      entry = { labels: labels(), histogram: new Histogram(STREAM_BUCKETS_SECONDS) };
      map.set(key, entry);
    }
    entry.histogram.observe(seconds);
  }

  return {
    observeDecision(model: string, mode: AdaptiveMode, decision: AdaptiveDecision): void {
      if (
        !bumpLabeled(
          adaptiveDecisions,
          `${model}\0${mode}\0${decision}`,
          () => [model, mode, decision] as const,
        )
      )
        overflow++;
    },
    observeAttempt(
      model: string,
      routeId: string,
      mode: Mode,
      dispatch: RoutingDispatch,
      outcome: RoutingOutcome,
    ): void {
      if (
        !bumpLabeled(
          attempts,
          `${model}\0${routeId}\0${mode}\0${dispatch}\0${outcome}`,
          () => [model, routeId, mode, dispatch, outcome] as const,
        )
      )
        overflow++;
    },
    observeDelivery(model: string, mode: Mode, outcome: RoutingOutcome, seconds: number): void {
      if (
        !bumpLabeled(
          deliveries,
          `${model}\0${mode}\0${outcome}`,
          () => [model, mode, outcome] as const,
        )
      )
        overflow++;
      if (outcome === "complete")
        observeTiming(
          completionTimes,
          `${model}\0${mode}`,
          () => `model="${escapeLabel(model)}",mode="${mode}"`,
          seconds,
        );
    },
    observeFirstContent(model: string, routeId: string, seconds: number): void {
      observeTiming(
        firstContent,
        `${model}\0${routeId}`,
        () => `model="${escapeLabel(model)}",route_id="${escapeLabel(routeId)}"`,
        seconds,
      );
    },
    render(): string[] {
      const lines = [
        "# TYPE gateway_routing_deliveries_total counter",
        "# TYPE gateway_routing_attempts_total counter",
        "# TYPE gateway_routing_observation_overflow_total counter",
        `gateway_routing_observation_overflow_total ${overflow}`,
        "# TYPE gateway_routing_upstream_first_content_seconds histogram",
        "# TYPE gateway_routing_completed_delivery_duration_seconds histogram",
        "# TYPE gateway_adaptive_routing_decisions_total counter",
      ];
      for (const {
        labels: [model, mode, decision],
        count,
      } of adaptiveDecisions.values()) {
        lines.push(
          `gateway_adaptive_routing_decisions_total{model="${escapeLabel(model)}",mode="${mode}",decision="${decision}"} ${count}`,
        );
      }
      for (const {
        labels: [model, mode, outcome],
        count,
      } of deliveries.values()) {
        lines.push(
          `gateway_routing_deliveries_total{model="${escapeLabel(model)}",mode="${mode}",outcome="${outcome}"} ${count}`,
        );
      }
      for (const {
        labels: [model, routeId, mode, dispatch, outcome],
        count,
      } of attempts.values()) {
        lines.push(
          `gateway_routing_attempts_total{model="${escapeLabel(model)}",route_id="${escapeLabel(routeId)}",mode="${mode}",dispatch="${dispatch}",outcome="${outcome}"} ${count}`,
        );
      }
      for (const { labels, histogram } of firstContent.values()) {
        lines.push(
          ...histogram
            .render("gateway_routing_upstream_first_content_seconds", "", labels)
            .slice(2),
        );
      }
      for (const { labels, histogram } of completionTimes.values()) {
        lines.push(
          ...histogram
            .render("gateway_routing_completed_delivery_duration_seconds", "", labels)
            .slice(2),
        );
      }
      return lines;
    },
  };
}
