import type {
  GatewayRouteReleaseResult,
  ModelRoutingView,
  RouteReleaseChange,
  RouteReleaseInput,
  RouteRollbackInput,
} from "@gateway/contracts";
import type { GatewayClient } from "./gateway-client.ts";

function modelPath(model: string) {
  return "/internal/v1/models/" + encodeURIComponent(model);
}

function releasePath(model: string) {
  return modelPath(model) + "/route-releases";
}

export function createRouteReleaseClient(gateway: GatewayClient) {
  return {
    view(model: string) {
      return gateway.request<ModelRoutingView>("catalog", "GET", modelPath(model) + "/routing");
    },
    validate(model: string, input: RouteReleaseInput) {
      return gateway.request<{ config_epoch: number; changes: RouteReleaseChange[] }>(
        "catalog",
        "POST",
        releasePath(model) + "/validate",
        input,
      );
    },
    publish(model: string, input: RouteReleaseInput, signal?: AbortSignal) {
      return gateway.request<GatewayRouteReleaseResult>(
        "catalog",
        "POST",
        releasePath(model),
        input,
        signal,
      );
    },
    get(model: string, id: string) {
      return gateway.request<GatewayRouteReleaseResult>(
        "catalog",
        "GET",
        releasePath(model) + "/" + encodeURIComponent(id),
      );
    },
    rollback(model: string, original: string, input: RouteRollbackInput, signal?: AbortSignal) {
      return gateway.request<GatewayRouteReleaseResult>(
        "catalog",
        "POST",
        releasePath(model) + "/" + encodeURIComponent(original) + "/rollback",
        input,
        signal,
      );
    },
  };
}
