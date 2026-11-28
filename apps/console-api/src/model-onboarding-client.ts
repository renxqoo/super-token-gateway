import type {
  GatewayModelOnboardingResult,
  ModelOnboardingInput,
  ModelOnboardingPreviewInput,
} from "@gateway/contracts";
import type { GatewayClient } from "./gateway-client.ts";

export function createModelOnboardingClient(gateway: GatewayClient) {
  const path = "/internal/v1/models/onboarding";
  return {
    epoch() {
      return gateway.request<{ config_epoch: number }>("catalog", "GET", path);
    },
    validate(input: ModelOnboardingInput) {
      return gateway.request<{ config_epoch: number; customer_price_id: string }>(
        "catalog",
        "POST",
        path + "/validate",
        input,
      );
    },
    preview(input: ModelOnboardingPreviewInput) {
      return gateway.request("auditor", "POST", path + "/preview", input);
    },
    publish(input: ModelOnboardingInput, signal?: AbortSignal) {
      return gateway.request<GatewayModelOnboardingResult>("catalog", "POST", path, input, signal);
    },
    get(model: string, id: string, signal?: AbortSignal) {
      return gateway.request<GatewayModelOnboardingResult>(
        "catalog",
        "GET",
        "/internal/v1/models/" + encodeURIComponent(model) + "/onboarding/" + id,
        undefined,
        signal,
      );
    },
  };
}
