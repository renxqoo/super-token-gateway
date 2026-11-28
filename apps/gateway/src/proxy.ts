import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import { fastUuid, ProxyRuntimeState } from "./proxy-state.ts";
import { parseOrCreateTrace } from "./observability.ts";
import type {
  InferenceContext,
  InferenceFastHandoff,
  ProxyDependencies,
} from "./inference/types.ts";
import { errorJson } from "./inference/http.ts";
import { createInferenceTelemetry } from "./inference/telemetry.ts";
import { createInferenceResources } from "./inference/resources.ts";
import { createInferenceAccounting } from "./inference/accounting.ts";
import { admitInferenceRequest } from "./inference/request.ts";
import { routeInference } from "./inference/route-attempts.ts";

const fallbackStates = new WeakMap<ProxyDependencies, ProxyRuntimeState>();

function getProxyState(deps: ProxyDependencies): ProxyRuntimeState {
  if (deps.state) return deps.state;
  let state = fallbackStates.get(deps);
  if (!state) {
    state = new ProxyRuntimeState();
    fallbackStates.set(deps, state);
  }
  return state;
}

export async function handleInferenceRequest(
  c: InferenceContext,
  deps: ProxyDependencies,
  operation: Operation,
  pathOverride?: { model: string; stream: boolean },
  peerAddress?: string | (() => string | undefined),
  handoff?: InferenceFastHandoff,
): Promise<Response> {
  const state = getProxyState(deps);
  const requestId = fastUuid();
  const trace = parseOrCreateTrace(c.req.raw.headers.get("traceparent") ?? undefined);
  const requestStart = performance.now();
  deps.metrics.requestsTotal++;
  const telemetry = createInferenceTelemetry(deps, operation, requestId, trace, requestStart);
  const resources = createInferenceResources(deps, state, c.req.raw.signal);
  try {
    const request = await admitInferenceRequest(
      c,
      deps,
      state,
      operation,
      requestId,
      trace,
      requestStart,
      resources,
      telemetry,
      pathOverride,
      peerAddress,
      handoff,
    );
    const accounting = createInferenceAccounting(request, telemetry);
    return await routeInference(request, resources, telemetry, accounting);
  } catch (error) {
    const status = error instanceof GatewayError ? error.status : 500;
    const code = error instanceof GatewayError ? error.code : "internal_error";
    telemetry.emitAccessLog(status, code);
    return errorJson(c, operation, error, requestId, trace);
  } finally {
    const released = resources.finishRequest();
    if (released) await released;
  }
}
