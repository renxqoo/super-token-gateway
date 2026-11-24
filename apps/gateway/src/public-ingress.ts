import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import { formatProtocolError } from "@gateway/protocol";
import { clientAddress, type ClusterCoordinator } from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { createPressureMonitor } from "./resource-pressure.ts";

export function createBodyBufferBudget(limit: number) {
  let held = 0;
  let rejected = 0;
  return {
    reserve(bytes: number): () => void {
      if (bytes > limit - held) {
        rejected++;
        throw new GatewayError(
          503,
          "request_body_capacity_unavailable",
          "Request body capacity unavailable",
        );
      }
      held += bytes;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        held -= bytes;
      };
    },
    metrics() {
      return [
        `gateway_ingress_body_buffer_reserved_bytes ${held}`,
        `gateway_ingress_body_buffer_limit_bytes ${limit}`,
        `gateway_ingress_body_buffer_rejections_total ${rejected}`,
      ];
    },
  };
}

export function declaredRequestSize(req: Request, maxBytes: number): number | undefined {
  const header = req.headers.get("content-length");
  if (header === null || !/^\d+$/.test(header)) return undefined;
  const length = Number(header);
  if (length > maxBytes) {
    throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
  }
  return length;
}

function canonicalIp(ip: string): string {
  if (!ip.includes(":")) return ip;
  const canonical = new URL(`http://[${ip}]`).hostname.slice(1, -1);
  const mapped = canonical.match(/^::ffff:([\da-f]+):([\da-f]+)$/);
  if (!mapped) return canonical;
  const high = Number.parseInt(mapped[1]!, 16);
  const low = Number.parseInt(mapped[2]!, 16);
  return `${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`;
}

function retryAfter(ms: number): string {
  return String(Math.max(1, Math.ceil(ms / 1_000)));
}

type PolicyMode = "observe" | "enforce";
type Denial = {
  status: number;
  code: string;
  message: string;
  headers?: Record<string, string>;
};

function reject(operation: Operation, denial: Denial): Response {
  const id = crypto.randomUUID();
  return Response.json(
    formatProtocolError(operation, denial.status, denial.code, denial.message, id),
    { status: denial.status, headers: { "x-request-id": id, ...denial.headers } },
  );
}

export function createPublicIngress(
  config: GatewayConfig,
  cluster: Pick<ClusterCoordinator, "takeIngressWindow">,
  pressure?: ReturnType<typeof createPressureMonitor>,
) {
  const ipSettings = config.ingressIp;
  const counts = new Map<string, number>();
  let pending = 0;
  let windowEnd = 0;
  let windowCount = 0;

  function count(reason: string, mode: PolicyMode) {
    const key = `{reason="${reason}",mode="${mode}"}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  async function checkIp(req: Request, peer: string | undefined): Promise<Denial | undefined> {
    const settings = ipSettings!;
    let ip: string;
    try {
      const address = clientAddress(
        peer,
        req.headers.get("x-forwarded-for"),
        config.trustedProxyCidrs,
      );
      if (!address) {
        return { status: 400, code: "invalid_client_ip", message: "Client address is unavailable" };
      }
      ip = canonicalIp(address);
    } catch (error) {
      return error instanceof GatewayError
        ? { status: error.status, code: error.code, message: error.message }
        : { status: 400, code: "invalid_client_ip", message: "Client address is invalid" };
    }

    const now = performance.now();
    if (now >= windowEnd) {
      windowEnd = now + settings.windowMs;
      windowCount = 0;
    }
    if (windowCount >= settings.nodeMaxRequests || pending >= settings.maxPending) {
      return {
        status: 503,
        code: "ingress_capacity_unavailable",
        message: "Node ingress capacity unavailable",
        headers: { "retry-after": retryAfter(windowEnd - now) },
      };
    }
    windowCount++;
    pending++;
    const wait = await cluster
      .takeIngressWindow(ip, settings.maxRequests, settings.windowMs, settings.timeoutMs)
      .finally(() => pending--);
    if (wait === 0) return;
    return wait === null
      ? {
          status: 503,
          code: "ingress_quota_unavailable",
          message: "Shared ingress quota unavailable",
          headers: { "retry-after": retryAfter(settings.timeoutMs) },
        }
      : {
          status: 429,
          code: "ip_rate_limited",
          message: "IP request rate exceeded",
          headers: { "retry-after": retryAfter(wait) },
        };
  }

  function checkBodyHeaders(req: Request, operation: Operation): Response | undefined {
    const value = req.headers.get("content-encoding");
    if (value !== null && value.trim().toLowerCase() !== "identity") {
      count("unsupported_content_encoding", "enforce");
      return reject(operation, {
        status: 415,
        code: "unsupported_content_encoding",
        message: "Request content encoding is unsupported",
        headers: { "accept-encoding": "identity" },
      });
    }
    try {
      declaredRequestSize(req, config.maxBodyBytes);
    } catch (error) {
      if (!(error instanceof GatewayError)) throw error;
      count(error.code, "enforce");
      return reject(operation, { status: error.status, code: error.code, message: error.message });
    }
  }

  return {
    admit(
      req: Request,
      operation: Operation,
      peer?: string | (() => string | undefined),
    ): Response | undefined | Promise<Response | undefined> {
      const reason = pressure?.reason();
      if (reason && config.ingressPressure) {
        const { mode, sampleMs } = config.ingressPressure;
        count(`pressure_${reason}`, mode);
        if (mode === "enforce")
          return reject(operation, {
            status: 503,
            code: "overloaded",
            message: "Gateway resource pressure",
            headers: { "retry-after": retryAfter(sampleMs) },
          });
      }
      if (!ipSettings) return checkBodyHeaders(req, operation);
      return checkIp(req, typeof peer === "function" ? peer() : peer).then((denial) => {
        if (req.signal.aborted)
          return reject(operation, {
            status: 499,
            code: "client_aborted",
            message: "Client disconnected",
          });
        if (denial) {
          count(denial.code, ipSettings.mode);
          if (ipSettings.mode === "enforce") return reject(operation, denial);
        }
        return checkBodyHeaders(req, operation);
      });
    },
    metrics(): string[] {
      return [
        `gateway_ingress_ip_pending ${pending}`,
        ...Array.from(
          counts,
          ([labels, total]) => `gateway_ingress_denials_total${labels} ${total}`,
        ),
      ];
    },
  };
}
