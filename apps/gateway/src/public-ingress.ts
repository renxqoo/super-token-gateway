import { GatewayError } from "@gateway/contracts";
import type { Operation } from "@gateway/kernel";
import { formatProtocolError } from "@gateway/protocol";
import { clientAddress, type ClusterCoordinator, type IngressIpCheck } from "@gateway/runtime";
import type { GatewayConfig } from "./config.ts";
import type { createPressureMonitor } from "./resource-pressure.ts";

export class IngressDenialError extends GatewayError {
  constructor(
    status: number,
    code: string,
    message: string,
    public readonly retryAfter: string,
  ) {
    super(status, code, message);
  }
}

export type DeferredIngressIp = {
  readonly spec: IngressIpCheck;
  beginWait(): void;
  endWait(): void;
  recordDenial(reason: "ip_rate_limited" | "ingress_quota_unavailable"): void;
  denyError(
    reason: "ip_rate_limited" | "ingress_quota_unavailable",
    retryAfterMs?: number,
  ): IngressDenialError;
  flushEarly(): Promise<void>;
};

export type DeferredIngressPrep =
  | { kind: "deny"; response: Response }
  | { kind: "allow"; declaredSize: number | undefined }
  | { kind: "fallback" }
  | { kind: "defer"; declaredSize: number; ip: string };

const DEFERRED_IP_MAX_BODY_BYTES = 65_536;

type IngressDeferredHost = {
  adjustPending(delta: number): void;
  recordDenial(reason: "ip_rate_limited" | "ingress_quota_unavailable"): void;
  makeDenialError(
    reason: "ip_rate_limited" | "ingress_quota_unavailable",
    retryAfterMs: number,
  ): IngressDenialError;
  takeWindow(spec: IngressIpCheck): Promise<number | null>;
};

class DeferredIngressToken implements DeferredIngressIp {
  constructor(
    readonly spec: IngressIpCheck,
    private readonly host: IngressDeferredHost,
  ) {}
  beginWait(): void {
    this.host.adjustPending(1);
  }
  endWait(): void {
    this.host.adjustPending(-1);
  }
  recordDenial(reason: "ip_rate_limited" | "ingress_quota_unavailable"): void {
    this.host.recordDenial(reason);
  }
  denyError(
    reason: "ip_rate_limited" | "ingress_quota_unavailable",
    retryAfterMs?: number,
  ): IngressDenialError {
    return this.host.makeDenialError(reason, retryAfterMs ?? this.spec.timeoutMs);
  }
  async flushEarly(): Promise<void> {
    this.host.adjustPending(1);
    let wait: number | null;
    try {
      wait = await this.host.takeWindow(this.spec);
    } finally {
      this.host.adjustPending(-1);
    }
    if (wait === 0) return;
    throw wait === null
      ? this.host.makeDenialError("ingress_quota_unavailable", this.spec.timeoutMs)
      : this.host.makeDenialError("ip_rate_limited", wait);
  }
}

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

const DECLARED_SIZE_CACHE = new WeakMap<Request, number | null>();

function parseContentLengthRaw(req: Request): number | null {
  const header = req.headers.get("content-length");
  if (header === null || header.length === 0) return null;
  let n = 0;
  for (let i = 0; i < header.length; i++) {
    const d = header.charCodeAt(i) - 48;
    if (d < 0 || d > 9) return null;
    n = n * 10 + d;
  }
  return n;
}

export function declaredRequestSize(req: Request, maxBytes: number): number | undefined {
  let cached = DECLARED_SIZE_CACHE.get(req);
  if (cached === undefined) {
    cached = parseContentLengthRaw(req);
    DECLARED_SIZE_CACHE.set(req, cached);
  }
  if (cached === null) return undefined;
  if (cached > maxBytes) {
    throw new GatewayError(413, "body_too_large", "Payload exceeds the configured size limit");
  }
  return cached;
}

const CANONICAL_IP_CACHE = new Map<string, string>();
const CANONICAL_IP_CACHE_MAX = 1024;

function canonicalIp(ip: string): string {
  if (!ip.includes(":")) return ip;
  const cached = CANONICAL_IP_CACHE.get(ip);
  if (cached !== undefined) return cached;
  const canonical = new URL(`http://[${ip}]`).hostname.slice(1, -1);
  const mapped = canonical.match(/^::ffff:([\da-f]+):([\da-f]+)$/);
  let out: string;
  if (!mapped) {
    out = canonical;
  } else {
    const high = Number.parseInt(mapped[1]!, 16);
    const low = Number.parseInt(mapped[2]!, 16);
    out = `${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`;
  }
  if (CANONICAL_IP_CACHE.size >= CANONICAL_IP_CACHE_MAX) CANONICAL_IP_CACHE.clear();
  CANONICAL_IP_CACHE.set(ip, out);
  return out;
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
    let wait: number | null;
    try {
      wait = await cluster.takeIngressWindow(
        ip,
        settings.maxRequests,
        settings.windowMs,
        settings.timeoutMs,
      );
    } finally {
      pending--;
    }
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

  function makeDenialError(
    reason: "ip_rate_limited" | "ingress_quota_unavailable",
    retryAfterMs: number,
  ): IngressDenialError {
    count(reason, "enforce");
    return reason === "ip_rate_limited"
      ? new IngressDenialError(
          429,
          "ip_rate_limited",
          "IP request rate exceeded",
          retryAfter(retryAfterMs),
        )
      : new IngressDenialError(
          503,
          "ingress_quota_unavailable",
          "Shared ingress quota unavailable",
          retryAfter(retryAfterMs),
        );
  }

  const ipLimitStr = ipSettings ? String(ipSettings.maxRequests) : "";
  const ipWindowStr = ipSettings ? String(ipSettings.windowMs) : "";
  const deferredHost: IngressDeferredHost = {
    adjustPending(delta: number) {
      pending += delta;
    },
    recordDenial(reason) {
      count(reason, "enforce");
    },
    makeDenialError,
    takeWindow(spec) {
      return cluster.takeIngressWindow(spec.ip, spec.limit, spec.windowMs, spec.timeoutMs);
    },
  };
  const specCache = new Map<string, IngressIpCheck>();

  function checkBodyHeadersDirect(
    req: Request,
    operation: Operation,
    cacheSize: boolean,
  ): { response: Response } | { declaredSize: number | undefined } {
    const value = req.headers.get("content-encoding");
    if (value !== null && value.trim().toLowerCase() !== "identity") {
      count("unsupported_content_encoding", "enforce");
      return {
        response: reject(operation, {
          status: 415,
          code: "unsupported_content_encoding",
          message: "Request content encoding is unsupported",
          headers: { "accept-encoding": "identity" },
        }),
      };
    }
    if (cacheSize) {
      try {
        return { declaredSize: declaredRequestSize(req, config.maxBodyBytes) };
      } catch (error) {
        if (!(error instanceof GatewayError)) throw error;
        count(error.code, "enforce");
        return {
          response: reject(operation, {
            status: error.status,
            code: error.code,
            message: error.message,
          }),
        };
      }
    }
    const raw = parseContentLengthRaw(req);
    if (raw !== null && raw > config.maxBodyBytes) {
      count("body_too_large", "enforce");
      return {
        response: reject(operation, {
          status: 413,
          code: "body_too_large",
          message: "Payload exceeds the configured size limit",
        }),
      };
    }
    return { declaredSize: raw === null ? undefined : raw };
  }

  return {
    activateDeferred(ip: string): DeferredIngressIp {
      windowCount++;
      let spec = specCache.get(ip);
      if (!spec) {
        if (specCache.size >= CANONICAL_IP_CACHE_MAX) specCache.clear();
        const settings = ipSettings!;
        spec = {
          ip,
          limit: settings.maxRequests,
          windowMs: settings.windowMs,
          timeoutMs: settings.timeoutMs,
          limitStr: ipLimitStr,
          windowMsStr: ipWindowStr,
        };
        specCache.set(ip, spec);
      }
      return new DeferredIngressToken(spec, deferredHost);
    },
    tryPrepareDeferred(
      req: Request,
      operation: Operation,
      peer?: string | (() => string | undefined),
    ): DeferredIngressPrep {
      const reason = pressure?.reason();
      if (reason && config.ingressPressure) {
        const { mode, sampleMs } = config.ingressPressure;
        count(`pressure_${reason}`, mode);
        if (mode === "enforce") {
          return {
            kind: "deny",
            response: reject(operation, {
              status: 503,
              code: "overloaded",
              message: "Gateway resource pressure",
              headers: { "retry-after": retryAfter(sampleMs) },
            }),
          };
        }
      }
      if (!ipSettings) {
        const checked = checkBodyHeadersDirect(req, operation, false);
        return "response" in checked
          ? { kind: "deny", response: checked.response }
          : { kind: "allow", declaredSize: checked.declaredSize };
      }
      if (ipSettings.mode !== "enforce") return { kind: "fallback" };
      if (
        operation === "transcription" ||
        operation === "translation" ||
        operation === "image_edits"
      ) {
        const ct = req.headers.get("content-type");
        if (ct && ct.toLowerCase().includes("multipart/form-data")) return { kind: "fallback" };
      }
      const checked = checkBodyHeadersDirect(req, operation, false);
      if ("response" in checked) return { kind: "deny", response: checked.response };
      if (checked.declaredSize === undefined || checked.declaredSize > DEFERRED_IP_MAX_BODY_BYTES) {
        return { kind: "fallback" };
      }
      let ip: string;
      try {
        const resolvedPeer = typeof peer === "function" ? peer() : peer;
        const address = clientAddress(
          resolvedPeer,
          req.headers.get("x-forwarded-for"),
          config.trustedProxyCidrs,
        );
        if (!address) {
          count("invalid_client_ip", "enforce");
          return {
            kind: "deny",
            response: reject(operation, {
              status: 400,
              code: "invalid_client_ip",
              message: "Client address is unavailable",
            }),
          };
        }
        ip = canonicalIp(address);
      } catch (error) {
        const denial =
          error instanceof GatewayError
            ? { status: error.status, code: error.code, message: error.message }
            : { status: 400, code: "invalid_client_ip", message: "Client address is invalid" };
        count(denial.code, "enforce");
        return { kind: "deny", response: reject(operation, denial) };
      }
      const now = performance.now();
      if (now >= windowEnd) {
        windowEnd = now + ipSettings.windowMs;
        windowCount = 0;
      }
      if (windowCount >= ipSettings.nodeMaxRequests || pending >= ipSettings.maxPending) {
        count("ingress_capacity_unavailable", "enforce");
        return {
          kind: "deny",
          response: reject(operation, {
            status: 503,
            code: "ingress_capacity_unavailable",
            message: "Node ingress capacity unavailable",
            headers: { "retry-after": retryAfter(windowEnd - now) },
          }),
        };
      }
      return { kind: "defer", declaredSize: checked.declaredSize, ip };
    },
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
      if (!ipSettings) {
        const checked = checkBodyHeadersDirect(req, operation, true);
        return "response" in checked ? checked.response : undefined;
      }
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
        const checked = checkBodyHeadersDirect(req, operation, true);
        return "response" in checked ? checked.response : undefined;
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
