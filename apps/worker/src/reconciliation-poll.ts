import type { createLedgerEngine } from "@gateway/ledger";
import { createTransport, readBounded, validateBaseUrl } from "@gateway/runtime";

type Ledger = ReturnType<typeof createLedgerEngine>;
type Scope = { account_id: string; channel_id: string; currency: string };
type Connector = Scope & {
  endpoint: URL;
  token: string;
  queryParameter: string;
  timeoutMs: number;
  intervalMs: number;
  maxIntervalMs: number;
  statusPath: string[];
  requestIdPath: string[];
  evidencePath: string[];
  currencyPath: string[];
  costPath: string[];
  finalValue: string;
  notExecutedValue: string | null;
};

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const forbiddenPathParts = new Set(["__proto__", "prototype", "constructor"]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function fieldPath(value: unknown, fallback: string): string[] {
  const field = value ?? fallback;
  if (
    typeof field !== "string" ||
    field.length > 128 ||
    !/^[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)*$/.test(field)
  )
    throw Error("Invalid reconciliation receipt field path");
  const parts = field.split(".");
  if (parts.some((part) => forbiddenPathParts.has(part)))
    throw Error("Invalid reconciliation receipt field path");
  return parts;
}

function config(raw: string | undefined, env: Record<string, string | undefined>): Connector[] {
  if (!raw) return [];
  const items: unknown = JSON.parse(raw);
  if (!Array.isArray(items) || items.length > 128) throw Error("Invalid reconciliation connectors");
  const seen = new Set<string>();
  return items.map((item: unknown) => {
    if (
      !isRecord(item) ||
      typeof item.account_id !== "string" ||
      !uuid.test(item.account_id) ||
      typeof item.channel_id !== "string" ||
      !uuid.test(item.channel_id) ||
      typeof item.currency !== "string" ||
      !/^[A-Z]{3,8}$/.test(item.currency) ||
      typeof item.endpoint !== "string" ||
      typeof item.token_env !== "string" ||
      !/^[A-Z][A-Z0-9_]{0,127}$/.test(item.token_env)
    )
      throw Error("Invalid reconciliation connector scope or endpoint");
    const key = [item.account_id, item.channel_id, item.currency].join("/");
    if (seen.has(key)) throw Error("Duplicate reconciliation connector scope");
    seen.add(key);
    const token = env[item.token_env];
    if (!token || !/^[\x21-\x7E]{1,4096}$/.test(token))
      throw Error("Invalid reconciliation connector credential");
    const timeoutMs = item.timeout_ms ?? 2500;
    const intervalMs = item.interval_ms ?? 60_000;
    const maxIntervalMs =
      item.max_interval_ms ?? (typeof intervalMs === "number" ? Math.max(intervalMs, 900_000) : 0);
    const queryParameter = item.request_id_parameter ?? "request_id";
    if (
      typeof timeoutMs !== "number" ||
      !Number.isSafeInteger(timeoutMs) ||
      timeoutMs < 1 ||
      typeof intervalMs !== "number" ||
      !Number.isSafeInteger(intervalMs) ||
      intervalMs <= timeoutMs ||
      intervalMs > 2_147_483_647 ||
      typeof maxIntervalMs !== "number" ||
      !Number.isSafeInteger(maxIntervalMs) ||
      maxIntervalMs < intervalMs ||
      maxIntervalMs > 2_147_483_647 ||
      timeoutMs > 2_147_483_647 ||
      typeof queryParameter !== "string" ||
      !/^[a-zA-Z0-9_-]{1,64}$/.test(queryParameter)
    )
      throw Error("Invalid reconciliation connector scheduling");
    const finalValue = item.final_value ?? "FINAL";
    const notExecutedValue = item.not_executed_value ?? null;
    if (
      typeof finalValue !== "string" ||
      !finalValue ||
      finalValue.length > 64 ||
      (notExecutedValue !== null &&
        (typeof notExecutedValue !== "string" ||
          !notExecutedValue ||
          notExecutedValue.length > 64)) ||
      finalValue === notExecutedValue
    )
      throw Error("Invalid reconciliation connector verdict");
    return {
      account_id: item.account_id,
      channel_id: item.channel_id,
      currency: item.currency,
      endpoint: validateBaseUrl(item.endpoint, false),
      token,
      queryParameter,
      timeoutMs,
      intervalMs,
      maxIntervalMs,
      statusPath: fieldPath(item.status_path, "verdict"),
      requestIdPath: fieldPath(item.request_id_path, "provider_request_id"),
      evidencePath: fieldPath(item.evidence_path, "evidence_ref"),
      currencyPath: fieldPath(item.currency_path, "currency"),
      costPath: fieldPath(item.cost_path, "supplier_cost_atoms"),
      finalValue,
      notExecutedValue,
    };
  });
}

function getField(value: unknown, path: string[]): unknown {
  for (const part of path) {
    if (!isRecord(value) || !Object.hasOwn(value, part)) return;
    value = value[part];
  }
  return value;
}

export function createReconciliationPoller(
  ledger: Ledger,
  env: Record<string, string | undefined> = process.env,
) {
  const connectors = config(env.WORKER_RECONCILIATION_CONNECTORS, env);
  const transport = connectors.length ? createTransport() : null;
  let cursor = 0;

  return {
    async run(limit: number, stop: AbortSignal) {
      if (!transport) return { requested: 0, imported: 0 };
      let requested = 0;
      let imported = 0;
      for (let count = 0; count < Math.min(limit, connectors.length) && !stop.aborted; count++) {
        const connector = connectors[cursor]!;
        cursor = (cursor + 1) % connectors.length;
        const scope = {
          account_id: connector.account_id,
          channel_id: connector.channel_id,
          currency: connector.currency,
        };
        const attempt = await ledger.claimReconciliationPoll(
          scope,
          connector.intervalMs,
          connector.maxIntervalMs,
        );
        if (!attempt || stop.aborted) continue;
        const url = new URL(connector.endpoint);
        url.searchParams.set(connector.queryParameter, attempt.provider_request_id);
        const dispatchDeadline = performance.now() + connector.timeoutMs;
        const signal = AbortSignal.any([stop, AbortSignal.timeout(connector.timeoutMs)]);
        requested++;
        try {
          const response = await transport.fetch(
            url,
            {
              method: "GET",
              headers: { authorization: "Bearer " + connector.token },
              signal,
              dispatchDeadline,
              redirect: "manual",
            },
            false,
          );
          if (!response.ok) {
            await response.body?.cancel();
            continue;
          }
          const raw: unknown = JSON.parse(
            new TextDecoder().decode(await readBounded(response.body, 64 * 1024, signal)),
          );
          if (
            getField(raw, connector.requestIdPath) !== attempt.provider_request_id ||
            getField(raw, connector.currencyPath) !== connector.currency
          )
            continue;
          const receiptId = getField(raw, connector.evidencePath);
          if (typeof receiptId !== "string" || !/^[A-Za-z0-9._:/-]{1,256}$/.test(receiptId))
            continue;
          const verdict = getField(raw, connector.statusPath);
          const cost = getField(raw, connector.costPath);
          const fields = {
            ...scope,
            ...attempt,
            evidence_ref: receiptId,
            actor: "provider-reconciliation-poll",
          };
          if (
            verdict === connector.finalValue &&
            typeof cost === "string" &&
            /^(0|[1-9][0-9]{0,37})$/.test(cost)
          ) {
            const result = await ledger.submitReconciliationReceipt({
              ...fields,
              verdict: "FINAL",
              supplier_cost_atoms: cost,
            });
            if (result.status === "accepted") imported++;
          } else if (
            connector.notExecutedValue !== null &&
            verdict === connector.notExecutedValue &&
            (cost === undefined || cost === null || cost === "0")
          ) {
            const result = await ledger.submitReconciliationReceipt({
              ...fields,
              verdict: "NOT_EXECUTED",
            });
            if (result.status === "accepted") imported++;
          }
        } catch {
          if (stop.aborted) break;
        }
      }
      return { requested, imported };
    },

    close() {
      transport?.close();
    },
  };
}
