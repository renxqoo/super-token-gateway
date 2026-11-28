import { GatewayError } from "@gateway/contracts";
import { canonical, normalizedNumber } from "@gateway/kernel";
import { formatProtocolError, numberLiteral } from "@gateway/protocol";
import { sha256 } from "@gateway/runtime";
import { responseHeaders } from "./http.ts";
import type { InferenceRequest } from "./types.ts";
import type { InferenceTelemetry } from "./telemetry.ts";

export type RequestIdentity = { key: string; fingerprint: string };

export async function identifyRequest(
  request: InferenceRequest,
  key: string,
): Promise<RequestIdentity> {
  return { key, fingerprint: `v1:${await fingerprintInference(request)}` };
}

export async function findReplayResponse(
  request: InferenceRequest,
  telemetry: InferenceTelemetry,
  identity: RequestIdentity,
): Promise<Response | null> {
  const { deps, billingAccount, operation } = request;
  const prior = await deps.ledger.findIdempotentHold(
    billingAccount.id,
    operation,
    identity.key,
    identity.fingerprint,
  );
  if (!prior) return null;
  if (!prior.matches) {
    throw new GatewayError(
      409,
      "idempotency_conflict",
      "Idempotency key reused with different request fingerprint",
    );
  }
  return replayResponse(request, telemetry, prior.requestId, prior.state);
}

export function replayResponse(
  { c, operation, trace }: InferenceRequest,
  telemetry: InferenceTelemetry,
  priorId: string,
  priorState: string | undefined,
): Response {
  const inProgress = priorState === "HELD";
  const code = inProgress ? "idempotency_in_progress" : "idempotency_replayed";
  const message = inProgress
    ? "Request with this Idempotency-Key is already in progress"
    : `Request already finalized with state ${priorState ?? "UNKNOWN"}`;
  telemetry.emitAccessLog(409, code);
  return c.json(
    formatProtocolError(operation, 409, code, message, priorId),
    409,
    responseHeaders(priorId, trace),
  );
}

async function fingerprintInference(request: InferenceRequest): Promise<string> {
  const { multipartEntries, operation, billingAccount, model, clientBody } = request;
  const body = multipartEntries
    ? await Promise.all(
        multipartEntries.map(async ([field, value]) => [
          field,
          typeof value === "string"
            ? value
            : {
                filename: value.name,
                type: value.type,
                sha256: new Bun.CryptoHasher("sha256")
                  .update(await value.arrayBuffer())
                  .digest("hex"),
              },
        ]),
      )
    : clientBody;
  return sha256(
    canonical(
      { billing_account_id: billingAccount.id, operation, model: model.name, body },
      (parent, key) => {
        const literal = numberLiteral(parent, key);
        if (!literal) return undefined;
        const rounded = String(Number(literal));
        if (literal === rounded) return undefined;
        if (!Number.isFinite(Number(literal))) return literal;
        const exact = normalizedNumber(literal);
        const value = normalizedNumber(rounded);
        return exact.digits === value.digits &&
          exact.exponent === value.exponent &&
          exact.negative === value.negative
          ? undefined
          : literal;
      },
    ),
  );
}
