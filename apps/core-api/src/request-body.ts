import { GatewayError } from "@gateway/contracts";
import { readBounded } from "@gateway/runtime";

export async function readJsonBody(
  request: Request,
  maxBytes: number,
  timeoutMs: number,
): Promise<unknown> {
  const deadline = AbortSignal.timeout(timeoutMs);
  let bytes: Uint8Array;
  try {
    bytes = await readBounded(request.body, maxBytes, AbortSignal.any([request.signal, deadline]));
  } catch (error) {
    if (deadline.aborted && !request.signal.aborted)
      throw new GatewayError(408, "body_timeout", "Request body was not received in time");
    throw error;
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}
