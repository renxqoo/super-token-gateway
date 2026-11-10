export class GatewayError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "GatewayError";
  }
}

export function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new GatewayError(400, "invalid_request", "Expected a JSON object");
  }
  return value as Record<string, unknown>;
}

export function optionalObject(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export function text(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

export function errorResponse(error: unknown, requestId: string): Response {
  const known = error instanceof GatewayError;
  return Response.json(
    {
      error: {
        type: known ? error.code : "internal_error",
        code: known ? error.code : "internal_error",
        message: known ? error.message : "The gateway could not complete this request",
      },
      request_id: requestId,
    },
    { status: known ? error.status : 500, headers: { "x-request-id": requestId } },
  );
}

export function providerObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new GatewayError(502, "invalid_upstream", "Provider returned an invalid object");
  return value as Record<string, unknown>;
}
