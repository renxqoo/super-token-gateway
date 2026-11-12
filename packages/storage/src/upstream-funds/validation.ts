import { GatewayError } from "@gateway/contracts";

export function fundingInput<T>(
  schema: { safeParse(input: unknown): { success: true; data: T } | { success: false } },
  input: unknown,
): T {
  const result = schema.safeParse(input);
  if (!result.success)
    throw new GatewayError(400, "invalid_funding_input", "Invalid upstream funding input");
  return result.data;
}
export function fundingActor(actor: string): string {
  if (typeof actor !== "string" || actor.length < 1 || actor.length > 200 || actor.trim() !== actor)
    throw new GatewayError(400, "invalid_funding_input", "Invalid trusted funding actor");
  return actor;
}
export function fundingId(id: string): string {
  if (
    typeof id !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id)
  )
    throw new GatewayError(400, "invalid_funding_input", "Invalid funding identity");
  return id.toLowerCase();
}
export function fundingFailure(error: unknown): never {
  if (error instanceof GatewayError) throw error;
  const code = (error as { errno?: string })?.errno;
  if (code === "22003")
    throw new GatewayError(
      409,
      "upstream_amount_overflow",
      "Upstream amount exceeds storage precision",
    );
  if (code === "23514" || code === "23503")
    throw new GatewayError(
      409,
      "upstream_funding_constraint",
      "Upstream funding constraint rejected the operation",
    );
  throw new GatewayError(
    503,
    "upstream_funding_unavailable",
    "Upstream funding storage operation failed",
  );
}
