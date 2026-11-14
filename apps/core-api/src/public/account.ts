import type { SQL } from "bun";
import { Hono } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { clientAddress } from "@gateway/runtime";
import { verifier } from "@gateway/runtime/crypto";
import { createCustomerFundsStore } from "@gateway/storage";

type AccountEnv = { Bindings: { peerAddress?: string }; Variables: { requestId: string } };

const accountSchema = z.discriminatedUnion("status", [
  z.object({ status: z.literal("unauthenticated") }),
  z.object({ status: z.literal("ip_denied") }),
  z.object({
    status: z.literal("authenticated"),
    project_id: z.uuid(),
    key_id: z.uuid(),
    role: z.string(),
    models: z.array(z.string()),
    operations: z.array(z.string()),
  }),
]);

function errorResponse(error: unknown, requestId: string) {
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

export function createCoreAccountApplication(options: {
  db: SQL;
  pepper: string;
  trustedProxyCidrs: readonly string[];
}) {
  const funds = createCustomerFundsStore(options.db);
  const app = new Hono<AccountEnv>();
  app.use("*", async (c, next) => {
    c.set("requestId", Bun.randomUUIDv7());
    await next();
    c.header("x-request-id", c.get("requestId"));
    c.header("x-content-type-options", "nosniff");
    c.header("cache-control", "no-store");
  });
  app.onError((error, c) =>
    errorResponse(
      error instanceof z.ZodError
        ? new GatewayError(400, "invalid_request", "Request failed schema validation")
        : error,
      c.get("requestId"),
    ),
  );
  app.notFound((c) =>
    errorResponse(new GatewayError(404, "not_found", "Unknown endpoint"), c.get("requestId")),
  );
  async function account(c: {
    env: AccountEnv["Bindings"] | undefined;
    req: { header(name: string): string | undefined };
  }) {
    const authorization = c.req.header("authorization");
    const token = authorization?.startsWith("Bearer ")
      ? authorization.slice(7)
      : (c.req.header("x-api-key") ?? c.req.header("x-goog-api-key") ?? "");
    if (!/^gw_[A-Za-z0-9_-]{43}$/.test(token))
      throw new GatewayError(401, "unauthenticated", "A valid project API key is required");
    const ip = clientAddress(
      c.env?.peerAddress,
      c.req.header("x-forwarded-for") ?? null,
      options.trustedProxyCidrs,
    );
    const rows =
      await options.db`SELECT core_account_auth_v1(${verifier(token, options.pepper)},${ip ?? null}) AS account`;
    const resolved = accountSchema.parse(rows[0]?.account);
    if (resolved.status === "unauthenticated")
      throw new GatewayError(
        401,
        "unauthenticated",
        "Project API key is invalid, revoked or expired",
      );
    if (resolved.status === "ip_denied")
      throw new GatewayError(403, "ip_denied", "Credential does not permit this client address");
    if (!["ADMIN", "BILLING"].includes(resolved.role))
      throw new GatewayError(403, "role_denied", "Credential role does not permit this operation");
    return resolved;
  }
  app.get("/v1/wallets", async (c) => {
    const resolved = await account(c);
    return c.json({ data: await funds.wallets(resolved.project_id) });
  });
  app.get("/v1/requests/:id/billing", async (c) => {
    const resolved = await account(c);
    const requestId = z.uuid().parse(c.req.param("id"));
    return c.json(await funds.receipt(requestId, resolved.project_id, resolved));
  });
  return app;
}
