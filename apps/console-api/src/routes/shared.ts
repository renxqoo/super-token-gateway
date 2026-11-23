import type { SQL } from "bun";
import type { Context, MiddlewareHandler } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { operations } from "@gateway/kernel";
import { authenticateSession, type AuthUser, type LoginRateLimiter } from "../auth.ts";
import type { ConsoleConfig } from "../config.ts";
import type { ConsoleDrizzle } from "../db/schema.ts";
import type { GatewayClient } from "../gateway-client.ts";
import type { ConsoleSyncEngine } from "../sync.ts";

export type ConsoleDeps = {
  config: ConsoleConfig;
  db: SQL;
  orm: ConsoleDrizzle;
  gateway: GatewayClient;
  sync: ConsoleSyncEngine;
  loginLimiter: LoginRateLimiter;
};

export type ConsoleEnv = {
  Variables: {
    user: AuthUser;
  };
};

export type ConsoleContext = Context<ConsoleEnv>;

export const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
export const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
export const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
export const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
export const operationEnum = z.enum(operations);
const uuidSchema = z.uuid();

export function parseUuid(value: string | undefined, label: string): string {
  const res = uuidSchema.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

export async function parseJsonBody<T>(c: Context, schema: z.ZodType<T>): Promise<T> {
  let raw: unknown;
  try {
    raw = await c.req.json();
  } catch {
    throw new GatewayError(400, "invalid_json", "Request body must be valid JSON");
  }
  const res = schema.safeParse(raw);
  if (!res.success) {
    const first = res.error.issues[0];
    throw new GatewayError(
      400,
      "validation_error",
      first ? `${first.path.join(".") || "body"}: ${first.message}` : "Validation failed",
    );
  }
  return res.data;
}

export function requireAuth(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"));
    c.set("user", user);
    await next();
  };
}

export function requirePlatformAdmin(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"));
    if (!user.isPlatformAdmin) {
      throw new GatewayError(403, "forbidden", "Platform administrator privileges required");
    }
    c.set("user", user);
    await next();
  };
}
