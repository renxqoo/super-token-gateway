import type { SQL } from "bun";
import type { Context, MiddlewareHandler } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { operations } from "@gateway/kernel";
import {
  authenticateSession,
  normalizeAndValidateCidr,
  resolveClientIp,
  type AuthUser,
  type LoginRateLimiter,
} from "../auth.ts";
import type { ConsoleConfig } from "../config.ts";
import type { apiKeyRecords, ConsoleDrizzle, projects } from "../db/schema.ts";
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
    peerIp?: string;
  };
};

export type ConsoleContext = Context<ConsoleEnv>;

export const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);
export const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);
export const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);
export const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);
export const operationEnum = z.enum(operations);
const uuidSchema = z.uuid();

export const cidrSchema = z
  .string()
  .trim()
  .min(2)
  .max(64)
  .transform((val, ctx) => {
    const normalized = normalizeAndValidateCidr(val);
    if (!normalized) {
      ctx.addIssue({ code: "custom", message: "Invalid IP or CIDR notation" });
      return z.NEVER;
    }
    return normalized;
  });

export function extractPgErrno(err: unknown): string | null {
  let cur: unknown = err;
  for (let depth = 0; depth < 6 && cur && typeof cur === "object"; depth++) {
    if ("errno" in cur && typeof cur.errno === "string" && /^[0-9A-Z]{5}$/.test(cur.errno)) {
      return cur.errno;
    }
    if ("code" in cur && typeof cur.code === "string" && /^[0-9A-Z]{5}$/.test(cur.code)) {
      return cur.code;
    }
    cur = "cause" in cur ? cur.cause : null;
  }
  return null;
}

export function parseUuid(value: string | undefined, label: string): string {
  const res = uuidSchema.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

export function parseLimitOffset(
  c: Context,
  defaultLimit = 50,
  maxLimit = 200,
): { limit: number; offset: number } {
  const rawLimit = c.req.query("limit");
  const rawOffset = c.req.query("offset");
  let limit = defaultLimit;
  let offset = 0;
  if (rawLimit !== undefined && rawLimit !== "") {
    if (!/^\d+$/.test(rawLimit)) {
      throw new GatewayError(400, "validation_error", "limit must be a positive integer");
    }
    const parsed = Number(rawLimit);
    if (!Number.isSafeInteger(parsed) || parsed < 1) {
      throw new GatewayError(400, "validation_error", "limit must be a positive integer");
    }
    limit = Math.min(maxLimit, parsed);
  }
  if (rawOffset !== undefined && rawOffset !== "") {
    if (!/^\d+$/.test(rawOffset)) {
      throw new GatewayError(400, "validation_error", "offset must be a non-negative integer");
    }
    const parsed = Number(rawOffset);
    if (!Number.isSafeInteger(parsed) || parsed < 0) {
      throw new GatewayError(400, "validation_error", "offset must be a non-negative integer");
    }
    offset = parsed;
  }
  return { limit, offset };
}

export function parseIsoDateParam(raw: string | undefined, label: string): string | undefined {
  if (raw === undefined || raw === "") return undefined;
  const trimmed = raw.trim();
  const datePart = /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
    ? trimmed
    : /^\d{4}-\d{2}-\d{2}T/.test(trimmed) && !Number.isNaN(Date.parse(trimmed))
      ? new Date(trimmed).toISOString().slice(0, 10)
      : null;
  if (!datePart) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid YYYY-MM-DD date`);
  }
  const parsed = new Date(`${datePart}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== datePart) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid calendar date`);
  }
  return datePart;
}

export function parseIsoDatetimeParam(raw: string | undefined, label: string): Date | undefined {
  if (raw === undefined || raw === "") return undefined;
  const trimmed = raw.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    const parsed = new Date(`${trimmed}T00:00:00.000Z`);
    if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== trimmed) {
      throw new GatewayError(400, "validation_error", `${label} must be a valid date`);
    }
    return parsed;
  }
  const res = z.iso.datetime({ offset: true }).safeParse(trimmed);
  if (!res.success) {
    throw new GatewayError(400, "validation_error", `${label} must be a valid ISO-8601 datetime`);
  }
  return new Date(res.data);
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

export async function parseOptionalJsonObject(c: Context): Promise<Record<string, unknown>> {
  const text = await c.req.text();
  if (!text.trim()) return {};
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    throw new GatewayError(400, "invalid_json", "Request body must be valid JSON");
  }
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new GatewayError(400, "validation_error", "Request body must be a JSON object");
  }
  return raw as Record<string, unknown>;
}

export function getClientIp(deps: ConsoleDeps, c: ConsoleContext): string | null {
  return resolveClientIp(
    c.get("peerIp"),
    c.req.header("x-forwarded-for"),
    deps.config.trustedProxyCidrs,
  );
}

export function requireClientAuth(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"), "CLIENT");
    c.set("user", user);
    await next();
  };
}

export function requirePlatformAdmin(deps: ConsoleDeps): MiddlewareHandler<ConsoleEnv> {
  return async (c, next) => {
    const user = await authenticateSession(deps.orm, c.req.header("authorization"), "ADMIN");
    c.set("user", user);
    await next();
  };
}

export function sanitizeProjectForClient(row: typeof projects.$inferSelect) {
  return {
    id: row.id,
    orgId: row.orgId,
    name: row.name,
    currency: row.currency,
    rpm: row.rpm,
    tpm: row.tpm,
    concurrency: row.concurrency,
    developerKeySpendLimitAtoms: row.developerKeySpendLimitAtoms,
    developerMaxActiveKeys: row.developerMaxActiveKeys,
    enabled: row.enabled,
    gatewaySynced: row.gatewaySynced,
    syncStatus: row.syncStatus,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

export function sanitizeApiKeyForClient(row: typeof apiKeyRecords.$inferSelect) {
  return {
    id: row.id,
    projectId: row.projectId,
    createdBy: row.createdBy,
    rotatedFromKeyId: row.rotatedFromKeyId,
    name: row.name,
    keyPrefix: row.keyPrefix,
    role: row.role,
    allowedCidrs: row.allowedCidrs,
    models: row.models,
    operations: row.operations,
    spendLimitAtoms: row.spendLimitAtoms,
    spendLimitWindow: row.spendLimitWindow,
    enabled: row.enabled,
    expiresAt: row.expiresAt ? row.expiresAt.toISOString() : null,
    revokedAt: row.revokedAt ? row.revokedAt.toISOString() : null,
    syncStatus: row.syncStatus,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}
