import type { Context } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { operations } from "@gateway/kernel";
import type { InternalPrincipal } from "./internal-auth.ts";

export const currencySchema = z.string().regex(/^[A-Z]{3,8}$/);

export const positiveAtomsSchema = z.string().regex(/^[1-9][0-9]{0,37}$/);

export const multiplierIntSchema = z.string().regex(/^[1-9][0-9]{0,12}$/);

export const nonNegativeAtomsSchema = z.string().regex(/^(0|[1-9][0-9]{0,37})$/);

export const signedAtomsSchema = z.string().regex(/^-?[1-9][0-9]{0,37}$/);

export const operationEnum = z.enum(operations);

const uuidParam = z.uuid();

export function parseBody<T>(schema: z.ZodType<T>, raw: unknown): T {
  const res = schema.safeParse(raw);
  if (!res.success) {
    const first = res.error.issues[0];
    throw new GatewayError(
      400,
      "validation_error",
      first ? `${first.path.join(".") || "body"}: ${first.message}` : "Invalid request payload",
    );
  }
  return res.data;
}

export function parseUuid(value: string | undefined, label: string): string {
  const res = uuidParam.safeParse(value);
  if (!res.success) throw new GatewayError(400, "validation_error", `Invalid UUID for ${label}`);
  return res.data;
}

export type AdminEnv = { Variables: { principal: InternalPrincipal } };

export type AdminContext = Context<AdminEnv>;
