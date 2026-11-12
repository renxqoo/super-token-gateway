import { z } from "zod";
const scalar = z.discriminatedUnion("type", [
  z
    .object({
      type: z.literal("string"),
      maxLength: z.number().int().min(1).max(2048),
      enum: z.array(z.string().max(200)).min(1).max(100).optional(),
    })
    .strict(),
  z
    .object({
      type: z.literal("integer"),
      minimum: z.number().int().min(0),
      maximum: z.number().int().max(Number.MAX_SAFE_INTEGER),
    })
    .strict(),
  z.object({ type: z.literal("boolean") }).strict(),
]);
const map = z
  .object({
    type: z.literal("object"),
    maxProperties: z.number().int().min(1).max(1000),
    additionalProperties: scalar,
  })
  .strict();
export const configSchemaSchema = z
  .object({
    type: z.literal("object"),
    additionalProperties: z.literal(false),
    required: z.array(z.string().regex(/^[a-z][a-z0-9_]{0,63}$/)).max(32),
    properties: z.record(z.string().regex(/^[a-z][a-z0-9_]{0,63}$/), z.union([scalar, map])),
  })
  .strict();
export type ConfigSchema = z.infer<typeof configSchemaSchema>;
export function validateConfig(schema: ConfigSchema, value: unknown): Record<string, unknown> {
  const source = z.record(z.string(), z.unknown()).parse(value);
  for (const key of schema.required)
    if (!Object.hasOwn(source, key)) throw new Error("CONFIG_REQUIRED");
  function validate(rule: z.infer<typeof scalar> | z.infer<typeof map>, v: unknown): void {
    if (rule.type === "string") {
      if (
        typeof v !== "string" ||
        v.length > rule.maxLength ||
        (rule.enum && !rule.enum.includes(v))
      )
        throw new Error("CONFIG_VALUE");
    } else if (rule.type === "integer") {
      if (!Number.isSafeInteger(v) || (v as number) < rule.minimum || (v as number) > rule.maximum)
        throw new Error("CONFIG_VALUE");
    } else if (rule.type === "boolean") {
      if (typeof v !== "boolean") throw new Error("CONFIG_VALUE");
    } else {
      const entries = Object.entries(z.record(z.string().min(1).max(200), z.unknown()).parse(v));
      if (entries.length > rule.maxProperties) throw new Error("CONFIG_VALUE");
      for (const [, child] of entries) validate(rule.additionalProperties, child);
    }
  }
  if (
    Object.keys(schema.properties).length > 32 ||
    schema.required.some((key) => !Object.hasOwn(schema.properties, key))
  )
    throw new Error("CONFIG_SCHEMA");
  for (const [key, child] of Object.entries(source)) {
    const rule = schema.properties[key];
    if (
      !rule ||
      /^(?:secret|credential|password|authorization|api_key|access_token|session_token|refresh_token)(?:_|$)/i.test(
        key,
      )
    )
      throw new Error("CONFIG_FIELD");
    validate(rule, child);
  }
  return JSON.parse(JSON.stringify(source)) as Record<string, unknown>;
}
