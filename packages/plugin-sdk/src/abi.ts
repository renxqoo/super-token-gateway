import { z } from "zod";
import { dimensions, operations, protocols } from "@gateway/kernel";
import { digestSchema, usageSchema } from "@gateway/contracts";

export const hooks = [
  "describe",
  "prepare",
  "response",
  "error",
  "stream_open",
  "stream_feed",
  "stream_drain",
  "stream_finish",
  "stream_dispose",
  "resource_prepare",
  "resource_response",
  "balance_prepare",
  "balance_parse",
] as const;
export type Hook = (typeof hooks)[number];
export const adapterFailureCodes = [
  "invalid_json",
  "json_limit",
  "invalid_object",
  "numeric_limit",
  "invalid_numeric_literal",
  "invalid_upstream_usage",
  "integer_limit",
  "invalid_base64",
  "abi_invalid",
  "usage_alias_conflict",
  "usage_regressed",
  "invalid_finish_reason",
  "incomplete_tool_call",
  "unsupported_semantics",
  "invalid_request",
  "inspection_missing",
  "scalar_budget",
  "invalid_messages",
  "model_unavailable",
  "unsupported_choice_count",
  "invalid_stream",
  "input_ceiling",
  "conflicting_output_ceiling",
  "invalid_output_ceiling",
  "usage_required",
  "invalid_stream_options",
  "upstream_error",
  "invalid_choices",
  "empty_response",
  "invalid_utf8",
  "stream_frame_budget",
  "output_budget",
  "data_after_terminal",
  "premature_terminal",
  "data_after_finish",
  "invalid_content",
  "invalid_tool_call",
  "tool_budget",
  "duplicate_finish",
  "terminal_budget",
  "unsupported_capability",
  "invalid_model",
  "stream_state",
  "stream_truncated",
  "batch_input_budget",
  "drain_input",
] as const;
export type AdapterFailureCode = (typeof adapterFailureCodes)[number];
export const base64Schema = z
  .string()
  .regex(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/);
const identity = z.string().min(1).max(200);
export const sessionSchema = z
  .object({
    id: z.uuid(),
    tenant: identity,
    attempt: identity,
    incarnation: z.uuid(),
    plugin: digestSchema,
    config: digestSchema,
    sequence: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
  })
  .strict();
export type SessionIdentity = z.infer<typeof sessionSchema>;
export const inputSchema = z
  .object({
    abi_version: z.literal(1),
    hook: z.enum(hooks),
    session: sessionSchema,
    config: z.record(z.string(), z.unknown()),
    payload: base64Schema,
    output_budget: z
      .number()
      .int()
      .min(1)
      .max(16 << 20),
  })
  .strict();
export type AbiInput = z.infer<typeof inputSchema>;
export type JSONDocumentDescriptor = { handle: string; bytes: number };
export type JSONNodeFact = {
  kind: "object" | "array" | "string" | "number" | "boolean" | "null" | "missing";
  bytes: number;
  count?: number;
  empty?: boolean;
  literal?: string;
  string_info?: { utf16_units: number; digest: string };
};
export type JSONDocumentPayload = {
  document: JSONDocumentDescriptor;
  inspection: Record<string, JSONNodeFact>;
};
export const jsonMetadataSchema = z
  .object({
    protocol: z.enum(protocols).optional(),
    operation: z.enum(operations).optional(),
    model: identity,
  })
  .strict();
export const capabilitiesSchema = z
  .object({
    operations: z.array(z.enum(operations)).min(1).max(32),
    dimensions: z.array(z.enum([...dimensions, "reasoning_tokens"])).max(32),
    protocols: z
      .array(z.enum(["chat", "responses", "messages", "gemini"]))
      .min(1)
      .max(4),
    hooks: z.array(z.enum(hooks)).min(1).max(32),
    auth: z
      .array(z.enum(["none", "bearer", "api-key"]))
      .min(1)
      .max(3),
    resources: z.boolean(),
    balance: z.boolean(),
    stream_codec: z.enum(["sse", "none"]),
  })
  .strict();
export type Capabilities = z.infer<typeof capabilitiesSchema>;
const headerSchema = z.record(
  z.string().regex(/^(?:content-type|accept|openai-beta|anthropic-version|x-[a-z0-9-]{1,60})$/),
  z
    .string()
    .max(1024)
    .refine((s) => !/[\r\n\0]/.test(s)),
);
export const jsonPlanSchema = z
  .object({
    document: z.uuid(),
    edits: z
      .array(
        z.discriminatedUnion("op", [
          z
            .object({
              op: z.literal("set"),
              pointer: z.string().max(2048),
              value: z.string().max(4096),
            })
            .strict(),
          z.object({ op: z.literal("remove"), pointer: z.string().max(2048) }).strict(),
        ]),
      )
      .max(32),
  })
  .strict();
export type JSONBodyPlan = z.infer<typeof jsonPlanSchema>;
export const framePlanSchema = z
  .object({
    handle: z.uuid(),
    disposition: z.enum(["emit", "hold"]),
    body: jsonPlanSchema.optional(),
  })
  .strict();
export const preparedSchema = z
  .object({
    method: z.enum(["GET", "POST", "DELETE"]),
    path: z
      .string()
      .min(1)
      .max(2048)
      .refine(
        (s) =>
          /^\/(?!\/)[A-Za-z0-9_./?=&%:-]*$/.test(s) &&
          !/%(?:2f|5c|2e)/i.test(s) &&
          !s.split(/[/?]/).some((x) => x === "." || x === "..") &&
          !s.includes("://"),
      ),
    headers: headerSchema,
    body: jsonPlanSchema,
    auth: z.discriminatedUnion("type", [
      z.object({ type: z.literal("none") }).strict(),
      z.object({ type: z.literal("bearer") }).strict(),
      z.object({ type: z.literal("api-key"), header: z.enum(["api-key", "x-api-key"]) }).strict(),
    ]),
    manifest: z
      .object({
        operation: z.enum(operations),
        model: identity,
        upstream_model: identity,
        stream: z.boolean(),
        ceilings: usageSchema,
        input_bytes: z
          .number()
          .int()
          .min(0)
          .max(16 << 20),
      })
      .strict(),
  })
  .strict();
export type PreparedRequest = z.infer<typeof preparedSchema>;
export const terminalSchema = z
  .object({ kind: z.enum(["complete", "incomplete"]), bytes: base64Schema })
  .strict();
export const resultSchema = z.discriminatedUnion("kind", [
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("buffer_append"),
      target: z.uuid().optional(),
      document: z.uuid(),
      pointers: z.array(z.string().max(2048)).min(1).max(16),
    })
    .strict(),
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("buffer_check"),
      handles: z.array(z.uuid()).min(1).max(16),
      seal: z.boolean(),
    })
    .strict(),
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("inspect"),
      document: z.uuid(),
      pointers: z.array(z.string().max(2048)).min(1).max(16),
    })
    .strict(),
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("description"),
      sdk_version: z.literal("1.0.0"),
      capabilities: capabilitiesSchema,
    })
    .strict(),
  z
    .object({ abi_version: z.literal(1), kind: z.literal("prepared"), request: preparedSchema })
    .strict(),
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("output"),
      consumed: z
        .number()
        .int()
        .min(0)
        .max(16 << 20),
      chunks: z.array(base64Schema).max(128),
      body: jsonPlanSchema.optional(),
      frame: framePlanSchema.optional(),
      usage: usageSchema.optional(),
      finality: z.enum(["FINAL", "PARTIAL", "UNKNOWN", "ESTIMATED"]),
      terminal: terminalSchema.optional(),
      needs_input: z.boolean(),
    })
    .strict(),
  z.object({ abi_version: z.literal(1), kind: z.literal("disposed") }).strict(),
  z
    .object({
      abi_version: z.literal(1),
      kind: z.literal("failure"),
      status: z.union([z.literal(400), z.literal(413), z.literal(422), z.literal(502)]),
      code: z.enum(adapterFailureCodes),
      usage: usageSchema.optional(),
      finality: z.enum(["FINAL", "PARTIAL", "UNKNOWN", "ESTIMATED"]).optional(),
    })
    .strict(),
]);
export type AbiResult = z.infer<typeof resultSchema>;
export type OutputResult = Extract<AbiResult, { kind: "output" }>;
export function sha256(bytes: Uint8Array | string): string {
  return new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
}
