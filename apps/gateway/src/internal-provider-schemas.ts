import { z } from "zod";
import { operations } from "@gateway/kernel";
import { assertSafeEndpointTemplate, assertSafeFieldPath } from "@gateway/plugin-engine";

export const providerNameSchema = z.string().regex(/^[a-z0-9][a-z0-9_-]{1,63}$/);

export const headerNameSchema = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/);

export const headerValueSchema = z
  .string()
  .max(2048)
  .regex(/^[\t\x20-\x7e]*$/, "Header value must contain only printable ASCII characters");

const safeFieldPathSchema = z
  .string()
  .min(1)
  .max(128)
  .superRefine((val, ctx) => {
    try {
      assertSafeFieldPath(val);
    } catch (err) {
      ctx.addIssue({
        code: "custom",
        message: err instanceof Error ? err.message : "Invalid field path",
      });
    }
  });

const safeEndpointPathSchema = z
  .string()
  .min(1)
  .max(512)
  .superRefine((val, ctx) => {
    try {
      assertSafeEndpointTemplate(val);
    } catch (err) {
      ctx.addIssue({
        code: "custom",
        message: err instanceof Error ? err.message : "Invalid endpoint path template",
      });
    }
  });

const safeRegexSchema = z
  .string()
  .min(1)
  .max(128)
  .superRefine((val, ctx) => {
    if (/\([^)]*[+*][^)]*\)[+*?]/.test(val)) {
      ctx.addIssue({ code: "custom", message: "Nested quantifiers are not allowed in regex" });
      return;
    }
    try {
      new RegExp(val);
    } catch {
      ctx.addIssue({ code: "custom", message: "Invalid regular expression" });
    }
  });

const authSchemeSchema = z.discriminatedUnion("scheme", [
  z.object({ scheme: z.literal("none") }).strict(),
  z.object({ scheme: z.literal("bearer") }).strict(),
  z
    .object({
      scheme: z.literal("header"),
      header_name: headerNameSchema,
      prefix: headerValueSchema.optional(),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("query"),
      param_name: z.string().regex(/^[A-Za-z0-9_.-]{1,64}$/),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("azure_key"),
      default_api_version: z
        .string()
        .regex(/^\d{4}-\d{2}-\d{2}(?:-preview)?$/)
        .optional(),
    })
    .strict(),
  z
    .object({
      scheme: z.literal("aws_sigv4"),
      service: z.string().regex(/^[a-z0-9-]{1,64}$/),
    })
    .strict(),
  z.object({ scheme: z.literal("gcp_vertex_oauth") }).strict(),
  z
    .object({
      scheme: z.literal("hmac_sha256"),
      signature_header: headerNameSchema,
      timestamp_header: headerNameSchema,
      key_id_header: headerNameSchema.optional(),
      signature_prefix: headerValueSchema.optional(),
      encoding: z.enum(["hex", "base64"]).optional(),
    })
    .strict(),
]);

const bodyTransformRuleSchema = z.discriminatedUnion("op", [
  z
    .object({
      op: z.literal("rename"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z
    .object({
      op: z.literal("move_to"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z.object({ op: z.literal("drop"), path: safeFieldPathSchema }).strict(),
  z
    .object({ op: z.literal("set_default"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
  z.object({ op: z.literal("force_set"), path: safeFieldPathSchema, value: z.unknown() }).strict(),
  z
    .object({
      op: z.literal("clamp"),
      path: safeFieldPathSchema,
      min: z.number().finite().optional(),
      max: z.number().finite().optional(),
    })
    .strict(),
  z
    .object({ op: z.literal("delete_if_equals"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
]);

const responseTransformRuleSchema = z.discriminatedUnion("op", [
  z
    .object({
      op: z.literal("rename"),
      from: safeFieldPathSchema,
      to: safeFieldPathSchema,
      overwrite: z.boolean().optional(),
    })
    .strict(),
  z.object({ op: z.literal("drop"), path: safeFieldPathSchema }).strict(),
  z
    .object({ op: z.literal("set_default"), path: safeFieldPathSchema, value: z.unknown() })
    .strict(),
]);

const errorMatchRuleSchema = z
  .object({
    when_http_status: z.array(z.number().int().min(100).max(599)).min(1).max(20).optional(),
    path: safeFieldPathSchema,
    equals: z.union([z.string(), z.number(), z.boolean()]).optional(),
    not_equals: z.union([z.string(), z.number(), z.boolean()]).optional(),
    in: z
      .array(z.union([z.string(), z.number()]))
      .min(1)
      .max(64)
      .optional(),
    regex: safeRegexSchema.optional(),
    retry_kind: z.enum(["retryable", "rate_limit", "auth_error", "client_error"]),
    execution: z.literal("NOT_EXECUTED").optional(),
    http_status: z.number().int().min(400).max(599).optional(),
    message_path: safeFieldPathSchema.optional(),
    retry_after_path: safeFieldPathSchema.optional(),
  })
  .strict()
  .refine(
    (r) =>
      r.execution === undefined ||
      (r.regex === undefined &&
        r.not_equals === undefined &&
        (r.equals !== undefined || r.in !== undefined)),
    { message: "An execution assertion requires exact provider error code matching" },
  )
  .refine(
    (r) =>
      r.equals !== undefined ||
      r.not_equals !== undefined ||
      r.in !== undefined ||
      r.regex !== undefined,
    {
      message: "error_rules entry must specify equals, not_equals, in, or regex",
    },
  );

const usageAliasRuleSchema = z
  .object({
    target: z.enum([
      "input_tokens",
      "output_tokens",
      "cached_input_tokens",
      "cache_write_tokens",
      "cache_write_5m_tokens",
      "cache_write_1h_tokens",
      "reasoning_tokens",
      "requests",
      "images",
      "audio_ms",
      "audio_seconds",
      "video_ms",
      "video_seconds",
      "music_ms",
      "characters",
      "web_search_calls",
      "code_interpreter_sessions",
      "file_search_calls",
      "search_units",
      "pages",
      "image_input_tokens",
      "image_output_tokens",
      "audio_input_tokens",
      "audio_output_tokens",
      "video_input_tokens",
    ]),
    source_path: safeFieldPathSchema,
  })
  .strict();

const endpointTemplateSchema = z
  .object({
    path: safeEndpointPathSchema,
    stream_path: safeEndpointPathSchema.optional(),
    query: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
    stream_query: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
  })
  .strict();

const mediaStreamTerminalSchema = z.union([
  z
    .object({
      event: z
        .string()
        .min(1)
        .max(128)
        .regex(/^[^\r\n\0]+$/),
    })
    .strict(),
  z
    .object({
      path: safeFieldPathSchema,
      equals: z.union([z.string().max(128), z.number().int(), z.boolean()]),
    })
    .strict(),
]);

const providerErrorDiagnosticsSchema = z
  .object({
    code_path: safeFieldPathSchema.optional(),
    code_header: z
      .string()
      .max(64)
      .regex(/^(?:[a-z0-9]+-)*(?:error-code|errorcode|error-type|errortype)$/i)
      .optional(),
    request_id_header: z
      .string()
      .max(64)
      .regex(/^(?:[a-z0-9]+-)*(?:request-id|requestid|correlation-id|trace-id)$/i)
      .optional(),
    summaries: z
      .record(
        z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/),
        z
          .string()
          .min(1)
          .max(160)
          .regex(/^[^\p{Cc}\p{Cf}]+$/u),
      )
      .refine((values) => Object.keys(values).length <= 64)
      .optional(),
  })
  .strict()
  .refine((value) => value.code_path || value.code_header || value.request_id_header);

export const adapterOverridesSchema = z
  .object({
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    query_params: z.record(z.string().min(1).max(64), z.string().max(256)).optional(),
    path_override: safeEndpointPathSchema.optional(),
    request_transforms: z.array(bodyTransformRuleSchema).max(64).optional(),
    response_transforms: z.array(responseTransformRuleSchema).max(64).optional(),
    error_rules: z.array(errorMatchRuleSchema).max(64).optional(),
    error_diagnostics: providerErrorDiagnosticsSchema.optional(),
    usage_aliases: z.array(usageAliasRuleSchema).max(32).optional(),
    media_stream_terminal: mediaStreamTerminalSchema.optional(),
    reconciliation_evidence_required: z.boolean().optional(),
    reconciliation_response_id_header: z
      .string()
      .max(64)
      .regex(/^(?:[A-Za-z0-9-]*-)?(?:request-id|requestid|correlation-id|trace-id)$/i)
      .optional(),
  })
  .strict();

const wireDialectSchema = z.enum([
  "openai_chat",
  "openai_responses",
  "anthropic_messages",
  "gemini",
  "passthrough",
]);

const usageDialectSchema = z.enum(["openai", "anthropic", "gemini", "custom"]);

const streamFramingSchema = z.enum(["sse", "bedrock_eventstream"]);

const asyncTaskProviderConfigSchema = z
  .object({
    query_path: safeEndpointPathSchema,
    query_param: z
      .string()
      .regex(/^[A-Za-z0-9_.-]{1,64}$/)
      .optional(),
    task_id_path: safeFieldPathSchema.optional(),
    status_path: safeFieldPathSchema.optional(),
    succeeded_values: z.array(z.string().min(1).max(64)).min(1).max(32).optional(),
    failed_values: z.array(z.string().min(1).max(64)).min(1).max(32).optional(),
    error_code_path: safeFieldPathSchema.optional(),
    error_message_path: safeFieldPathSchema.optional(),
    refundable_system_error_codes: z
      .array(z.union([z.string(), z.number()]))
      .max(64)
      .optional(),
    non_refundable_error_codes: z
      .array(z.union([z.string(), z.number()]))
      .max(64)
      .optional(),
    billed_indicator_path: safeFieldPathSchema.optional(),
  })
  .strict();

const endpointKeySet = new Set<string>([...operations, "default"]);

export const declarativeProviderConfigSchema = z
  .object({
    base_dialect: wireDialectSchema,
    usage_dialect: usageDialectSchema,
    stream_framing: streamFramingSchema.optional(),
    auth: authSchemeSchema,
    endpoints: z
      .record(z.string(), endpointTemplateSchema)
      .refine((rec) => Object.keys(rec).length > 0, {
        message: "endpoints must define at least one operation or default",
      })
      .refine((rec) => Object.keys(rec).every((k) => endpointKeySet.has(k)), {
        message: "endpoints contains unsupported operation key",
      }),
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    model_field: safeFieldPathSchema.nullable().optional(),
    stream_field: safeFieldPathSchema.nullable().optional(),
    include_stream_usage_option: z.boolean().optional(),
    request_transforms: z.array(bodyTransformRuleSchema).max(64).optional(),
    response_transforms: z.array(responseTransformRuleSchema).max(64).optional(),
    error_rules: z.array(errorMatchRuleSchema).max(64).optional(),
    error_diagnostics: providerErrorDiagnosticsSchema.optional(),
    usage_aliases: z.array(usageAliasRuleSchema).max(32).optional(),
    media_stream_terminal: mediaStreamTerminalSchema.optional(),
    reasoning_field: safeFieldPathSchema.optional(),
    async_task_config: asyncTaskProviderConfigSchema.optional(),
  })
  .strict();

export const wasmProviderConfigSchema = z
  .object({
    base_dialect: wireDialectSchema,
    usage_dialect: usageDialectSchema,
    stream_framing: streamFramingSchema.optional(),
    auth: authSchemeSchema,
    extra_headers: z.record(headerNameSchema, headerValueSchema).optional(),
    error_diagnostics: providerErrorDiagnosticsSchema.optional(),
    hooks: z
      .array(z.enum(["adapt_request", "adapt_response", "adapt_stream_event"]))
      .min(1)
      .max(3)
      .refine((hooks) => hooks.includes("adapt_request"), {
        message: "WASM provider hooks must include adapt_request",
      }),
    grants: z.array(z.literal("extract_usage")).max(4).optional(),
    fuel_budget: z.number().int().min(10_000).max(50_000_000).optional(),
    max_memory_bytes: z
      .number()
      .int()
      .min(65_536)
      .max(64 * 1024 * 1024)
      .optional(),
    failure_policy: z.literal("FAIL_CLOSED").optional(),
  })
  .strict();
