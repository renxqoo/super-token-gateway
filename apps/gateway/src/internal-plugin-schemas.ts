import { z } from "zod";
import {
  headerNameSchema,
  headerValueSchema,
  providerNameSchema,
  declarativeProviderConfigSchema,
  wasmProviderConfigSchema,
  adapterOverridesSchema,
} from "./internal-provider-schemas.ts";
import { pluginStepSchema } from "./internal-catalog-schemas.ts";
import { operationEnum } from "./internal-request.ts";

const webhookConfigSchema = z
  .object({
    url: z.url().max(1000),
    headers: z.record(headerNameSchema, headerValueSchema).optional(),
    timeout_ms: z.number().int().min(50).max(30_000).optional(),
    stream_mode: z.enum(["HOLD_UNTIL_APPROVED", "STREAM_AND_CUTOFF"]).optional(),
    window_chars: z.number().int().min(1).max(4096).optional(),
    window_ms: z.number().int().min(10).max(5_000).optional(),
    max_buffer_bytes: z.number().int().min(64).max(16384).optional(),
    max_concurrency: z.number().int().min(1).max(256).optional(),
    max_response_bytes: z
      .number()
      .int()
      .min(1024)
      .max(4 * 1024 * 1024)
      .optional(),
    failure_policy: z.enum(["FAIL_CLOSED", "FAIL_OPEN"]).optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
    circuit_failure_threshold: z.number().int().min(1).max(100).optional(),
    circuit_open_ms: z.number().int().min(1000).max(300_000).optional(),
  })
  .strict();

const pluginManifestSchema = z
  .object({
    hooks: z
      .array(z.enum(["on_request", "on_response", "on_stream_chunk", "on_stream_end"]))
      .max(4)
      .optional(),
    max_memory_bytes: z
      .number()
      .int()
      .min(65_536)
      .max(64 * 1024 * 1024)
      .optional(),
    fuel_budget: z.number().int().min(10_000).max(50_000_000).optional(),
    fuel_per_byte: z.number().int().min(0).max(5_000).optional(),
    max_input_bytes: z
      .number()
      .int()
      .min(1024)
      .max(4 * 1024 * 1024)
      .optional(),
    deterministic_text_only: z.boolean().optional(),
    window_bytes: z.number().int().min(0).max(4096).optional(),
    charge_policy: z.enum(["CHARGE_CUSTOMER", "PLATFORM_ABSORB"]).optional(),
    failure_policy: z.enum(["FAIL_CLOSED", "FAIL_OPEN"]).optional(),
  })
  .strict();

export const upsertPluginSchema = z
  .object({
    id: z.uuid().optional(),
    name: providerNameSchema,
    version: z.string().min(1).max(64),
    capability: z.enum(["CONTENT", "PROVIDER"]).default("CONTENT"),
    kind: z.enum(["WASM", "WEBHOOK", "DECLARATIVE"]),
    wasm_bytes_base64: z.string().min(4).optional(),
    webhook_config: webhookConfigSchema.nullable().optional(),
    provider_config: z
      .union([declarativeProviderConfigSchema, wasmProviderConfigSchema])
      .nullable()
      .optional(),
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: pluginManifestSchema.default({}),
  })
  .strict();

export const installBuiltinPluginSchema = z
  .object({
    signature: z.string().max(1024).nullable().optional(),
    signing_key_id: z.string().max(120).nullable().optional(),
    manifest: pluginManifestSchema.optional(),
    provider_config: declarativeProviderConfigSchema.partial().optional(),
  })
  .strict();

export const pluginReleaseSchema = z
  .object({
    id: z.uuid(),
    expected_epoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    targets: z
      .array(
        z.union([
          z.object({ channel_id: z.uuid(), provider_plugin_id: z.uuid() }).strict(),
          z.object({ plugin_id: z.uuid() }).strict(),
          z
            .object({ route_id: z.uuid(), plugin_pipeline: z.array(pluginStepSchema).max(20) })
            .strict(),
        ]),
      )
      .min(1)
      .max(50),
    samples: z
      .record(
        z.uuid(),
        z
          .object({
            request: z.record(z.string(), z.unknown()).optional(),
            response: z.record(z.string(), z.unknown()).optional(),
            event: z
              .object({ name: z.string().max(256), data: z.string().max(65_536) })
              .strict()
              .optional(),
            text: z.string().max(65_536).optional(),
            webhook_reply: z.record(z.string(), z.unknown()).optional(),
          })
          .strict(),
      )
      .default({}),
  })
  .strict();

export const rollbackPluginReleaseSchema = z
  .object({
    id: z.uuid(),
    expected_epoch: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    samples: pluginReleaseSchema.shape.samples,
  })
  .strict();

export const testPluginSchema = z
  .object({
    hook: z.enum(["on_request", "on_response", "on_stream_chunk", "on_stream_end"]),
    text: z.string().max(65_536),
    model: z.string().min(1).max(150).default("test-model"),
    operation: operationEnum.default("chat"),
  })
  .strict();

export const previewProviderSchema = z
  .object({
    provider: providerNameSchema,
    provider_plugin_id: z.uuid().nullable().optional(),
    base_url: z.url().max(1000),
    operation: operationEnum.default("chat"),
    upstream_model: z.string().min(1).max(200),
    stream: z.boolean().default(false),
    client_body: z.record(z.string(), z.unknown()),
    channel_adapter_config: adapterOverridesSchema.optional(),
    adapter_overrides: adapterOverridesSchema.optional(),
    mock_upstream_response: z
      .object({
        status: z.number().int().min(100).max(599).default(200),
        headers: z.record(z.string(), z.string()).optional(),
        body: z.record(z.string(), z.unknown()),
      })
      .strict()
      .optional(),
  })
  .strict();
