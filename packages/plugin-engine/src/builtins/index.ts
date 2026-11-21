import type {
  DeclarativeProviderConfig,
  PluginCapability,
  WasmPluginManifest,
} from "@gateway/ledger";
import { getBuiltinProvider, listBuiltinProviders } from "../providers/builtins.ts";
import {
  PII_GUARD_WASM_BASE64,
  PII_GUARD_WASM_BYTES_LENGTH,
  PII_GUARD_WASM_DIGEST,
} from "./pii-guard-data.ts";

export type BuiltinPluginDefinition =
  | {
      name: string;
      version: string;
      capability: "CONTENT";
      kind: "WASM";
      description: string;
      digest: string;
      size_bytes: number;
      manifest: WasmPluginManifest;
      wasm_bytes: Uint8Array;
    }
  | {
      name: string;
      version: string;
      capability: "PROVIDER";
      kind: "DECLARATIVE";
      description: string;
      digest: string;
      provider_config: DeclarativeProviderConfig;
      manifest: Record<string, unknown>;
    };

export type BuiltinPluginSummary =
  | Omit<Extract<BuiltinPluginDefinition, { capability: "CONTENT" }>, "wasm_bytes">
  | Extract<BuiltinPluginDefinition, { capability: "PROVIDER" }>;

let cachedPiiGuardBytes: Uint8Array | null = null;

function getPiiGuardBytes(): Uint8Array {
  if (!cachedPiiGuardBytes) {
    cachedPiiGuardBytes = new Uint8Array(Buffer.from(PII_GUARD_WASM_BASE64, "base64"));
  }
  return new Uint8Array(cachedPiiGuardBytes);
}

const PII_GUARD_MANIFEST: WasmPluginManifest = {
  hooks: ["on_request", "on_response", "on_stream_chunk", "on_stream_end"],
  window_bytes: 64,
  fuel_budget: 5_000_000,
  max_memory_bytes: 4 * 1024 * 1024,
  failure_policy: "FAIL_CLOSED",
  charge_policy: "CHARGE_CUSTOMER",
};

const CONTENT_BUILTINS: readonly Extract<BuiltinPluginSummary, { capability: "CONTENT" }>[] = [
  {
    name: "pii-guard",
    version: "1.0.0",
    capability: "CONTENT",
    kind: "WASM",
    description:
      "Built-in DLP & PII guard: blocks PEM private keys and prompt injection; redacts API keys, AWS/GitHub tokens, emails, Chinese ID cards (GB 11643-1999), Chinese mobile phones, US SSNs, and Luhn-valid payment cards across unary and streaming SSE.",
    digest: PII_GUARD_WASM_DIGEST,
    size_bytes: PII_GUARD_WASM_BYTES_LENGTH,
    manifest: PII_GUARD_MANIFEST,
  },
];

export function listBuiltinPlugins(capability?: PluginCapability): BuiltinPluginSummary[] {
  const out: BuiltinPluginSummary[] = [];
  if (!capability || capability === "CONTENT") {
    for (const item of CONTENT_BUILTINS) {
      out.push({
        ...item,
        manifest: { ...item.manifest, hooks: [...item.manifest.hooks] },
      });
    }
  }
  if (!capability || capability === "PROVIDER") {
    for (const prov of listBuiltinProviders()) {
      out.push({
        name: prov.name,
        version: prov.version,
        capability: "PROVIDER",
        kind: "DECLARATIVE",
        description: prov.description,
        digest: prov.digest,
        provider_config: prov.provider_config,
        manifest: {},
      });
    }
  }
  return out;
}

export function getBuiltinPlugin(name: string): BuiltinPluginDefinition | null {
  if (name === "pii-guard") {
    const meta = CONTENT_BUILTINS[0]!;
    return {
      ...meta,
      manifest: { ...meta.manifest, hooks: [...meta.manifest.hooks] },
      wasm_bytes: getPiiGuardBytes(),
    };
  }
  const prov = getBuiltinProvider(name);
  if (prov) {
    return {
      name: prov.name,
      version: prov.version,
      capability: "PROVIDER",
      kind: "DECLARATIVE",
      description: prov.description,
      digest: prov.digest,
      provider_config: prov.provider_config,
      manifest: {},
    };
  }
  return null;
}
