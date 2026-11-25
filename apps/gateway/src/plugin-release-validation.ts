import { GatewayError } from "@gateway/contracts";
import type {
  ChannelRecord,
  ConfigSnapshot,
  PluginRecord,
  PluginReleaseChange,
  PluginReleaseTarget,
  RouteRecord,
  WasmPluginManifest,
} from "@gateway/ledger";
import {
  invokeWasmProviderStreamEvent,
  resolveProviderPlugin,
  verifyPluginIntegrity,
  type WasmRuntimeExecutor,
} from "@gateway/plugin-engine";
import { validateUsageObject } from "@gateway/protocol";
import { validateAuthCredentials } from "@gateway/runtime";
import type { Operation } from "@gateway/kernel";
import {
  isConversationalOperation,
  previewProviderRequest,
  upstreamProtocolFor,
} from "./upstream.ts";

type PluginReleaseSample = {
  request?: Record<string, unknown> | undefined;
  response?: Record<string, unknown> | undefined;
  event?: { name: string; data: string } | undefined;
  text?: string | undefined;
  webhook_reply?: Record<string, unknown> | undefined;
};

export async function validatePluginRelease(options: {
  snapshot: ConfigSnapshot;
  targets: readonly PluginReleaseTarget[];
  getPlugin: (id: string) => Promise<PluginRecord | null>;
  decrypt: (channel: ChannelRecord) => string;
  executor: WasmRuntimeExecutor | null;
  signingPublicKeyPem: string | undefined;
  samples: Record<string, PluginReleaseSample>;
}): Promise<PluginReleaseChange[]> {
  const { snapshot, targets, executor, signingPublicKeyPem } = options;
  const routes = [...snapshot.routesByModel.values()].flat();
  const routesById = new Map(routes.map((route) => [route.id, route]));
  const revisions = new Map(snapshot.plugins);
  const drafts = new Map<string, PluginRecord>();
  const covered = new Set<string>();
  const selectedTargets = new Set<string>();
  const invalid = (message: string) => new GatewayError(422, "plugin_preflight_failed", message);

  async function getRevision(
    id: string,
    capability: PluginRecord["capability"],
  ): Promise<PluginRecord> {
    const plugin = snapshot.plugins.get(id) ?? drafts.get(id) ?? (await options.getPlugin(id));
    if (!plugin || plugin.capability !== capability || snapshot.revokedPlugins.has(id))
      throw invalid("Plugin revision is missing or incompatible");
    if (plugin.kind === "WASM") {
      const check = verifyPluginIntegrity(plugin, signingPublicKeyPem);
      if (!check.ok) {
        throw invalid(
          check.reason === "digest_mismatch"
            ? "Plugin digest does not match binary"
            : check.reason === "missing_bytes"
              ? "Plugin binary is missing"
              : "Plugin signature is invalid",
        );
      }
    }
    if (!plugin.enabled) drafts.set(id, plugin);
    revisions.set(id, { ...plugin, enabled: true });
    return plugin;
  }

  async function validateProvider(
    channel: ChannelRecord | null,
    revisionId: string,
  ): Promise<void> {
    const plugin = await getRevision(revisionId, "PROVIDER");
    const resolved = resolveProviderPlugin(channel?.kind ?? plugin.name, revisionId, {
      ...snapshot,
      plugins: revisions,
    });
    if (!resolved) throw invalid("Provider configuration is unavailable");
    if (channel) validateAuthCredentials(resolved.config.auth, options.decrypt(channel));
    const consumers = channel ? routes.filter((route) => route.channel_id === channel.id) : [];
    const sample = options.samples[revisionId];
    const bindingOptions = consumers.length ? consumers : [null];
    for (const route of bindingOptions) {
      const operations: Operation[] =
        route?.operations ??
        (resolved.kind === "DECLARATIVE"
          ? [
              (Object.keys(resolved.config.endpoints).find(
                (name) => name !== "default",
              ) as Operation) ?? "chat",
            ]
          : ["chat"]);
      for (const operation of operations) {
        if (resolved.kind === "DECLARATIVE") {
          const endpoints = resolved.config.endpoints;
          if (
            !endpoints[operation] &&
            !route?.adapter_overrides.path_override &&
            !channel?.adapter_config.path_override &&
            !(isConversationalOperation(operation) && endpoints.default)
          ) {
            throw invalid(`Provider does not declare an endpoint for ${operation}`);
          }
        } else if (!sample?.request) {
          throw invalid("A WASM provider release requires a request sample");
        }
        const usageRequired =
          route &&
          [route.billing_price_id, route.cost_version_id].some(
            (id) => snapshot.prices.get(id)?.basis === "PROVIDER_USAGE",
          );
        const protocol = upstreamProtocolFor(resolved, operation);
        const clientBody =
          sample?.request ??
          (operation === "responses"
            ? { input: "plugin validation" }
            : operation === "gemini"
              ? { contents: [{ role: "user", parts: [{ text: "plugin validation" }] }] }
              : { messages: [{ role: "user", content: "plugin validation" }], max_tokens: 16 });
        if (
          resolved.kind === "WASM" &&
          resolved.config.hooks.includes("adapt_response") &&
          !sample?.response
        ) {
          throw invalid("A WASM provider release requires a response sample");
        }
        const preview = await previewProviderRequest({
          provider: channel?.kind ?? plugin.name,
          providerPluginId: revisionId,
          baseUrl: channel?.base_url ?? "https://example.invalid",
          operation,
          upstreamModel: route?.upstream_model ?? "fixture",
          stream: false,
          clientBody,
          channelAdapterConfig: channel?.adapter_config ?? {},
          ...(route ? { routeAdapterOverrides: route.adapter_overrides } : {}),
          ...(sample?.response
            ? { mockUpstreamResponse: { status: 200, body: sample.response } }
            : {}),
          snapshot: { ...snapshot, plugins: revisions },
          wasmExecutor: executor,
          ...(signingPublicKeyPem ? { signingPublicKeyPem } : {}),
        });
        if (
          resolved.kind === "WASM" &&
          usageRequired &&
          !preview.decoded_response?.extracted_usage
        ) {
          throw invalid("The response sample must establish metered usage");
        }
        if (resolved.kind === "WASM" && resolved.config.hooks.includes("adapt_stream_event")) {
          if (!sample?.event)
            throw invalid("A WASM provider release requires a terminal stream event sample");
          const result = await invokeWasmProviderStreamEvent({
            executor,
            plugin,
            config: resolved.config,
            ...(signingPublicKeyPem ? { signingPublicKeyPem } : {}),
            input: {
              operation,
              upstream_model: route?.upstream_model ?? "fixture",
              client_protocol: isConversationalOperation(operation) ? operation : protocol,
              event: sample.event.name,
              data: sample.event.data,
              state: "",
            },
          });
          if (result.done !== true || (usageRequired && !result.usage)) {
            throw invalid("The stream sample must produce a terminal event and required usage");
          }
          if (result.usage !== undefined) validateUsageObject(result.usage);
        }
      }
    }
  }

  async function validateContent(
    route: Pick<RouteRecord, "model_name" | "operations">,
    revisionId: string,
  ): Promise<void> {
    if (covered.has(revisionId)) return;
    const plugin = await getRevision(revisionId, "CONTENT");
    if (!plugin.enabled) {
      const sample = options.samples[revisionId];
      if (plugin.kind === "WEBHOOK") {
        validateContentDecision(sample?.webhook_reply);
      } else {
        if (!executor || !plugin.wasm_bytes || typeof sample?.text !== "string") {
          throw invalid("A draft content WASM revision requires a text sample and a WASM runtime");
        }
        const manifest = plugin.manifest as Partial<WasmPluginManifest>;
        const hooks = manifest.hooks ?? [
          "on_request",
          "on_response",
          "on_stream_chunk",
          "on_stream_end",
        ];
        executor.compile(revisionId, plugin.digest, plugin.wasm_bytes);
        for (const hook of hooks) {
          const input = Buffer.from(
            JSON.stringify({
              hook,
              request_id: "validation",
              billing_account_id: "validation",
              model: route.model_name,
              operation: route.operations[0],
              text: sample.text,
              final: hook !== "on_stream_chunk",
            }),
          );
          const response = await executor.invoke(revisionId, hook, input, {
            fuelBudget: manifest.fuel_budget ?? 2_000_000,
            maxMemoryBytes: manifest.max_memory_bytes ?? 32 * 1024 * 1024,
            compileTimeoutMs: 5000,
          });
          validateContentDecision(JSON.parse(Buffer.from(response).toString("utf8")));
        }
      }
    }
    covered.add(revisionId);
  }

  function validateContentDecision(result: Record<string, unknown> | undefined): void {
    if (result?.action === "PASS" || result?.action === "BLOCK") return;
    if (result?.action === "TRANSFORM" && typeof result.text === "string") return;
    throw invalid("Content sample must produce PASS, BLOCK, or TRANSFORM with text");
  }

  const changes: PluginReleaseChange[] = [];
  for (const target of targets) {
    const id =
      "channel_id" in target
        ? target.channel_id
        : "route_id" in target
          ? target.route_id
          : target.plugin_id;
    if (selectedTargets.has(id)) throw invalid("Duplicate release target");
    selectedTargets.add(id);
    if ("channel_id" in target) {
      const channel = snapshot.channels.get(id);
      if (!channel?.provider_plugin_id) throw invalid("Channel has no rollback revision");
      await validateProvider(channel, target.provider_plugin_id);
      changes.push({
        before: { channel_id: id, provider_plugin_id: channel.provider_plugin_id },
        after: target,
      });
    } else if ("route_id" in target) {
      const route = routesById.get(id);
      if (!route) throw invalid("Enabled route was not found");
      for (const step of target.plugin_pipeline) {
        if (step.enabled !== false) await validateContent(route, step.plugin_id);
      }
      changes.push({
        before: { route_id: id, plugin_pipeline: route.plugin_pipeline },
        after: target,
      });
    } else if (target.available) {
      const plugin = snapshot.plugins.get(id) ?? (await options.getPlugin(id));
      if (!plugin || snapshot.revokedPlugins.has(id))
        throw invalid("Plugin revision is missing or revoked");
      if (plugin.capability === "PROVIDER") await validateProvider(null, id);
      else await validateContent({ model_name: "fixture", operations: ["chat"] }, id);
      changes.push({ before: { plugin_id: id, available: plugin.enabled }, after: target });
    }
  }
  return changes;
}
