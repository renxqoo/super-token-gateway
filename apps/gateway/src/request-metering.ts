import { GatewayError, optionalObject } from "@gateway/contracts";
import type { AttrValue, HostedToolDimension, Operation } from "@gateway/kernel";
import { numberLiteral } from "@gateway/protocol";

const clientMessageTools = /^(?:custom|(?:bash|text_editor|computer|memory)_\d{8})$/;
const clientResponseTools = new Set([
  "function",
  "custom",
  "computer_use_preview",
  "local_shell",
  "apply_patch",
]);
const clientGeminiTools = new Set([
  "functionDeclarations",
  "function_declarations",
  "computerUse",
  "computer_use",
]);
const standardServiceTiers = new Set(["auto", "default", "standard_only"]);
const standardGeminiServiceTiers = new Set([
  "standard",
  "STANDARD",
  "unspecified",
  "UNSPECIFIED",
  "auto",
  "default",
]);

function classifyHostedTool(
  value: unknown,
  operation: Operation,
): HostedToolDimension | "client" | "unknown" {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return "unknown";
  const tool = value as Record<string, unknown>;
  if (operation === "gemini") {
    const keys = Object.keys(tool);
    if (keys.length === 0) return "unknown";
    if (keys.every((key) => clientGeminiTools.has(key))) return "client";
    if (keys.every((key) => key === "googleSearch" || key === "google_search")) {
      return "web_search_calls";
    }
    if (keys.every((key) => key === "codeExecution" || key === "code_execution")) {
      return "code_interpreter_sessions";
    }
    return "unknown";
  }
  if (operation === "chat") {
    if (tool.type === "function" || tool.type === "custom") return "client";
    return "unknown";
  }
  if (operation === "responses") {
    const type = String(tool.type ?? "");
    if (type === "shell") {
      return optionalObject(tool.environment).type === "local" ? "client" : "unknown";
    }
    if (clientResponseTools.has(type)) return "client";
    if (
      type === "web_search" ||
      type === "web_search_preview" ||
      type.startsWith("web_search_preview_")
    ) {
      return "web_search_calls";
    }
    if (type === "code_interpreter") return "code_interpreter_sessions";
    if (type === "file_search") return "file_search_calls";
    return "unknown";
  }
  if (operation === "messages") {
    if (tool.type === undefined || clientMessageTools.test(String(tool.type))) return "client";
    const type = String(tool.type);
    if (/^web_search(?:_\d{8})?$/.test(type)) return "web_search_calls";
    if (/^code_execution(?:_\d{8})?$/.test(type)) return "code_interpreter_sessions";
    return "unknown";
  }
  return "unknown";
}

function hasValue(value: unknown): boolean {
  return value !== undefined && value !== null;
}

function requestsMultipleCandidates(body: Record<string, unknown>, key: string): boolean {
  return hasValue(body[key]) && (body[key] !== 1 || numberLiteral(body, key) !== undefined);
}

export type MeteredRequestSignals = {
  serviceTier?: string;
  hasInvalidServiceTier: boolean;
  hostedTools: Set<HostedToolDimension>;
  hasUnknownServerTool: boolean;
  attrs?: Record<string, AttrValue>;
};

export type MeteredValidationOptions = {
  allowPricedServiceTier?: boolean;
  allowPricedHostedTools?: boolean;
  allowedServiceTiers?: ReadonlySet<string>;
  allowedHostedTools?: ReadonlySet<HostedToolDimension>;
};

export function inspectMeteredRequestSignals(
  body: Record<string, unknown>,
  operation: Operation,
): MeteredRequestSignals {
  const allowedServiceTiers =
    operation === "gemini" ? standardGeminiServiceTiers : standardServiceTiers;
  const serviceTierValues =
    operation === "gemini" ? [body.service_tier, body.serviceTier] : [body.service_tier];
  let serviceTier: string | undefined;
  let hasInvalidServiceTier = false;
  for (const tier of serviceTierValues) {
    if (!hasValue(tier)) continue;
    if (typeof tier !== "string") {
      hasInvalidServiceTier = true;
      break;
    }
    if (!allowedServiceTiers.has(tier)) {
      const normalized = tier.trim().toLowerCase();
      if (!normalized || (serviceTier !== undefined && serviceTier !== normalized)) {
        hasInvalidServiceTier = true;
        break;
      }
      serviceTier = normalized;
    }
  }

  const hostedTools = new Set<HostedToolDimension>();
  let hasUnknownServerTool = false;
  if (hasValue(body.web_search_options)) {
    hostedTools.add("web_search_calls");
  }
  if (hasValue(body.tools)) {
    if (!Array.isArray(body.tools)) {
      hasUnknownServerTool = true;
    } else {
      for (const tool of body.tools) {
        const classified = classifyHostedTool(tool, operation);
        if (classified === "unknown") {
          hasUnknownServerTool = true;
          break;
        }
        if (classified !== "client") {
          hostedTools.add(classified);
        }
      }
    }
  }

  const attrs: Record<string, AttrValue> = {};
  const copyStringAttr = (key: string, source: Record<string, unknown> = body) => {
    const val = source[key];
    if (typeof val === "string" && val.length > 0 && val.length <= 120) {
      attrs[key] = val;
    }
  };
  const copyBoolAttr = (key: string, source: Record<string, unknown> = body) => {
    const val = source[key];
    if (typeof val === "boolean") attrs[key] = val;
  };

  if (operation === "images" || operation === "image_edits") {
    copyStringAttr("size");
    copyStringAttr("quality");
    copyStringAttr("background");
    copyStringAttr("style");
  } else if (operation === "videos") {
    copyStringAttr("resolution");
    copyStringAttr("size");
    copyStringAttr("quality");
    copyStringAttr("mode");
    copyStringAttr("aspect_ratio");
    copyBoolAttr("has_audio");
  } else if (
    operation === "speech" ||
    operation === "transcription" ||
    operation === "translation"
  ) {
    copyStringAttr("response_format");
    copyStringAttr("voice");
  } else if (
    operation === "chat" ||
    operation === "responses" ||
    operation === "messages" ||
    operation === "gemini"
  ) {
    if (typeof body.reasoning_effort === "string" && body.reasoning_effort.length > 0) {
      attrs.reasoning_effort = body.reasoning_effort;
    } else {
      const reasoningObj = optionalObject(body.reasoning);
      if (typeof reasoningObj.effort === "string" && reasoningObj.effort.length > 0) {
        attrs.reasoning_effort = reasoningObj.effort;
      }
    }
    const thinking = optionalObject(body.thinking);
    if (thinking.type === "enabled" || thinking.type === "adaptive") {
      attrs.thinking_mode = true;
    }
  }

  return {
    ...(serviceTier !== undefined ? { serviceTier } : {}),
    hasInvalidServiceTier,
    hostedTools,
    hasUnknownServerTool,
    ...(Object.keys(attrs).length > 0 ? { attrs } : {}),
  };
}

export class MeteredRequestError extends GatewayError {
  constructor(code: "billing_ceiling" | "unmetered_context" | "unmetered_tool", message: string) {
    super(422, code, message);
  }
}

export function validateMeteredRequest(
  body: Record<string, unknown>,
  operation: Operation,
  options?: MeteredValidationOptions,
): void {
  if (
    ((operation === "chat" || operation === "completions") &&
      requestsMultipleCandidates(body, "n")) ||
    (operation === "completions" &&
      (requestsMultipleCandidates(body, "best_of") ||
        (Array.isArray(body.prompt) &&
          body.prompt.length > 1 &&
          body.prompt.some((prompt) => typeof prompt !== "number")))) ||
    (operation === "gemini" &&
      [body.generationConfig, body.generation_config].some((value) => {
        const config = optionalObject(value);
        return (
          requestsMultipleCandidates(config, "candidateCount") ||
          requestsMultipleCandidates(config, "candidate_count")
        );
      }))
  ) {
    throw new MeteredRequestError("billing_ceiling", "Only one generation candidate is supported");
  }

  if (
    (operation === "responses" &&
      (hasValue(body.previous_response_id) ||
        hasValue(body.conversation) ||
        hasValue(body.prompt) ||
        (Array.isArray(body.input) &&
          body.input.some((item) => optionalObject(item).type === "item_reference")))) ||
    (operation === "gemini" && (hasValue(body.cachedContent) || hasValue(body.cached_content))) ||
    (operation === "messages" && hasValue(body.container))
  ) {
    throw new MeteredRequestError(
      "unmetered_context",
      "Server-stored context without a verifiable reservation is not supported",
    );
  }

  const signals = inspectMeteredRequestSignals(body, operation);
  const serviceTierAllowed =
    !signals.serviceTier ||
    options?.allowPricedServiceTier === true ||
    options?.allowedServiceTiers?.has(signals.serviceTier) === true;
  if (signals.hasInvalidServiceTier || !serviceTierAllowed) {
    throw new MeteredRequestError(
      "billing_ceiling",
      "Non-default service_tier without a billing dimension is not supported",
    );
  }

  const hostedToolsAllowed =
    signals.hostedTools.size === 0 ||
    options?.allowPricedHostedTools === true ||
    (options?.allowedHostedTools !== undefined &&
      [...signals.hostedTools].every((tool) => options.allowedHostedTools!.has(tool)));
  if (signals.hasUnknownServerTool || !hostedToolsAllowed) {
    throw new MeteredRequestError(
      "unmetered_tool",
      "Server-executed tools without a billing dimension are not supported",
    );
  }
}
