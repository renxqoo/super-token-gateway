import { GatewayError, optionalObject } from "@gateway/contracts";
import type { Operation, Protocol } from "@gateway/kernel";
import type { ChannelKind, ChannelRecord, RouteRecord } from "@gateway/ledger";
import {
  copyExact,
  decodeRequest,
  encodeRequest,
  encodeSSE,
  stringifyExact,
  validateRequestPolicy,
  type SSEEvent,
} from "@gateway/protocol";
import {
  awsAmzDate,
  awsCredentials,
  awsSigningKey,
  signAwsRequest,
  type VertexTokens,
} from "@gateway/runtime";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export function isConversationalOperation(op: Operation): op is Protocol {
  return op === "chat" || op === "responses" || op === "messages" || op === "gemini";
}

export function upstreamProtocolFor(kind: ChannelKind, clientOp: Operation): Protocol | null {
  if (!isConversationalOperation(clientOp)) return null;
  if (kind === "anthropic" || kind === "bedrock") return "messages";
  if (kind === "gemini" || kind === "vertex") return "gemini";
  if (kind === "openai" || kind === "ollama") {
    return clientOp === "responses" ? "responses" : "chat";
  }
  return "chat";
}

function parseAzureSecret(secret: string): { apiKey: string; apiVersion: string } {
  if (secret.startsWith("{")) {
    const obj = JSON.parse(secret) as Record<string, unknown>;
    return {
      apiKey: String(obj.apiKey ?? ""),
      apiVersion: String(obj.apiVersion ?? "2024-10-21"),
    };
  }
  return { apiKey: secret, apiVersion: "2024-10-21" };
}

function joinBase(baseUrl: string, path: string): URL {
  const base = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
  return new URL(`${base}${path}`);
}

export type PreparedUpstreamRequest = {
  url: URL;
  headers: Headers;
  body: Uint8Array;
  upstreamProtocol: Protocol | null;
  isBedrockStream: boolean;
};

async function encodeMultipartWithModel(
  sourceEntries: readonly [string, string | File][],
  upstreamModel: string,
): Promise<{ contentType: string; bytes: Uint8Array }> {
  const form = new FormData();
  let replaced = false;
  for (const [k, v] of sourceEntries) {
    if (k === "model") {
      form.append("model", upstreamModel);
      replaced = true;
    } else {
      form.append(k, v);
    }
  }
  if (!replaced) form.append("model", upstreamModel);
  const tmp = new Response(form);
  const contentType = tmp.headers.get("content-type") ?? "multipart/form-data";
  const bytes = new Uint8Array(await tmp.arrayBuffer());
  return { contentType, bytes };
}

export async function prepareUpstreamRequest(options: {
  channel: ChannelRecord;
  route: RouteRecord;
  secret: string;
  operation: Operation;
  clientBody: Record<string, unknown>;
  multipartEntries?: readonly [string, string | File][];
  isStream: boolean;
  vertexTokens: VertexTokens;
  traceparent?: string;
  signal: AbortSignal;
}): Promise<PreparedUpstreamRequest> {
  const {
    channel,
    route,
    secret,
    operation,
    clientBody,
    multipartEntries,
    isStream,
    vertexTokens,
    traceparent,
    signal,
  } = options;
  if (route.request_policy && Object.keys(route.request_policy).length > 0) {
    validateRequestPolicy(clientBody, route.request_policy);
  }

  const headers = new Headers();
  if (traceparent) headers.set("traceparent", traceparent);
  if (isStream) headers.set("accept", "text/event-stream");

  if (multipartEntries) {
    const path =
      operation === "transcription"
        ? "/v1/audio/transcriptions"
        : operation === "translation"
          ? "/v1/audio/translations"
          : "/v1/images/edits";
    const url = joinBase(channel.base_url, path);
    if (secret) headers.set("authorization", `Bearer ${secret}`);
    const encoded = await encodeMultipartWithModel(multipartEntries, route.upstream_model);
    headers.set("content-type", encoded.contentType);
    return {
      url,
      headers,
      body: encoded.bytes,
      upstreamProtocol: null,
      isBedrockStream: false,
    };
  }

  headers.set("content-type", "application/json");
  const upstreamProtocol = upstreamProtocolFor(channel.kind, operation);
  let payload: Record<string, unknown>;
  if (isConversationalOperation(operation) && upstreamProtocol) {
    if (operation === upstreamProtocol) {
      payload = copyExact(clientBody, { ...clientBody });
      if (upstreamProtocol !== "gemini") {
        payload.model = route.upstream_model;
      }
      if (upstreamProtocol === "chat" && isStream) {
        payload.stream_options = { include_usage: true };
      }
      if (
        (channel.kind === "deepseek" || channel.kind === "ollama") &&
        upstreamProtocol === "chat"
      ) {
        if (payload.max_completion_tokens !== undefined && payload.max_tokens === undefined) {
          payload.max_tokens = payload.max_completion_tokens;
        }
        delete payload.max_completion_tokens;
      }
    } else {
      const normalized = decodeRequest(clientBody, operation);
      payload = encodeRequest(normalized, upstreamProtocol, route.upstream_model);
      if (
        (channel.kind === "deepseek" || channel.kind === "ollama") &&
        upstreamProtocol === "chat"
      ) {
        if (payload.max_completion_tokens !== undefined && payload.max_tokens === undefined) {
          payload.max_tokens = payload.max_completion_tokens;
        }
        delete payload.max_completion_tokens;
      }
    }
  } else {
    payload = copyExact(clientBody, { ...clientBody, model: route.upstream_model });
  }

  let url: URL;
  let isBedrockStream = false;

  switch (channel.kind) {
    case "openai":
    case "openai_compatible":
    case "deepseek":
    case "ollama": {
      const path =
        operation === "embeddings"
          ? "/v1/embeddings"
          : operation === "moderations"
            ? "/v1/moderations"
            : operation === "completions"
              ? "/v1/completions"
              : operation === "images"
                ? "/v1/images/generations"
                : operation === "image_edits"
                  ? "/v1/images/edits"
                  : operation === "speech"
                    ? "/v1/audio/speech"
                    : upstreamProtocol === "responses"
                      ? "/v1/responses"
                      : "/v1/chat/completions";
      url = joinBase(channel.base_url, path);
      if (secret) headers.set("authorization", `Bearer ${secret}`);
      break;
    }
    case "azure": {
      const az = parseAzureSecret(secret);
      const opPath =
        operation === "embeddings"
          ? "embeddings"
          : operation === "completions"
            ? "completions"
            : "chat/completions";
      url = joinBase(
        channel.base_url,
        `/openai/deployments/${encodeURIComponent(route.upstream_model)}/${opPath}`,
      );
      url.searchParams.set("api-version", az.apiVersion);
      headers.set("api-key", az.apiKey);
      break;
    }
    case "anthropic": {
      url = joinBase(channel.base_url, "/v1/messages");
      headers.set("x-api-key", secret);
      headers.set("anthropic-version", "2023-06-01");
      break;
    }
    case "bedrock": {
      const creds = awsCredentials(secret);
      const action = isStream ? "invoke-with-response-stream" : "invoke";
      url = joinBase(
        channel.base_url,
        `/model/${encodeURIComponent(route.upstream_model)}/${action}`,
      );
      delete payload.model;
      delete payload.stream;
      payload.anthropic_version = "bedrock-2023-05-31";
      isBedrockStream = isStream;
      if (isStream) headers.set("accept", "application/vnd.amazon.eventstream");
      const rawBytes = encoder.encode(stringifyExact(payload));
      const amzDate = awsAmzDate();
      const signingKey = awsSigningKey(
        creds.secretAccessKey,
        amzDate.slice(0, 8),
        creds.region,
        "bedrock",
      );
      signAwsRequest(url, rawBytes, headers, {
        accessKeyId: creds.accessKeyId,
        sessionToken: creds.sessionToken,
        region: creds.region,
        service: "bedrock",
        amzDate,
        signingKey,
      });
      return { url, headers, body: rawBytes, upstreamProtocol, isBedrockStream };
    }
    case "gemini": {
      const action = isStream ? "streamGenerateContent" : "generateContent";
      url = joinBase(
        channel.base_url,
        `/v1beta/models/${encodeURIComponent(route.upstream_model)}:${action}`,
      );
      if (isStream) url.searchParams.set("alt", "sse");
      headers.set("x-goog-api-key", secret);
      delete payload.model;
      delete payload.stream;
      break;
    }
    case "vertex": {
      const token = await vertexTokens.get(secret, signal);
      const action = isStream ? "streamGenerateContent" : "generateContent";
      url = joinBase(
        channel.base_url,
        `/models/${encodeURIComponent(route.upstream_model)}:${action}`,
      );
      if (isStream) url.searchParams.set("alt", "sse");
      headers.set("authorization", `Bearer ${token}`);
      delete payload.model;
      delete payload.stream;
      break;
    }
    default:
      throw new GatewayError(500, "unsupported_channel", `Unsupported channel kind`);
  }

  return {
    url,
    headers,
    body: encoder.encode(stringifyExact(payload)),
    upstreamProtocol,
    isBedrockStream,
  };
}

export function parseRetryAfterMs(header: string | null): number | undefined {
  if (!header) return undefined;
  const trimmed = header.trim();
  if (/^\d+(?:\.\d+)?$/.test(trimmed)) {
    return Math.max(0, Math.round(Number(trimmed) * 1000));
  }
  const ts = Date.parse(trimmed);
  if (Number.isFinite(ts)) {
    return Math.max(0, ts - Date.now());
  }
  return undefined;
}

export class BedrockEventStreamDecoder {
  private buffer = new Uint8Array(0);

  feed(chunk: Uint8Array): SSEEvent[] {
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer, 0);
    merged.set(chunk, this.buffer.length);
    let offset = 0;
    const events: SSEEvent[] = [];

    while (merged.length - offset >= 16) {
      const view = new DataView(merged.buffer, merged.byteOffset + offset, merged.length - offset);
      const totalLen = view.getUint32(0, false);
      const headersLen = view.getUint32(4, false);
      if (totalLen < 16 || totalLen > 4 * 1024 * 1024) {
        throw new GatewayError(502, "invalid_upstream", "Invalid Bedrock event stream frame");
      }
      if (merged.length - offset < totalLen) break;
      const payloadStart = offset + 12 + headersLen;
      const payloadEnd = offset + totalLen - 4;
      const payloadBytes = merged.subarray(payloadStart, payloadEnd);
      offset += totalLen;
      try {
        const wrapper = JSON.parse(decoder.decode(payloadBytes)) as { bytes?: string };
        if (typeof wrapper.bytes === "string") {
          const decoded = Buffer.from(wrapper.bytes, "base64").toString("utf8");
          const parsed = JSON.parse(decoded) as { type?: string };
          const eventType = typeof parsed.type === "string" ? parsed.type : "message";
          events.push({
            event: eventType,
            data: decoded,
            id: "",
            wire: `event: ${eventType}\ndata: ${decoded}\n\n`,
          });
        }
      } catch {
        throw new GatewayError(502, "invalid_upstream", "Malformed Bedrock event payload");
      }
    }

    this.buffer = merged.slice(offset);
    return events;
  }
}

export function extractRequestText(body: Record<string, unknown>, operation: Operation): string {
  const parts: string[] = [];
  if (operation === "chat" && Array.isArray(body.messages)) {
    for (const m of body.messages) {
      const msg = optionalObject(m);
      if (typeof msg.content === "string") parts.push(msg.content);
      else if (Array.isArray(msg.content)) {
        for (const p of msg.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") parts.push(part.text);
        }
      }
    }
  } else if (operation === "messages") {
    if (typeof body.system === "string") parts.push(body.system);
    if (Array.isArray(body.messages)) {
      for (const m of body.messages) {
        const msg = optionalObject(m);
        if (typeof msg.content === "string") parts.push(msg.content);
        else if (Array.isArray(msg.content)) {
          for (const p of msg.content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "responses") {
    if (typeof body.instructions === "string") parts.push(body.instructions);
    if (typeof body.input === "string") parts.push(body.input);
    else if (Array.isArray(body.input)) {
      for (const item of body.input) {
        const obj = optionalObject(item);
        if (typeof obj.content === "string") parts.push(obj.content);
        else if (Array.isArray(obj.content)) {
          for (const p of obj.content) {
            const part = optionalObject(p);
            if (typeof part.text === "string") parts.push(part.text);
          }
        }
      }
    }
  } else if (operation === "gemini" && Array.isArray(body.contents)) {
    for (const c of body.contents) {
      const content = optionalObject(c);
      if (Array.isArray(content.parts)) {
        for (const p of content.parts) {
          const part = optionalObject(p);
          if (typeof part.text === "string") parts.push(part.text);
        }
      }
    }
  } else if (typeof body.prompt === "string") {
    parts.push(body.prompt);
  } else if (typeof body.input === "string") {
    parts.push(body.input);
  }
  return parts.join("\n");
}

export function replaceRequestText(
  body: Record<string, unknown>,
  operation: Operation,
  nextText: string,
): Record<string, unknown> {
  if ((operation === "chat" || operation === "messages") && Array.isArray(body.messages)) {
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (let i = messages.length - 1; i >= 0; i--) {
      const msg = messages[i]!;
      if (msg.role === "user") {
        if (typeof msg.content === "string") {
          msg.content = nextText;
          return copyExact(body, { ...body, messages });
        }
        if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          const idx = parts.findIndex((p) => typeof p.text === "string");
          if (idx >= 0) {
            parts[idx]!.text = nextText;
            msg.content = parts;
            return copyExact(body, { ...body, messages });
          }
        }
      }
    }
    return copyExact(body, { ...body, messages });
  }
  if (operation === "responses") {
    if (typeof body.input === "string") {
      return copyExact(body, { ...body, input: nextText });
    }
    if (Array.isArray(body.input) && body.input.length > 0) {
      const items = body.input.map((it) => ({ ...optionalObject(it) }));
      for (let i = items.length - 1; i >= 0; i--) {
        if (typeof items[i]!.content === "string") {
          items[i]!.content = nextText;
          return copyExact(body, { ...body, input: items });
        }
      }
    }
    return body;
  }
  if (operation === "gemini" && Array.isArray(body.contents) && body.contents.length > 0) {
    const contents = body.contents.map((c) => ({ ...optionalObject(c) }));
    const last = contents[contents.length - 1]!;
    last.parts = [{ text: nextText }];
    return copyExact(body, { ...body, contents });
  }
  if (typeof body.prompt === "string") {
    return copyExact(body, { ...body, prompt: nextText });
  }
  if (typeof body.input === "string") {
    return copyExact(body, { ...body, input: nextText });
  }
  return body;
}

export async function transformRequestTexts(
  body: Record<string, unknown>,
  operation: Operation,
  transform: (text: string) => Promise<{ blocked: boolean; text: string }>,
): Promise<{ blocked: boolean; body: Record<string, unknown> }> {
  if (operation === "chat" && Array.isArray(body.messages)) {
    let changed = false;
    const messages = body.messages.map((m) => ({ ...optionalObject(m) }));
    for (const msg of messages) {
      if (typeof msg.content === "string" && msg.content.length > 0) {
        const res = await transform(msg.content);
        if (res.blocked) return { blocked: true, body };
        if (res.text !== msg.content) {
          msg.content = res.text;
          changed = true;
        }
      } else if (Array.isArray(msg.content)) {
        const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
        let partChanged = false;
        for (const part of parts) {
          if (typeof part.text === "string" && part.text.length > 0) {
            const res = await transform(part.text);
            if (res.blocked) return { blocked: true, body };
            if (res.text !== part.text) {
              part.text = res.text;
              partChanged = true;
            }
          }
        }
        if (partChanged) {
          msg.content = parts;
          changed = true;
        }
      }
    }
    return { blocked: false, body: changed ? copyExact(body, { ...body, messages }) : body };
  }

  if (operation === "messages") {
    let changed = false;
    let nextSystem = body.system;
    if (typeof body.system === "string" && body.system.length > 0) {
      const res = await transform(body.system);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.system) {
        nextSystem = res.text;
        changed = true;
      }
    }
    const messages = Array.isArray(body.messages)
      ? body.messages.map((m) => ({ ...optionalObject(m) }))
      : null;
    if (messages) {
      for (const msg of messages) {
        if (typeof msg.content === "string" && msg.content.length > 0) {
          const res = await transform(msg.content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== msg.content) {
            msg.content = res.text;
            changed = true;
          }
        } else if (Array.isArray(msg.content)) {
          const parts = msg.content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            msg.content = parts;
            changed = true;
          }
        }
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextSystem !== body.system ? { system: nextSystem } : {}),
        ...(messages ? { messages } : {}),
      }),
    };
  }

  if (operation === "responses") {
    let changed = false;
    let nextInstructions = body.instructions;
    if (typeof body.instructions === "string" && body.instructions.length > 0) {
      const res = await transform(body.instructions);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.instructions) {
        nextInstructions = res.text;
        changed = true;
      }
    }
    let nextInput = body.input;
    if (typeof body.input === "string" && body.input.length > 0) {
      const res = await transform(body.input);
      if (res.blocked) return { blocked: true, body };
      if (res.text !== body.input) {
        nextInput = res.text;
        changed = true;
      }
    } else if (Array.isArray(body.input)) {
      const items = body.input.map((it) => ({ ...optionalObject(it) }));
      let inputChanged = false;
      for (const item of items) {
        if (typeof item.content === "string" && item.content.length > 0) {
          const res = await transform(item.content);
          if (res.blocked) return { blocked: true, body };
          if (res.text !== item.content) {
            item.content = res.text;
            inputChanged = true;
          }
        } else if (Array.isArray(item.content)) {
          const parts = item.content.map((p) => ({ ...optionalObject(p) }));
          let partChanged = false;
          for (const part of parts) {
            if (typeof part.text === "string" && part.text.length > 0) {
              const res = await transform(part.text);
              if (res.blocked) return { blocked: true, body };
              if (res.text !== part.text) {
                part.text = res.text;
                partChanged = true;
              }
            }
          }
          if (partChanged) {
            item.content = parts;
            inputChanged = true;
          }
        }
      }
      if (inputChanged) {
        nextInput = items;
        changed = true;
      }
    }
    if (!changed) return { blocked: false, body };
    return {
      blocked: false,
      body: copyExact(body, {
        ...body,
        ...(nextInstructions !== body.instructions ? { instructions: nextInstructions } : {}),
        ...(nextInput !== body.input ? { input: nextInput } : {}),
      }),
    };
  }

  if (operation === "gemini" && Array.isArray(body.contents)) {
    let changed = false;
    const contents = body.contents.map((c) => ({ ...optionalObject(c) }));
    for (const c of contents) {
      if (Array.isArray(c.parts)) {
        const parts = c.parts.map((p) => ({ ...optionalObject(p) }));
        let partChanged = false;
        for (const part of parts) {
          if (typeof part.text === "string" && part.text.length > 0) {
            const res = await transform(part.text);
            if (res.blocked) return { blocked: true, body };
            if (res.text !== part.text) {
              part.text = res.text;
              partChanged = true;
            }
          }
        }
        if (partChanged) {
          c.parts = parts;
          changed = true;
        }
      }
    }
    return { blocked: false, body: changed ? copyExact(body, { ...body, contents }) : body };
  }

  const singleText = extractRequestText(body, operation);
  if (!singleText) return { blocked: false, body };
  const res = await transform(singleText);
  if (res.blocked) return { blocked: true, body };
  if (res.text === singleText) return { blocked: false, body };
  return { blocked: false, body: replaceRequestText(body, operation, res.text) };
}

export function extractResponseText(body: Record<string, unknown>, protocol: Protocol): string {
  if (protocol === "chat") {
    const choices = Array.isArray(body.choices) ? body.choices : [];
    const first = optionalObject(choices[0]);
    const msg = optionalObject(first.message);
    return typeof msg.content === "string" ? msg.content : "";
  }
  if (protocol === "messages") {
    const content = Array.isArray(body.content) ? body.content : [];
    return content
      .map((b) => {
        const block = optionalObject(b);
        return block.type === "text" && typeof block.text === "string" ? block.text : "";
      })
      .join("");
  }
  if (protocol === "responses") {
    const output = Array.isArray(body.output) ? body.output : [];
    const out: string[] = [];
    for (const item of output) {
      const obj = optionalObject(item);
      if (Array.isArray(obj.content)) {
        for (const p of obj.content) {
          const part = optionalObject(p);
          if (typeof part.text === "string") out.push(part.text);
        }
      }
    }
    return out.join("");
  }
  const candidates = Array.isArray(body.candidates) ? body.candidates : [];
  const content = optionalObject(optionalObject(candidates[0]).content);
  const parts = Array.isArray(content.parts) ? content.parts : [];
  return parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" ? part.text : "";
    })
    .join("");
}

export function replaceResponseText(
  body: Record<string, unknown>,
  protocol: Protocol,
  nextText: string,
): Record<string, unknown> {
  if (protocol === "chat" && Array.isArray(body.choices) && body.choices.length > 0) {
    const choice = { ...optionalObject(body.choices[0]) };
    const message = { ...optionalObject(choice.message), content: nextText };
    choice.message = message;
    return { ...body, choices: [choice, ...body.choices.slice(1)] };
  }
  if (protocol === "messages") {
    const existing = Array.isArray(body.content) ? body.content : [];
    const nonText = existing.filter((b) => optionalObject(b).type !== "text");
    return { ...body, content: [{ type: "text", text: nextText }, ...nonText] };
  }
  if (protocol === "responses") {
    const existing = Array.isArray(body.output) ? body.output : [];
    const nonMsg = existing.filter((it) => optionalObject(it).type !== "message");
    return {
      ...body,
      output: [
        {
          id: "msg_0",
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text: nextText, annotations: [] }],
        },
        ...nonMsg,
      ],
    };
  }
  if (protocol === "gemini" && Array.isArray(body.candidates) && body.candidates.length > 0) {
    const cand = { ...optionalObject(body.candidates[0]) };
    const existingParts = Array.isArray(optionalObject(cand.content).parts)
      ? (optionalObject(cand.content).parts as unknown[])
      : [];
    const nonTextParts = existingParts.filter((p) => typeof optionalObject(p).text !== "string");
    cand.content = { role: "model", parts: [{ text: nextText }, ...nonTextParts] };
    return { ...body, candidates: [cand, ...body.candidates.slice(1)] };
  }
  return body;
}

export function extractChunkTextFromEncodedSse(
  chunkBytes: Uint8Array,
  protocol: Protocol,
): string | null {
  const raw = decoder.decode(chunkBytes);
  const dataLines = raw
    .split("\n")
    .filter((l) => l.startsWith("data: "))
    .map((l) => l.slice(6));
  if (dataLines.length === 0) return null;
  const payload = dataLines.join("\n").trim();
  if (!payload || payload === "[DONE]") return null;
  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(payload) as Record<string, unknown>;
  } catch {
    return null;
  }
  if (protocol === "chat") {
    const choices = Array.isArray(obj.choices) ? obj.choices : [];
    const delta = optionalObject(optionalObject(choices[0]).delta);
    return typeof delta.content === "string" ? delta.content : null;
  }
  if (protocol === "messages") {
    const delta = optionalObject(obj.delta);
    return delta.type === "text_delta" && typeof delta.text === "string" ? delta.text : null;
  }
  if (protocol === "responses") {
    return obj.type === "response.output_text.delta" && typeof obj.delta === "string"
      ? obj.delta
      : null;
  }
  const cands = Array.isArray(obj.candidates) ? obj.candidates : [];
  const parts = Array.isArray(optionalObject(optionalObject(cands[0]).content).parts)
    ? (optionalObject(optionalObject(cands[0]).content).parts as unknown[])
    : [];
  const texts = parts
    .map((p) => {
      const part = optionalObject(p);
      return typeof part.text === "string" ? part.text : "";
    })
    .join("");
  return texts.length > 0 ? texts : null;
}

export function buildTextDeltaSse(
  protocol: Protocol,
  model: string,
  text: string,
  streamId: string,
): Uint8Array {
  if (protocol === "chat") {
    return encodeSSE({
      id: streamId,
      object: "chat.completion.chunk",
      model,
      choices: [{ index: 0, delta: { content: text }, finish_reason: null }],
    });
  }
  if (protocol === "messages") {
    return encodeSSE(
      {
        type: "content_block_delta",
        index: 0,
        delta: { type: "text_delta", text },
      },
      "content_block_delta",
    );
  }
  if (protocol === "responses") {
    return encodeSSE(
      {
        type: "response.output_text.delta",
        output_index: 0,
        item_id: "msg_0",
        content_index: 0,
        delta: text,
      },
      "response.output_text.delta",
    );
  }
  return encodeSSE({
    candidates: [{ index: 0, content: { role: "model", parts: [{ text }] } }],
  });
}
