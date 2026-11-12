import { decrypt } from "@gateway/runtime";
import { providerObject, GatewayError } from "@gateway/contracts";
import { parseProviderJson, stringifyExact } from "@gateway/connectors";
import type { Operation, Protocol } from "@gateway/kernel";
import type { Route } from "@gateway/contracts";
import { awsCredentials, azureCredentials, signBedrock, VertexTokens } from "@gateway/runtime";
import { bedrockEventStream } from "@gateway/connectors";
import type { Dependencies } from "./context.ts";
import type { Config } from "./config.ts";
const vertexTokens = new VertexTokens();

export function upstreamHeaders(route: Route, config: Config, incoming?: Headers): Headers {
  const key = decrypt(route.secret, config.ENCRYPTION_KEY, route.provider_id);
  const headers = new Headers({
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  });
  if (["azure", "bedrock", "vertex"].includes(route.kind)) return headers;
  if (route.kind === "anthropic") {
    headers.set("x-api-key", key);
    headers.set("anthropic-version", "2023-06-01");
    const beta = incoming?.get("anthropic-beta");
    if (beta && /^[a-zA-Z0-9,._-]{1,500}$/.test(beta)) headers.set("anthropic-beta", beta);
  } else if (route.kind === "gemini") headers.set("x-goog-api-key", key);
  else headers.set("authorization", `Bearer ${key}`);
  return headers;
}

const paths: Partial<Record<Operation, string>> = {
  chat: "chat/completions",
  responses: "responses",
  messages: "messages",
  embeddings: "embeddings",
  images: "images/generations",
  image_edits: "images/edits",
  speech: "audio/speech",
  transcription: "audio/transcriptions",
  translation: "audio/translations",
  videos: "videos",
  files: "files",
  batches: "batches",
  moderations: "moderations",
  completions: "completions",
};

export function upstreamUrl(route: Route, operation: Operation | Protocol, stream: boolean): URL {
  const base = route.base_url.replace(/\/$/, "");
  if (route.kind === "bedrock")
    return new URL(
      `${base}/model/${encodeURIComponent(route.upstream_model)}/${stream ? "invoke-with-response-stream" : "invoke"}`,
    );
  if (route.kind === "azure")
    return new URL(
      `${base}/openai/deployments/${encodeURIComponent(route.upstream_model)}/${paths[operation] ?? operation}`,
    );
  if (operation === "gemini" || route.kind === "vertex")
    return new URL(
      `${base}/models/${encodeURIComponent(route.upstream_model)}:${stream ? "streamGenerateContent?alt=sse" : "generateContent"}`,
    );
  if (operation === "videos" && route.kind === "gemini")
    return new URL(`${base}/models/${encodeURIComponent(route.upstream_model)}:predictLongRunning`);
  return new URL(`${base}/${paths[operation] ?? operation}`);
}

export async function sendUpstream(
  route: Route,
  config: Config,
  transport: Dependencies["transport"],
  operation: Operation | Protocol,
  body: BodyInit,
  stream: boolean,
  signal: AbortSignal,
  incoming?: Headers,
  contentType?: string,
  reserveMemory?: (bytes: number) => void,
): Promise<Response> {
  let headers: Headers;
  let url: URL;
  try {
    headers = upstreamHeaders(route, config, incoming);
    url = upstreamUrl(route, operation, stream);
    if (contentType) headers.set("content-type", contentType);
    if (route.kind === "azure") {
      const credentials = azureCredentials(
        decrypt(route.secret, config.ENCRYPTION_KEY, route.provider_id),
      );
      headers.delete("authorization");
      headers.set("api-key", credentials.key);
      url.searchParams.set("api-version", credentials.version);
    } else if (route.kind === "vertex") {
      headers.set(
        "authorization",
        `Bearer ${await vertexTokens.get(decrypt(route.secret, config.ENCRYPTION_KEY, route.provider_id), signal)}`,
      );
      headers.delete("x-goog-api-key");
    } else if (route.kind === "bedrock") {
      if (typeof body !== "string") throw new Error("Bedrock requires a JSON request");
      const input = providerObject(parseProviderJson(body));
      delete input.model;
      delete input.stream;
      input.anthropic_version = "bedrock-2023-05-31";
      const beta = incoming?.get("anthropic-beta");
      if (beta && /^[a-zA-Z0-9,._-]{1,500}$/.test(beta)) input.anthropic_beta = beta.split(",");
      body = stringifyExact(input);
      headers.delete("authorization");
      headers.delete("anthropic-version");
      headers.set("accept", "application/json");
      if (stream) headers.set("x-amzn-bedrock-accept", "application/json");
      signBedrock(
        url,
        body,
        headers,
        awsCredentials(decrypt(route.secret, config.ENCRYPTION_KEY, route.provider_id)),
      );
    }
    signal.throwIfAborted();
  } catch {
    throw new GatewayError(
      503,
      "upstream_not_dispatched",
      "Upstream authentication or request preparation failed before dispatch",
    );
  }
  const response = await transport.fetch(
    url,
    { method: "POST", headers, body, signal },
    config.ALLOW_PRIVATE_UPSTREAM,
    config.PRIVATE_UPSTREAM_CIDRS,
  );
  if (route.kind === "bedrock" && stream && response.ok && response.body) {
    if (!response.headers.get("content-type")?.includes("application/vnd.amazon.eventstream")) {
      await response.body.cancel();
      throw new Error("Bedrock returned an invalid stream content type");
    }
    const outputHeaders = new Headers(response.headers);
    outputHeaders.delete("content-length");
    outputHeaders.set("content-type", "text/event-stream");
    return new Response(bedrockEventStream(response.body, config.MAX_EVENT_BYTES, reserveMemory), {
      status: response.status,
      headers: outputHeaders,
    });
  }
  return response;
}
