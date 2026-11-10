import { decrypt } from "@gateway/kernel";
import type { Operation, Protocol, Route } from "@gateway/kernel";
import { safeFetch } from "@gateway/runtime";
import type { Config } from "./config.ts";

export function upstreamHeaders(route: Route, config: Config, incoming?: Headers): Headers {
  const key = decrypt(route.secret, config.ENCRYPTION_KEY, route.provider_id);
  const headers = new Headers({
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
  });
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
  if (operation === "gemini")
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
  operation: Operation | Protocol,
  body: BodyInit,
  stream: boolean,
  signal: AbortSignal,
  incoming?: Headers,
  contentType?: string,
): Promise<Response> {
  const headers = upstreamHeaders(route, config, incoming);
  if (contentType) headers.set("content-type", contentType);
  return safeFetch(
    upstreamUrl(route, operation, stream),
    { method: "POST", headers, body, signal },
    config.ALLOW_PRIVATE_UPSTREAM,
    config.PRIVATE_UPSTREAM_CIDRS,
  );
}
