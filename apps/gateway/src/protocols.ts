import {
  SSEParser,
  StreamTranslator,
  copyExact,
  decodeRequest,
  decodeResponse,
  encodeRequest,
  encodeResponse,
  parseClientJson,
  parseProviderJson,
  stringifyExact,
} from "@gateway/adapters/protocol";
import { object } from "@gateway/contracts";
import type { Protocol } from "@gateway/kernel";

const encoder = new TextEncoder();
const decoder = () => new TextDecoder("utf-8", { fatal: true });

export function requestAsChat(
  body: Uint8Array,
  source: Protocol,
  model: string,
  path?: { model: string; stream: boolean },
): Uint8Array {
  const parsed = object(parseClientJson(decoder().decode(body)));
  const value = path
    ? copyExact(parsed, { ...parsed, model: path.model, stream: path.stream })
    : parsed;
  return encoder.encode(stringifyExact(encodeRequest(decodeRequest(value, source), "chat", model)));
}

export function responseFromChat(body: Uint8Array, target: Protocol, model: string): Uint8Array {
  const completion = decodeResponse(parseProviderJson(decoder().decode(body)), "chat", "openai");
  return encoder.encode(stringifyExact(encodeResponse(completion, target, model)));
}

export function streamFromChat(target: Protocol, model: string) {
  const parser = new SSEParser(),
    translator = new StreamTranslator("chat", target, "openai", model);
  const translate = (bytes: Uint8Array, flush: boolean) =>
    parser.feed(bytes, flush).flatMap((event) => translator.consume(event).chunks);
  return {
    feed: (bytes: Uint8Array) => translate(bytes, false),
    finish: () => [...translate(new Uint8Array(), true), ...translator.finalize()],
  };
}
