import { parse } from "lossless-json";
declare const Host: {
  inputString(): string;
  inputBytes(): ArrayBuffer;
  outputString(value: string): void;
};
declare const Http: { request(value: unknown): unknown };
declare const module: { exports: Record<string, unknown> };
type Session = {
  id: string;
  pending: number[];
  buffer: string;
  text: string;
  args: string;
  terminal: boolean;
  usage: Record<string, string> | null;
  frames: number;
};
let session: Session | null = null;
function completePrefix(bytes: number[]): number {
  if (!bytes.length) return 0;
  let start = bytes.length - 1;
  while (start >= 0 && (bytes[start]! & 0xc0) === 0x80) start--;
  if (start < 0) throw new Error("invalid_utf8");
  const lead = bytes[start]!;
  const count = lead < 0x80 ? 1 : lead < 0xe0 ? 2 : lead < 0xf0 ? 3 : 4;
  return bytes.length - start < count ? start : bytes.length;
}
function consume(input: Uint8Array) {
  if (!session) throw new Error("session_missing");
  const bytes = session.pending.concat(Array.from(input));
  const end = completePrefix(bytes);
  session.pending = bytes.slice(end);
  session.buffer += new TextDecoder("utf-8", { fatal: true }).decode(
    new Uint8Array(bytes.slice(0, end)),
  );
  if (session.buffer.length > 65536) throw new Error("frame_limit");
  const events: unknown[] = [];
  let boundary: number;
  while ((boundary = session.buffer.indexOf("\n\n")) >= 0) {
    const frame = session.buffer.slice(0, boundary);
    session.buffer = session.buffer.slice(boundary + 2);
    const raw = frame
      .split("\n")
      .filter((x) => x.startsWith("data: "))
      .map((x) => x.slice(6))
      .join("\n");
    if (!raw) continue;
    if (session.terminal) throw new Error("data_after_terminal");
    session.frames++;
    if (raw === "[DONE]") {
      session.terminal = true;
      events.push({ type: "terminal", usage: session.usage });
      continue;
    }
    const value = parse(raw, undefined, (n: string) => n) as {
      error?: unknown;
      choices?: {
        delta?: {
          content?: string;
          tool_calls?: {
            index: string;
            id?: string;
            function?: { arguments?: string; name?: string };
          }[];
        };
      }[];
      usage?: { prompt_tokens: string; completion_tokens: string };
      opaque?: string;
    };
    if (value.error) throw new Error("provider_error");
    for (const choice of value.choices ?? []) {
      if (choice.delta?.content) {
        session.text += choice.delta.content;
        events.push({ type: "text", text: choice.delta.content });
      }
      for (const tool of choice.delta?.tool_calls ?? []) {
        session.args += tool.function?.arguments ?? "";
        events.push({
          type: "tool",
          index: tool.index,
          id: tool.id,
          name: tool.function?.name,
          args: tool.function?.arguments ?? "",
        });
      }
    }
    if (value.usage)
      session.usage = {
        input_tokens: value.usage.prompt_tokens,
        output_tokens: value.usage.completion_tokens,
      };
    if (value.opaque !== undefined) events.push({ type: "exact", decimal: value.opaque });
    if (session.text.length + session.args.length > 65536) throw new Error("aggregate_limit");
  }
  return { events, terminal: session.terminal };
}
function loop() {
  while (true) {}
}
function exhaustMemory() {
  const blocks = [];
  for (let i = 0; i < 256; i++) blocks.push(new Uint8Array(1024 * 1024).fill(1));
  Host.outputString(JSON.stringify({ blocks: blocks.length }));
}
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
function fromBase64(s: string): Uint8Array {
  const out: number[] = [];
  let bits = 0,
    n = 0;
  for (const ch of s) {
    if (ch === "=") break;
    const v = alphabet.indexOf(ch);
    if (v < 0) throw new Error("invalid_base64");
    bits = (bits << 6) | v;
    n += 6;
    if (n >= 8) {
      n -= 8;
      out.push((bits >> n) & 255);
    }
  }
  return new Uint8Array(out);
}
function toBase64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let result = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const n = (bytes[i]! << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
    result +=
      alphabet[(n >>> 18) & 63]! +
      alphabet[(n >>> 12) & 63]! +
      (i + 1 < bytes.length ? alphabet[(n >>> 6) & 63] : "=") +
      (i + 2 < bytes.length ? alphabet[n & 63] : "=");
  }
  return result;
}
let hidden: string | null = null;
function invoke() {
  const envelope = JSON.parse(Host.inputString());
  const value = JSON.parse(new TextDecoder().decode(fromBase64(envelope.input)));
  session = envelope.state;
  let result: unknown;
  if (value.op === "begin") {
    session = {
      id: value.id,
      pending: [],
      buffer: "",
      text: "",
      args: "",
      terminal: false,
      usage: null,
      frames: 0,
    };
    result = { phase: "begin" };
  } else if (value.op === "push") {
    result = consume(new Uint8Array(value.bytes));
  } else if (value.op === "end") {
    if (!session?.terminal || session.pending.length || session.buffer.length)
      throw new Error("stream_truncated");
    result = session;
    session = null;
  } else if (value.op === "sticky") {
    result = { previous: hidden, current: value.value };
    hidden = value.value;
  } else if (value.op === "log") {
    console.log(value.value);
    result = { logged: true };
  } else if (value.op === "network") {
    try {
      Http.request({ url: value.url, method: "GET" });
      result = { allowed: true };
    } catch {
      result = { allowed: false };
    }
  } else if (value.op === "state") {
    session = value.state;
    result = { ok: true };
  } else if (value.op === "invalid") {
    Host.outputString("not-json");
    return;
  } else if (value.op === "large") {
    result = { data: "a".repeat(value.bytes) };
  } else if (value.op === "echo") {
    result = value.value;
  } else throw new Error("unknown_fixture_op");
  Host.outputString(JSON.stringify({ output: toBase64(JSON.stringify(result)), state: session }));
}
module.exports = { invoke, loop, exhaustMemory };
