import { expect, test } from "bun:test";
import { estimateTokensFromPlaintext, SSEParser, StreamTranslator } from "../index.ts";

function feedEvents(translator: StreamTranslator, raw: string) {
  const parser = new SSEParser();
  for (const event of parser.feed(new TextEncoder().encode(raw), true)) {
    translator.consume(event);
  }
}

test("StreamTranslator counts ASCII, CJK, reasoning, and tool deltas across chat, responses, messages, and gemini", () => {
  const chat = new StreamTranslator("chat", "chat", "openai", "m");
  feedEvents(
    chat,
    'data: {"choices":[{"index":0,"delta":{"reasoning_content":"思考","content":"hello"}}]}\n\n' +
      'data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"calc","arguments":"{\\"x\\":1}"}}]}}]}\n\n',
  );
  expect(chat.observedOutput).toEqual({ asciiChars: 16, nonAsciiChars: 2, chunks: 4 });
  expect(
    estimateTokensFromPlaintext(chat.observedOutput.asciiChars, chat.observedOutput.nonAsciiChars),
  ).toBe(6);
  expect(estimateTokensFromPlaintext(0, 0)).toBe(1);
  expect(estimateTokensFromPlaintext(1, 0)).toBe(1);
  expect(estimateTokensFromPlaintext(5, 0)).toBe(2);
  expect(estimateTokensFromPlaintext(3, 2)).toBe(3);
  expect(estimateTokensFromPlaintext(0, 3)).toBe(3);

  const responses = new StreamTranslator("responses", "chat", "openai", "m");
  feedEvents(
    responses,
    'event: response.reasoning_summary_text.delta\ndata: {"type":"response.reasoning_summary_text.delta","output_index":0,"delta":"plan"}\n\n' +
      'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","output_index":1,"delta":"你好"}\n\n' +
      'event: response.output_item.added\ndata: {"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","call_id":"fc1","name":"go"}}\n\n' +
      'event: response.function_call_arguments.delta\ndata: {"type":"response.function_call_arguments.delta","output_index":2,"delta":"{}"}\n\n',
  );
  expect(responses.observedOutput).toEqual({ asciiChars: 8, nonAsciiChars: 2, chunks: 4 });

  const messages = new StreamTranslator("messages", "messages", "anthropic", "m");
  feedEvents(
    messages,
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"想"}}\n\n' +
      'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}\n\n' +
      'event: content_block_delta\ndata: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\\"a\\":1}"}}\n\n',
  );
  expect(messages.observedOutput).toEqual({ asciiChars: 12, nonAsciiChars: 1, chunks: 3 });

  const gemini = new StreamTranslator("gemini", "gemini", "gemini", "m");
  feedEvents(
    gemini,
    'data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"thought":true,"text":"思"},{"text":"ok"}]}}]}\n\n',
  );
  expect(gemini.observedOutput).toEqual({ asciiChars: 2, nonAsciiChars: 1, chunks: 2 });
});
