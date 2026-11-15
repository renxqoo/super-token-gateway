import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { exposeBatchFiles, parseBatchInput, parseBatchOutput, sumQuantities } from "../batches.ts";

const encode = (text: string) => new TextEncoder().encode(text);
const line = (custom: unknown, body: Record<string, unknown> = {}, extra = {}) =>
  JSON.stringify({
    custom_id: custom,
    method: "POST",
    url: "/v1/chat/completions",
    body: { model: "m", messages: [], ...body },
    ...extra,
  });
const code = (text: string) => {
  try {
    parseBatchInput(encode(text));
  } catch (error) {
    return [(error as { status: number }).status, (error as { code: string }).code];
  }
  return undefined;
};

test("a batch file yields its model, endpoint, rows and the sorted identity digest", () => {
  const parsed = parseBatchInput(
    encode(
      `${line("b", { max_tokens: 0 }).replace('"max_tokens":0', '"max_tokens":12345678901234567890')}\r\n\n${line("a")}\n`,
    ),
  );
  expect(parsed.model).toBe("m");
  expect(parsed.endpoint).toBe("/v1/chat/completions");
  expect(parsed.rows.map((row) => row.customId)).toEqual(["b", "a"]);
  expect(new TextDecoder().decode(parsed.rows[0]!.body)).toContain(
    '"max_tokens":12345678901234567890',
  );
  expect(parsed.idsSha256).toBe(createHash("sha256").update('["a","b"]').digest("hex"));
  expect(parsed.images).toBeUndefined();
});

test("batch files violating the request rules are refused with invalid_batch", () => {
  expect(code("")).toEqual([400, "invalid_batch"]);
  expect(code(Array.from({ length: 50001 }, (_, i) => line(`r${i}`)).join("\n"))).toEqual([
    400,
    "invalid_batch",
  ]);
  expect(code(`${line("a")}\n${line("a")}`)).toEqual([400, "invalid_batch"]);
  expect(code(line("x".repeat(201)))).toEqual([400, "invalid_batch"]);
  expect(code(line(""))).toEqual([400, "invalid_batch"]);
  expect(code(line("a", {}, { method: "GET" }))).toEqual([400, "invalid_batch"]);
  expect(code(line("a", {}, { extra: true }))).toEqual([400, "invalid_batch"]);
  expect(code(line("a", { stream: true }))).toEqual([422, "invalid_batch"]);
  expect(code(line("a", {}, { url: "/v1/audio/speech" }))).toEqual([422, "invalid_batch"]);
  expect(code(line("a", { model: "" }))).toEqual([400, "invalid_batch"]);
  expect(code(`${line("a")}\n${line("b", { model: "n" })}`)).toEqual([400, "invalid_batch"]);
  expect(code(`${line("a")}\n${line("b", {}, { url: "/v1/completions" })}`)).toEqual([
    400,
    "invalid_batch",
  ]);
  expect(code("not json")).toEqual([400, "invalid_batch"]);
  expect(code(line("a", { tools: [{ file_id: "file-up" }] }))).toEqual([422, "unmanaged_resource"]);
});

test("a batch row carrying a server-executed tool is refused like a direct request instead of reaching the upstream unmetered", () => {
  expect(code(`${line("a")}\n${line("b", { tools: [{ type: "web_search" }] })}`)).toEqual([
    422,
    "unmetered_tool",
  ]);
  expect(
    code(
      JSON.stringify({
        custom_id: "r",
        method: "POST",
        url: "/v1/responses",
        body: {
          model: "m",
          input: "x",
          tools: [{ type: "web_search_preview" }],
        },
      }),
    ),
  ).toEqual([422, "unmetered_tool"]);
  expect(
    code(line("a", { tools: [{ type: "function", function: { name: "f" } }] })),
  ).toBeUndefined();
});

test("embedding inputs and image counts are bounded across the whole batch", () => {
  const embed = (custom: string, count: number) =>
    line(custom, { input: Array.from({ length: count }, () => "x") }, { url: "/v1/embeddings" });
  expect(code(`${embed("a", 25000)}\n${embed("b", 25000)}`)).toBeUndefined();
  expect(code(`${embed("a", 25000)}\n${embed("b", 25001)}`)).toEqual([400, "invalid_batch"]);
  const image = (custom: string, n?: unknown) =>
    line(custom, n === undefined ? {} : { n }, { url: "/v1/images/generations" });
  expect(parseBatchInput(encode(`${image("a", 3)}\n${image("b")}`)).images).toBe(4);
  for (const n of [0, 1.5, "2"]) expect(code(image("a", n))).toEqual([400, "invalid_batch"]);
});

test("decimal quantities are summed exactly without floating point drift", () => {
  const total: Record<string, string> = {};
  sumQuantities(total, { input_tokens: "9007199254740993", audio_seconds: "0.1" });
  sumQuantities(total, { input_tokens: "1", audio_seconds: "0.2", output_tokens: "3" });
  expect(total).toEqual({
    input_tokens: "9007199254740994",
    audio_seconds: "0.3",
    output_tokens: "3",
  });
});

test("batch output lines report success only for 2xx responses carrying a body", () => {
  const lines = parseBatchOutput(
    encode(
      [
        JSON.stringify({ custom_id: "a", response: { status_code: 200, body: { usage: {} } } }),
        JSON.stringify({ custom_id: "b", response: { status_code: 429, body: { error: {} } } }),
        JSON.stringify({ custom_id: "c", response: null, error: { code: "batch_expired" } }),
      ].join("\n"),
    ),
  );
  expect(lines.map((entry) => [entry.customId, entry.ok, entry.body !== undefined])).toEqual([
    ["a", true, true],
    ["b", false, false],
    ["c", false, false],
  ]);
  expect(() => parseBatchOutput(encode(JSON.stringify({ response: {} })))).toThrow();
});

test("upstream file identities in a batch object are replaced by gateway identities or hidden", () => {
  const body = encode(
    JSON.stringify({
      id: "batch_x",
      input_file_id: "file-in",
      output_file_id: "file-out",
      error_file_id: null,
    }),
  );
  expect(
    JSON.parse(
      new TextDecoder().decode(exposeBatchFiles(body, new Map([["file-in", "file_public"]]))),
    ),
  ).toEqual({
    id: "batch_x",
    input_file_id: "file_public",
    output_file_id: null,
    error_file_id: null,
  });
  expect(() =>
    exposeBatchFiles(encode(JSON.stringify({ output_file_id: "a b" })), new Map()),
  ).toThrow(expect.objectContaining({ code: "upstream_invalid_response" }));
});
