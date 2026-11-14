import { expect, test } from "bun:test";
import { collectReferences } from "../references.ts";

const refused = { status: 422, code: "unmanaged_resource" };
const owned = "response_0123456789abcdef0123456789abcdef";

test("client-defined schemas and tool payloads may use resource-like field names", () => {
  expect(
    collectReferences(
      {
        tools: [
          {
            type: "function",
            function: {
              parameters: { type: "object", properties: { file_id: { type: "string" } } },
            },
          },
        ],
        messages: [
          { role: "assistant", content: [{ type: "tool_use", input: { file_id: "x" } }] },
          { role: "user", parts: [{ functionCall: { args: { conversation: "c" } } }] },
        ],
      },
      false,
    ),
  ).toEqual([]);
});

test("inline file data and explicit nulls are not upstream resource references", () => {
  expect(
    collectReferences(
      {
        messages: [
          { content: [{ type: "file", file: { file_data: "data:application/pdf;base64,AA" } }] },
        ],
        previous_response_id: null,
        prompt: "text prompt",
        container: { type: "auto" },
      },
      false,
    ),
  ).toEqual([]);
});

test("references nested anywhere outside opaque payloads are refused", () => {
  for (const body of [
    { input: [{ content: [{ type: "input_image", file_id: "file-x" }] }] },
    { messages: [{ content: [{ source: { type: "file", file_id: "file-x" } }] }] },
    { contents: [{ parts: [{ fileData: { fileUri: "gs://b/o" } }] }] },
    { cachedContent: "cachedContents/x" },
    { tools: [{ type: "code_interpreter", container: "cntr_x" }] },
  ])
    expect(() => collectReferences(body, true)).toThrow(expect.objectContaining(refused));
});

test("a gateway response identity is collected with its pointer only where it may be resolved", () => {
  expect(collectReferences({ input: "x", previous_response_id: owned }, true)).toEqual([
    { pointer: "/previous_response_id", resourceId: owned },
  ]);
  for (const [body, resolvable] of [
    [{ previous_response_id: owned }, false],
    [{ previous_response_id: "resp_upstream" }, true],
    [{ previous_response_id: "file_0123456789abcdef0123456789abcdef" }, true],
    [{ input: [{ previous_response_id: owned }] }, true],
  ] as const)
    expect(() => collectReferences(body, resolvable)).toThrow(expect.objectContaining(refused));
});

test("a gateway file identity is collected wherever a file_id appears, while upstream file identities stay refused", () => {
  const id = `file_${"c".repeat(32)}`;
  const collected = collectReferences(
    {
      input: [{ role: "user", content: [{ type: "input_file", file_id: id }] }],
      messages: [{ content: [{ type: "file", file: { file_id: id } }] }],
    },
    true,
  );
  expect(collected).toHaveLength(2);
  expect(collected).toEqual(
    expect.arrayContaining([
      { pointer: "/messages/0/content/0/file/file_id", resourceId: id },
      { pointer: "/input/0/content/0/file_id", resourceId: id },
    ]),
  );
  for (const [body, resolvable] of [
    [{ input: [{ file_id: "file-upstream" }] }, true],
    [{ input: [{ file_id: id }] }, false],
  ] as const)
    expect(() => collectReferences(body, resolvable)).toThrow(
      expect.objectContaining({ code: "unmanaged_resource" }),
    );
});
test("deeply nested bodies are walked without exhausting the call stack", () => {
  let nested: unknown = { file_id: "file-x" };
  for (let depth = 0; depth < 100000; depth++) nested = [nested];
  expect(() => collectReferences({ input: nested }, false)).toThrow(
    expect.objectContaining(refused),
  );
});
