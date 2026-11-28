import { expect, test } from "bun:test";
import { transformRequestTexts } from "../upstream-content.ts";

test("Responses request policy includes text in both forms of function-call results", async () => {
  const result = await transformRequestTexts(
    {
      input: [
        { type: "function_call_output", call_id: "call_one", output: "first" },
        {
          type: "function_call_output",
          call_id: "call_two",
          output: [{ type: "input_text", text: "second" }],
        },
      ],
    },
    "responses",
    async (text) => ({ blocked: false, text: text.toUpperCase() }),
  );

  expect(result).toEqual({
    blocked: false,
    body: {
      input: [
        { type: "function_call_output", call_id: "call_one", output: "FIRST" },
        {
          type: "function_call_output",
          call_id: "call_two",
          output: [{ type: "input_text", text: "SECOND" }],
        },
      ],
    },
  });
});
