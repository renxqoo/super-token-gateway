import type { Hono } from "hono";
import { obj } from "./http.ts";

export function registerBedrock(app: Hono): void {
  app.post("/bedrock/model/:model/:action", async (c) => {
    const input = obj(await c.req.json());
    if (
      !c.req.header("authorization")?.startsWith("AWS4-HMAC-SHA256 ") ||
      input.anthropic_version !== "bedrock-2023-05-31" ||
      input.model ||
      input.stream !== undefined
    )
      return c.json({ error: "invalid bedrock request" }, 400);
    input.model = c.req.param("model");
    input.stream = c.req.param("action") === "invoke-with-response-stream";
    const response = await app.fetch(
      new Request("http://fixture/anthropic/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": "fixture-bedrock",
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify(input),
      }),
    );
    if (!input.stream || !response.ok) return response;
    const crc = (bytes: Uint8Array) => {
      let result = 0xffffffff;
      for (const byte of bytes) {
        result ^= byte;
        for (let i = 0; i < 8; i++)
          result = result & 1 ? (result >>> 1) ^ 0xedb88320 : result >>> 1;
      }
      return (result ^ 0xffffffff) >>> 0;
    };
    const frames = (await response.text()).split("\n\n").flatMap((event) => {
      const line = event.split("\n").find((line) => line.startsWith("data: "));
      if (!line) return [];
      const header = Buffer.concat(
        Object.entries({
          ":message-type": "event",
          ":event-type": "chunk",
          ":content-type": "application/json",
        }).map(([name, value]) => {
          const length = Buffer.alloc(2);
          length.writeUInt16BE(value.length);
          return Buffer.concat([
            Buffer.from([name.length]),
            Buffer.from(name),
            Buffer.from([7]),
            length,
            Buffer.from(value),
          ]);
        }),
      );
      const payload = Buffer.from(
        JSON.stringify({ bytes: Buffer.from(line.slice(6)).toString("base64") }),
      );
      const frame = Buffer.alloc(header.length + payload.length + 16);
      frame.writeUInt32BE(frame.length);
      frame.writeUInt32BE(header.length, 4);
      frame.writeUInt32BE(crc(frame.subarray(0, 8)), 8);
      header.copy(frame, 12);
      payload.copy(frame, 12 + header.length);
      frame.writeUInt32BE(crc(frame.subarray(0, -4)), frame.length - 4);
      return [frame];
    });
    return new Response(Buffer.concat(frames), {
      headers: { "content-type": "application/vnd.amazon.eventstream" },
    });
  });
}
