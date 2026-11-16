import { expect, test } from "bun:test";
import { GatewayError } from "@gateway/contracts";
import { readJsonBody } from "../request-body.ts";

function post(body: ReadableStream<Uint8Array> | string, signal?: AbortSignal) {
  return new Request("http://core.test/", {
    method: "POST",
    body,
    ...(signal ? { signal } : {}),
    duplex: "half",
  } as RequestInit);
}

function stalled(prefix: string, released: { value: boolean }) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(prefix));
    },
    cancel() {
      released.value = true;
    },
  });
}

async function failure(pending: Promise<unknown>) {
  try {
    await pending;
  } catch (error) {
    return error;
  }
  throw new Error("expected a failure");
}

test("a body that stops arriving before its time budget fails with 408 body_timeout and releases its source", async () => {
  const released = { value: false };
  const error = await failure(readJsonBody(post(stalled('{"name":"x', released)), 1024, 50));
  expect(error).toBeInstanceOf(GatewayError);
  expect([(error as GatewayError).status, (error as GatewayError).code]).toEqual([
    408,
    "body_timeout",
  ]);
  expect(released.value).toBe(true);
});

test("a body larger than its byte budget fails with 413 body_too_large, not as a timeout", async () => {
  const error = await failure(readJsonBody(post(`{"name":"${"x".repeat(2000)}"}`), 1024, 5000));
  expect([(error as GatewayError).status, (error as GatewayError).code]).toEqual([
    413,
    "body_too_large",
  ]);
});

test("a client that disconnects is not reported as a body timeout", async () => {
  const controller = new AbortController();
  const pending = readJsonBody(post(stalled("{", { value: false }), controller.signal), 1024, 5000);
  controller.abort();
  const error = await failure(pending);
  expect(error instanceof GatewayError && error.code === "body_timeout").toBe(false);
});

test("a complete body within both budgets is parsed as JSON", async () => {
  expect(await readJsonBody(post('{"name":"项目"}'), 1024, 5000)).toEqual({ name: "项目" });
});
