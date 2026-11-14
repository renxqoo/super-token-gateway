import { expect, test } from "bun:test";
import {
  allowOperation,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

type Stack = Parameters<Parameters<typeof withGatewayStack>[1]>[0];

const tokenTariff = {
  pricing: {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "10" },
      output_tokens: { numerator: "2", denominator: "10" },
    },
  },
  ceilings: { input_tokens: "5000", output_tokens: "1000" },
  basis: "PROVIDER_USAGE",
};

function completion(custom: string, input: number, output: number) {
  return JSON.stringify({
    id: `out-${custom}`,
    custom_id: custom,
    response: {
      status_code: 200,
      body: {
        id: `chatcmpl-${custom}`,
        object: "chat.completion",
        model: "synthetic",
        choices: [
          { index: 0, message: { role: "assistant", content: "好" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: input, completion_tokens: output, total_tokens: input + output },
      },
    },
    error: null,
  });
}

function upstream(final: "completed" | "cancelled", output: string[]) {
  const batches = new Map<string, { polls: number; cancelled: boolean; input: string }>();
  return (call: UpstreamCall) => {
    if (call.path === "/v1/files" && call.method === "POST")
      return Response.json({
        id: `file-up-${call.index}`,
        object: "file",
        bytes: 1,
        filename: "batch.jsonl",
        purpose: "batch",
        status: "processed",
      });
    if (call.path === "/v1/batches" && call.method === "POST") {
      const id = `batch_up_${call.index}`;
      batches.set(id, { polls: 0, cancelled: false, input: String(call.body.input_file_id) });
      return Response.json({
        id,
        object: "batch",
        endpoint: "/v1/chat/completions",
        input_file_id: call.body.input_file_id,
        status: "validating",
        output_file_id: null,
        error_file_id: null,
      });
    }
    const match = /^\/v1\/batches\/(batch_up_\d+)(\/cancel)?$/.exec(call.path);
    const entry = match ? batches.get(match[1]!) : undefined;
    if (match && entry) {
      if (match[2]) entry.cancelled = true;
      const done = match[2] ? false : entry.polls++ >= 1 || entry.cancelled;
      const status = match[2]
        ? "cancelling"
        : done
          ? entry.cancelled
            ? "cancelled"
            : final
          : "in_progress";
      return Response.json({
        id: match[1],
        object: "batch",
        endpoint: "/v1/chat/completions",
        input_file_id: entry.input,
        status,
        output_file_id: done ? "file-out" : null,
        error_file_id: done && entry.cancelled ? "file-err" : null,
      });
    }
    if (call.path === "/v1/files/file-out/content")
      return new Response(`${output.join("\n")}\n`, {
        headers: { "content-type": "application/jsonl" },
      });
    if (call.path === "/v1/files/file-err/content")
      return new Response(
        `${JSON.stringify({
          id: "err-b",
          custom_id: "b",
          response: null,
          error: { code: "batch_cancelled", message: "cancelled" },
        })}\n`,
        { headers: { "content-type": "application/jsonl" } },
      );
    if (call.path === "/v1/files/file-out")
      return Response.json({
        id: "file-out",
        object: "file",
        bytes: 1,
        filename: "out.jsonl",
        purpose: "batch_output",
        status: "processed",
      });
    return new Response("not found", { status: 404 });
  };
}

function rows(stack: Stack, ids: string[]) {
  return ids
    .map((id) =>
      JSON.stringify({
        custom_id: id,
        method: "POST",
        url: "/v1/chat/completions",
        body: {
          model: stack.setup.model,
          max_tokens: 20,
          messages: [{ role: "user", content: `你好 ${id}` }],
        },
      }),
    )
    .join("\n");
}

async function prepare(stack: Stack) {
  await allowOperation(stack, "files");
  await allowOperation(stack, "batches", tokenTariff);
  const authorization = `Bearer ${stack.setup.key.key}`;
  const upload = (content: string, purpose = "batch") => {
    const form = new FormData();
    form.set("purpose", purpose);
    form.set("file", new File([content], "batch.jsonl", { type: "application/jsonl" }));
    return fetch(`${stack.gateway.origin}/v1/files`, {
      method: "POST",
      headers: { authorization },
      body: form,
    });
  };
  const create = (body: Record<string, unknown>) =>
    fetch(`${stack.gateway.origin}/v1/batches`, {
      method: "POST",
      headers: { authorization, "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  const call = (path: string, method = "GET") =>
    fetch(`${stack.gateway.origin}${path}`, { method, headers: { authorization } });
  return { upload, create, call };
}

async function errorCode(response: Response) {
  return ((await response.json()) as { error: { code: string } }).error.code;
}

test(
  "a batch upload is validated and rewritten per row, its batch is admitted against the summed row ceilings and settled from the output file without exposing upstream file identities",
  () =>
    withGatewayStack(
      {
        reply: upstream("completed", [completion("a", 10, 5), completion("b", 20, 15)]),
        supplierCredit: "1000000",
      },
      async (stack) => {
        const api = await prepare(stack);
        const before = stack.received.length;
        for (const [content, code] of [
          [`${rows(stack, ["a"])}\n${rows(stack, ["a"])}`, "invalid_batch"],
          [rows(stack, ["a"]).replace("/v1/chat/completions", "/v1/audio/speech"), "invalid_batch"],
          [
            rows(stack, ["a"]).replace(
              '"messages"',
              '"tools":[{"type":"file_search","file_id":"file-x"}],"messages"',
            ),
            "unmanaged_resource",
          ],
        ] as const) {
          const refused = await api.upload(content);
          expect(refused.status).toBeGreaterThanOrEqual(400);
          expect(await errorCode(refused)).toBe(code);
        }
        expect(stack.received.length).toBe(before);

        const uploaded = await api.upload(rows(stack, ["a", "b"]));
        expect(uploaded.status).toBe(200);
        const fileId = `file_${uploaded.headers.get("x-request-id")!.replaceAll("-", "")}`;
        expect(((await uploaded.json()) as { id: string }).id).toBe(fileId);
        const sent = stack.received.at(-1)!;
        const lines = sent.raw
          .split(/\r?\n/)
          .filter((line) => line.startsWith('{"custom_id"'))
          .map((line) => JSON.parse(line) as { custom_id: string; body: { model: string } });
        expect(lines.map((line) => [line.custom_id, line.body.model])).toEqual([
          ["a", "synthetic"],
          ["b", "synthetic"],
        ]);
        const stored = (
          await stack.db`SELECT batch_endpoint,batch_upstream_model,batch_manifest,batch_ceilings FROM resources WHERE id=${fileId}`
        )[0];
        expect(stored).toMatchObject({
          batch_endpoint: "/v1/chat/completions",
          batch_upstream_model: "synthetic",
          batch_manifest: { items: 2 },
        });
        expect(Number(stored.batch_ceilings.output_tokens)).toBe(40);

        expect(
          await errorCode(
            await api.create({
              input_file_id: fileId,
              endpoint: "/v1/embeddings",
              completion_window: "24h",
            }),
          ),
        ).toBe("invalid_batch");

        const created = await api.create({
          input_file_id: fileId,
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
        });
        expect(created.status).toBe(200);
        const requestId = created.headers.get("x-request-id")!,
          batchId = `batch_${requestId.replaceAll("-", "")}`;
        const createdText = await created.text();
        expect(JSON.parse(createdText)).toMatchObject({
          id: batchId,
          input_file_id: fileId,
          status: "validating",
        });
        expect(createdText).not.toContain("file-up");
        expect(stack.received.at(-1)!.body.input_file_id).toMatch(/^file-up-/);

        expect(await settledCharge(stack.db, requestId)).toEqual({
          amount: "7",
          state: "SUCCEEDED",
          billing_outcome: "SUCCESS",
        });
        expect(
          await stack.db`SELECT r.source,r.finality FROM core_evidence_receipts r JOIN attempts a ON a.id=r.attempt_id WHERE a.request_id=${requestId}::uuid AND r.source LIKE 'batch_%'`,
        ).toEqual([{ source: "batch_completed", finality: "FINAL" }]);

        const retrieved = await api.call(`/v1/batches/${batchId}`);
        expect(retrieved.status).toBe(200);
        const retrievedText = await retrieved.text();
        const batch = JSON.parse(retrievedText) as Record<string, string>;
        expect(batch).toMatchObject({ id: batchId, status: "completed", input_file_id: fileId });
        expect(batch.output_file_id).toMatch(/^file_[0-9a-f]{32}$/);
        expect(retrievedText).not.toContain("file-out");
        expect(retrievedText).not.toContain("file-up");
        const content = await api.call(`/v1/files/${batch.output_file_id}/content`);
        expect(content.status).toBe(200);
        expect(await content.text()).toContain('"custom_id":"b"');
        const listed = (await (await api.call("/v1/batches")).json()) as { data: { id: string }[] };
        expect(listed.data.map((row) => row.id)).toEqual([batchId]);

        const plain = await api.upload("合成", "assistants");
        expect(plain.status).toBe(200);
        const plainId = `file_${plain.headers.get("x-request-id")!.replaceAll("-", "")}`;
        const calls = stack.received.length;
        expect(
          (
            await api.create({
              input_file_id: plainId,
              endpoint: "/v1/chat/completions",
              completion_window: "24h",
            })
          ).status,
        ).toBe(404);
        expect(stack.received.length).toBe(calls);
      },
    ),
  240000,
);

test(
  "a cancelled batch whose rows are all accounted for is charged only for the rows that finished instead of holding its reservation",
  () =>
    withGatewayStack(
      {
        reply: upstream("completed", [completion("a", 10, 5)]),
        supplierCredit: "1000000",
        gateway: { resource_poll_ms: 600000 },
      },
      async (stack) => {
        const api = await prepare(stack);
        const uploaded = await api.upload(rows(stack, ["a", "b"]));
        const fileId = `file_${uploaded.headers.get("x-request-id")!.replaceAll("-", "")}`;
        const created = await api.create({
          input_file_id: fileId,
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
        });
        expect(created.status).toBe(200);
        const requestId = created.headers.get("x-request-id")!,
          batchId = `batch_${requestId.replaceAll("-", "")}`;
        const cancelled = await api.call(`/v1/batches/${batchId}/cancel`, "POST");
        expect(cancelled.status).toBe(200);
        expect(await cancelled.json()).toMatchObject({ id: batchId, status: "cancelling" });
        await stack.startGateway({ resource_poll_ms: 200 }, "gatewayB");
        expect(await settledCharge(stack.db, requestId)).toEqual({
          amount: "2",
          state: "CANCELLED",
          billing_outcome: "CANCELLED",
        });
        expect(
          await stack.db`SELECT r.source,r.finality FROM core_evidence_receipts r JOIN attempts a ON a.id=r.attempt_id WHERE a.request_id=${requestId}::uuid AND r.source LIKE 'batch_%'`,
        ).toEqual([{ source: "batch_cancelled", finality: "FINAL" }]);
      },
    ),
  240000,
);
