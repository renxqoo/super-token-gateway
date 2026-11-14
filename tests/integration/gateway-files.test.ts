import { expect, test } from "bun:test";
import { z } from "zod";
import {
  allowOperation,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

const content = "合成文件内容\n";

function file(id: string) {
  return {
    id,
    object: "file",
    bytes: new TextEncoder().encode(content).length,
    filename: "notes.txt",
    purpose: "assistants",
    status: "processed",
  };
}

function reply(call: UpstreamCall) {
  if (call.path === "/v1/files" && call.method === "POST")
    return Response.json(file(`file-up-${call.index}`));
  const match = /^\/v1\/files\/(file-up-\d+)(\/content)?$/.exec(call.path);
  if (match && match[2])
    return new Response(content, { headers: { "content-type": "text/plain; charset=utf-8" } });
  if (match && call.method === "DELETE")
    return Response.json({ id: match[1], object: "file", deleted: true });
  if (match) return Response.json(file(match[1]!));
  if (call.path === "/v1/responses")
    return Response.json({
      id: `resp_${call.index}`,
      object: "response",
      status: "completed",
      model: "synthetic",
      output: [],
      usage: { input_tokens: 9, output_tokens: 6 },
    });
  return new Response("not found", { status: 404 });
}

test(
  "uploaded files get a gateway identity, are retrieved, streamed, listed, referenced and deleted on their original channel, and malformed batch input is refused before reaching the upstream",
  () =>
    withGatewayStack({ reply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "files");
      await allowOperation(stack, "responses");
      const authorization = `Bearer ${stack.setup.key.key}`;
      const upload = (purpose: string) => {
        const form = new FormData();
        form.set("purpose", purpose);
        form.set("file", new File([content], "notes.txt", { type: "text/plain" }));
        return fetch(`${stack.gateway.origin}/v1/files`, {
          method: "POST",
          headers: { authorization },
          body: form,
        });
      };
      const call = (path: string, method = "GET") =>
        fetch(`${stack.gateway.origin}${path}`, { method, headers: { authorization } });

      const uploaded = await upload("assistants");
      expect(uploaded.status).toBe(200);
      const requestId = uploaded.headers.get("x-request-id")!,
        id = `file_${requestId.replaceAll("-", "")}`;
      const uploadedText = await uploaded.text();
      expect(JSON.parse(uploadedText)).toMatchObject({ id, object: "file", purpose: "assistants" });
      expect(uploadedText).not.toContain("file-up");
      const sent = stack.received.at(-1)!;
      expect(sent).toMatchObject({ method: "POST", path: "/v1/files" });
      expect(sent.authorization).toBe("Bearer synthetic-upstream-secret");
      expect(sent.raw).toContain('name="purpose"');
      expect(sent.raw).toContain(content);
      expect(sent.raw).not.toContain('name="model"');
      expect(await settledCharge(stack.db, requestId)).toMatchObject({ state: "SUCCEEDED" });

      const retrieved = await call(`/v1/files/${id}`);
      expect(retrieved.status).toBe(200);
      const retrievedText = await retrieved.text();
      expect(JSON.parse(retrievedText)).toMatchObject({ id, object: "file" });
      expect(retrievedText).not.toContain("file-up");
      expect(stack.received.at(-1)!.path).toBe("/v1/files/file-up-0");

      const streamed = await call(`/v1/files/${id}/content`);
      expect(streamed.status).toBe(200);
      expect(streamed.headers.get("content-type")).toBe("text/plain; charset=utf-8");
      expect(streamed.headers.get("cache-control")).toBe("no-store");
      expect(await streamed.text()).toBe(content);

      const listed = (await (await call("/v1/files")).json()) as { data: { id: string }[] };
      expect(listed.data.map((row) => row.id)).toEqual([id]);

      const referenced = await fetch(`${stack.gateway.origin}/v1/responses`, {
        method: "POST",
        headers: { authorization, "content-type": "application/json" },
        body: JSON.stringify({
          model: stack.setup.model,
          max_output_tokens: 30,
          input: [{ role: "user", content: [{ type: "input_file", file_id: id }] }],
        }),
      });
      expect(referenced.status).toBe(200);
      expect(stack.received.at(-1)!.body.input).toEqual([
        { role: "user", content: [{ type: "input_file", file_id: "file-up-0" }] },
      ]);

      const before = stack.received.length;
      const batch = await upload("batch");
      expect(batch.status).toBe(400);
      expect(((await batch.json()) as { error: { code: string } }).error.code).toBe(
        "invalid_batch",
      );
      const project = z.object({ id: z.uuid() }).parse(
        (
          await stack.f.request("/projects", {
            name: `files-${crypto.randomUUID()}`,
            billing_currency: "USD",
            rpm: 1000,
            tpm: 10000000,
            concurrency: 100,
          })
        ).value,
      );
      const foreign = z.object({ key: z.string() }).parse(
        (
          await stack.f.request(`/projects/${project.id}/keys`, {
            name: "isolation",
            models: [stack.setup.model],
            operations: ["files"],
          })
        ).value,
      ).key;
      for (const path of [`/v1/files/${id}`, `/v1/files/${id}/content`])
        expect(
          (
            await fetch(`${stack.gateway.origin}${path}`, {
              headers: { authorization: `Bearer ${foreign}` },
            })
          ).status,
        ).toBe(404);
      expect(stack.received.length).toBe(before);

      const deleted = await call(`/v1/files/${id}`, "DELETE");
      expect(deleted.status).toBe(200);
      expect(await deleted.json()).toMatchObject({ id, deleted: true });
      expect(stack.received.at(-1)).toMatchObject({
        method: "DELETE",
        path: "/v1/files/file-up-0",
      });
      const after = stack.received.length;
      expect((await call(`/v1/files/${id}`)).status).toBe(404);
      expect(stack.received.length).toBe(after);
    }),
  180000,
);
