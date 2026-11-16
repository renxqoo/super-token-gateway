import { expect, test } from "bun:test";
import { z } from "zod";
import { financialRpcSession } from "../support/financial-execution.ts";
import {
  allowOperation,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

const usage = { input_tokens: 9, output_tokens: 6 };
const remote = (index: number) => `resp_remote_${index}`;

function responseObject(id: string, status = "completed") {
  return { id, object: "response", status, model: "synthetic", output: [], usage };
}

function reply(call: UpstreamCall) {
  const match = /^\/v1\/responses\/(resp_remote_\d+)(\/cancel|\/input_items)?$/.exec(call.path);
  if (match) {
    const [, id, suffix] = match;
    if (suffix === "/input_items")
      return Response.json({
        object: "list",
        data: [{ id: "msg_1", type: "message" }],
        first_id: "msg_1",
        last_id: "msg_1",
        has_more: false,
      });
    if (suffix === "/cancel") return Response.json(responseObject(id!, "cancelled"));
    if (call.method === "DELETE") return Response.json({ id, object: "response", deleted: true });
    return Response.json(responseObject(id!));
  }
  if (call.path !== "/v1/responses") return new Response("not found", { status: 404 });
  if (call.body.stream) {
    const id = remote(call.index);
    const frames = [
      { type: "response.created", sequence_number: 0, response: responseObject(id, "in_progress") },
      { type: "response.output_text.delta", sequence_number: 1, item_id: "m1", delta: "好" },
      { type: "response.completed", sequence_number: 2, response: responseObject(id) },
    ];
    return new Response(
      frames.map((frame) => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(""),
      { headers: { "content-type": "text/event-stream" } },
    );
  }
  return Response.json(responseObject(remote(call.index)));
}

function publicId(response: Response) {
  return `response_${response.headers.get("x-request-id")!.replaceAll("-", "")}`;
}

test(
  "stored responses get a stable gateway identity in JSON and SSE and are retrieved, listed, continued, cancelled and deleted on the original channel without exposing the upstream identity",
  () =>
    withGatewayStack({ reply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "responses");
      const headers = {
        authorization: `Bearer ${stack.setup.key.key}`,
        "content-type": "application/json",
      };
      const create = (body: Record<string, unknown>) =>
        fetch(`${stack.gateway.origin}/v1/responses`, {
          method: "POST",
          headers,
          body: JSON.stringify({ model: stack.setup.model, max_output_tokens: 30, ...body }),
        });
      const call = (path: string, method = "GET") =>
        fetch(`${stack.gateway.origin}${path}`, { method, headers });

      const json = await create({ input: "你好" });
      expect(json.status).toBe(200);
      const first = publicId(json);
      const text = await json.text();
      expect(JSON.parse(text)).toMatchObject({ id: first, object: "response" });
      expect(text).not.toContain("resp_remote");

      const streamed = await create({ input: "你好", stream: true });
      expect(streamed.status).toBe(200);
      const second = publicId(streamed);
      const events = await streamed.text();
      expect(events).toContain(`"id":"${second}"`);
      expect(events).not.toContain("resp_remote");
      expect(events).toContain("response.completed");

      const before = stack.received.length;
      const retrieved = await call(`/v1/responses/${first}`);
      expect(retrieved.status).toBe(200);
      const retrievedText = await retrieved.text();
      expect(JSON.parse(retrievedText)).toMatchObject({ id: first, model: stack.setup.model });
      expect(retrievedText).not.toContain("resp_remote");
      expect(stack.received.at(-1)).toMatchObject({
        path: `/v1/responses/${remote(0)}`,
        authorization: "Bearer synthetic-upstream-secret",
      });

      const items = await call(`/v1/responses/${first}/input_items`);
      expect(items.status).toBe(200);
      expect(await items.json()).toMatchObject({ object: "list", first_id: "msg_1" });

      const listed = await call("/v1/responses?limit=1");
      expect(listed.status).toBe(200);
      const page = (await listed.json()) as { data: { id: string }[]; has_more: boolean };
      expect(page.data.map((row) => row.id)).toEqual([second]);
      expect(page.has_more).toBe(true);
      const next = (await (await call(`/v1/responses?after=${second}`)).json()) as {
        data: { id: string }[];
      };
      expect(next.data.map((row) => row.id)).toEqual([first]);
      expect(stack.received.length).toBe(before + 2);

      const continued = await create({ input: "继续", previous_response_id: first });
      expect(continued.status).toBe(200);
      expect(stack.received.at(-1)!.body.previous_response_id).toBe(remote(0));
      expect(await settledCharge(stack.db, continued.headers.get("x-request-id")!)).toMatchObject({
        state: "SUCCEEDED",
      });

      const cancelled = await call(`/v1/responses/${second}/cancel`, "POST");
      expect(cancelled.status).toBe(200);
      expect(await cancelled.json()).toMatchObject({ id: second, status: "cancelled" });
      expect(stack.received.at(-1)!.path).toBe(`/v1/responses/${remote(1)}/cancel`);

      const deleted = await call(`/v1/responses/${first}`, "DELETE");
      expect(deleted.status).toBe(200);
      expect(stack.received.at(-1)!.path).toBe(`/v1/responses/${remote(0)}`);
      const calls = stack.received.length;
      expect((await call(`/v1/responses/${first}`)).status).toBe(404);
      expect((await create({ input: "继续", previous_response_id: first })).status).toBe(404);
      expect(stack.received.length).toBe(calls);
    }),
  180000,
);

test(
  "a stored response is invisible to other projects and to keys restricted to other models, and only its owning Gateway session may record it",
  () =>
    withGatewayStack({ reply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "responses");
      const created = await fetch(`${stack.gateway.origin}/v1/responses`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${stack.setup.key.key}`,
          "content-type": "application/json",
        },
        body: JSON.stringify({ model: stack.setup.model, max_output_tokens: 30, input: "x" }),
      });
      expect(created.status).toBe(200);
      const id = publicId(created);

      const project = z.object({ id: z.uuid() }).parse(
        (
          await stack.f.request("/projects", {
            name: `other-${crypto.randomUUID()}`,
            billing_currency: "USD",
            rpm: 1000,
            tpm: 10000000,
            concurrency: 100,
          })
        ).value,
      );
      const keyIn = async (projectId: string, models: string[]) =>
        z.object({ key: z.string() }).parse(
          (
            await stack.f.request(`/projects/${projectId}/keys`, {
              name: "isolation",
              models,
              operations: ["responses"],
            })
          ).value,
        ).key;
      const foreign = await keyIn(project.id, [stack.setup.model]);
      const unrelated = await keyIn(stack.setup.project.id, [`unrelated-${crypto.randomUUID()}`]);
      const calls = stack.received.length;
      for (const key of [foreign, unrelated]) {
        const response = await fetch(`${stack.gateway.origin}/v1/responses/${id}`, {
          headers: { authorization: `Bearer ${key}` },
        });
        expect(response.status).toBe(404);
        const listed = await fetch(`${stack.gateway.origin}/v1/responses`, {
          headers: { authorization: `Bearer ${key}` },
        });
        expect(((await listed.json()) as { data: unknown[] }).data).toEqual([]);
      }
      const unknown = await fetch(
        `${stack.gateway.origin}/v1/responses/response_${"0".repeat(32)}`,
        { headers: { authorization: `Bearer ${stack.setup.key.key}` } },
      );
      expect(unknown.status).toBe(404);
      expect(stack.received.length).toBe(calls);

      const session = await financialRpcSession(stack.f);
      const action = {
        customer_token: stack.setup.key.key,
        ip: "127.0.0.1",
        resource_id: id,
        action: "cancel",
        auth: { type: "bearer" },
      };
      const command = {
        commandId: crypto.randomUUID(),
        deadlineAt: new Date(Date.now() + 20000).toISOString(),
      };
      const granted = await session.send("execution_resource_v1", action, command);
      expect(granted.status).toBe(200);
      expect(granted.value).toMatchObject({
        state: "GRANTED",
        auth_material: { type: "bearer", value: "synthetic-upstream-secret" },
      });
      const replayed = await session.send("execution_resource_v1", action, command);
      expect(replayed.value.state).toBe("REPLAYED");
      expect(replayed.value.auth_material).toBeUndefined();

      const attempt = (
        await stack.db`SELECT a.id::text, p.permit_epoch::text epoch FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id WHERE a.request_id=${created.headers.get("x-request-id")!}::uuid`
      )[0] as { id: string; epoch: string };
      const forged = await session.send("execution_resource_commit_v1", {
        attempt_id: attempt.id,
        permit_epoch: attempt.epoch,
        kind: "response",
        remote_id: "resp_attacker",
        state: "COMPLETED",
      });
      expect(forged.status).toBe(403);
      expect(forged.value.code).toBe("internal_owner_mismatch");
      expect(
        (await stack.db`SELECT count(*)::int n FROM resources WHERE remote_id='resp_attacker'`)[0]
          .n,
      ).toBe(0);
    }),
  180000,
);

test(
  "an upstream identity already recorded for another request is not handed out under a second gateway identity",
  () =>
    withGatewayStack(
      { reply: () => Response.json(responseObject("resp_reused")), supplierCredit: "1000000" },
      async (stack) => {
        await allowOperation(stack, "responses");
        const send = () =>
          fetch(`${stack.gateway.origin}/v1/responses`, {
            method: "POST",
            headers: {
              authorization: `Bearer ${stack.setup.key.key}`,
              "content-type": "application/json",
            },
            body: JSON.stringify({ model: stack.setup.model, max_output_tokens: 30, input: "x" }),
          });
        const first = await send();
        expect(first.status).toBe(200);
        const second = await send();
        expect(second.status).toBe(502);
        const body = await second.text();
        expect(JSON.parse(body).error.code).toBe("upstream_invalid_response");
        expect(body).not.toContain("resp_reused");
        expect(body).not.toContain(publicId(first));
        expect(await stack.db`SELECT id FROM resources WHERE remote_id='resp_reused'`).toEqual([
          { id: publicId(first) },
        ]);
      },
    ),
  180000,
);

test(
  "VIEWER and BILLING keys of the owning project cannot cancel or delete a stored response, and the upstream resource is never touched",
  () =>
    withGatewayStack({ reply, supplierCredit: "1000000" }, async (stack) => {
      await allowOperation(stack, "responses");
      const owner = { authorization: `Bearer ${stack.setup.key.key}` };
      const created = await fetch(`${stack.gateway.origin}/v1/responses`, {
        method: "POST",
        headers: { ...owner, "content-type": "application/json" },
        body: JSON.stringify({ model: stack.setup.model, max_output_tokens: 30, input: "x" }),
      });
      expect(created.status).toBe(200);
      const id = publicId(created);
      const calls = stack.received.length;
      for (const role of ["VIEWER", "BILLING"]) {
        const { key } = z.object({ key: z.string() }).parse(
          (
            await stack.f.request(`/projects/${stack.setup.project.id}/keys`, {
              name: role.toLowerCase(),
              role,
              models: [stack.setup.model],
              operations: ["responses"],
            })
          ).value,
        );
        for (const [path, method] of [
          [`/v1/responses/${id}/cancel`, "POST"],
          [`/v1/responses/${id}`, "DELETE"],
        ] as const) {
          const response = await fetch(`${stack.gateway.origin}${path}`, {
            method,
            headers: { authorization: `Bearer ${key}` },
          });
          expect([role, method, response.status, (await response.json()).error.code]).toEqual([
            role,
            method,
            403,
            "permission_denied",
          ]);
        }
      }
      expect(stack.received.length).toBe(calls);
      const retrieved = await fetch(`${stack.gateway.origin}/v1/responses/${id}`, {
        headers: owner,
      });
      expect(retrieved.status).toBe(200);
      expect(await retrieved.json()).toMatchObject({ id, status: "completed" });
    }),
  180000,
);
