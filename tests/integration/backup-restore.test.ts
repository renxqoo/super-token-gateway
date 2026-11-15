import { expect, test } from "bun:test";
import { rm } from "node:fs/promises";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import {
  allowOperation,
  chatReplies,
  eventually,
  settledCharge,
  withGatewayStack,
  type UpstreamCall,
} from "../support/gateway-process.ts";

function reply(call: UpstreamCall) {
  if (call.path === "/v1/files" && call.method === "POST")
    return Response.json({
      id: `file-up-${call.index}`,
      object: "file",
      bytes: 3,
      filename: "notes.txt",
      purpose: "assistants",
      status: "processed",
    });
  return chatReplies.json("机密回答");
}

async function verifyBackup(databaseUrl: string, namespace: string) {
  const child = Bun.spawn(["bun", "scripts/verify-backup.ts", "--quiesced"], {
    env: {
      PATH: process.env.PATH,
      DATABASE_URL: databaseUrl,
      QUEUE_REDIS_URL: process.env.QUEUE_REDIS_URL,
      REDIS_NAMESPACE: namespace,
      PG_TOOLS_DIRECTORY: process.env.PG_TOOLS_DIRECTORY,
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, code] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { code, stdout, stderr };
}

test(
  "a quiesced backup of v2 traffic is refused while settlement evidence is queued and restores identical ledgers, acknowledgements, plugin catalog and resources",
  () =>
    withGatewayStack({ reply, supplierCredit: "1000000" }, async (stack) => {
      const { f, db } = stack;
      await allowOperation(stack, "files");
      const chat = await stack.send({ content: "机密提示" });
      expect(chat.status).toBe(200);
      const form = new FormData();
      form.set("purpose", "assistants");
      form.set("file", new File(["abc"], "notes.txt", { type: "text/plain" }));
      const uploaded = await fetch(`${stack.gateway.origin}/v1/files`, {
        method: "POST",
        headers: { authorization: `Bearer ${stack.setup.key.key}` },
        body: form,
      });
      expect(uploaded.status).toBe(200);
      for (const response of [chat, uploaded])
        await settledCharge(db, response.headers.get("x-request-id")!);
      const consumer = String(
        (await f.request("/consumers", { name: "backup", replay: true })).value.id,
      );
      const claimed = (await f.request(`/consumers/${consumer}/claim`, { limit: 500 })).value as {
        lease_token: string;
        events: { id: string }[];
      };
      expect(claimed.events.length).toBeGreaterThan(0);
      await f.request(`/consumers/${consumer}/ack`, {
        lease_token: claimed.lease_token,
        event_ids: claimed.events.map((event) => event.id),
      });

      const url = new URL(process.env.DATABASE_URL!);
      url.pathname = `/${String((await db`SELECT current_database() AS name`)[0].name)}`;
      await stack.worker.stop();
      const pending = await stack.send({ content: "机密提示-2" });
      expect(pending.status).toBe(200);
      await stack.gateway.stop();
      const refused = await verifyBackup(url.toString(), stack.namespace);
      expect(refused.code).not.toBe(0);
      expect(refused.stderr).toContain(
        "Queues must have no waiting, active, delayed or failed evidence",
      );

      const worker = await stack.startWorker();
      await settledCharge(db, pending.headers.get("x-request-id")!);
      const queue = new UsageProducer({
        channel: evidenceChannel,
        url: process.env.QUEUE_REDIS_URL!,
        namespace: stack.namespace,
        replicas: 0,
        timeoutMs: 2000,
      });
      try {
        await eventually(
          async () => Object.values(await queue.stats()).every((n) => n === 0),
          "settlement queue drained",
        );
      } finally {
        queue.close();
      }
      await worker.stop();
      const verified = await verifyBackup(url.toString(), stack.namespace);
      expect(verified.stderr).toBe("");
      expect(verified.code).toBe(0);
      const result = JSON.parse(verified.stdout.trim().split("\n").at(-1)!) as Record<
        string,
        unknown
      >;
      try {
        expect(result).toMatchObject({
          backup_restore: "pass",
          inconsistent_wallets: 0,
          inconsistent_credit: 0,
          inconsistent_upstream: 0,
          unbalanced_upstream: 0,
          unbalanced: 0,
          deliveries: { ACKED: String(claimed.events.length) },
        });
        expect(Number((result.evidence_kinds as Record<string, string>).RAW)).toBeGreaterThan(0);
        expect(
          (await db`SELECT count(*)::int AS n FROM resources WHERE binding_id IS NOT NULL`)[0].n,
        ).toBe(1);
        expect(verified.stdout).not.toContain("机密");
      } finally {
        await rm(String(result.file), { force: true });
      }
      expect(
        (
          await db`SELECT count(*)::int AS n FROM pg_database WHERE datname LIKE 'gateway_restore_%'`
        )[0].n,
      ).toBe(0);
    }),
  240000,
);
