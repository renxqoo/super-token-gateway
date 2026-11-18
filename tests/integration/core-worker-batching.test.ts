import { expect, test } from "bun:test";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { createSettlementWorker } from "../../apps/core-worker/src/settlement/worker.ts";
import { withFinancial } from "../support/financial-execution.ts";
import { event, granted } from "../support/core-settlement.ts";
import { createUsageQueueCleanup } from "../support/usage-queue-cleanup.ts";

test("separate ready evidence jobs share bounded persistence instead of each opening its own batch", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "100000");
    const client = f.client();
    await client.startup();
    const groups = [];
    for (let index = 0; index < 8; index++) {
      const attempt = await granted(db, setup, client);
      groups.push({
        events: [
          event(attempt.owner, attempt.prepared.attemptId, {
            finality: "FINAL",
            usage: { input_tokens: "1", output_tokens: "0" },
            final_dimensions: ["input_tokens", "output_tokens"],
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
          }),
        ],
      });
    }
    await db.unsafe(`
      CREATE TABLE public.worker_persistence_trace(groups integer NOT NULL);
      ALTER FUNCTION public.persist_core_evidence_groups_v1(jsonb) RENAME TO worker_persistence_original;
      CREATE FUNCTION public.persist_core_evidence_groups_v1(p_groups jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
      BEGIN
        INSERT INTO public.worker_persistence_trace VALUES(jsonb_array_length(p_groups));
        PERFORM pg_sleep(0.05);
        RETURN public.worker_persistence_original(p_groups);
      END $$;
    `);
    const namespace = `test-usage:worker-batching:${crypto.randomUUID()}`;
    const cleanup = createUsageQueueCleanup(namespace);
    const options = {
      url: process.env.QUEUE_REDIS_URL!,
      namespace,
      replicas: 0,
      timeoutMs: 1000,
    };
    const producer = new UsageProducer({ ...options, channel: evidenceChannel, batchSize: 1 });
    let worker: ReturnType<typeof createSettlementWorker> | undefined;
    const drained = async () => {
      const until = Date.now() + 15000;
      for (;;) {
        const stats = await producer.stats();
        if (Object.values(stats).every((count) => count === 0)) return;
        if (Date.now() >= until) throw new Error("worker batching drain deadline");
        await Bun.sleep(10);
      }
    };
    try {
      await producer.ready();
      await Promise.all(groups.map((group) => producer.publish(group)));
      expect((await producer.stats()).waiting).toBe(8);
      worker = createSettlementWorker({ ...options, db, concurrency: 8 });
      await worker.ready();
      await drained();
      const batches = await db<{ groups: number }[]>`SELECT groups FROM worker_persistence_trace`;
      expect(batches.some((batch) => batch.groups > 1)).toBe(true);
      expect(batches.every((batch) => batch.groups >= 1 && batch.groups <= 128)).toBe(true);
      expect(
        await db`SELECT count(*)::int n FROM core_evidence_receipts WHERE outcome='ACCEPTED'`,
      ).toEqual([{ n: 8 }]);
      expect(await db`SELECT count(*)::int n FROM billing_charges`).toEqual([{ n: 8 }]);
      expect(
        await db`SELECT sum(reserved_atoms)::text reserved,sum(spent_atoms)::text spent FROM wallets`,
      ).toEqual([{ reserved: "0", spent: "800" }]);
      expect(
        await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM core_upstream_balances_v1 WHERE account_id=${setup.provider.account_id}`,
      ).toEqual([{ balance: "99992", reserved: "0", spent: "8" }]);
      await Promise.all(groups.map((group) => producer.publish(group)));
      await drained();
      expect(await db`SELECT count(*)::int n FROM core_evidence_receipts`).toEqual([{ n: 8 }]);
      expect(await db`SELECT count(*)::int n FROM billing_charges`).toEqual([{ n: 8 }]);
      expect(
        await db`SELECT sum(reserved_atoms)::text reserved,sum(spent_atoms)::text spent FROM wallets`,
      ).toEqual([{ reserved: "0", spent: "800" }]);
    } finally {
      await worker?.close();
      producer.close();
      await cleanup.remove(options.url);
      expect(await cleanup.keys(options.url)).toEqual([]);
    }
  }));
