import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { createSettlementStore } from "@gateway/storage/settlement";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";
import { financialRejection } from "../support/financial-rejection.ts";

test(
  "technical quota expiry skips locked requests, clears missing Redis tombstones after the total deadline, preserves unknown money and accepts late real usage",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      const worker = f.client(f.identity.b.port, "worker");
      await client.startup();
      await worker.startup();
      const deadline = new Date(Date.now() + 3500).toISOString();
      const unknown = await granted(db, setup, client, { deadline });
      const success = await granted(db, setup, client, { deadline });
      const store = createSettlementStore(db);
      committed(
        (
          await store.persist([
            {
              events: [
                event(success.owner, success.prepared.attemptId, {
                  finality: "FINAL",
                  usage: { input_tokens: "30", output_tokens: "15" },
                  final_dimensions: ["input_tokens", "output_tokens"],
                  attempt_state: "SUCCEEDED",
                  request_state: "SUCCEEDED",
                }),
              ],
            },
          ])
        )[0],
      );
      const leases = await db`SELECT request_id,descriptor FROM execution_quota_leases`;
      const redis = new RedisClient(process.env.REDIS_URL!);
      try {
        for (const lease of leases) {
          const keys = (await redis.send("KEYS", [
            `${lease.descriptor.namespace}:*:lease:${lease.descriptor.lease_id}`,
          ])) as string[];
          expect(keys).toHaveLength(1);
          await redis.send("DEL", keys);
        }
      } finally {
        redis.close();
      }
      expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
        released: 0,
        pending: 3,
        failed: 3,
        expired: 0,
        expiry_more: false,
      });
      expect(
        await db`SELECT execution_closed_at FROM gateway_requests WHERE id=${unknown.prepared.requestId}`,
      ).toEqual([{ execution_closed_at: null }]);
      await Bun.sleep(Math.max(0, Date.parse(deadline) - Date.now() + 20));
      await db.begin(async (tx) => {
        await tx`SELECT id FROM gateway_requests ORDER BY id FOR UPDATE`;
        expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
          released: 0,
          pending: 3,
          failed: 3,
          expired: 0,
          expiry_more: false,
        });
      });
      expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
        released: 0,
        pending: 3,
        failed: 3,
        expired: 2,
        expiry_more: false,
      });
      expect(
        await db`SELECT state,count(*)::integer n FROM execution_quota_leases GROUP BY state`,
      ).toEqual([{ state: "EXPIRED", n: 6 }]);
      expect(
        await db`SELECT execution_closed_at IS NOT NULL closed FROM gateway_requests WHERE id=${unknown.prepared.requestId}`,
      ).toEqual([{ closed: true }]);
      expect(
        await db`SELECT state FROM execution_permits WHERE attempt_id=${unknown.prepared.attemptId}`,
      ).toEqual([{ state: "SEND_GRANTED" }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "30",
        spent: "15",
        state: "ACTIVE",
      });
      expect(
        await db`SELECT reserved_atoms::text,spent_atoms::text FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ reserved_atoms: "100", spent_atoms: "100" }]);
      const next = await prepareFinancialAdmission(setup, client, {
        requestId: unknown.prepared.requestId,
      });
      const rejection = await financialRejection(
        client.execution(
          "execution_next_v1",
          {
            context_id: next.data.context_id,
            dispatch_projection: next.data.dispatch_projection,
            dispatched_digest: next.data.dispatched_digest,
            manifest: next.data.manifest,
            prep_session_id: next.data.prep_session_id,
            prep_binding_digest: next.data.prep_binding_digest,
            request_id: unknown.prepared.requestId,
            previous_attempt_id: unknown.prepared.attemptId,
            expected_ordinal: 2,
            visible_output: false,
          },
          { deadlineAt: next.deadline },
        ),
        409,
      );
      expect(rejection.receipt.code).toBe("execution_closed");
      committed(
        (
          await store.persist([
            {
              events: [
                event(unknown.owner, unknown.prepared.attemptId, {
                  finality: "FINAL",
                  usage: { input_tokens: "30", output_tokens: "15" },
                  final_dimensions: ["input_tokens", "output_tokens"],
                  attempt_state: "SUCCEEDED",
                  request_state: "SUCCEEDED",
                }),
              ],
            },
          ])
        )[0],
      );
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "970",
        reserved: "0",
        spent: "30",
        state: "ACTIVE",
      });
      expect(
        await db`SELECT reserved_atoms::text,spent_atoms::text FROM wallets WHERE project_id=${setup.project.id}`,
      ).toEqual([{ reserved_atoms: "0", spent_atoms: "200" }]);
      expect(await worker.execution("execution_quota_recovery_v1", { limit: 100 })).toEqual({
        released: 0,
        pending: 0,
        failed: 0,
        expired: 0,
        expiry_more: false,
      });
      expect(await db`SELECT count(*)::integer n FROM attempts`).toEqual([{ n: 2 }]);
    }),
  30000,
);
