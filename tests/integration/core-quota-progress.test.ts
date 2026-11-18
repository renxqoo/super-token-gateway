import { expect, test } from "bun:test";
import type { CoreVerifiedCallerProjection } from "@gateway/contracts";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { createExecutionStore } from "@gateway/storage/execution";
import { createSettlementStore } from "@gateway/storage/settlement";
import { createQuotaLeases } from "@gateway/runtime/quota-leases";
import { sha256 } from "@gateway/runtime/crypto";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { committed, event, granted, supplier } from "../support/core-settlement.ts";

test(
  "short retry leases in an ineligible prefix cannot starve a later fully expired request",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      expect(
        (
          await f.request(
            `/providers/${setup.provider.id}`,
            { concurrency: 100, rpm: 1000 },
            undefined,
            "PATCH",
          )
        ).status,
      ).toBe(200);
      const client = f.client();
      const worker = f.client(f.identity.b.port, "worker");
      await client.startup();
      await worker.startup();
      const budget = new Date(Date.now() + 60000).toISOString();
      for (let i = 0; i < 9; i++) {
        const first = await granted(db, setup, client, { deadline: budget });
        const next = await prepareFinancialAdmission(setup, client, {
          requestId: first.prepared.requestId,
          deadline: new Date(Date.now() + 1000).toISOString(),
        });
        await client.execution(
          "execution_next_v1",
          {
            context_id: next.data.context_id,
            dispatch_projection: next.data.dispatch_projection,
            dispatched_digest: next.data.dispatched_digest,
            manifest: next.data.manifest,
            prep_session_id: next.data.prep_session_id,
            prep_binding_digest: next.data.prep_binding_digest,
            request_id: first.prepared.requestId,
            previous_attempt_id: first.prepared.attemptId,
            expected_ordinal: 2,
            visible_output: false,
          },
          { deadlineAt: next.deadline },
        );
      }
      const deadline = new Date(Date.now() + 3500).toISOString();
      const tail = await granted(db, setup, client, { deadline });
      await Bun.sleep(Math.max(0, Date.parse(deadline) - Date.now() + 20));
      let expired = 0;
      for (let i = 0; i < 10; i++) {
        const result = (await worker.execution("execution_quota_recovery_v1", { limit: 1 })) as {
          expired: number;
        };
        expired += result.expired;
      }
      expect(expired).toBe(1);
      expect(
        await db`SELECT execution_closed_at IS NOT NULL closed FROM gateway_requests WHERE id=${tail.prepared.requestId}`,
      ).toEqual([{ closed: true }]);
      expect(
        await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE state='EXPIRED'`,
      ).toEqual([{ n: 3 }]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "1000",
        reserved: "570",
        spent: "0",
        state: "ACTIVE",
      });
    }),
  30000,
);

test(
  "a completed Redis release racing with technical expiry stays expired and does not roll back other released projections",
  () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const client = f.client();
      const worker = f.client(f.identity.b.port, "worker");
      await client.startup();
      const owner = await worker.startup();
      const deadline = new Date(Date.now() + 2500).toISOString();
      const expiring = await granted(db, setup, client, { deadline });
      const current = await granted(db, setup, client);
      const settlement = createSettlementStore(db);
      for (const attempt of [expiring, current]) {
        committed(
          (
            await settlement.persist([
              {
                events: [
                  event(attempt.owner, attempt.prepared.attemptId, {
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
      }
      const rows = await db`SELECT descriptor FROM execution_quota_leases ORDER BY lease_id`;
      const quota = createQuotaLeases({
        urls: [process.env.REDIS_URL!],
        namespace: rows[0].descriptor.namespace,
        generation: rows[0].descriptor.generation,
      });
      const completed = [];
      try {
        for (const { descriptor } of rows) {
          completed.push(
            (
              await quota.release(descriptor, {
                state: "RELEASE_PENDING",
                attempt_id: descriptor.attempt_id,
                permit_epoch: descriptor.permit_epoch,
              })
            ).descriptor,
          );
        }
      } finally {
        quota.close();
      }
      await Bun.sleep(Math.max(0, Date.parse(deadline) - Date.now() + 20));
      await db`UPDATE execution_quota_leases SET state='EXPIRED' WHERE request_id=${expiring.prepared.requestId}`;
      const identities = createInternalIdentityStore(db);
      const peer = (await identities.readPeerDirectory(owner.certificate_sha256))!;
      const session = (await identities.readStartupSession(owner.session_id))!;
      const projection: CoreVerifiedCallerProjection = {
        service: session.service,
        instance_id: session.instance_id,
        certificate_sha256: session.certificate_sha256,
        instance_revision: peer.instance_revision,
        certificate_revision: peer.certificate_revision,
        raw_body_sha256: sha256("completed-quota-release"),
        session_id: session.session_id,
        incarnation_id: session.incarnation_id,
        key_sha256: session.public_key_sha256,
        session_revision: session.session_revision,
        command_type: "execution_quota_recovery_v1",
        command_id: crypto.randomUUID(),
        method: "POST",
        normalized_path: "/_internal/core/v1/quota-recovery",
        deadline_at: new Date(Date.now() + 20000).toISOString(),
        trace_id: "quota-expiry-race",
      };
      const store = createExecutionStore(db);
      expect(await store.recoverQuota(projection, completed)).toEqual({ released: 6 });
      expect(
        await db`SELECT state,count(*)::integer n FROM execution_quota_leases GROUP BY state ORDER BY state`,
      ).toEqual([
        { state: "EXPIRED", n: 3 },
        { state: "RELEASED", n: 3 },
      ]);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "970",
        reserved: "0",
        spent: "30",
        state: "ACTIVE",
      });
    }),
  30000,
);
