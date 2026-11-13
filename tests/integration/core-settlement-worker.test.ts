import { expect, test } from "bun:test";
import type { SQL } from "bun";
import type { CoreEvidenceGroupV1, ExecutionReply } from "@gateway/contracts";
import { createSettlementWorker } from "../../apps/core-api/src/settlement/worker.ts";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

async function eventually(check: () => Promise<boolean>, label: string) {
  const end = Date.now() + 15000;
  while (!(await check())) {
    if (Date.now() > end) throw new Error(`Timed out waiting for ${label}`);
    await Bun.sleep(25);
  }
}

async function supplier(db: SQL, account: string) {
  return (
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`
  )[0];
}

test("the settlement worker acknowledges a job only after its group commits and redelivery adds no money", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const prepared = await prepareFinancialAdmission(setup, client);
    const admission = (await prepared.admit()) as ExecutionReply;
    await client.execution("execution_begin_v1", prepared.beginData(admission.receipt), {
      deadlineAt: prepared.deadline,
    });
    const instance = String(
      (
        await db`SELECT gateway_instance_id FROM execution_permits WHERE attempt_id=${prepared.attemptId}`
      )[0].gateway_instance_id,
    );
    const fact = client.signEvidence({
      event_id: crypto.randomUUID(),
      attempt_id: prepared.attemptId,
      permit_epoch: "1",
      source: "execution",
      source_revision: "1",
      finality: "FINAL",
      usage: { input_tokens: "30", output_tokens: "15" },
      final_dimensions: ["input_tokens", "output_tokens"],
      attempt_state: "SUCCEEDED",
      request_state: "SUCCEEDED",
    });
    const group: CoreEvidenceGroupV1 = { events: [fact] };
    const options = {
      url: process.env.QUEUE_REDIS_URL!,
      namespace: `core-evidence:${crypto.randomUUID()}`,
      replicas: 0,
      timeoutMs: 1000,
    };
    const producer = new UsageProducer({ ...options, channel: evidenceChannel });
    let rejected = 0;
    const worker = createSettlementWorker({
      ...options,
      db,
      onRejected: (count) => {
        rejected += count;
      },
    });
    const receipts = async () =>
      Number(
        (
          await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE event_id=${fact.event_id}`
        )[0].n,
      );
    const drained = async () =>
      Object.values(await producer.stats()).reduce((sum, count) => sum + count, 0) === 0;
    try {
      await producer.ready();
      await worker.ready();
      const held = new Error("release identity lock");
      await db
        .begin(async (tx) => {
          await tx`SELECT 1 FROM internal_service_instances WHERE service='gateway' AND instance_id=${instance} FOR UPDATE`;
          await producer.publish(group);
          await eventually(async () => (await producer.stats()).delayed === 1, "retry backoff");
          expect(await receipts()).toBe(0);
          expect(await supplier(db, setup.provider.account_id)).toEqual({
            balance: "1000",
            reserved: "30",
            spent: "0",
          });
          throw held;
        })
        .catch((error) => {
          if (error !== held) throw error;
        });
      await eventually(drained, "committed acknowledgement");
      expect(await receipts()).toBe(1);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "0",
        spent: "15",
      });

      await producer.publish(group);
      await eventually(drained, "redelivery acknowledgement");
      expect(await receipts()).toBe(1);
      expect(await supplier(db, setup.provider.account_id)).toEqual({
        balance: "985",
        reserved: "0",
        spent: "15",
      });
      expect(
        await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`,
      ).toEqual([{ amount_atoms: "100" }]);

      await producer.publish({
        events: [{ ...fact, event_id: crypto.randomUUID(), source_revision: "2" }],
      });
      await eventually(drained, "rejected acknowledgement");
      expect(rejected).toBe(1);
      expect(
        (
          await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${prepared.attemptId}`
        )[0].n,
      ).toBe(1);
    } finally {
      await worker.close();
      producer.close();
    }
  }));
