import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { UsageProducer, evidenceChannel } from "@gateway/runtime";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";
import {
  chatReplies,
  eventually,
  startCoreWorker,
  withGatewayStack,
} from "../support/gateway-process.ts";

type Stack = Parameters<Parameters<typeof withGatewayStack>[1]>[0];

async function queueCounts(namespace: string) {
  const queue = new UsageProducer({
    channel: evidenceChannel,
    url: process.env.QUEUE_REDIS_URL!,
    namespace,
    replicas: 0,
    timeoutMs: 2000,
  });
  try {
    return await queue.stats();
  } finally {
    queue.close();
  }
}

async function ledger(db: SQL, account: string, requestId: string) {
  const [charges, receipts, supplier, wallets] = await Promise.all([
    db`SELECT amount_atoms::text amount FROM billing_charges WHERE request_id=${requestId}`,
    db`SELECT count(*)::int n FROM core_evidence_receipts r JOIN attempts a ON a.id=r.attempt_id WHERE a.request_id=${requestId}`,
    db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`,
    db`SELECT sum(reserved_atoms)::text reserved,sum(spent_atoms)::text spent FROM wallets`,
  ]);
  return {
    charges: [...charges] as { amount: string }[],
    receipts: Number(receipts[0].n),
    supplier: supplier[0] as { balance: string; reserved: string; spent: string },
    customer: wallets[0] as { reserved: string; spent: string },
  };
}

async function openSettlements(db: SQL, role: string) {
  return Number(
    (
      await db`SELECT count(*)::int n FROM pg_stat_activity WHERE usename=${role} AND state='idle in transaction'`
    )[0].n,
  );
}

async function crashDuringSettlement(
  stack: Stack,
  mode: "commit" | "acknowledgement",
  whileHeld: (requestId: string) => Promise<void>,
) {
  await stack.worker.stop();
  const proxy = await createCommitAckProxy();
  let doomed: Awaited<ReturnType<typeof startCoreWorker>> | undefined;
  try {
    const url = new URL(stack.workerDatabaseUrl);
    url.port = String(proxy.port);
    const held = proxy.arm(['"attempt_state":"SUCCEEDED"'], 20000, mode);
    doomed = await startCoreWorker(stack.namespace, {
      databaseUrl: url.toString(),
      env: { SETTLEMENT_CONCURRENCY: "1" },
    });
    const response = await stack.send();
    expect(response.status).toBe(200);
    const requestId = response.headers.get("x-request-id")!;
    await held;
    expect(await queueCounts(stack.namespace)).toEqual({
      waiting: 0,
      active: 1,
      delayed: 0,
      failed: 0,
    });
    await whileHeld(requestId);
    doomed.kill("SIGKILL");
    await doomed.exited();
    await proxy.close();
    return requestId;
  } finally {
    doomed?.kill("SIGKILL");
    await proxy.close();
  }
}

test(
  "a settlement worker killed with its commit unsent moves no money and the restarted worker settles the evidence exactly once",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      const { db, setup } = stack;
      const role = new URL(stack.workerDatabaseUrl).username;
      let before: Awaited<ReturnType<typeof ledger>> | undefined;
      const requestId = await crashDuringSettlement(stack, "commit", async (id) => {
        expect(await openSettlements(db, role)).toBe(1);
        before = await ledger(db, setup.provider.account_id, id);
        expect(before.charges).toEqual([]);
      });
      await eventually(
        async () => (await openSettlements(db, role)) === 0,
        "rolled back settlement",
      );
      expect(await ledger(db, setup.provider.account_id, requestId)).toEqual(before!);

      await stack.startWorker();
      await eventually(
        async () => Object.values(await queueCounts(stack.namespace)).every((n) => n === 0),
        "stalled evidence redelivered and acknowledged",
        60000,
      );
      const after = await ledger(db, setup.provider.account_id, requestId);
      expect(after.charges).toEqual([{ amount: "100" }]);
      expect(after.receipts).toBe(before!.receipts + 1);
      expect(after.supplier).toEqual({ balance: "985", reserved: "0", spent: "15" });
      expect(BigInt(after.customer.spent) - BigInt(before!.customer.spent)).toBe(100n);
    }),
  120000,
);

test(
  "a settlement worker killed after its commit but before acknowledging the job charges the redelivered evidence only once",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async (stack) => {
      const { db, setup } = stack;
      let committed: Awaited<ReturnType<typeof ledger>> | undefined;
      const requestId = await crashDuringSettlement(stack, "acknowledgement", async (id) => {
        committed = await ledger(db, setup.provider.account_id, id);
        expect(committed.charges).toEqual([{ amount: "100" }]);
        expect(committed.receipts).toBeGreaterThan(0);
        expect(committed.supplier).toEqual({ balance: "985", reserved: "0", spent: "15" });
      });

      await stack.startWorker();
      await eventually(
        async () => Object.values(await queueCounts(stack.namespace)).every((n) => n === 0),
        "stalled evidence redelivered and acknowledged",
        60000,
      );
      expect(await ledger(db, setup.provider.account_id, requestId)).toEqual(committed!);
    }),
  120000,
);
