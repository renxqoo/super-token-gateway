import { expect, test } from "bun:test";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";

for (const relation of ["permit", "quota-lease"])
  test(
    `begin survives a brief ${relation} lock during confirmed Redis quota adoption without partial writes or a lost receipt`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap();
        const client = f.client();
        await client.startup();
        const prepared = await prepareFinancialAdmission(setup, client);
        const admitted = await prepared.admit();
        const held = Promise.withResolvers<void>();
        const release = Promise.withResolvers<void>();
        const locked = db.begin(async (tx) => {
          if (relation === "permit")
            await tx`SELECT attempt_id FROM execution_permits WHERE attempt_id=${prepared.attemptId} FOR UPDATE`;
          else
            await tx`SELECT lease_id FROM execution_quota_leases WHERE attempt_id=${prepared.attemptId} ORDER BY lease_id DESC LIMIT 1 FOR UPDATE`;
          held.resolve();
          await release.promise;
        });
        let pending: Promise<unknown> | undefined;
        try {
          await Promise.race([held.promise, locked]);
          let completed = false;
          pending = client
            .execution("execution_begin_v1", prepared.beginData(admitted.receipt), {
              deadlineAt: prepared.deadline,
            })
            .catch((error: unknown) => error)
            .finally(() => {
              completed = true;
            });
          await Bun.sleep(100);
          expect(completed).toBe(false);
          expect(
            (
              await db`SELECT count(*)::integer n FROM execution_quota_leases WHERE attempt_id=${prepared.attemptId} AND state<>'ADOPTION_PENDING'`
            )[0].n,
          ).toBe(0);
          release.resolve();
          await locked;
          expect(await pending).toMatchObject({
            receipt: { command_state: "COMMITTED", state: "SEND_GRANTED" },
            transient_send: { attempt_id: prepared.attemptId },
          });
          expect(
            (
              await db`SELECT count(*)::integer n FROM execution_commands WHERE kind='execution_begin_v1' AND attempt_id=${prepared.attemptId}`
            )[0].n,
          ).toBe(1);
        } finally {
          release.resolve();
          await locked;
          await pending;
        }
      }),
    30000,
  );
