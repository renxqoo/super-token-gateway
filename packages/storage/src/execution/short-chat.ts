import type { SQL } from "bun";
import {
  executionReceiptSchema,
  shortChatFundingGroup,
  type CoreVerifiedCallerProjection,
  type ExecutionEnvelope,
} from "@gateway/contracts";
import { Batcher } from "@gateway/runtime/batcher";
import { withDatabaseConnection, withDatabaseTransaction } from "../transaction.ts";

type Item = {
  projection: CoreVerifiedCallerProjection;
  envelope: ExecutionEnvelope;
  data: {
    project_id: string;
    dispatch_projection: { proposed_attempt_id: string };
  };
  computed: Record<string, unknown>;
  proofDeadline: number;
  signal: AbortSignal | undefined;
  adopted: { lease_id: string; revision: string }[];
};

type Result = Record<string, unknown>;

export async function lockShortChatFundingGroup(db: SQL, group: number, epoch: number) {
  await db`SELECT public.core_short_chat_funding_v1(${group}::smallint,${epoch}::bigint)`;
}

export function createShortChatFundingScheduler() {
  const pending = Array.from({ length: 16 }, () => Promise.resolve());
  return async function schedule<T>(group: number, write: () => Promise<T>) {
    const result = pending[group]!.then(write);
    pending[group] = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  };
}

export function createShortChatAdmissionStore(
  primary: SQL,
  standby: SQL,
  epoch: number,
  schedule: ReturnType<typeof createShortChatFundingScheduler>,
) {
  async function run(group: number, items: Item[], slow = false): Promise<Result[]> {
    const started = performance.now();
    const live = items.filter(
      (item) => !item.signal?.aborted && item.proofDeadline - started > 1000,
    );
    if (live.length !== items.length) {
      const valid = new Set(live);
      const remaining = live.length ? await run(group, live, slow) : [];
      let index = 0;
      return items.map((item) =>
        valid.has(item) ? remaining[index++]! : { error: "quota_unavailable" },
      );
    }
    const deadline = Math.min(...items.map((i) => i.proofDeadline - 1000), started + 3000);
    const written = await schedule(group, () =>
      withDatabaseTransaction(
        primary,
        { deadline, synchronousCommit: "remote_apply" },
        async (tx) => {
          await lockShortChatFundingGroup(tx, group, epoch);
          const active = items.filter((item) => !item.signal?.aborted);
          const admissions = active.map(
            ({ projection, envelope, data, computed, proofDeadline, adopted }) => ({
              projection,
              envelope,
              data,
              computed: {
                ...computed,
                adopted_leases: adopted,
                proof_remaining_ms: Math.floor(proofDeadline - performance.now()),
              },
            }),
          );
          if (!admissions.length)
            return {
              results: items.map((): Result => ({ error: "quota_unavailable" })),
              expected: [],
            };
          const applied: Result[] = slow
            ? [
                (await tx`SELECT public.core_execution_slow_v1(${admissions[0]}::jsonb) value`)[0]!
                  .value,
              ]
            : (await tx`SELECT public.core_execution_batch_v1(${admissions}::jsonb) value`)[0]!
                .value;
          if (applied.length !== active.length) throw new Error("Short Chat grant batch mismatch");
          const byItem = new Map(active.map((item, index) => [item, applied[index]!]));
          const results = items.map(
            (item): Result => byItem.get(item) ?? { error: "quota_unavailable" },
          );
          const expected = results.flatMap((result, i) => {
            if (typeof result.command_row_id !== "string") return [];
            const receipt = executionReceiptSchema.parse(result.receipt);
            const { projection, envelope, data } = items[i]!;
            if (
              receipt.command_state !== "REJECTED" &&
              (receipt.command_state !== "COMMITTED" ||
                receipt.state !== "SEND_GRANTED" ||
                receipt.attempt_id !== data.dispatch_projection.proposed_attempt_id ||
                !receipt.authorization_plan_digest)
            )
              throw new Error("Short Chat grant row mismatch");
            return [
              {
                row_id: result.command_row_id,
                command_id: envelope.command_id,
                body_digest: envelope.body_digest,
                owner_session_id: projection.session_id,
                incarnation_id: projection.incarnation_id,
                caller_identity: `${projection.service}:${projection.instance_id}`,
                command_state: receipt.command_state,
                attempt_id: receipt.attempt_id,
                authorization_plan_digest: receipt.authorization_plan_digest,
              },
            ];
          });
          return { results, expected };
        },
      ),
    );
    const witness = written.expected.length
      ? await withDatabaseConnection(
          standby,
          { deadline },
          (db) =>
            db<{ commands: string[] }[]>`
        SELECT coalesce(array_agg(e.row_id::text),'{}'::text[]) commands
        FROM jsonb_to_recordset(${written.expected}::jsonb) e(row_id uuid,command_id uuid,body_digest text,
          owner_session_id uuid,incarnation_id uuid,caller_identity text,attempt_id uuid,
          authorization_plan_digest text,command_state text)
        JOIN public.execution_commands c ON c.id=e.row_id AND c.command_id=e.command_id
          AND c.caller_identity=e.caller_identity AND c.attempt_id IS NOT DISTINCT FROM e.attempt_id
          AND c.body_digest=e.body_digest AND c.owner_session_id=e.owner_session_id
          AND c.caller_incarnation=e.incarnation_id AND c.kind='execution_admit_v1' AND c.state=e.command_state
        LEFT JOIN public.execution_permits p ON p.attempt_id=e.attempt_id
          AND p.authorization_plan_digest=e.authorization_plan_digest
          AND p.owner_session_id=e.owner_session_id AND p.incarnation_id=e.incarnation_id
          AND p.permit_epoch=1 AND p.state='SEND_GRANTED'
        WHERE pg_is_in_recovery() AND current_setting('fsync')='on'
          AND current_setting('full_page_writes')='on'
          AND (e.command_state='REJECTED' OR p.attempt_id IS NOT NULL)
      `,
        )
      : [];
    const confirmed = new Set(witness[0]?.commands);
    return Promise.all(
      written.results.map(async (result, i) => {
        if (result.error === "funding_slow" && !slow)
          return (
            await run(group, [items[i]!], true).catch(() => [{ error: "outcome_unknown" }])
          )[0]!;
        if (
          result.fresh_transition === true &&
          (!confirmed.has(String(result.command_row_id)) || deadline <= performance.now())
        )
          return { error: "outcome_unknown" };
        return typeof result.command_row_id === "string" &&
          confirmed.has(result.command_row_id) &&
          result.fresh_transition !== true
          ? { ...result, witnessed_rejection: true }
          : result;
      }),
    );
  }

  const groups = Array.from(
    { length: 16 },
    (_, group) =>
      new Batcher<Item, Result>((items) => run(group, items), 32, 2, 1, 128, {
        collectWhenIdle: true,
      }),
  );
  return {
    add: (item: Item) => groups[shortChatFundingGroup(item.data.project_id)]!.add(item),
  };
}
