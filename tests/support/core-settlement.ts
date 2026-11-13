import { expect } from "bun:test";
import type { SQL } from "bun";
import type {
  CoreEvidenceEventV1,
  CoreEvidenceGroupResult,
  ExecutionReply,
} from "@gateway/contracts";
import { prepareFinancialAdmission, withFinancial } from "./financial-execution.ts";

export type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
export type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;
export type Client = ReturnType<Fixture["client"]>;

export async function granted(
  db: SQL,
  setup: Setup,
  client: Client,
  options: Parameters<typeof prepareFinancialAdmission>[2] = {},
) {
  const prepared = await prepareFinancialAdmission(setup, client, options);
  const admission = (await prepared.admit()) as ExecutionReply;
  await client.execution("execution_begin_v1", prepared.beginData(admission.receipt), {
    deadlineAt: prepared.deadline,
  });
  const permit = (
    await db`SELECT permit_epoch::text,state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`
  )[0];
  expect(permit.state).toBe("SEND_GRANTED");
  return { prepared, owner: { client, permit_epoch: String(permit.permit_epoch) } };
}

export type Signer = { client: Client; permit_epoch: string };
export type EventBody = Omit<
  CoreEvidenceEventV1,
  "origin_identity" | "origin_incarnation" | "origin_session_id"
>;

export function body(owner: Signer, attempt: string, fields: Partial<EventBody>): EventBody {
  return {
    event_id: crypto.randomUUID(),
    attempt_id: attempt,
    permit_epoch: owner.permit_epoch,
    source: "execution",
    source_revision: "1",
    finality: "PARTIAL",
    usage: {},
    final_dimensions: [],
    ...fields,
  };
}

export function event(owner: Signer, attempt: string, fields: Partial<EventBody>) {
  return owner.client.signEvidence(body(owner, attempt, fields));
}

export async function supplier(db: SQL, account: string) {
  const row = (
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM upstream_account_balances WHERE account_id=${account}`
  )[0];
  const legs = Object.fromEntries(
    (
      await db`SELECT e.account,sum(e.atoms)::text atoms FROM upstream_entries e JOIN upstream_journals j ON j.id=e.journal_id WHERE j.account_id=${account} GROUP BY e.account`
    ).map((leg: { account: string; atoms: string }) => [leg.account, leg.atoms]),
  );
  expect(BigInt(legs["upstream:available"] ?? "0") + BigInt(legs["upstream:reserved"] ?? "0")).toBe(
    BigInt(row.balance),
  );
  expect(legs["upstream:reserved"] ?? "0").toBe(row.reserved);
  expect(legs["upstream:expense"] ?? "0").toBe(row.spent);
  const state = (await db`SELECT state FROM upstream_accounts WHERE id=${account}`)[0].state;
  return { ...row, state };
}

export function committed(result: CoreEvidenceGroupResult | undefined) {
  if (result?.status !== "COMMITTED") throw new Error(JSON.stringify(result));
  return result;
}
