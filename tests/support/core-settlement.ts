import { expect } from "bun:test";
import type { SQL } from "bun";
import type {
  CoreEvidenceEventV1,
  CoreEvidenceGroupResult,
  CoreVerifiedCallerProjection,
  ExecutionReply,
  StartupReceipt,
} from "@gateway/contracts";
import { CoreRpcError } from "@gateway/core-client";
import { sha256 } from "@gateway/runtime/crypto";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { createSettlementStore } from "@gateway/storage/settlement";
import { prepareFinancialAdmission, withFinancial } from "./financial-execution.ts";

export type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
export type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;
export type Client = ReturnType<Fixture["client"]>;

export async function quotaRecoveryProjection(
  db: SQL,
  owner: StartupReceipt,
  traceId: string,
): Promise<CoreVerifiedCallerProjection> {
  const identities = createInternalIdentityStore(db);
  const peer = (await identities.readPeerDirectory(owner.certificate_sha256))!;
  const session = (await identities.readStartupSession(owner.session_id))!;
  return {
    service: session.service,
    instance_id: session.instance_id,
    certificate_sha256: session.certificate_sha256,
    instance_revision: peer.instance_revision,
    certificate_revision: peer.certificate_revision,
    raw_body_sha256: sha256(traceId),
    session_id: session.session_id,
    incarnation_id: session.incarnation_id,
    key_sha256: session.public_key_sha256,
    session_revision: session.session_revision,
    command_type: "execution_quota_recovery_v1",
    command_id: crypto.randomUUID(),
    method: "POST",
    normalized_path: "/_internal/core/v1/quota-recovery",
    deadline_at: new Date(Date.now() + 20000).toISOString(),
    trace_id: traceId,
  };
}

export async function granted(
  db: SQL,
  setup: Setup,
  client: Client,
  options: Parameters<typeof prepareFinancialAdmission>[2] = {},
) {
  const until = Date.now() + 15000;
  const prepared = await sameCommand(
    () => prepareFinancialAdmission(setup, client, options),
    until,
  );
  const admission = (await sameCommand(() => prepared.admit(), until)) as ExecutionReply;
  const begin = crypto.randomUUID();
  await sameCommand(
    () =>
      client.execution("execution_begin_v1", prepared.beginData(admission.receipt), {
        commandId: begin,
        deadlineAt: prepared.deadline,
      }),
    until,
  );
  const permit = (
    await db`SELECT permit_epoch::text,state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`
  )[0];
  expect(permit.state).toBe("SEND_GRANTED");
  return { prepared, owner: { client, permit_epoch: String(permit.permit_epoch) } };
}

async function sameCommand<T>(send: () => Promise<T>, until: number): Promise<T> {
  for (;;) {
    try {
      return await send();
    } catch (error) {
      if (
        !(error instanceof CoreRpcError) ||
        error.detail.retryability !== "SAME_COMMAND" ||
        Date.now() >= until
      )
        throw error;
      await Bun.sleep(10);
    }
  }
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
    await db`SELECT balance_atoms::text balance,reserved_atoms::text reserved,spent_atoms::text spent FROM core_upstream_balances_v1 WHERE account_id=${account}`
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

export async function settledCharge(db: SQL, setup: Setup, client: Client) {
  const { prepared, owner } = await granted(db, setup, client);
  await createSettlementStore(db).persist([
    {
      events: [
        event(owner, prepared.attemptId, {
          finality: "FINAL",
          usage: { input_tokens: "31", output_tokens: "2" },
          final_dimensions: ["input_tokens", "output_tokens"],
          attempt_state: "SUCCEEDED",
          request_state: "SUCCEEDED",
        }),
      ],
    },
  ]);
  const rows =
    await db`SELECT id,amount_atoms::text FROM billing_charges WHERE request_id=${prepared.requestId}`;
  expect(rows).toEqual([{ id: expect.any(String), amount_atoms: "100" }]);
  return { request: prepared.requestId, charge: String(rows[0].id) };
}
