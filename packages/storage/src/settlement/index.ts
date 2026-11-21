import { createPublicKey, verify, type KeyObject } from "node:crypto";
import type { SQL } from "bun";
import {
  coreEvidenceGroupResultSchema,
  coreEvidenceGroupV1Schema,
  evidenceProofFields,
  isInitialStreamTerminalGroup,
  shortChatFundingGroup,
  type CoreEvidenceGroupResult,
  type CoreEvidenceGroupV1,
  type SignedCoreEvidenceEventV1,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { Batcher } from "@gateway/runtime/batcher";
import { z } from "zod";
import {
  lockShortChatFundingGroup,
  createShortChatFundingScheduler,
} from "../execution/short-chat.ts";
import {
  DatabaseCommitUnknown,
  withDatabaseConnection,
  withDatabaseTransaction,
} from "../transaction.ts";

const resultsSchema = z.array(coreEvidenceGroupResultSchema);
const groupsSchema = z.array(coreEvidenceGroupV1Schema).min(1).max(128);
const sessionKeyCache = new Map<string, { key: KeyObject; digest: string }>();

function cachedSessionKey(sessionId: string, spki: Buffer, digest: string) {
  const existing = sessionKeyCache.get(sessionId);
  if (existing && existing.digest === digest) return existing;
  const entry = {
    key: createPublicKey({
      key: Buffer.from(spki),
      format: "der",
      type: "spki",
    }),
    digest,
  };
  if (sessionKeyCache.size >= 4096) sessionKeyCache.clear();
  sessionKeyCache.set(sessionId, entry);
  return entry;
}

export class TerminalSettlementRejected extends Error {
  constructor() {
    super("TERMINAL_SETTLEMENT_NOT_ACCEPTED");
  }
}

export class StreamTerminalDisabled extends Error {}

function signaturePayload(body: Omit<SignedCoreEvidenceEventV1, "signature">) {
  return Buffer.from(JSON.stringify(evidenceProofFields(body, sha256(canonical(body)))));
}

function authenticate(event: SignedCoreEvidenceEventV1, key: KeyObject | undefined) {
  if (!key) return undefined;
  const { signature, ...body } = event;
  const payload = signaturePayload(body);
  const bytes = Buffer.from(signature, "base64url");
  if (bytes.length !== 64 || !verify(null, payload, key, bytes)) return undefined;
  return {
    ...body,
    signature: bytes.toString("hex"),
    signature_payload_digest: sha256(payload),
  };
}

function settlementStore(db: SQL, replicatedTerminal = false) {
  return {
    async persist(
      groups: CoreEvidenceGroupV1[],
      limits: { deadline: number; signal?: AbortSignal } = { deadline: performance.now() + 8000 },
      acquire?: (tx: SQL) => Promise<void>,
    ): Promise<CoreEvidenceGroupResult[]> {
      const transactionLimits = replicatedTerminal
        ? { ...limits, synchronousCommit: "remote_apply" as const }
        : limits;
      const parsed = groupsSchema.parse(groups);
      const sessions = [
        ...new Set(parsed.flatMap((g) => g.events.map((e) => e.origin_session_id))),
      ];
      const keys = new Map<string, { key: KeyObject; digest: string }>();
      const missing: string[] = [];
      for (const id of sessions) {
        const cached = sessionKeyCache.get(id);
        if (cached) keys.set(id, cached);
        else missing.push(id);
      }
      if (missing.length) {
        const sessionRows = await withDatabaseConnection(
          db,
          limits,
          (connection) =>
            connection<
              { session_id: string; public_key_spki: Buffer; public_key_sha256: string }[]
            >`SELECT session_id::text,public_key_spki,public_key_sha256 FROM internal_startup_sessions WHERE session_id=ANY(${missing}::uuid[])`,
        );
        for (const row of sessionRows)
          keys.set(
            row.session_id,
            cachedSessionKey(row.session_id, row.public_key_spki, row.public_key_sha256),
          );
      }
      const verified = parsed.map((group) => {
        const events = group.events.map((event) => {
          const session = keys.get(event.origin_session_id);
          const body = authenticate(event, session?.key);
          return body && session ? { ...body, producer_key_sha256: session.digest } : undefined;
        });
        return events.every((event) => event !== undefined)
          ? { events }
          : {
              rejected: "evidence_signature_invalid",
              events: group.events.map((event) => ({
                origin_identity: event.origin_identity,
                origin_session_id: event.origin_session_id,
                event_id: event.event_id,
                attempt_id: event.attempt_id,
              })),
            };
      });
      const chunks: (typeof verified)[] = [];
      let chunk: typeof verified = [];
      let attempts = new Set<string>();
      for (const group of verified) {
        const combined = new Set([...attempts, ...group.events.map((event) => event.attempt_id)]);
        if (combined.size > 128 && chunk.length) {
          chunks.push(chunk);
          chunk = [];
          attempts = new Set();
        }
        chunk.push(group);
        for (const event of group.events) attempts.add(event.attempt_id);
      }
      if (chunk.length) chunks.push(chunk);
      const persisted: CoreEvidenceGroupResult[] = [];
      for (const chunk of chunks) {
        const results = resultsSchema.parse(
          await withDatabaseTransaction(db, transactionLimits, async (tx) => {
            await acquire?.(tx);
            return (await tx`SELECT persist_core_evidence_groups_v1(${chunk}::jsonb) value`)[0]
              .value;
          }),
        );
        if (results.length !== chunk.length) throw new Error("evidence_batch_result_mismatch");
        for (let i = 0; i < results.length; i++) {
          const result = results[i]!;
          if (result.status === "ERROR" && result.error === "P0F01")
            results[i] = coreEvidenceGroupResultSchema.parse(
              await withDatabaseTransaction(db, transactionLimits, async (tx) => {
                await acquire?.(tx);
                return (await tx`SELECT persist_core_evidence_slow_v1(${chunk[i]}::jsonb) value`)[0]
                  .value;
              }),
            );
        }
        persisted.push(...results);
      }
      if (persisted.length !== verified.length) throw new Error("evidence_batch_result_mismatch");
      return persisted;
    },
  };
}

export function createSettlementStore(db: SQL) {
  return settlementStore(db);
}

type TerminalLimits = { deadline: number; signal?: AbortSignal };
type TerminalInput = { group: CoreEvidenceGroupV1; limits: TerminalLimits };
type TerminalResult = { requestId: string } | Error;

export function createReplicatedTerminalSettlementStore(
  primary: SQL,
  standby: SQL,
  fundingEpoch?: number,
  schedule = createShortChatFundingScheduler(),
  allowStreamTerminal = false,
) {
  const store = settlementStore(primary, true);
  const groups = Array.from(
    { length: 16 },
    (_, group) =>
      new Batcher<TerminalInput, TerminalResult>(
        (inputs) => persistAndConfirm(inputs, group),
        32,
        2,
        1,
        128,
        { collectWhenIdle: true },
      ),
  );

  async function persistAndConfirm(
    inputs: TerminalInput[],
    group?: number,
  ): Promise<TerminalResult[]> {
    const limits =
      inputs.length === 1 && group === undefined
        ? inputs[0]!.limits
        : { deadline: Math.max(...inputs.map((input) => input.limits.deadline)) };
    const persist = () =>
      store.persist(
        inputs.map(({ group }) => group),
        limits,
        group !== undefined
          ? (tx) => lockShortChatFundingGroup(tx, group, fundingEpoch!)
          : undefined,
      );
    const persisted = await (group === undefined ? persist() : schedule(group, persist));
    const expected = persisted.flatMap((result, index) => {
      const { events } = inputs[index]!.group;
      if (
        result.status !== "COMMITTED" ||
        result.receipts.length !== events.length ||
        result.receipts.some((receipt) => receipt.outcome !== "ACCEPTED")
      )
        return [];
      const receipts = new Map(
        result.receipts.map((receipt) => [receipt.event_id, receipt.receipt_id]),
      );
      return events.map(({ signature, ...body }) => ({
        batch_index: index,
        attempt_id: body.attempt_id,
        receipt_id: receipts.get(body.event_id),
        event_id: body.event_id,
        origin_identity: body.origin_identity,
        origin_session_id: body.origin_session_id,
        origin_incarnation: body.origin_incarnation,
        signature: Buffer.from(signature, "base64url").toString("hex"),
        signature_payload_digest: sha256(signaturePayload(body)),
        expected_receipts: events.length,
      }));
    });
    if (!expected.length) return inputs.map(() => new Error("TERMINAL_SETTLEMENT_UNCONFIRMED"));
    try {
      const outcome = await withDatabaseConnection(
        standby,
        limits,
        (connection) =>
          connection<{ batch_index: number; request_id: string }[]>`
          WITH witnessed AS MATERIALIZED (
            SELECT e.batch_index,e.attempt_id,e.expected_receipts,r.canonical_evidence_id
            FROM jsonb_to_recordset(${expected}::jsonb) e(batch_index int,attempt_id uuid,
              receipt_id uuid,event_id uuid,origin_identity text,origin_session_id uuid,
              origin_incarnation uuid,signature text,signature_payload_digest text,expected_receipts int)
            JOIN public.core_evidence_receipts r ON r.id=e.receipt_id AND r.event_id=e.event_id
              AND r.attempt_id=e.attempt_id AND r.origin_identity=e.origin_identity
              AND r.origin_session_id=e.origin_session_id AND r.origin_incarnation=e.origin_incarnation
              AND r.outcome='ACCEPTED' AND r.signature=decode(e.signature,'hex')
              AND r.signature_payload_digest=e.signature_payload_digest
          ), confirmed AS (
            SELECT batch_index,attempt_id,array_agg(canonical_evidence_id) ids FROM witnessed
            GROUP BY batch_index,attempt_id,expected_receipts HAVING count(*)=expected_receipts
          )
          SELECT w.batch_index,q.id::text AS request_id
          FROM confirmed w
          JOIN public.attempts a ON a.id=w.attempt_id
          JOIN public.gateway_requests q ON q.id=a.request_id AND q.selected_attempt_id=a.id
          JOIN public.billing_charges c ON c.request_id=q.id
          JOIN public.upstream_holds h ON h.attempt_id=a.id
          WHERE q.operation='chat' AND q.state='SUCCEEDED' AND q.billing_outcome='SUCCESS'
            AND q.execution_closed_at IS NOT NULL AND a.state='SUCCEEDED'
            AND h.state='SETTLED' AND h.final_cost_atoms IS NOT NULL
            AND pg_is_in_recovery() AND current_setting('fsync')='on'
            AND current_setting('full_page_writes')='on'
            AND c.evidence_id=ANY(w.ids)
            AND EXISTS (
              SELECT 1 FROM public.upstream_costs cost
              WHERE cost.attempt_id=a.id AND cost.complete AND NOT cost.disputed
                AND cost.evidence_id=ANY(w.ids)
            )
        `,
      );
      const confirmed = new Map(outcome.map((row) => [row.batch_index, row.request_id]));
      return inputs.map((input, index) => {
        const requestId = confirmed.get(index);
        return requestId &&
          performance.now() < input.limits.deadline &&
          !input.limits.signal?.aborted
          ? { requestId }
          : new DatabaseCommitUnknown(new Error("TERMINAL_SETTLEMENT_REPLICA_UNCONFIRMED"));
      });
    } catch (error) {
      throw new DatabaseCommitUnknown(error);
    }
  }

  return {
    async settle(
      input: CoreEvidenceGroupV1,
      limits: TerminalLimits,
      streamMode?: "online" | "recovery",
    ): Promise<{ requestId: string }> {
      const group = coreEvidenceGroupV1Schema.parse(input);
      const final = group.events.at(-1)!;
      if (
        group.events.length > 2 ||
        new Set(group.events.map((event) => event.event_id)).size !== group.events.length ||
        final.attempt_state !== "SUCCEEDED" ||
        final.request_state !== "SUCCEEDED" ||
        group.events.some(
          (event) =>
            event.attempt_id !== final.attempt_id ||
            event.finality !== "FINAL" ||
            event.source !== "execution",
        )
      )
        throw new TerminalSettlementRejected();

      if (streamMode && !isInitialStreamTerminalGroup(group.events))
        throw new TerminalSettlementRejected();
      const [authorized] = await withDatabaseConnection(
        primary,
        limits,
        (db) =>
          db<{ project_id: string }[]>`SELECT q.project_id::text FROM public.execution_permits p
          JOIN public.attempts a ON a.id=p.attempt_id
          JOIN public.gateway_requests q ON q.id=a.request_id
          WHERE p.attempt_id=${final.attempt_id}::uuid
            AND p.manifest->>'operation'='chat' AND p.manifest->'resource_ids'='[]'::jsonb
            AND ${
              streamMode
                ? db`EXISTS (SELECT 1 FROM public.core_terminal_stream_grants s WHERE s.attempt_id=p.attempt_id
                AND s.owner_session_id=${final.origin_session_id}::uuid AND s.incarnation_id=${final.origin_incarnation}::uuid
                AND s.permit_epoch=${final.permit_epoch}::numeric AND s.priced_dimensions<@${final.final_dimensions}::text[])`
                : db`p.manifest->'stream'='false'::jsonb`
            }`,
      );
      if (!authorized) throw new TerminalSettlementRejected();
      if (streamMode) {
        const [session] = await withDatabaseConnection(
          primary,
          limits,
          (db) =>
            db<
              { public_key_spki: Buffer }[]
            >`SELECT public_key_spki FROM public.internal_startup_sessions WHERE session_id=${final.origin_session_id}::uuid`,
        );
        const key =
          session && createPublicKey({ key: session.public_key_spki, format: "der", type: "spki" });
        if (!group.events.every((event) => authenticate(event, key)))
          throw new TerminalSettlementRejected();
        if (streamMode === "online" && !allowStreamTerminal) throw new StreamTerminalDisabled();
      }
      const request = { group, limits };
      const outcome =
        fundingEpoch === undefined
          ? (await persistAndConfirm([request]))[0]!
          : await groups[shortChatFundingGroup(authorized.project_id)]!.add(request);
      if (outcome instanceof Error) throw outcome;
      return outcome;
    },
  };
}
