import { createPublicKey, verify, type KeyObject } from "node:crypto";
import type { SQL } from "bun";
import {
  coreEvidenceGroupResultSchema,
  coreEvidenceGroupV1Schema,
  evidenceProofFields,
  type CoreEvidenceGroupResult,
  type CoreEvidenceGroupV1,
  type SignedCoreEvidenceEventV1,
} from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { z } from "zod";
import { withDatabaseConnection, withDatabaseTransaction } from "../transaction.ts";

const resultsSchema = z.array(coreEvidenceGroupResultSchema);

function authenticate(event: SignedCoreEvidenceEventV1, key: KeyObject | undefined) {
  if (!key) return undefined;
  const { signature, ...body } = event;
  const payload = Buffer.from(JSON.stringify(evidenceProofFields(body, sha256(canonical(body)))));
  const bytes = Buffer.from(signature, "base64url");
  if (bytes.length !== 64 || !verify(null, payload, key, bytes)) return undefined;
  return {
    ...body,
    signature: bytes.toString("hex"),
    signature_payload_digest: sha256(payload),
  };
}

export function createSettlementStore(db: SQL) {
  return {
    async persist(groups: CoreEvidenceGroupV1[]): Promise<CoreEvidenceGroupResult[]> {
      const deadline = performance.now() + 8000;
      const parsed = z.array(coreEvidenceGroupV1Schema).min(1).max(128).parse(groups);
      const sessions = [
        ...new Set(parsed.flatMap((g) => g.events.map((e) => e.origin_session_id))),
      ];
      const keys = new Map<string, { key: KeyObject; digest: string }>();
      const sessionRows = await withDatabaseConnection(
        db,
        { deadline },
        (connection) =>
          connection<
            { session_id: string; public_key_spki: Buffer; public_key_sha256: string }[]
          >`SELECT session_id::text,public_key_spki,public_key_sha256 FROM internal_startup_sessions WHERE session_id=ANY(${sessions}::uuid[])`,
      );
      for (const row of sessionRows)
        keys.set(row.session_id, {
          key: createPublicKey({
            key: Buffer.from(row.public_key_spki),
            format: "der",
            type: "spki",
          }),
          digest: row.public_key_sha256,
        });
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
          await withDatabaseTransaction(
            db,
            { deadline },
            async (tx) =>
              (await tx`SELECT persist_core_evidence_groups_v1(${chunk}::jsonb) value`)[0].value,
          ),
        );
        if (results.length !== chunk.length) throw new Error("evidence_batch_result_mismatch");
        for (let i = 0; i < results.length; i++) {
          const result = results[i]!;
          if (result.status === "ERROR" && result.error === "P0F01")
            results[i] = coreEvidenceGroupResultSchema.parse(
              await withDatabaseTransaction(
                db,
                { deadline },
                async (tx) =>
                  (await tx`SELECT persist_core_evidence_slow_v1(${chunk[i]}::jsonb) value`)[0]
                    .value,
              ),
            );
        }
        persisted.push(...results);
      }
      if (persisted.length !== verified.length) throw new Error("evidence_batch_result_mismatch");
      return persisted;
    },
  };
}
