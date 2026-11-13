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

const resultsSchema = z.array(coreEvidenceGroupResultSchema);
const rejected = { status: "REJECTED", error: "evidence_signature_invalid" } as const;

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
      const parsed = z.array(coreEvidenceGroupV1Schema).min(1).max(128).parse(groups);
      const sessions = [
        ...new Set(parsed.flatMap((g) => g.events.map((e) => e.origin_session_id))),
      ];
      const keys = new Map<string, { key: KeyObject; digest: string }>();
      for (const row of await db<
        { session_id: string; public_key_spki: Buffer; public_key_sha256: string }[]
      >`SELECT session_id::text,public_key_spki,public_key_sha256 FROM internal_startup_sessions WHERE session_id=ANY(${sessions}::uuid[])`)
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
        return events.every((event) => event !== undefined) ? { events } : undefined;
      });
      const accepted = verified.filter((group) => group !== undefined);
      const persisted =
        accepted.length === 0
          ? []
          : resultsSchema.parse(
              await db.begin(
                async (tx) =>
                  (await tx`SELECT persist_core_evidence_groups_v1(${accepted}::jsonb) value`)[0]
                    .value,
              ),
            );
      if (persisted.length !== accepted.length) throw new Error("evidence_batch_result_mismatch");
      let next = 0;
      return verified.map((group) => (group ? persisted[next++]! : rejected));
    },
  };
}
