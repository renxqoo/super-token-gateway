import type { SQL } from "bun";
import {
  coreEvidenceGroupResultSchema,
  coreEvidenceGroupV1Schema,
  type CoreEvidenceGroupResult,
  type CoreEvidenceGroupV1,
} from "@gateway/contracts";
import { z } from "zod";

const resultsSchema = z.array(coreEvidenceGroupResultSchema);
export function createSettlementStore(db: SQL) {
  return {
    async persist(groups: CoreEvidenceGroupV1[]): Promise<CoreEvidenceGroupResult[]> {
      const parsed = z.array(coreEvidenceGroupV1Schema).min(1).max(128).parse(groups);
      const value = await db.begin(
        async (tx) =>
          (await tx`SELECT persist_core_evidence_groups_v1(${parsed}::jsonb) value`)[0].value,
      );
      const results = resultsSchema.parse(value);
      if (results.length !== parsed.length) throw new Error("evidence_batch_result_mismatch");
      return results;
    },
  };
}
