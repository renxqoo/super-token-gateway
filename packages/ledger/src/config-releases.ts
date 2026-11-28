import type { SQL } from "bun";
import { DomainError } from "@gateway/kernel";

const nodeAckExpirySeconds = 6;

type ReleaseScope =
  { kind: "PLUGIN"; model_name: null } | { kind: "ROUTE" | "MODEL"; model_name: string };

export type ConfigReleaseRecord<Change> = {
  id: string;
  actor: string;
  request_digest: string;
  config_epoch: number;
  expected_nodes: string[];
  changes: Change[];
  rollback_of: string | null;
};

type ReleaseIdentity = Pick<ConfigReleaseRecord<unknown>, "id" | "actor" | "request_digest">;

export function createGatewayConfigNodeRepository(db: SQL) {
  return {
    async report(nodeId: string, epoch: number, observedEpochs: number[]): Promise<void> {
      await db`
        WITH node AS (
          INSERT INTO public.gateway_config_nodes (id, applied_epoch)
          VALUES (${nodeId}, ${epoch})
          ON CONFLICT (id) DO UPDATE SET
            applied_epoch = GREATEST(gateway_config_nodes.applied_epoch, EXCLUDED.applied_epoch),
            last_seen_at = clock_timestamp()
          RETURNING id
        )
        INSERT INTO public.config_release_acks (config_epoch, node_id)
        SELECT r.config_epoch, node.id FROM public.config_releases r CROSS JOIN node
        WHERE r.config_epoch = ANY(ARRAY(SELECT jsonb_array_elements_text(${JSON.stringify(observedEpochs)}::text::jsonb)::bigint))
        ON CONFLICT DO NOTHING
      `;
    },

    async stop(nodeId: string): Promise<void> {
      await db`UPDATE public.gateway_config_nodes SET stopped_at = clock_timestamp() WHERE id = ${nodeId}`;
    },

    async pruneStaleNodes(olderThanHours = 24): Promise<number> {
      const rows = await db<{ id: string }[]>`
        DELETE FROM public.gateway_config_nodes n
        WHERE (n.stopped_at IS NOT NULL OR n.last_seen_at < clock_timestamp() - ${olderThanHours} * interval '1 hour')
          AND NOT EXISTS (
            SELECT 1 FROM public.config_release_acks a WHERE a.node_id = n.id
          )
          AND NOT EXISTS (
            SELECT 1 FROM public.config_releases r WHERE n.id = ANY(r.expected_nodes)
          )
        RETURNING n.id
      `;
      return rows.length;
    },
  };
}

export function createConfigReleaseStore<Change>(
  db: SQL,
  scope: ReleaseScope,
  conflictCode: string,
) {
  function conflict(message: string): never {
    throw new DomainError(conflictCode, message);
  }

  async function get(id: string, conn: SQL = db): Promise<ConfigReleaseRecord<Change> | null> {
    const [row] = await conn<ConfigReleaseRecord<Change>[]>`
      SELECT id, actor, request_digest, config_epoch, expected_nodes, changes, rollback_of
      FROM public.config_releases
      WHERE id = ${id} AND kind = ${scope.kind} AND model_name IS NOT DISTINCT FROM ${scope.model_name}::text
    `;
    return row ? { ...row, config_epoch: Number(row.config_epoch) } : null;
  }

  return {
    get,

    async lockAndReplay(tx: SQL, input: ReleaseIdentity) {
      await tx`SELECT pg_advisory_xact_lock(hashtextextended(${input.id}, 671014))`;
      const previous = await get(input.id, tx);
      if (
        previous &&
        (previous.actor !== input.actor || previous.request_digest !== input.request_digest)
      ) {
        conflict("Release identifier already belongs to another request");
      }
      return previous;
    },

    async record(
      tx: SQL,
      input: ReleaseIdentity & {
        expected_epoch: number;
        expected_lineage?: string;
        changes: Change[];
        rollback_of: string | null;
      },
    ): Promise<ConfigReleaseRecord<Change>> {
      const [epoch] = await tx`
        UPDATE public.config_epoch SET version = version + 1, updated_at = now()
        WHERE id = 1 AND version = ${input.expected_epoch}
          AND lineage_token = COALESCE(${input.expected_lineage ?? null}::uuid, lineage_token)
        RETURNING version
      `;
      if (!epoch) conflict("Configuration epoch changed. Validate against the current epoch.");
      const roster = await tx<{ id: string }[]>`
        SELECT id FROM public.gateway_config_nodes WHERE stopped_at IS NULL
          AND last_seen_at >= clock_timestamp() - ${nodeAckExpirySeconds} * interval '1 second'
        ORDER BY id
      `;
      const record = {
        id: input.id,
        actor: input.actor,
        request_digest: input.request_digest,
        config_epoch: Number(epoch.version),
        expected_nodes: roster.map(({ id }) => id),
        changes: input.changes,
        rollback_of: input.rollback_of,
      };
      const [inserted] = await tx`
        INSERT INTO public.config_releases
          (id, actor, request_digest, config_epoch, expected_nodes, changes, rollback_of, kind, model_name)
        VALUES (
          ${record.id}, ${record.actor}, ${record.request_digest}, ${record.config_epoch},
          ARRAY(SELECT jsonb_array_elements_text(${JSON.stringify(record.expected_nodes)}::text::jsonb)::uuid),
          ${JSON.stringify(record.changes)}::text::jsonb, ${record.rollback_of}, ${scope.kind}, ${scope.model_name}
        )
        ON CONFLICT (id) DO NOTHING RETURNING id
      `;
      if (!inserted) conflict("Release identifier already belongs to another request");
      return record;
    },

    async confirmations(release: ConfigReleaseRecord<Change>) {
      return db`
        SELECT n.id AS node_id, n.applied_epoch::float8 AS applied_epoch, n.last_seen_at, n.stopped_at, a.applied_at,
          CASE
            WHEN a.applied_at IS NOT NULL THEN 'APPLIED'
            WHEN n.applied_epoch > ${release.config_epoch} THEN 'SUPERSEDED'
            WHEN n.stopped_at IS NOT NULL
              OR n.last_seen_at < clock_timestamp() - ${nodeAckExpirySeconds} * interval '1 second'
              THEN 'OFFLINE'
            ELSE 'PENDING'
          END AS status
        FROM public.gateway_config_nodes n
        LEFT JOIN public.config_release_acks a ON a.node_id = n.id AND a.config_epoch = ${release.config_epoch}
        WHERE n.id = ANY(ARRAY(SELECT jsonb_array_elements_text(${JSON.stringify(release.expected_nodes)}::text::jsonb)::uuid))
        ORDER BY n.id
      `;
    },
  };
}
