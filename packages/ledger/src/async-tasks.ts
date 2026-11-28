import type { SQL } from "bun";
import {
  and,
  desc,
  eq,
  getTableColumns,
  inArray,
  isNull,
  lt,
  ne,
  or,
  sql,
  type SQL as DrizzleSQL,
} from "drizzle-orm";
import { alias } from "drizzle-orm/pg-core";
import { apiKeys, asyncTasks, createDrizzle, type LedgerDrizzle } from "./schema.ts";
import type {
  ApiKeyRecord,
  AsyncTaskFailureCategory,
  AsyncTaskRecord,
  AsyncTaskRefundEligibility,
  AsyncTaskStatus,
} from "./types.ts";

function mapTaskRow(r: typeof asyncTasks.$inferSelect): AsyncTaskRecord {
  return {
    id: r.id,
    request_id: r.requestId,
    charge_id: r.chargeId,
    billing_account_id: r.billingAccountId,
    api_key_id: r.apiKeyId,
    channel_id: r.channelId,
    model: r.model,
    operation: r.operation,
    upstream_task_id: r.upstreamTaskId,
    status: r.status,
    user_cancelled: r.userCancelled,
    upstream_billed: r.upstreamBilled,
    failure_category: r.failureCategory,
    refund_eligibility: r.refundEligibility,
    refund_id: r.refundId,
    result_json: r.resultJson,
    created_at: new Date(r.createdAt).toISOString(),
    updated_at: new Date(r.updatedAt).toISOString(),
    completed_at: r.completedAt ? new Date(r.completedAt).toISOString() : null,
  };
}

type CheckTaskKeyPolicy = (
  task: Pick<AsyncTaskRecord, "model" | "operation">,
  key: Pick<ApiKeyRecord, "allowed_cidrs" | "models" | "operations">,
) => void;

const taskOwner = alias(apiKeys, "task_owner");

async function findTaskForKey(
  orm: Pick<LedgerDrizzle, "select">,
  scope: DrizzleSQL,
  keyId: string,
  checkPolicy: CheckTaskKeyPolicy,
) {
  const [row] = await orm
    .select({
      task: getTableColumns(asyncTasks),
      key: {
        allowed_cidrs: apiKeys.allowedCidrs,
        models: apiKeys.models,
        operations: apiKeys.operations,
      },
    })
    .from(asyncTasks)
    .innerJoin(
      apiKeys,
      and(eq(apiKeys.id, keyId), eq(apiKeys.billingAccountId, asyncTasks.billingAccountId)),
    )
    .leftJoin(
      taskOwner,
      and(
        eq(taskOwner.id, asyncTasks.apiKeyId),
        eq(taskOwner.billingAccountId, apiKeys.billingAccountId),
      ),
    )
    .where(
      and(
        scope,
        eq(apiKeys.enabled, true),
        ne(apiKeys.role, "VIEWER"),
        or(isNull(apiKeys.expiresAt), sql`${apiKeys.expiresAt} > clock_timestamp()`),
        or(
          eq(apiKeys.role, "ADMIN"),
          sql`COALESCE(${taskOwner.resourceScopeId}, ${taskOwner.id}) = COALESCE(${apiKeys.resourceScopeId}, ${apiKeys.id})`,
        ),
      ),
    );
  if (!row) return null;
  checkPolicy(row.task, row.key);
  return row.task;
}

type CreateTaskInput = {
  id: string;
  request_id: string;
  charge_id: string | null;
  billing_account_id: string;
  api_key_id: string | null;
  channel_id: string;
  model: string;
  operation: string;
  upstream_task_id: string;
  status?: AsyncTaskStatus;
  result_json?: Record<string, unknown>;
};

async function lockTaskAdmissionAccount(tx: SQL, billingAccountId: string): Promise<void> {
  await tx`SELECT pg_advisory_xact_lock(88421207, hashtext(${billingAccountId}::text))`;
}

async function insertAsyncTask(
  orm: Pick<LedgerDrizzle, "insert" | "select">,
  input: CreateTaskInput,
): Promise<AsyncTaskRecord> {
  const [row] = await orm
    .insert(asyncTasks)
    .values({
      id: input.id,
      requestId: input.request_id,
      chargeId: input.charge_id,
      billingAccountId: input.billing_account_id,
      apiKeyId: input.api_key_id,
      channelId: input.channel_id,
      model: input.model,
      operation: input.operation,
      upstreamTaskId: input.upstream_task_id,
      status: input.status ?? "QUEUED",
      resultJson: input.result_json ?? {},
    })
    .onConflictDoNothing({ target: asyncTasks.requestId })
    .returning();
  if (row) return mapTaskRow(row);
  const [existing] = await orm
    .select()
    .from(asyncTasks)
    .where(eq(asyncTasks.requestId, input.request_id));
  return mapTaskRow(existing!);
}

export async function registerAdmittedTask(
  tx: SQL,
  input: CreateTaskInput,
): Promise<AsyncTaskRecord> {
  await lockTaskAdmissionAccount(tx, input.billing_account_id);
  const [admission] = await tx`SELECT request_id FROM public.async_task_admissions
    WHERE request_id = ${input.request_id} AND billing_account_id = ${input.billing_account_id}
      AND expires_at > statement_timestamp() FOR UPDATE`;
  const taskOrm = createDrizzle(tx);
  if (!admission) {
    const [existing] = await taskOrm
      .select()
      .from(asyncTasks)
      .where(
        and(
          eq(asyncTasks.requestId, input.request_id),
          eq(asyncTasks.billingAccountId, input.billing_account_id),
        ),
      );
    if (existing) return mapTaskRow(existing);
    throw new Error("Video task admission is unavailable");
  }
  const task = await insertAsyncTask(taskOrm, input);
  await tx`DELETE FROM public.async_task_admissions WHERE request_id = ${input.request_id}`;
  return task;
}

export function createAsyncTasksRepository(db: SQL) {
  const orm = createDrizzle(db);

  return {
    createTask(input: CreateTaskInput): Promise<AsyncTaskRecord> {
      return insertAsyncTask(orm, input);
    },

    async reserveTaskSlot(input: {
      requestId: string;
      billingAccountId: string;
      limit: number;
      cancelCooldownMs: number;
      leaseMs: number;
    }): Promise<boolean> {
      return db.begin("isolation level read committed", async (tx) => {
        await lockTaskAdmissionAccount(tx, input.billingAccountId);
        const [result] = await tx<{ admitted: boolean }[]>`
          WITH reserved AS (
            INSERT INTO public.async_task_admissions (request_id, billing_account_id, expires_at)
            SELECT ${input.requestId}::uuid, ${input.billingAccountId}::uuid,
              statement_timestamp() + ${input.leaseMs}::bigint * interval '1 millisecond'
            WHERE (
              SELECT count(*) FROM public.async_tasks
              WHERE billing_account_id = ${input.billingAccountId}
                AND (status IN ('QUEUED', 'PROCESSING') OR (
                  ${input.cancelCooldownMs}::bigint > 0 AND status = 'CANCELLED' AND user_cancelled
                  AND completed_at >= statement_timestamp() - ${input.cancelCooldownMs}::bigint * interval '1 millisecond'
                ))
            ) + (
              SELECT count(*) FROM public.async_task_admissions
              WHERE billing_account_id = ${input.billingAccountId} AND expires_at > statement_timestamp()
            ) < ${input.limit}::int
            ON CONFLICT (request_id) DO NOTHING
            RETURNING request_id
          )
          SELECT EXISTS (SELECT 1 FROM reserved) OR EXISTS (
            SELECT 1 FROM public.async_task_admissions
            WHERE request_id = ${input.requestId} AND billing_account_id = ${input.billingAccountId}
              AND expires_at > statement_timestamp()
          ) AS admitted
        `;
        return result?.admitted === true;
      });
    },

    async releaseTaskSlot(requestId: string, billingAccountId: string): Promise<void> {
      await db`DELETE FROM public.async_task_admissions
        WHERE request_id = ${requestId} AND billing_account_id = ${billingAccountId}`;
    },

    async registerAdmittedTask(input: CreateTaskInput): Promise<AsyncTaskRecord> {
      return db.begin("isolation level read committed", (tx) => registerAdmittedTask(tx, input));
    },

    async expireTaskAdmissions(limit = 100): Promise<void> {
      await db`WITH expired AS (
        SELECT request_id FROM public.async_task_admissions WHERE expires_at <= statement_timestamp()
        ORDER BY expires_at, request_id LIMIT ${limit} FOR UPDATE SKIP LOCKED
      ) DELETE FROM public.async_task_admissions AS a USING expired WHERE a.request_id = expired.request_id`;
    },

    async getTask(id: string): Promise<AsyncTaskRecord | null> {
      const [row] = await orm.select().from(asyncTasks).where(eq(asyncTasks.id, id));
      return row ? mapTaskRow(row) : null;
    },

    async getTaskForKey(
      id: string,
      billingAccountId: string,
      keyId: string,
      checkPolicy: CheckTaskKeyPolicy,
    ) {
      const scope = and(eq(asyncTasks.id, id), eq(asyncTasks.billingAccountId, billingAccountId))!;
      const row = await findTaskForKey(orm, scope, keyId, checkPolicy);
      return row ? mapTaskRow(row) : null;
    },

    async countRecentAutoRefunds(billingAccountId: string, windowHours = 24): Promise<number> {
      const since = new Date(Date.now() - windowHours * 3_600_000).toISOString();
      const [row] = await orm
        .select({ count: sql<number>`count(*)::int` })
        .from(asyncTasks)
        .where(
          and(
            eq(asyncTasks.billingAccountId, billingAccountId),
            eq(asyncTasks.refundEligibility, "AUTO_REFUNDED"),
            sql`${asyncTasks.completedAt} >= ${since}::timestamptz`,
          ),
        );
      return Number(row?.count ?? 0);
    },

    async markUserCancelled(
      id: string,
      billingAccountId: string,
      keyId: string,
      checkPolicy: CheckTaskKeyPolicy,
    ): Promise<AsyncTaskRecord | null> {
      const scope = and(eq(asyncTasks.id, id), eq(asyncTasks.billingAccountId, billingAccountId))!;
      if (!(await findTaskForKey(orm, scope, keyId, checkPolicy))) return null;
      return orm.transaction(
        async (tx) => {
          const [task] = await tx
            .select({ id: asyncTasks.id })
            .from(asyncTasks)
            .where(scope)
            .for("no key update");
          if (!task) return null;
          const [key] = await tx
            .select({ id: apiKeys.id })
            .from(apiKeys)
            .where(and(eq(apiKeys.id, keyId), eq(apiKeys.billingAccountId, billingAccountId)))
            .for("share");
          if (!key || !(await findTaskForKey(tx, scope, keyId, checkPolicy))) return null;
          const [row] = await tx
            .update(asyncTasks)
            .set({
              userCancelled: true,
              status: sql`CASE WHEN ${asyncTasks.status} IN ('QUEUED', 'PROCESSING') THEN 'CANCELLED' ELSE ${asyncTasks.status} END`,
              failureCategory: sql`CASE WHEN ${asyncTasks.status} IN ('QUEUED', 'PROCESSING') THEN 'USER_CANCELLED' ELSE ${asyncTasks.failureCategory} END`,
              refundEligibility: sql`CASE WHEN ${asyncTasks.refundEligibility} = 'PENDING' THEN 'DENIED_USER_CANCELLED' ELSE ${asyncTasks.refundEligibility} END`,
              completedAt: sql`COALESCE(${asyncTasks.completedAt}, clock_timestamp())`,
              updatedAt: sql`clock_timestamp()`,
            })
            .where(scope)
            .returning();
          return row ? mapTaskRow(row) : null;
        },
        { isolationLevel: "read committed" },
      );
    },

    async updateFromPoll(input: {
      id: string;
      status: AsyncTaskStatus;
      upstream_billed?: boolean;
      failure_category?: AsyncTaskFailureCategory | null;
      refund_eligibility?: AsyncTaskRefundEligibility;
      refund_id?: string | null;
      result_json?: Record<string, unknown>;
    }): Promise<AsyncTaskRecord | null> {
      const terminal =
        input.status === "SUCCEEDED" || input.status === "FAILED" || input.status === "CANCELLED";
      const [row] = await orm
        .update(asyncTasks)
        .set({
          status: sql`CASE WHEN ${asyncTasks.userCancelled} THEN 'CANCELLED' ELSE ${input.status} END`,
          ...(input.upstream_billed !== undefined ? { upstreamBilled: input.upstream_billed } : {}),
          ...(input.failure_category !== undefined
            ? {
                failureCategory: sql`CASE WHEN ${asyncTasks.userCancelled} THEN 'USER_CANCELLED' ELSE ${input.failure_category} END`,
              }
            : {}),
          ...(input.refund_eligibility !== undefined
            ? {
                refundEligibility: sql`CASE WHEN ${asyncTasks.userCancelled} THEN 'DENIED_USER_CANCELLED' ELSE ${input.refund_eligibility} END`,
              }
            : {}),
          ...(input.refund_id !== undefined ? { refundId: input.refund_id } : {}),
          ...(input.result_json !== undefined ? { resultJson: input.result_json } : {}),
          ...(terminal ? { completedAt: sql`COALESCE(${asyncTasks.completedAt}, now())` } : {}),
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(asyncTasks.id, input.id),
            inArray(asyncTasks.status, ["QUEUED", "PROCESSING"]),
            eq(asyncTasks.refundEligibility, "PENDING"),
          ),
        )
        .returning();
      if (row) return mapTaskRow(row);
      const [existing] = await orm.select().from(asyncTasks).where(eq(asyncTasks.id, input.id));
      return existing ? mapTaskRow(existing) : null;
    },

    async markZeroChargeNotApplicable(id: string): Promise<AsyncTaskRecord | null> {
      const [row] = await orm
        .update(asyncTasks)
        .set({
          refundEligibility: "NOT_APPLICABLE",
          updatedAt: sql`now()`,
        })
        .where(
          and(
            eq(asyncTasks.id, id),
            eq(asyncTasks.refundEligibility, "AUTO_REFUNDED"),
            isNull(asyncTasks.refundId),
          ),
        )
        .returning();
      if (row) return mapTaskRow(row);
      const [existing] = await orm.select().from(asyncTasks).where(eq(asyncTasks.id, id));
      return existing ? mapTaskRow(existing) : null;
    },

    async claimAutoRefund(input: {
      id: string;
      max_per_24h: number;
      result_json: Record<string, unknown>;
    }): Promise<
      | { outcome: "not_found" }
      | { outcome: "already_terminal"; task: AsyncTaskRecord }
      | { outcome: "denied_user_cancelled"; task: AsyncTaskRecord }
      | { outcome: "denied_abuse_limit"; task: AsyncTaskRecord }
      | { outcome: "claimed"; task: AsyncTaskRecord }
    > {
      return db.begin(async (tx) => {
        const rows = await tx`
          SELECT id, request_id, charge_id, billing_account_id, api_key_id, channel_id, model, operation,
            upstream_task_id, status, user_cancelled, upstream_billed, failure_category,
            refund_eligibility, refund_id, result_json, created_at, updated_at, completed_at
          FROM public.async_tasks
          WHERE id = ${input.id}
          FOR UPDATE
        `;
        if (!rows.length) return { outcome: "not_found" };
        const raw = rows[0]!;
        const toRec = (r: Record<string, unknown>): AsyncTaskRecord => ({
          id: String(r.id),
          request_id: String(r.request_id),
          charge_id: r.charge_id ? String(r.charge_id) : null,
          billing_account_id: String(r.billing_account_id),
          api_key_id: r.api_key_id ? String(r.api_key_id) : null,
          channel_id: String(r.channel_id),
          model: String(r.model),
          operation: String(r.operation),
          upstream_task_id: String(r.upstream_task_id),
          status: String(r.status) as AsyncTaskStatus,
          user_cancelled: Boolean(r.user_cancelled),
          upstream_billed: Boolean(r.upstream_billed),
          failure_category: r.failure_category
            ? (String(r.failure_category) as AsyncTaskFailureCategory)
            : null,
          refund_eligibility: String(r.refund_eligibility) as AsyncTaskRefundEligibility,
          refund_id: r.refund_id ? String(r.refund_id) : null,
          result_json: (r.result_json ?? {}) as Record<string, unknown>,
          created_at: new Date(String(r.created_at)).toISOString(),
          updated_at: new Date(String(r.updated_at)).toISOString(),
          completed_at: r.completed_at ? new Date(String(r.completed_at)).toISOString() : null,
        });
        if (raw.user_cancelled) {
          const [upd] = await tx`
            UPDATE public.async_tasks
            SET status = 'CANCELLED',
                failure_category = 'USER_CANCELLED',
                refund_eligibility = 'DENIED_USER_CANCELLED',
                result_json = ${input.result_json}::jsonb,
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE id = ${input.id}
            RETURNING *
          `;
          return { outcome: "denied_user_cancelled", task: toRec(upd!) };
        }
        const status = String(raw.status);
        const eligibility = String(raw.refund_eligibility);
        if (
          eligibility !== "PENDING" ||
          status === "SUCCEEDED" ||
          status === "FAILED" ||
          status === "CANCELLED"
        ) {
          return { outcome: "already_terminal", task: toRec(raw) };
        }
        const billingAccountId = String(raw.billing_account_id);
        await tx`SELECT pg_advisory_xact_lock(hashtext('async_refund:' || ${billingAccountId}))`;
        const [cntRow] = await tx`
          SELECT count(*)::int AS cnt
          FROM public.async_tasks
          WHERE billing_account_id = ${billingAccountId}
            AND refund_eligibility = 'AUTO_REFUNDED'
            AND completed_at >= now() - interval '24 hours'
        `;
        const recent = Number(cntRow?.cnt ?? 0);
        if (recent >= input.max_per_24h) {
          const [upd] = await tx`
            UPDATE public.async_tasks
            SET status = 'FAILED',
                failure_category = 'UPSTREAM_SYSTEM',
                refund_eligibility = 'DENIED_ABUSE_LIMIT',
                result_json = ${input.result_json}::jsonb,
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE id = ${input.id}
            RETURNING *
          `;
          return { outcome: "denied_abuse_limit", task: toRec(upd!) };
        }
        const [upd] = await tx`
          UPDATE public.async_tasks
          SET status = 'FAILED',
              failure_category = 'UPSTREAM_SYSTEM',
              refund_eligibility = 'AUTO_REFUNDED',
              result_json = ${input.result_json}::jsonb,
              completed_at = COALESCE(completed_at, now()),
              updated_at = now()
          WHERE id = ${input.id}
          RETURNING *
        `;
        return { outcome: "claimed", task: toRec(upd!) };
      });
    },

    async finalizeAutoRefund(
      id: string,
      refundId: string | null,
      fallbackToManualReview = false,
    ): Promise<AsyncTaskRecord | null> {
      const [row] = await orm
        .update(asyncTasks)
        .set(
          fallbackToManualReview
            ? { refundEligibility: "MANUAL_REVIEW", updatedAt: sql`now()` }
            : { refundId, updatedAt: sql`now()` },
        )
        .where(eq(asyncTasks.id, id))
        .returning();
      return row ? mapTaskRow(row) : null;
    },

    async expireStaleTasks(olderThanMs: number, limit = 100): Promise<number> {
      const cutoff = new Date(Date.now() - olderThanMs).toISOString();
      const stale = await orm
        .select({ id: asyncTasks.id })
        .from(asyncTasks)
        .where(
          and(
            inArray(asyncTasks.status, ["QUEUED", "PROCESSING"]),
            lt(asyncTasks.updatedAt, cutoff),
          ),
        )
        .limit(limit);
      if (stale.length === 0) return 0;
      const updated = await orm
        .update(asyncTasks)
        .set({
          status: "FAILED",
          failureCategory: "TIMEOUT",
          refundEligibility: sql`CASE WHEN ${asyncTasks.userCancelled} THEN 'DENIED_USER_CANCELLED' ELSE 'MANUAL_REVIEW' END`,
          completedAt: sql`now()`,
          updatedAt: sql`now()`,
        })
        .where(
          and(
            inArray(
              asyncTasks.id,
              stale.map((s) => s.id),
            ),
            inArray(asyncTasks.status, ["QUEUED", "PROCESSING"]),
          ),
        )
        .returning({ id: asyncTasks.id });
      return updated.length;
    },

    async listTasks(input: {
      billing_account_id?: string;
      status?: AsyncTaskStatus;
      refund_eligibility?: AsyncTaskRefundEligibility;
      limit?: number;
      offset?: number;
    }) {
      const limit = Math.min(200, Math.max(1, input.limit ?? 50));
      const offset = Math.max(0, input.offset ?? 0);
      const conds: DrizzleSQL[] = [];
      if (input.billing_account_id)
        conds.push(eq(asyncTasks.billingAccountId, input.billing_account_id));
      if (input.status) conds.push(eq(asyncTasks.status, input.status));
      if (input.refund_eligibility) {
        conds.push(eq(asyncTasks.refundEligibility, input.refund_eligibility));
      }
      const where = conds.length > 0 ? and(...conds) : undefined;
      const [rows, countRows] = await Promise.all([
        orm
          .select()
          .from(asyncTasks)
          .where(where)
          .orderBy(desc(asyncTasks.createdAt), desc(asyncTasks.id))
          .limit(limit)
          .offset(offset),
        orm
          .select({ total: sql<number>`count(*)::int` })
          .from(asyncTasks)
          .where(where),
      ]);
      return {
        items: rows.map(mapTaskRow),
        total: Number(countRows[0]?.total ?? 0),
        limit,
        offset,
      };
    },
  };
}
