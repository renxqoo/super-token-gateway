import type { CoreEvidenceGroupResult, CoreEvidenceGroupV1 } from "@gateway/contracts";
import { Batcher } from "@gateway/runtime";

type Persist = (groups: CoreEvidenceGroupV1[]) => Promise<CoreEvidenceGroupResult[]>;

const lockConflicts = new Set(["55P03", "40P01"]);
const lockRetryBudgetMs = 2000;
const batchGroupLimit = 128;
const maxJobsPerBatch = 4;
const maxConcurrentBatches = 4;

function lockConflict(result: CoreEvidenceGroupResult) {
  return result.status === "ERROR" && lockConflicts.has(result.error);
}

async function persistThroughLockConflicts(persist: Persist, groups: CoreEvidenceGroupV1[]) {
  const results = await persist(groups);
  const deadline = Date.now() + lockRetryBudgetMs;
  let pending = results.flatMap((result, index) => (lockConflict(result) ? [index] : []));
  for (let ceiling = 10; pending.length; ceiling = Math.min(ceiling * 2, 200)) {
    const delay = 1 + Math.floor(Math.random() * ceiling);
    if (Date.now() + delay >= deadline) break;
    await Bun.sleep(delay);
    const retried = await persist(pending.map((index) => groups[index]!));
    pending.forEach((index, at) => (results[index] = retried[at]!));
    pending = pending.filter((index) => lockConflict(results[index]!));
  }
  return results;
}

export function createSettlementProcessor(options: {
  persist: Persist;
  onRejected?: (count: number) => void;
  onQuarantined?: (count: number) => void;
}) {
  const counters = {
    calls: 0,
    jobs: 0,
    groups: 0,
    events: 0,
    max_jobs: 0,
    max_groups: 0,
    lock_conflicts: 0,
    errors: 0,
  };
  const batch = new Batcher<CoreEvidenceGroupV1[], CoreEvidenceGroupResult[]>(
    async (jobs) => {
      const groups = jobs.flat();
      counters.calls++;
      counters.jobs += jobs.length;
      counters.groups += groups.length;
      counters.events += groups.reduce((sum, group) => sum + group.events.length, 0);
      counters.max_jobs = Math.max(counters.max_jobs, jobs.length);
      counters.max_groups = Math.max(counters.max_groups, groups.length);
      try {
        const results = await options.persist(groups);
        if (results.length !== groups.length) throw new Error("evidence_batch_result_mismatch");
        counters.lock_conflicts += results.filter(lockConflict).length;
        let offset = 0;
        return jobs.map((job) => {
          const result = results.slice(offset, offset + job.length);
          offset += job.length;
          return result;
        });
      } catch (error) {
        counters.errors++;
        throw error;
      }
    },
    batchGroupLimit,
    1,
    maxConcurrentBatches,
    4096,
    {
      weight: (groups) =>
        groups.length ? Math.max(groups.length, batchGroupLimit / maxJobsPerBatch) : 0,
    },
  );
  const active = new Set<Promise<CoreEvidenceGroupResult[]>>();
  let closing: Promise<void> | undefined;
  async function run(groups: CoreEvidenceGroupV1[]) {
    const results = await persistThroughLockConflicts((items) => batch.add(items), groups);
    const quarantined = results
      .flatMap((result) => (result.status === "COMMITTED" ? result.receipts : []))
      .filter((receipt) => receipt.outcome === "QUARANTINED" && !receipt.duplicate).length;
    if (quarantined > 0) options.onQuarantined?.(quarantined);
    if (results.some((result) => result.status === "ERROR"))
      throw new Error("evidence_group_retry");
    const rejected = results.filter((result) => result.status === "REJECTED").length;
    if (rejected > 0) options.onRejected?.(rejected);
    return results;
  }
  return {
    stats: () => ({ ...counters, active_jobs: active.size }),
    process(groups: CoreEvidenceGroupV1[]) {
      if (closing) return Promise.reject(new Error("evidence_processor_stopping"));
      const processing = run(groups).finally(() => active.delete(processing));
      active.add(processing);
      return processing;
    },
    close() {
      closing ??= Promise.allSettled(active).then(() => undefined);
      return closing;
    },
  };
}
