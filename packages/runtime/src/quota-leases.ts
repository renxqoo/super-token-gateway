import { Limits } from "./limits.ts";
import { sha256 } from "./crypto.ts";
export const quotaLeaseTiming = Object.freeze({
  draftMs: 30000,
  liveMs: 30000,
  renewMs: 10000,
  tombstoneMs: 30000,
  safetyMs: 1000,
});
export type QuotaScope = {
  kind: "organization" | "principal" | "project" | "key" | "channel";
  id: string;
  rpm: number;
  tpm: number;
  concurrency: number;
};
export type QuotaDescriptor = {
  lease_id: string;
  namespace: string;
  generation: string;
  shard: number;
  scope_kind: QuotaScope["kind"];
  scope_id: string;
  command_id: string;
  kind: string;
  caller: string;
  body_digest: string;
  allocation_digest: string;
  owner_session_id: string;
  deadline_ms: number;
  tokens: number;
  bucket: string;
  revision: string;
  state: "DRAFT" | "ADOPTED" | "RELEASED" | "EXPIRED";
  attempt_id: string | null;
  permit_epoch: string | null;
  remaining_ms: number;
};
export type QuotaCleanup =
  | { state: "REJECTED"; command_id: string; body_digest: string }
  | { state: "RELEASE_PENDING"; attempt_id: string; permit_epoch: string };
const script = `
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local action=ARGV[1]
local input=cjson.decode(ARGV[2])
local raw=redis.call('GET',KEYS[1])
local record=raw and cjson.decode(raw) or nil
local function save() redis.call('SET',KEYS[1],cjson.encode(record),'PXAT',record.deadline_ms+30000) end
local function reply()
 record.remaining_ms=math.max(0,(record.live_until or 0)-now)
 return cjson.encode(record)
end
redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
redis.call('ZREMRANGEBYSCORE',KEYS[3],'-inf',now-60000)
if action=='ACQUIRE' then
 if record then
  if record.identity~=input.identity then return redis.error_reply('QUOTA_CONFLICT') end
  if record.state=='RELEASED' or record.state=='EXPIRED' or record.live_until<=now then return redis.error_reply('QUOTA_EXPIRED') end
  return reply()
 end
 if input.deadline_ms<=now or input.deadline_ms>now+3600000 then return redis.error_reply('QUOTA_DEADLINE') end
 local bucket=tostring(math.floor(now/60000))
 local tokenKey=KEYS[4]..':'..bucket
 local tokens=tonumber(redis.call('GET',tokenKey) or '0')
 if redis.call('ZCARD',KEYS[2])>=input.concurrency or redis.call('ZCARD',KEYS[3])>=input.rpm or tokens+input.tokens>input.tpm then return redis.error_reply('QUOTA_LIMIT') end
 record=input
 record.state='DRAFT';record.bucket=bucket;record.revision='0';record.operations={};record.operation_count=0;record.attempt_id=cjson.null;record.permit_epoch=cjson.null
 record.live_until=math.min(now+30000,record.deadline_ms)
 redis.call('ZADD',KEYS[2],record.live_until,record.lease_id)
 redis.call('ZADD',KEYS[3],now,record.lease_id)
 redis.call('PEXPIREAT',KEYS[2],math.max(record.deadline_ms+30000,now+90000))
 redis.call('PEXPIRE',KEYS[3],90000)
 redis.call('INCRBY',tokenKey,record.tokens)
 redis.call('PEXPIREAT',tokenKey,(tonumber(bucket)+2)*60000)
 save();return reply()
end
if not record or record.identity~=input.identity then return redis.error_reply('QUOTA_MISSING') end
if action=='RELEASE' then
 if input.authority=='REJECTED' then
  if record.state=='ADOPTED' or record.attempt_id~=cjson.null then return redis.error_reply('QUOTA_AUTHORITY') end
 elseif input.authority=='RELEASE_PENDING' then
  if record.attempt_id~=cjson.null and (record.attempt_id~=input.attempt_id or record.permit_epoch~=input.permit_epoch) then return redis.error_reply('QUOTA_AUTHORITY') end
 else return redis.error_reply('QUOTA_AUTHORITY') end
 record.state='RELEASED';record.live_until=0;redis.call('ZREM',KEYS[2],record.lease_id);save();return reply()
end
if action=='RECONCILE' then
 if record.attempt_id==cjson.null or record.attempt_id~=input.attempt_id or record.permit_epoch~=input.permit_epoch then return redis.error_reply('QUOTA_OWNER') end
 local tokenKey=KEYS[4]..':'..record.bucket
 if redis.call('EXISTS',tokenKey)==1 then
  redis.call('INCRBY',tokenKey,input.target-record.tokens)
 end
 record.tokens=input.target;save();return reply()
end
if record.state=='RELEASED' or record.state=='EXPIRED' or record.live_until<=now or record.deadline_ms<=now then
 if record.state~='RELEASED' then record.state='EXPIRED';record.live_until=0;save() end
 return redis.error_reply('QUOTA_EXPIRED')
end
if action=='ADOPT' or action=='RENEW' then
 if record.attempt_id~=cjson.null and (record.attempt_id~=input.attempt_id or record.permit_epoch~=input.permit_epoch) then return redis.error_reply('QUOTA_OWNER') end
 if record.operations[input.operation_id] then
  local saved=record.operations[input.operation_id];record.revision=saved.revision;record.live_until=saved.live_until;return reply()
 end
 if record.revision~=input.revision or (action=='RENEW' and record.state~='ADOPTED') then return redis.error_reply('QUOTA_REVISION') end
 if record.operation_count>=4096 then return redis.error_reply('QUOTA_OPERATIONS') end
 record.state='ADOPTED';record.attempt_id=input.attempt_id;record.permit_epoch=input.permit_epoch
 record.revision=tostring(tonumber(record.revision)+1);record.last_op=input.operation_id
 record.live_until=math.min(now+30000,record.deadline_ms)
 record.operation_count=record.operation_count+1;record.operations[input.operation_id]={revision=record.revision,live_until=record.live_until}
 redis.call('ZADD',KEYS[2],record.live_until,record.lease_id);save();return reply()
end
return redis.error_reply('QUOTA_ACTION')`;
export function createQuotaLeases(options: {
  urls: readonly string[];
  namespace: string;
  generation: string;
  timeoutMs?: number;
}) {
  if (
    !/^[A-Za-z0-9_-]{1,80}$/.test(options.namespace) ||
    !/^[A-Za-z0-9_-]{1,80}$/.test(options.generation)
  )
    throw new Error("Invalid quota generation");
  const shards = options.urls.map(
    (url) => new Limits(url, options.namespace, options.timeoutMs ?? 1000),
  );
  if (shards.length < 1 || shards.length > 8) throw new Error("Invalid quota shards");
  function keys(
    d: Pick<
      QuotaDescriptor,
      "scope_kind" | "scope_id" | "lease_id" | "generation" | "namespace" | "shard"
    >,
  ) {
    if (
      d.namespace !== options.namespace ||
      d.generation !== options.generation ||
      !shards[d.shard]
    )
      throw new Error("Quota generation unavailable");
    const base = `${options.namespace}:{${sha256(`${d.scope_kind}:${d.scope_id}`)}}:${options.generation}`;
    return [`${base}:lease:${d.lease_id}`, `${base}:active`, `${base}:rpm`, `${base}:tokens`];
  }
  const identity = (
    d: Pick<
      QuotaDescriptor,
      | "caller"
      | "kind"
      | "command_id"
      | "body_digest"
      | "allocation_digest"
      | "owner_session_id"
      | "deadline_ms"
      | "scope_kind"
      | "scope_id"
    >,
  ) =>
    sha256(
      JSON.stringify([
        d.caller,
        d.kind,
        d.command_id,
        d.body_digest,
        d.allocation_digest,
        d.owner_session_id,
        d.deadline_ms,
        d.scope_kind,
        d.scope_id,
      ]),
    );
  async function call(action: string, d: QuotaDescriptor, input: Record<string, unknown> = {}) {
    const started = performance.now();
    const result = await shards[d.shard]!.command("EVAL", [
      script,
      "4",
      ...keys(d),
      action,
      JSON.stringify({ ...d, identity: identity(d), ...input }),
    ]);
    const r = JSON.parse(String(result)) as QuotaDescriptor;
    const elapsed = performance.now() - started;
    const proofDeadline =
      performance.now() + Math.max(0, r.remaining_ms - elapsed - quotaLeaseTiming.safetyMs);
    return {
      descriptor: {
        lease_id: r.lease_id,
        namespace: r.namespace,
        generation: r.generation,
        shard: r.shard,
        scope_kind: r.scope_kind,
        scope_id: r.scope_id,
        command_id: r.command_id,
        kind: r.kind,
        caller: r.caller,
        body_digest: r.body_digest,
        allocation_digest: r.allocation_digest,
        owner_session_id: r.owner_session_id,
        deadline_ms: r.deadline_ms,
        tokens: r.tokens,
        bucket: r.bucket,
        revision: r.revision,
        state: r.state,
        attempt_id: r.attempt_id,
        permit_epoch: r.permit_epoch,
        remaining_ms: r.remaining_ms,
      },
      proofDeadline,
    };
  }
  return {
    async acquire(input: {
      scope: QuotaScope;
      caller: string;
      kind: string;
      command_id: string;
      body_digest: string;
      owner_session_id: string;
      deadline_ms: number;
      tokens: number;
    }) {
      const s = input.scope;
      if (
        !/^[A-Za-z0-9_-]{1,100}$/.test(s.id) ||
        ![s.rpm, s.tpm, s.concurrency, input.tokens].every(
          (n) => Number.isSafeInteger(n) && n >= 0,
        ) ||
        s.rpm < 1 ||
        s.tpm < 1 ||
        s.concurrency < 1
      )
        throw new Error("Invalid quota scope");
      const lease = sha256(
        JSON.stringify([input.caller, input.kind, input.command_id, s.kind, s.id]),
      );
      const d: QuotaDescriptor = {
        lease_id: lease,
        namespace: options.namespace,
        generation: options.generation,
        shard: parseInt(sha256(`${s.kind}:${s.id}`).slice(0, 8), 16) % shards.length,
        scope_kind: s.kind,
        scope_id: s.id,
        caller: input.caller,
        kind: input.kind,
        command_id: input.command_id,
        body_digest: input.body_digest,
        allocation_digest: sha256(JSON.stringify([s, input.tokens])),
        owner_session_id: input.owner_session_id,
        deadline_ms: input.deadline_ms,
        tokens: input.tokens,
        bucket: "",
        revision: "0",
        state: "DRAFT",
        attempt_id: null,
        permit_epoch: null,
        remaining_ms: 0,
      };
      return call("ACQUIRE", d, { rpm: s.rpm, tpm: s.tpm, concurrency: s.concurrency });
    },
    adopt(d: QuotaDescriptor, attempt_id: string, permit_epoch: string, operation_id: string) {
      return call("ADOPT", d, { attempt_id, permit_epoch, operation_id });
    },
    renew(d: QuotaDescriptor, attempt_id: string, permit_epoch: string, operation_id: string) {
      return call("RENEW", d, { attempt_id, permit_epoch, operation_id });
    },
    release(d: QuotaDescriptor, authority: QuotaCleanup) {
      if (
        authority.state === "REJECTED" &&
        (authority.command_id !== d.command_id || authority.body_digest !== d.body_digest)
      )
        throw new Error("Quota cleanup denied");
      return call("RELEASE", d, { authority: authority.state, ...authority });
    },
    reconcile(d: QuotaDescriptor, attempt_id: string, permit_epoch: string, target: number) {
      if (!Number.isSafeInteger(target) || target < 0) throw new Error("Invalid quota target");
      return call("RECONCILE", d, { attempt_id, permit_epoch, target });
    },
    close() {
      for (const shard of shards) shard.close();
    },
  };
}
export type QuotaLeases = ReturnType<typeof createQuotaLeases>;
