SET LOCAL statement_timeout = '5min';

ALTER TABLE public.execution_quota_leases SET (fillfactor=70);
ALTER TABLE public.execution_quota_leases ADD COLUMN live_deadline_ms bigint;

CREATE OR REPLACE FUNCTION public.core_quota_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_OP='UPDATE' AND (
  (NEW.lease_id,NEW.attempt_id,NEW.command_id,NEW.request_id,NEW.scope_kind,NEW.scope_id)
   IS DISTINCT FROM (OLD.lease_id,OLD.attempt_id,OLD.command_id,OLD.request_id,OLD.scope_kind,OLD.scope_id)
  OR NEW.revision<OLD.revision OR OLD.state IN ('RELEASE_PENDING','RELEASED','EXPIRED') AND NEW.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED')
 ) THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
 NEW.live_deadline_ms=CASE WHEN NEW.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') THEN (NEW.descriptor->>'deadline_ms')::bigint END;
 RETURN NEW;
END $$;
CREATE OR REPLACE TRIGGER core_quota_guard BEFORE INSERT OR UPDATE OR DELETE ON public.execution_quota_leases FOR EACH ROW EXECUTE FUNCTION public.core_quota_guard_v1();

UPDATE public.execution_quota_leases SET live_deadline_ms=(descriptor->>'deadline_ms')::bigint
 WHERE state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING');

DROP INDEX public.execution_quota_live_deadline_idx;
CREATE INDEX execution_quota_live_deadline_idx ON public.execution_quota_leases (live_deadline_ms,request_id,lease_id) WHERE live_deadline_ms IS NOT NULL;

CREATE OR REPLACE FUNCTION public.core_quota_expire_v1(batch integer,after_key jsonb DEFAULT NULL) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE request uuid;q public.gateway_requests%ROWTYPE;expired integer=0;changed integer;now_ms bigint=floor(extract(epoch FROM clock_timestamp())*1000)::bigint;due record;requests uuid[]='{}';scanned integer=0;next_key jsonb;BEGIN
 IF batch IS NULL OR batch NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 IF after_key IS NOT NULL AND after_key<>'null'::jsonb THEN
  IF jsonb_typeof(after_key) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
  IF jsonb_array_length(after_key)<>3 OR jsonb_typeof(after_key->0) IS DISTINCT FROM 'number' OR jsonb_typeof(after_key->1) IS DISTINCT FROM 'string' OR jsonb_typeof(after_key->2) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 END IF;
 FOR due IN
  SELECT l.request_id,l.lease_id,l.live_deadline_ms deadline FROM public.execution_quota_leases l
  WHERE l.live_deadline_ms<=now_ms
   AND (l.live_deadline_ms,l.request_id,l.lease_id)>(coalesce((after_key->>0)::bigint,-1),coalesce((after_key->>1)::uuid,'00000000-0000-0000-0000-000000000000'::uuid),coalesce(after_key->>2,''))
  ORDER BY l.live_deadline_ms,l.request_id,l.lease_id LIMIT batch
 LOOP
  requests=array_append(requests,due.request_id);scanned=scanned+1;next_key=jsonb_build_array(due.deadline,due.request_id,due.lease_id);
 END LOOP;
 FOR request IN SELECT DISTINCT id FROM unnest(requests) id ORDER BY id LOOP
  BEGIN
   SELECT * INTO q FROM public.gateway_requests WHERE id=request FOR UPDATE NOWAIT;
   IF q.id IS NULL OR ((q.execution_budget->>'deadline_at')::timestamptz<=clock_timestamp()) IS NOT TRUE THEN CONTINUE;END IF;
   PERFORM ep.attempt_id FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request ORDER BY ep.attempt_id FOR NO KEY UPDATE OF ep NOWAIT;
   PERFORM l.lease_id FROM public.execution_quota_leases l WHERE l.request_id=request ORDER BY l.lease_id FOR NO KEY UPDATE NOWAIT;
   IF NOT EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request)
    OR EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request AND (ep.state='RESERVED' OR ep.expires_at>clock_timestamp()))
    OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=request AND l.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') AND (l.live_deadline_ms<=now_ms) IS NOT TRUE) THEN CONTINUE;END IF;
   UPDATE public.execution_quota_leases SET state='EXPIRED' WHERE request_id=request AND state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING');
   GET DIAGNOSTICS changed=ROW_COUNT;
   IF changed>0 THEN
    UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()) WHERE id=request;
    expired=expired+1;
   END IF;
  EXCEPTION WHEN lock_not_available THEN CONTINUE;
  END;
 END LOOP;
 RETURN jsonb_build_object('expired',expired,'cursor',CASE WHEN scanned=batch THEN next_key ELSE NULL END,'more',scanned=batch AND expired>0);
END $$;

SET LOCAL statement_timeout = DEFAULT;
