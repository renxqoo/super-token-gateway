SET LOCAL statement_timeout = '5min';

ALTER TABLE public.execution_quota_leases ADD COLUMN release_pending boolean NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION public.core_quota_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_OP='UPDATE' AND (
  (NEW.lease_id,NEW.attempt_id,NEW.command_id,NEW.request_id,NEW.scope_kind,NEW.scope_id)
   IS DISTINCT FROM (OLD.lease_id,OLD.attempt_id,OLD.command_id,OLD.request_id,OLD.scope_kind,OLD.scope_id)
  OR NEW.revision<OLD.revision OR OLD.state IN ('RELEASE_PENDING','RELEASED','EXPIRED') AND NEW.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED')
 ) THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
 NEW.live_deadline_ms=CASE WHEN NEW.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') THEN (NEW.descriptor->>'deadline_ms')::bigint END;
 NEW.release_pending=NEW.state='RELEASE_PENDING';
 RETURN NEW;
END $$;

UPDATE public.execution_quota_leases SET release_pending=true WHERE state='RELEASE_PENDING';
CREATE INDEX execution_quota_pending_release_idx ON public.execution_quota_leases (lease_id) WHERE release_pending;

CREATE OR REPLACE FUNCTION public.core_quota_recovery_v1(p jsonb,items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE item jsonb;l public.execution_quota_leases%ROWTYPE;result jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_quota_recovery_v1' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(items)='number' AND items::text::integer BETWEEN 1 AND 100 THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('attempt_id',attempt_id,'descriptor',descriptor)),'[]'::jsonb) INTO result FROM (SELECT attempt_id,descriptor FROM public.execution_quota_leases WHERE release_pending ORDER BY lease_id LIMIT items::text::integer) pending;
  RETURN result;
 END IF;
 IF jsonb_typeof(items)='object' AND items-ARRAY['limit','after']='{}'::jsonb AND jsonb_typeof(items->'limit')='number' AND items->>'limit' ~ '^[0-9]{1,3}$' AND (items->>'limit')::integer BETWEEN 1 AND 100 THEN
  RETURN public.core_quota_expire_v1((items->>'limit')::integer,items->'after');
 END IF;
 IF jsonb_typeof(items) IS DISTINCT FROM 'array' OR jsonb_array_length(items)>100 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR l.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED') OR item->>'state' IS DISTINCT FROM 'RELEASED' OR (l.descriptor-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) IS DISTINCT FROM (item-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) OR (item->>'revision')::numeric<l.revision OR (item->>'attempt_id' IS NOT NULL AND item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text) OR (item->>'permit_epoch' IS NOT NULL AND item->>'permit_epoch' IS DISTINCT FROM '1') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
  IF l.state<>'EXPIRED' THEN UPDATE public.execution_quota_leases SET state='RELEASED',descriptor=item WHERE lease_id=l.lease_id;END IF;
 END LOOP;RETURN jsonb_build_object('released',jsonb_array_length(items));
END $$;

SET LOCAL statement_timeout = DEFAULT;
