CREATE FUNCTION public.core_quota_expire_v1(batch integer,after_key jsonb DEFAULT NULL) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE request uuid;q public.gateway_requests%ROWTYPE;expired integer=0;changed integer;now_ms bigint=floor(extract(epoch FROM clock_timestamp())*1000)::bigint;due record;requests uuid[]='{}';scanned integer=0;next_key jsonb;BEGIN
 IF batch IS NULL OR batch NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 IF after_key IS NOT NULL AND after_key<>'null'::jsonb THEN
  IF jsonb_typeof(after_key) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
  IF jsonb_array_length(after_key)<>3 OR jsonb_typeof(after_key->0) IS DISTINCT FROM 'number' OR jsonb_typeof(after_key->1) IS DISTINCT FROM 'string' OR jsonb_typeof(after_key->2) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 END IF;
 FOR due IN
  SELECT l.request_id,l.lease_id,(l.descriptor->>'deadline_ms')::bigint deadline FROM public.execution_quota_leases l
  WHERE l.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') AND (l.descriptor->>'deadline_ms')::bigint<=now_ms
   AND ((l.descriptor->>'deadline_ms')::bigint,l.request_id,l.lease_id)>(coalesce((after_key->>0)::bigint,-1),coalesce((after_key->>1)::uuid,'00000000-0000-0000-0000-000000000000'::uuid),coalesce(after_key->>2,''))
  ORDER BY (l.descriptor->>'deadline_ms')::bigint,l.request_id,l.lease_id LIMIT batch
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
    OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=request AND l.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') AND (((l.descriptor->>'deadline_ms')::bigint<=now_ms) IS NOT TRUE)) THEN CONTINUE;END IF;
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
REVOKE ALL ON FUNCTION public.core_quota_expire_v1(integer,jsonb) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.core_quota_recovery_v1(p jsonb,items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE item jsonb;l public.execution_quota_leases%ROWTYPE;result jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_quota_recovery_v1' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(items)='number' AND items::text::integer BETWEEN 1 AND 100 THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('attempt_id',attempt_id,'descriptor',descriptor)),'[]'::jsonb) INTO result FROM (SELECT attempt_id,descriptor FROM public.execution_quota_leases WHERE state='RELEASE_PENDING' ORDER BY lease_id LIMIT items::text::integer) pending;
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
