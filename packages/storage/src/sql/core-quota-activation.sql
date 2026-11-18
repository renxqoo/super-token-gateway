CREATE OR REPLACE FUNCTION public.core_quota_v1(p jsonb,attempt uuid,action text,items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;item jsonb;l public.execution_quota_leases%ROWTYPE;
 activate_together boolean;ids text[];adopted jsonb='{}';BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' NOT IN ('execution_admit_v1','execution_next_v1','execution_begin_v1','execution_abort_v1','execution_quota_v1') OR action IS NULL OR action NOT IN ('ACTIVE','RELEASED') OR jsonb_typeof(items) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=attempt FOR NO KEY UPDATE NOWAIT;
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 activate_together=action='ACTIVE' AND permit.state='RESERVED' AND jsonb_array_length(items) BETWEEN 2 AND 3;
 IF activate_together THEN
  SELECT array_agg(DISTINCT entry->>'lease_id') INTO ids FROM jsonb_array_elements(items) entry;
  activate_together=cardinality(ids)=jsonb_array_length(items);
 END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  IF jsonb_typeof(item->'revision') IS DISTINCT FROM 'string' OR item->>'revision' !~ '^(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
  IF action='ACTIVE' AND (jsonb_typeof(item->'remaining_ms') IS DISTINCT FROM 'number' OR item->>'remaining_ms' !~ '^[0-9]+$') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR NOT (l.attempt_id=attempt OR l.scope_kind<>'channel' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt) OR action='RELEASED' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt)) OR (l.descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) IS DISTINCT FROM (item-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) OR (item->>'revision')::numeric<l.revision THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
  IF action='ACTIVE' THEN
   IF (SELECT execution_closed_at FROM public.gateway_requests WHERE id=l.request_id) IS NOT NULL OR l.state NOT IN ('ADOPTION_PENDING','ACTIVE') OR permit.state NOT IN ('RESERVED','SEND_GRANTED','UNKNOWN') OR item->>'state' IS DISTINCT FROM 'ADOPTED' OR item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text OR item->>'permit_epoch' IS DISTINCT FROM permit.permit_epoch::text OR (item->>'remaining_ms')::numeric<=1000 THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  ELSE
   IF l.state NOT IN ('RELEASE_PENDING','RELEASED') OR item->>'state' IS DISTINCT FROM 'RELEASED' THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  END IF;
  IF activate_together THEN adopted=jsonb_set(adopted,ARRAY[l.lease_id],item);
  ELSE UPDATE public.execution_quota_leases SET state=action,revision=(item->>'revision')::numeric,descriptor=item WHERE lease_id=l.lease_id;
  END IF;
 END LOOP;
 IF activate_together THEN
  UPDATE public.execution_quota_leases lease SET state=action,revision=(adopted->lease.lease_id->>'revision')::numeric,descriptor=adopted->lease.lease_id
  WHERE lease.lease_id=ANY(ids);
 END IF;
 RETURN jsonb_build_object('updated',jsonb_array_length(items));
END $$;
