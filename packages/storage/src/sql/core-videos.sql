CREATE OR REPLACE FUNCTION public.core_resource_claim_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.resources%ROWTYPE;leases jsonb='[]';BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_claim_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['limit']<>'{}'::jsonb OR jsonb_typeof(d->'limit') IS DISTINCT FROM 'number' OR (d->>'limit')!~'^[0-9]{1,2}$' OR (d->>'limit')::integer NOT BETWEEN 1 AND 16 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR r IN WITH due AS (
  SELECT x.id FROM public.resources x JOIN public.attempts a ON a.id=x.attempt_id JOIN public.execution_permits ep ON ep.attempt_id=a.id
  WHERE (x.kind IN ('response','video') OR x.kind='batch' AND x.batch_manifest IS NOT NULL) AND x.binding_id IS NOT NULL AND x.attempt_id IS NOT NULL AND a.state='PARTIAL' AND x.next_poll_at<=clock_timestamp() AND (x.poll_until IS NULL OR x.poll_until<clock_timestamp())
  ORDER BY x.next_poll_at,x.id LIMIT (d->>'limit')::integer FOR UPDATE OF x SKIP LOCKED
 ) UPDATE public.resources x SET lease_session_id=(p->>'session_id')::uuid,lease_epoch=x.lease_epoch+1,poll_until=clock_timestamp()+CASE WHEN x.kind='batch' THEN interval '900 seconds' ELSE interval '60 seconds' END FROM due WHERE x.id=due.id RETURNING x.*
 LOOP
  leases=leases||jsonb_build_array(jsonb_build_object('resource_id',r.id,'attempt_id',r.attempt_id,'permit_epoch',(SELECT permit_epoch::text FROM public.execution_permits WHERE attempt_id=r.attempt_id),'lease_epoch',r.lease_epoch::text,'resolution',public.core_resource_resolution_v1(r,r.poll_until)));
 END LOOP;
 RETURN jsonb_build_object('leases',leases);
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_lease_v1(p jsonb,d jsonb,live boolean) RETURNS public.resources LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.resources%ROWTYPE;BEGIN
 IF NOT (d->>'resource_id' ~ '^(response|batch|video)_[0-9a-f]{32}$') OR jsonb_typeof(d->'lease_epoch') IS DISTINCT FROM 'string' OR d->>'lease_epoch' !~ '^[1-9][0-9]{0,37}$' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO r FROM public.resources WHERE id=d->>'resource_id' FOR UPDATE;
 IF r.id IS NULL OR r.lease_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR r.lease_epoch IS DISTINCT FROM (d->>'lease_epoch')::numeric OR live AND (r.poll_until IS NULL OR r.poll_until<=clock_timestamp()) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 RETURN r;
END $$;
