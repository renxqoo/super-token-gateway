CREATE OR REPLACE FUNCTION core_funding.bind_chat_context_v1(p jsonb,d jsonb,route jsonb)
RETURNS public.execution_contexts LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ctx public.execution_contexts;route_key uuid;route_project uuid;route_expiry timestamptz;route_snapshot jsonb;
BEGIN
   IF jsonb_typeof(route) IS DISTINCT FROM 'object' OR jsonb_typeof(route->'snapshot') IS DISTINCT FROM 'object'
    OR route->>'verifier' IS NULL OR route->>'ip' IS NULL OR route->>'model' IS NULL OR route->>'expires_at' IS NULL
    OR (route->>'fingerprint') !~ '^[0-9a-f]{64}$' OR route->>'fingerprint' IS NULL
    OR d->'manifest'->>'operation' IS DISTINCT FROM 'chat' OR jsonb_typeof(d->'manifest'->'stream') IS DISTINCT FROM 'boolean'
    OR d->'manifest'->'resource_ids' IS DISTINCT FROM '[]'::jsonb
    OR d->'manifest'->>'model' IS DISTINCT FROM route->>'model' THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
   route_snapshot=route->'snapshot';
   route_expiry=(route->>'expires_at')::timestamptz;
   IF route_expiry>clock_timestamp()+interval '30 seconds'
    OR route_snapshot-ARRAY['execution_budget','scopes','customer_currency','candidate_digests','execution_runtimes']<>'{}'::jsonb
    OR jsonb_typeof(route_snapshot->'execution_budget'->'max_attempts') IS DISTINCT FROM 'number'
    OR (route_snapshot->'execution_budget'->>'max_attempts')::integer NOT BETWEEN 1 AND 5
    THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
   SELECT k.id,k.project_id INTO route_key,route_project FROM public.api_keys k WHERE k.verifier=route->>'verifier';
   IF route_key IS NULL THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 ctx.id=(d->>'context_id')::uuid;ctx.project_id=route_project;ctx.key_id=route_key;ctx.owner_session_id=(p->>'session_id')::uuid;
 ctx.operation='chat';ctx.model=route->>'model';ctx.ip=route->>'ip';ctx.fingerprint=route->>'fingerprint';ctx.snapshot=route_snapshot;ctx.expires_at=route_expiry;
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at)
 VALUES(ctx.id,ctx.project_id,ctx.key_id,ctx.owner_session_id,ctx.operation,ctx.model,ctx.ip,ctx.fingerprint,ctx.snapshot,ctx.expires_at)
 ON CONFLICT DO NOTHING;
 IF NOT FOUND THEN ctx.id=NULL;END IF;
 RETURN ctx;
END $$;
