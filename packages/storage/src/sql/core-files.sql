CREATE OR REPLACE FUNCTION public.core_context_v1(p jsonb,id uuid,input jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k uuid;key_row public.api_keys%ROWTYPE;auth jsonb;candidates jsonb;snapshot jsonb;expiry timestamptz;refs jsonb='[]';channel uuid;r public.resources%ROWTYPE;ref text;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_context_v1' OR jsonb_typeof(input) IS DISTINCT FROM 'object' OR input-ARRAY['verifier','ip','operation','model','fingerprint','max_attempts','resource_ids']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['verifier','ip','operation','fingerprint'])f WHERE jsonb_typeof(input->f) IS DISTINCT FROM 'string') OR input ? 'model' AND jsonb_typeof(input->'model') IS DISTINCT FROM 'string' OR NOT input ? 'model' AND input->>'operation' IS DISTINCT FROM 'files' OR input ? 'resource_ids' AND (jsonb_typeof(input->'resource_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(input->'resource_ids') NOT BETWEEN 1 AND 16 OR EXISTS(SELECT 1 FROM jsonb_array_elements(input->'resource_ids') x WHERE jsonb_typeof(x) IS DISTINCT FROM 'string' OR NOT (x#>>'{}' ~ '^(response|file)_[0-9a-f]{32}$'))) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT api_keys.id INTO k FROM public.api_keys WHERE verifier=input->>'verifier';
 IF NOT input ? 'model' THEN
  input=input||jsonb_build_object('model',(SELECT m.name FROM public.models m JOIN public.api_keys x ON x.id=k WHERE m.enabled AND 'files'=ANY(m.operations) AND (cardinality(x.models)=0 OR m.name=ANY(x.models)) ORDER BY m.name LIMIT 1));
  IF input->>'model' IS NULL THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 END IF;
 IF jsonb_typeof(input->'max_attempts') IS DISTINCT FROM 'number' OR (input->>'max_attempts')::integer NOT BETWEEN 1 AND 5 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 auth=public.core_auth_v1(k,input->>'ip',input->>'operation',input->>'model');
 candidates=public.core_candidates_v1(input->>'model',input->>'operation',auth->>'customer_currency');
 IF input ? 'resource_ids' THEN
  SELECT * INTO key_row FROM public.api_keys WHERE api_keys.id=k;
  FOR ref IN SELECT DISTINCT x FROM jsonb_array_elements_text(input->'resource_ids') x ORDER BY x LOOP
   SELECT * INTO r FROM public.resources WHERE resources.id=ref AND kind=split_part(ref,'_',1) AND project_id=(auth->>'project_id')::uuid AND state<>'DELETED' AND binding_id IS NOT NULL FOR SHARE;
   IF r.id IS NULL OR cardinality(key_row.models)>0 AND (r.model IS NULL OR NOT r.model=ANY(key_row.models)) THEN RAISE EXCEPTION 'resource_not_found' USING ERRCODE='P0404';END IF;
   IF channel IS NOT NULL AND channel<>r.provider_id THEN RAISE EXCEPTION 'resource_provider_mismatch' USING ERRCODE='P0422';END IF;
   channel=r.provider_id;
   refs=refs||jsonb_build_array(jsonb_build_object('resource_id',r.id,'remote_id',r.remote_id));
  END LOOP;
  SELECT coalesce(jsonb_agg(c ORDER BY o),'[]') INTO candidates FROM jsonb_array_elements(candidates) WITH ORDINALITY x(c,o) WHERE (c->'ref'->>'channel_id')::uuid=channel;
 END IF;
 expiry=least((p->>'deadline_at')::timestamptz,clock_timestamp()+interval '30 seconds');
 snapshot=auth||jsonb_build_object('owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'max_output_tokens',(SELECT max_output_tokens FROM public.models WHERE name=input->>'model'),'routing_strategy',(SELECT routing_strategy FROM public.models WHERE name=input->>'model'),'candidates',candidates,'operation',input->>'operation','model',input->>'model','execution_budget',jsonb_build_object('max_attempts',(input->>'max_attempts')::integer))||CASE WHEN input ? 'resource_ids' THEN jsonb_build_object('resources',refs) ELSE '{}'::jsonb END;
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) VALUES(id,(auth->>'project_id')::uuid,k,(p->>'session_id')::uuid,input->>'operation',input->>'model',input->>'ip',input->>'fingerprint',snapshot,expiry);
 RETURN snapshot||jsonb_build_object('context_id',id,'expires_at',to_char(expiry AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_commit_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;q public.gateway_requests%ROWTYPE;rid text;remote text=d->>'remote_id';found_id text;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_commit_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['attempt_id','permit_epoch','kind','remote_id','state']<>'{}'::jsonb OR d->>'kind' NOT IN ('response','file','batch','video') OR d->>'state' NOT IN ('ACTIVE','COMPLETED','FAILED','CANCELLED') OR remote IS NULL OR length(remote) NOT BETWEEN 1 AND 500 OR remote!~'^[A-Za-z0-9_:/.-]+$' OR remote LIKE '%://%' OR EXISTS(SELECT 1 FROM unnest(string_to_array(remote,'/')) part WHERE part IN ('.','..')) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid FOR SHARE;
 IF permit.attempt_id IS NULL OR permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.permit_epoch IS DISTINCT FROM (d->>'permit_epoch')::numeric OR permit.state NOT IN ('SEND_GRANTED','UNKNOWN','TERMINAL') THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 SELECT * INTO a FROM public.attempts WHERE id=permit.attempt_id;
 SELECT * INTO q FROM public.gateway_requests WHERE id=a.request_id;
 IF q.operation IS DISTINCT FROM public.core_resource_operation_v1(d->>'kind') OR a.binding_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 rid=(d->>'kind')||'_'||replace(q.id::text,'-','');
 INSERT INTO public.resources(id,project_id,provider_id,remote_id,kind,model,attempt_id,binding_id,state) VALUES(rid,q.project_id,a.provider_id,remote,d->>'kind',q.model,a.id,a.binding_id,d->>'state') ON CONFLICT DO NOTHING;
 SELECT id INTO found_id FROM public.resources WHERE project_id=q.project_id AND provider_id=a.provider_id AND kind=d->>'kind' AND remote_id=remote;
 IF found_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 RETURN jsonb_build_object('resource_id',found_id);
END $$;
