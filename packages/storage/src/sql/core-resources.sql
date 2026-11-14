UPDATE public.resources r SET binding_id=c.binding_id FROM public.channels c WHERE r.binding_id IS NULL AND c.id=r.provider_id AND c.binding_id IS NOT NULL;
CREATE TABLE public.core_resource_actions(
 command_id uuid PRIMARY KEY,
 caller_identity text NOT NULL,
 resource_id text NOT NULL REFERENCES public.resources(id),
 action text NOT NULL CHECK(action IN ('delete','cancel')),
 created_at timestamptz NOT NULL DEFAULT now()
);
REVOKE ALL ON public.core_resource_actions FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.core_caller_v1(p jsonb) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE path text;BEGIN
 path=CASE p->>'command_type' WHEN 'execution_context_v1' THEN 'context' WHEN 'execution_admit_v1' THEN 'admissions' WHEN 'execution_next_v1' THEN 'attempts' WHEN 'execution_begin_v1' THEN 'begin' WHEN 'execution_abort_v1' THEN 'abort' WHEN 'execution_query_v1' THEN 'commands' WHEN 'execution_quota_v1' THEN 'quota' WHEN 'execution_quota_recovery_v1' THEN 'quota-recovery' WHEN 'execution_abort_recovery_v1' THEN 'abort-recovery' WHEN 'execution_resource_commit_v1' THEN 'resources/commit' WHEN 'execution_resource_v1' THEN 'resource-permits' WHEN 'execution_resource_list_v1' THEN 'resources/list' END;
 IF path IS NULL OR NOT (p->>'service'='gateway' AND p->>'command_type' NOT IN ('execution_quota_recovery_v1','execution_abort_recovery_v1') OR p->>'service' IN ('core-api','core-worker') AND p->>'command_type' IN ('execution_quota_recovery_v1','execution_abort_recovery_v1','execution_query_v1')) OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/'||path THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_current_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'));
END $$;
CREATE FUNCTION public.core_resource_operation_v1(kind text) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
 SELECT CASE kind WHEN 'response' THEN 'responses' WHEN 'file' THEN 'files' WHEN 'batch' THEN 'batches' WHEN 'video' THEN 'videos' END
$$;
CREATE FUNCTION public.core_resource_commit_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;q public.gateway_requests%ROWTYPE;rid text;remote text=d->>'remote_id';found_id text;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_commit_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['attempt_id','permit_epoch','kind','remote_id','state']<>'{}'::jsonb OR d->>'kind' NOT IN ('response','file','batch','video') OR d->>'state' NOT IN ('ACTIVE','COMPLETED','FAILED','CANCELLED') OR remote IS NULL OR length(remote) NOT BETWEEN 1 AND 500 OR remote!~'^[A-Za-z0-9_:/.-]+$' OR remote LIKE '%://%' OR EXISTS(SELECT 1 FROM unnest(string_to_array(remote,'/')) part WHERE part IN ('.','..')) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid FOR SHARE;
 IF permit.attempt_id IS NULL OR permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.permit_epoch IS DISTINCT FROM (d->>'permit_epoch')::numeric OR permit.state NOT IN ('SEND_GRANTED','UNKNOWN','TERMINAL') THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 SELECT * INTO a FROM public.attempts WHERE id=permit.attempt_id;
 SELECT * INTO q FROM public.gateway_requests WHERE id=a.request_id;
 IF q.operation IS DISTINCT FROM public.core_resource_operation_v1(d->>'kind') OR a.binding_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 rid=CASE d->>'kind' WHEN 'response' THEN 'response_'||replace(q.id::text,'-','') ELSE (d->>'kind')||'_'||replace(gen_random_uuid()::text,'-','') END;
 INSERT INTO public.resources(id,project_id,provider_id,remote_id,kind,model,attempt_id,binding_id,state) VALUES(rid,q.project_id,a.provider_id,remote,d->>'kind',q.model,a.id,a.binding_id,d->>'state') ON CONFLICT DO NOTHING;
 SELECT id INTO found_id FROM public.resources WHERE project_id=q.project_id AND provider_id=a.provider_id AND kind=d->>'kind' AND remote_id=remote;
 IF found_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 RETURN jsonb_build_object('resource_id',found_id);
END $$;
CREATE FUNCTION public.core_resource_key_v1(verifier text,ip text,op text) RETURNS public.api_keys LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;BEGIN
 SELECT * INTO k FROM public.api_keys WHERE api_keys.verifier=core_resource_key_v1.verifier;
 IF k.id IS NULL THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.core_auth_v1(k.id,ip,op,CASE WHEN cardinality(k.models)>0 THEN k.models[1] END);
 RETURN k;
END $$;
CREATE FUNCTION public.core_resource_permit_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;r public.resources%ROWTYPE;b public.channel_plugin_bindings%ROWTYPE;v public.plugin_versions%ROWTYPE;c public.channels%ROWTYPE;manifest jsonb;rkind text=split_part(d->>'resource_id','_',1);act text=d->>'action';resolution jsonb;outcome text='RESOLVED';caller text=(p->>'service')||':'||(p->>'instance_id');BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['verifier','ip','resource_id','action','auth','granted_command_id']<>'{}'::jsonb OR NOT (d->>'resource_id' ~ '^(response|file|batch|video)_[0-9a-f]{32}$') OR act NOT IN ('get','delete','cancel','content','input_items') OR d ? 'granted_command_id' AND (act<>'delete' OR d ? 'auth') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 k=public.core_resource_key_v1(d->>'verifier',d->>'ip',public.core_resource_operation_v1(rkind));
 SELECT * INTO r FROM public.resources WHERE id=d->>'resource_id' AND kind=split_part(d->>'resource_id','_',1) AND project_id=k.project_id AND state<>'DELETED' AND binding_id IS NOT NULL FOR SHARE;
 IF r.id IS NULL OR cardinality(k.models)>0 AND (r.model IS NULL OR NOT r.model=ANY(k.models)) OR NOT (rkind='response' AND act IN ('get','delete','cancel','input_items') OR rkind='file' AND act IN ('get','delete','content') OR rkind='batch' AND act IN ('get','cancel') OR rkind='video' AND act IN ('get','delete','content')) THEN RAISE EXCEPTION 'resource_not_found' USING ERRCODE='P0404';END IF;
 PERFORM public.core_auth_v1(k.id,d->>'ip',public.core_resource_operation_v1(rkind),r.model);
 SELECT * INTO b FROM public.channel_plugin_bindings WHERE id=r.binding_id;
 SELECT * INTO v FROM public.plugin_versions WHERE id=b.plugin_version_id;
 SELECT * INTO c FROM public.channels WHERE id=b.channel_id;
 SELECT pk.manifest INTO manifest FROM public.plugin_packages pk WHERE pk.digest=v.package_digest;
 IF v.quarantined OR v.state NOT IN ('PUBLISHED','DISABLED') OR NOT c.enabled OR (manifest->'capabilities'->>'resources')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'no_route' USING ERRCODE='P0503';END IF;
 resolution=jsonb_build_object('resource_id',r.id,'kind',r.kind,'remote_id',r.remote_id,'model',r.model,'state',r.state,'channel_id',c.id,'package_digest',v.package_digest,'config',b.config,'origin',regexp_replace(c.base_url,'^(https://[^/]+).*$','\1'),'deadline_at',to_char(least((p->>'deadline_at')::timestamptz,clock_timestamp()+interval '30 seconds') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
 IF d ? 'granted_command_id' THEN
  IF NOT EXISTS(SELECT 1 FROM public.core_resource_actions WHERE command_id=(d->>'granted_command_id')::uuid AND caller_identity=caller AND resource_id=r.id AND core_resource_actions.action='delete') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
  UPDATE public.resources SET state='DELETED',updated_at=now() WHERE id=r.id;
  RETURN jsonb_build_object('resolution',resolution,'state','DELETED');
 END IF;
 IF d ? 'auth' THEN
  IF d->'auth'->>'type' IS DISTINCT FROM b.config->>'auth' AND NOT (d->'auth'->>'type'='bearer' AND b.config->>'auth'='google-oauth') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
  outcome='GRANTED';
  IF act IN ('delete','cancel') THEN
   INSERT INTO public.core_resource_actions(command_id,caller_identity,resource_id,action) VALUES((p->>'command_id')::uuid,caller,r.id,act) ON CONFLICT DO NOTHING;
   IF NOT FOUND THEN outcome='REPLAYED';END IF;
  END IF;
 END IF;
 RETURN jsonb_build_object('resolution',resolution,'state',outcome,'channel_id',c.id,'credential_ciphertext',CASE WHEN outcome='GRANTED' THEN c.secret END,'key_id',k.id,'rpm',jsonb_build_object('key',coalesce(k.rpm,(SELECT rpm FROM public.projects WHERE id=k.project_id)),'channel',c.rpm));
END $$;
CREATE FUNCTION public.core_resource_list_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;cursor_row public.resources%ROWTYPE;rows jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_list_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['verifier','ip','kind','limit','after']<>'{}'::jsonb OR d->>'kind' NOT IN ('response','file','batch','video') OR jsonb_typeof(d->'limit') IS DISTINCT FROM 'number' OR (d->>'limit')::integer NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 k=public.core_resource_key_v1(d->>'verifier',d->>'ip',public.core_resource_operation_v1(d->>'kind'));
 IF d ? 'after' THEN
  SELECT * INTO cursor_row FROM public.resources WHERE id=d->>'after' AND project_id=k.project_id AND kind=d->>'kind' AND (cardinality(k.models)=0 OR model=ANY(k.models));
  IF cursor_row.id IS NULL THEN RAISE EXCEPTION 'resource_not_found' USING ERRCODE='P0404';END IF;
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('id',x.id,'state',x.state,'created_at',floor(extract(epoch FROM x.created_at))::bigint) ORDER BY x.created_at DESC,x.id DESC),'[]') INTO rows FROM (SELECT id,state,created_at FROM public.resources WHERE project_id=k.project_id AND kind=d->>'kind' AND state<>'DELETED' AND (cardinality(k.models)=0 OR model=ANY(k.models)) AND (cursor_row.id IS NULL OR (created_at,id)<(cursor_row.created_at,cursor_row.id)) ORDER BY created_at DESC,id DESC LIMIT (d->>'limit')::integer+1) x;
 RETURN jsonb_build_object('data',rows,'rpm',coalesce(k.rpm,(SELECT rpm FROM public.projects WHERE id=k.project_id)),'key_id',k.id);
END $$;
REVOKE ALL ON FUNCTION public.core_resource_operation_v1(text),public.core_resource_commit_v1(jsonb,jsonb),public.core_resource_key_v1(text,text,text),public.core_resource_permit_v1(jsonb,jsonb),public.core_resource_list_v1(jsonb,jsonb) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.core_context_v1(p jsonb,id uuid,input jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k uuid;key_row public.api_keys%ROWTYPE;auth jsonb;candidates jsonb;snapshot jsonb;expiry timestamptz;refs jsonb='[]';channel uuid;r public.resources%ROWTYPE;ref text;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_context_v1' OR jsonb_typeof(input) IS DISTINCT FROM 'object' OR input-ARRAY['verifier','ip','operation','model','fingerprint','max_attempts','resource_ids']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['verifier','ip','operation','model','fingerprint'])f WHERE jsonb_typeof(input->f) IS DISTINCT FROM 'string') OR input ? 'resource_ids' AND (jsonb_typeof(input->'resource_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(input->'resource_ids') NOT BETWEEN 1 AND 16 OR EXISTS(SELECT 1 FROM jsonb_array_elements(input->'resource_ids') x WHERE jsonb_typeof(x) IS DISTINCT FROM 'string' OR NOT (x#>>'{}' ~ '^(response|file)_[0-9a-f]{32}$'))) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT api_keys.id INTO k FROM public.api_keys WHERE verifier=input->>'verifier';
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
