CREATE OR REPLACE FUNCTION public.core_context_v1(p jsonb,id uuid,input jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k uuid;key_row public.api_keys%ROWTYPE;auth jsonb;candidates jsonb;snapshot jsonb;expiry timestamptz;refs jsonb='[]';channel uuid;r public.resources%ROWTYPE;ref text;batch jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_context_v1' OR jsonb_typeof(input) IS DISTINCT FROM 'object' OR input-ARRAY['verifier','ip','operation','model','fingerprint','max_attempts','resource_ids']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['verifier','ip','operation','fingerprint'])f WHERE jsonb_typeof(input->f) IS DISTINCT FROM 'string') OR input ? 'model' AND jsonb_typeof(input->'model') IS DISTINCT FROM 'string' OR NOT input ? 'model' AND input->>'operation' NOT IN ('files','batches') OR input->>'operation'='batches' AND (input ? 'model' OR CASE WHEN jsonb_typeof(input->'resource_ids')='array' THEN jsonb_array_length(input->'resource_ids') END IS DISTINCT FROM 1 OR NOT (input->'resource_ids'->>0 ~ '^file_')) OR input ? 'resource_ids' AND (jsonb_typeof(input->'resource_ids') IS DISTINCT FROM 'array' OR jsonb_array_length(input->'resource_ids') NOT BETWEEN 1 AND 16 OR EXISTS(SELECT 1 FROM jsonb_array_elements(input->'resource_ids') x WHERE jsonb_typeof(x) IS DISTINCT FROM 'string' OR NOT (x#>>'{}' ~ '^(response|file)_[0-9a-f]{32}$'))) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT api_keys.id INTO k FROM public.api_keys WHERE verifier=input->>'verifier';
 IF input->>'operation'='batches' THEN
  IF k IS NULL THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
  SELECT * INTO r FROM public.resources WHERE resources.id=input->'resource_ids'->>0 AND kind='file' AND project_id=(SELECT x.project_id FROM public.api_keys x WHERE x.id=k) AND state<>'DELETED' AND binding_id IS NOT NULL AND batch_manifest IS NOT NULL AND batch_ceilings IS NOT NULL AND model IS NOT NULL;
  IF r.id IS NULL THEN RAISE EXCEPTION 'resource_not_found' USING ERRCODE='P0404';END IF;
  input=input||jsonb_build_object('model',r.model);
  PERFORM public.core_auth_v1(k,input->>'ip',CASE r.batch_endpoint WHEN '/v1/chat/completions' THEN 'chat' WHEN '/v1/responses' THEN 'responses' WHEN '/v1/embeddings' THEN 'embeddings' WHEN '/v1/completions' THEN 'completions' WHEN '/v1/moderations' THEN 'moderations' WHEN '/v1/images/generations' THEN 'images' WHEN '/v1/images/edits' THEN 'image_edits' END,r.model);
  batch=jsonb_build_object('endpoint',r.batch_endpoint,'ceilings',r.batch_ceilings,'items',(r.batch_manifest->>'items')::integer);
 END IF;
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
 snapshot=auth||jsonb_build_object('owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'max_output_tokens',(SELECT max_output_tokens FROM public.models WHERE name=input->>'model'),'routing_strategy',(SELECT routing_strategy FROM public.models WHERE name=input->>'model'),'candidates',candidates,'execution_runtimes',core_execution_runtime.allowed_v1(candidates),'operation',input->>'operation','model',input->>'model','execution_budget',jsonb_build_object('max_attempts',(input->>'max_attempts')::integer))||CASE WHEN input ? 'resource_ids' THEN jsonb_build_object('resources',refs) ELSE '{}'::jsonb END||CASE WHEN batch IS NOT NULL THEN jsonb_build_object('batch',batch) ELSE '{}'::jsonb END;
 IF id IS NULL AND (input->>'operation' IS DISTINCT FROM 'chat' OR input ? 'resource_ids') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 IF id IS NULL THEN
  snapshot=snapshot||jsonb_build_object('candidate_digests',ARRAY(SELECT encode(sha256(convert_to(c::text,'UTF8')),'hex') FROM jsonb_array_elements(candidates) c));
 ELSE
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) VALUES(id,(auth->>'project_id')::uuid,k,(p->>'session_id')::uuid,input->>'operation',input->>'model',input->>'ip',input->>'fingerprint',snapshot,expiry);
 END IF;
 RETURN snapshot||jsonb_build_object('context_id',id,'expires_at',to_char(expiry AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;

CREATE FUNCTION public.core_route_authority_v1(p jsonb,credential text,client_ip text,chat_model text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k uuid;
BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' NOT IN ('execution_admit_v1','execution_next_v1') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT id INTO k FROM public.api_keys WHERE verifier=credential;
 RETURN public.core_auth_v1(k,client_ip,'chat',chat_model);
END $$;

REVOKE ALL ON FUNCTION public.core_route_authority_v1(jsonb,text,text,text) FROM PUBLIC;
DO $$
DECLARE owner name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='public.core_context_v1(jsonb,uuid,jsonb)'::regprocedure)); grantee name;
BEGIN
 EXECUTE format('ALTER FUNCTION public.core_route_authority_v1(jsonb,text,text,text) OWNER TO %I',owner);
 FOR grantee IN
  SELECT r.rolname FROM pg_roles r WHERE NOT r.rolsuper
   AND NOT has_function_privilege(r.oid,'public.core_route_authority_v1(jsonb,text,text,text)','EXECUTE')
   AND has_function_privilege(r.oid,'public.core_context_v1(jsonb,uuid,jsonb)','EXECUTE')
   AND has_function_privilege(r.oid,'public.core_preflight_v1(jsonb,uuid,uuid)','EXECUTE') AND has_function_privilege(r.oid,'public.core_lookup_v1(jsonb,jsonb)','EXECUTE')
 LOOP EXECUTE format('GRANT EXECUTE ON FUNCTION public.core_route_authority_v1(jsonb,text,text,text) TO %I',grantee);END LOOP;
END $$;

CREATE FUNCTION core_funding.bind_chat_context_v1(p jsonb,d jsonb,route jsonb)
RETURNS public.execution_contexts LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ctx public.execution_contexts;route_key uuid;route_project uuid;route_expiry timestamptz;route_snapshot jsonb;
BEGIN

   IF jsonb_typeof(route) IS DISTINCT FROM 'object' OR jsonb_typeof(route->'snapshot') IS DISTINCT FROM 'object'
    OR route->>'verifier' IS NULL OR route->>'ip' IS NULL OR route->>'model' IS NULL OR route->>'expires_at' IS NULL
    OR (route->>'fingerprint') !~ '^[0-9a-f]{64}$' OR route->>'fingerprint' IS NULL
    OR d->'manifest'->>'operation' IS DISTINCT FROM 'chat' OR d->'manifest'->'stream' IS DISTINCT FROM 'false'::jsonb
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
REVOKE ALL ON FUNCTION core_funding.bind_chat_context_v1(jsonb,jsonb,jsonb) FROM PUBLIC;
DO $$
DECLARE context_owner name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='public.core_context_v1(jsonb,uuid,jsonb)'::regprocedure));
 execution_owner name=pg_get_userbyid((SELECT proowner FROM pg_proc WHERE oid='core_funding.run_execution_v1(jsonb,jsonb[],integer[],jsonb[],jsonb[],boolean,numeric[])'::regprocedure));
BEGIN
 EXECUTE format('ALTER FUNCTION core_funding.bind_chat_context_v1(jsonb,jsonb,jsonb) OWNER TO %I',context_owner);
 EXECUTE format('GRANT EXECUTE ON FUNCTION core_funding.bind_chat_context_v1(jsonb,jsonb,jsonb) TO %I',execution_owner);
END $$;

CREATE OR REPLACE FUNCTION core_funding.execution_item_v1(p jsonb,e jsonb,d jsonb,computed jsonb,authority_times jsonb,cache jsonb,pending jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
<<execution>>
DECLARE ctx public.execution_contexts%ROWTYPE;cmd public.execution_commands%ROWTYPE;q public.gateway_requests%ROWTYPE;permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;h public.upstream_holds%ROWTYPE;funding_cell smallint;
 kind text=p->>'command_type';caller text=p->>'service'||':'||(p->>'instance_id');keys text[];k text;auth jsonb;candidates jsonb;c jsonb;projection jsonb;ref jsonb;manifest jsonb;quote jsonb;estimated jsonb='{}';dim text;bound numeric;qty numeric;amount numeric;response jsonb;result jsonb;plan jsonb;command_pk uuid=gen_random_uuid();qid uuid;aid uuid;ordinal integer=1;account uuid;wallet uuid;descriptor jsonb;failure text;rejection text;fresh boolean=false;started timestamptz=clock_timestamp();rebuilt jsonb;
 route jsonb=computed->'route'; expected_context public.execution_contexts%ROWTYPE;
 saved_cache jsonb=cache;saved_pending jsonb=pending;customer jsonb;catalog jsonb;allowed jsonb;allocation jsonb;funding jsonb;admission jsonb;journal uuid;
BEGIN
 IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR e-ARRAY['command_id','body_digest','caller_incarnation','deadline_at','trace_id','contract_version']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['command_id','body_digest','caller_incarnation','deadline_at','trace_id'])f WHERE jsonb_typeof(e->f) IS DISTINCT FROM 'string') OR jsonb_typeof(e->'contract_version') IS DISTINCT FROM 'number' OR e->>'contract_version' IS DISTINCT FROM '1' OR (e->>'command_id',e->>'caller_incarnation',e->>'deadline_at',e->>'trace_id') IS DISTINCT FROM (p->>'command_id',p->>'incarnation_id',p->>'deadline_at',p->>'trace_id') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 IF public.core_digest_v1(jsonb_build_object('kind',kind,'contract_version',1,'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'deadline_at',p->>'deadline_at','data',d)) IS DISTINCT FROM e->>'body_digest' THEN RAISE EXCEPTION 'internal_proof_invalid' USING ERRCODE='P0401';END IF;
 keys=ARRAY['command:'||caller||':'||kind||':'||(e->>'command_id')];
 IF kind IN ('execution_admit_v1','execution_next_v1') THEN keys=keys||ARRAY['uuid-request:'||(d->'dispatch_projection'->>'proposed_request_id'),'uuid-attempt:'||(d->'dispatch_projection'->>'proposed_attempt_id')];END IF;
 IF kind='execution_admit_v1' AND d->>'intent_digest' IS NOT NULL THEN keys=array_append(keys,'intent:'||(d->>'project_id')||':'||(d->>'intent_digest'));END IF;
 FOR k IN SELECT value FROM unnest(keys) x(value) ORDER BY value COLLATE "C" LOOP IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.execution.v1:'||k,0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;END LOOP;
 PERFORM public.core_caller_projection_v2(p);
 IF jsonb_typeof(authority_times) IS DISTINCT FROM 'object' OR EXISTS(SELECT 1 FROM unnest(ARRAY['session_expires_at','certificate_not_before','certificate_expires_at']) f WHERE jsonb_typeof(authority_times->f) IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF (authority_times->>'certificate_not_before')::timestamptz>clock_timestamp() OR (authority_times->>'certificate_expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF (authority_times->>'session_expires_at')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF kind IN ('execution_admit_v1','execution_next_v1') THEN
  IF route IS NULL THEN
   SELECT * INTO ctx FROM public.execution_contexts WHERE id=(d->>'context_id')::uuid;
  ELSE
   ctx=core_funding.bind_chat_context_v1(p,d,route);
   IF ctx.id IS NULL THEN
    expected_context=ctx;
    SELECT * INTO ctx FROM public.execution_contexts WHERE id=(d->>'context_id')::uuid;
    IF (ctx.key_id,ctx.project_id,ctx.owner_session_id,ctx.operation,ctx.model,ctx.ip,ctx.fingerprint,ctx.expires_at,ctx.snapshot)
     IS DISTINCT FROM (expected_context.key_id,expected_context.project_id,expected_context.owner_session_id,expected_context.operation,expected_context.model,expected_context.ip,expected_context.fingerprint,expected_context.expires_at,expected_context.snapshot)
     THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
   END IF;
  END IF;
  IF route IS NULL AND ctx.snapshot?'candidate_digests' THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 ELSE
  IF kind='execution_query_v1' THEN
   SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=d->>'kind' AND command_id=(d->>'command_id')::uuid;
   IF NOT FOUND THEN RETURN jsonb_build_object('result',jsonb_build_object('receipt',jsonb_build_object('command_state','NOT_FOUND')),'cache',cache,'pending',pending);END IF;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=cmd.attempt_id;
  ELSE SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid;END IF;
  SELECT * INTO ctx FROM public.execution_contexts WHERE id=CASE WHEN kind='execution_query_v1' THEN cmd.context_id ELSE permit.context_id END;
 END IF;
 IF ctx.id IS NULL THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF NOT (kind='execution_query_v1' AND cmd.kind='execution_abort_recovery_v1') THEN
  customer=cache->'customers'->ctx.key_id::text;
  IF customer IS NULL THEN auth=public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);
  ELSE auth=core_funding.auth_decision_v1(jsonb_populate_record(NULL::public.api_keys,NULLIF(customer->'key','null'::jsonb)),jsonb_populate_record(NULL::public.projects,NULLIF(customer->'project','null'::jsonb)),jsonb_populate_record(NULL::public.organizations,NULLIF(customer->'organization','null'::jsonb)),jsonb_populate_record(NULL::public.principals,NULLIF(customer->'principal','null'::jsonb)),ctx.ip,ctx.operation,ctx.model,clock_timestamp());END IF;
 END IF;
 IF kind='execution_query_v1' THEN
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',cmd.receipt||CASE WHEN permit.attempt_id IS NOT NULL THEN jsonb_build_object('state',permit.state) ELSE '{}'::jsonb END),'cache',cache,'pending',pending);
 END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=execution.kind AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',cmd.receipt),'cache',cache,'pending',pending);
 END IF;
 BEGIN
  IF kind IN ('execution_admit_v1','execution_next_v1') THEN
   IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR ctx.expires_at<=clock_timestamp() OR ctx.snapshot->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' OR ctx.snapshot->'scopes' IS DISTINCT FROM auth->'scopes' THEN RAISE EXCEPTION 'context_stale' USING ERRCODE='P0422';END IF;
   IF computed->>'rejection' IN ('candidate_stale','cost_unbounded','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request','cost_ceiling') THEN RAISE EXCEPTION '%',computed->>'rejection' USING ERRCODE='P0422';END IF;
   catalog=core_funding.catalog_v1(cache,ctx.model,ctx.operation,auth->>'customer_currency');cache=catalog->'cache';candidates=catalog->'frame'->'candidates';allowed=catalog->'frame'->'runtimes';
   projection=d->'dispatch_projection';ref=projection->'candidate_ref';manifest=d->'manifest';
   SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=ref;
    IF c IS NOT NULL AND allowed IS NULL THEN allowed=core_execution_runtime.allowed_v1(jsonb_build_array(c));END IF;
   IF projection->'version' IS DISTINCT FROM '2'::jsonb OR jsonb_typeof(projection) IS DISTINCT FROM 'object' OR projection-ARRAY['version','proposed_request_id','proposed_attempt_id','origin','method','operation','request_target_sha256','final_headers_digest','final_body_sha256','candidate_ref','auth','credential_ciphertext_sha256','billable_manifest_sha256','deadline_at','execution']<>'{}'::jsonb OR (SELECT count(*) FROM jsonb_object_keys(projection))<>15 OR jsonb_typeof(manifest) IS DISTINCT FROM 'object' OR manifest-ARRAY['version','operation','model','upstream_model','stream','input_bytes','ceilings','quantities','input_characters','images','resource_ids']<>'{}'::jsonb OR jsonb_typeof(manifest->'quantities') IS DISTINCT FROM 'object' OR jsonb_typeof(manifest->'resource_ids') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0422';END IF;
   IF c IS NULL OR (CASE WHEN route IS NULL THEN NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ctx.snapshot->'candidates') WHERE value=c) ELSE (ctx.snapshot->'candidate_digests' ? encode(sha256(convert_to(c::text,'UTF8')),'hex')) IS DISTINCT FROM true END) OR projection->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'model' IS DISTINCT FROM ctx.model OR manifest->>'upstream_model' IS DISTINCT FROM c->>'upstream_model' OR projection->>'origin' IS DISTINCT FROM c->>'origin' OR projection->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' OR projection->>'billable_manifest_sha256' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.billable-manifest','1',manifest)) OR d->>'dispatched_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.dispatch-projection','2',projection)) OR projection->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR (projection->'auth'->>'type') IS DISTINCT FROM c->'config'->>'auth' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF jsonb_typeof(projection->'execution') IS DISTINCT FROM 'object' OR (projection->'execution')-ARRAY['version','kind','artifact_digest']<>'{}'::jsonb OR projection->'execution'->'version' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(projection->'execution'->'kind') IS DISTINCT FROM 'string' OR projection->'execution'->>'kind' NOT IN ('wasm','builtin-worker') OR jsonb_typeof(projection->'execution'->'artifact_digest') IS DISTINCT FROM 'string' OR projection->'execution'->>'artifact_digest'!~'^[a-f0-9]{64}$' OR (ctx.snapshot->'execution_runtimes'->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true OR (allowed->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF d->>'prep_binding_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.prep-binding','2',jsonb_build_object('execution',projection->'execution','session',jsonb_build_object('id',d->>'prep_session_id','tenant',ctx.project_id,'attempt',projection->>'proposed_attempt_id','incarnation',p->>'incarnation_id','plugin',ref->>'package_digest','config',public.core_digest_v1(c->'config')),'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id'),'request_id',projection->>'proposed_request_id','operation',projection->>'operation','binding_id',ref->>'binding_id','config_revision',ref->>'config_revision','config_digest',ref->>'config_digest'))) THEN RAISE EXCEPTION 'prep_binding_invalid' USING ERRCODE='P0422';END IF;
   IF jsonb_typeof(c->'cost_rates') IS DISTINCT FROM 'object' OR c->'cost_rates'='{}'::jsonb THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
   FOR dim IN SELECT jsonb_object_keys(c->'cost_rates') LOOP
    IF jsonb_typeof(c->'cost_ceilings'->dim) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
    bound=(c->'cost_ceilings'->>dim)::numeric;qty=coalesce((manifest->'quantities'->>dim)::numeric,(manifest->'ceilings'->>dim)::numeric,bound);
    IF manifest->'quantities'?dim AND manifest->'ceilings'?dim AND (manifest->'quantities'->>dim)::numeric>(manifest->'ceilings'->>dim)::numeric THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
    IF qty<0 OR qty>bound*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) OR dim='requests' AND qty<1 THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
    estimated=estimated||jsonb_build_object(dim,qty::text);
   END LOOP;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'input_tokens' THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'output_tokens' OR (manifest->'ceilings'->>'output_tokens')::numeric>(SELECT max_output_tokens FROM public.models WHERE name=ctx.model)*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) THEN RAISE EXCEPTION 'output_bound' USING ERRCODE='P0422';END IF;
   IF c->'config'?'max_output_tokens' AND (manifest->'ceilings'->>'output_tokens')::numeric>(c->'config'->>'max_output_tokens')::numeric*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) OR c->'config'?'max_input_tokens' AND (manifest->'ceilings'->>'input_tokens')::numeric>(c->'config'->>'max_input_tokens')::numeric*(CASE WHEN ctx.operation='batches' THEN (ctx.snapshot->'batch'->>'items')::numeric ELSE 1 END) THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
   amount=core_funding.cost_v1((ref->>'cost_version_id')::uuid,estimated);
   IF computed->'estimated_quantities' IS DISTINCT FROM estimated OR computed->>'supplier_atoms' IS DISTINCT FROM amount::text THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
   quote=computed->'quote';
   IF quote->>'currency' IS DISTINCT FROM auth->>'customer_currency' OR NOT quote->'prices'? (ref->>'channel_id') OR quote->'prices'->>(ref->>'channel_id') IS DISTINCT FROM ref->>'customer_price_id' THEN RAISE EXCEPTION 'quote_invalid' USING ERRCODE='P0422';END IF;
   qid=(projection->>'proposed_request_id')::uuid;aid=(projection->>'proposed_attempt_id')::uuid;
   SELECT account_id INTO account FROM public.channels WHERE id=(ref->>'channel_id')::uuid;
   PERFORM core_funding.share_v1(ARRAY[account]);
   SELECT id INTO wallet FROM public.wallets WHERE project_id=ctx.project_id AND currency=auth->>'customer_currency' FOR NO KEY UPDATE NOWAIT;
   IF wallet IS NULL THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
    funding=core_funding.reserve_batch_v1(account,c->>'cost_currency',aid,amount,pending->'funding');funding_cell=(funding->>'cell')::smallint;pending=jsonb_set(pending,ARRAY['funding'],funding->'pending');
   ELSE funding_cell=core_funding.reserve_v1(account,c->>'cost_currency',aid,amount);END IF;
   IF kind='execution_admit_v1' THEN
    IF d->>'project_id' IS DISTINCT FROM ctx.project_id::text OR d->>'key_id' IS DISTINCT FROM ctx.key_id::text OR d->>'fingerprint' IS DISTINCT FROM ctx.fingerprint OR d->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0422';END IF;
    IF d->>'intent_digest' IS NOT NULL THEN
     SELECT * INTO q FROM public.gateway_requests WHERE project_id=ctx.project_id AND intent_digest=d->>'intent_digest';
     IF FOUND THEN qid=q.id;aid=NULL;RAISE EXCEPTION '%',CASE WHEN q.fingerprint=ctx.fingerprint THEN 'already_accepted' ELSE 'idempotency_conflict' END USING ERRCODE='P0409';END IF;
    END IF;
    IF EXISTS(SELECT 1 FROM public.gateway_requests WHERE id=qid) OR EXISTS(SELECT 1 FROM public.attempts WHERE id=aid) THEN RAISE EXCEPTION 'proposed_id_conflict' USING ERRCODE='P0409';END IF;
    allocation=jsonb_build_object('id',qid,'attempt_id',aid,'project_id',ctx.project_id,'key_id',ctx.key_id,'operation',ctx.operation,'model',ctx.model,'digest',d->>'intent_digest','fingerprint',ctx.fingerprint,'quote',quote,'route_id',ref->>'route_id','cost_pricing_id',ref->>'cost_version_id');
    IF pending IS NULL THEN
     result=public.allocate_customer_batch_v1(jsonb_build_array(allocation))->'0';
    IF result?'error' THEN
     IF result->>'error'='P0402' THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
     IF result->>'error'='55P03' THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
     RAISE EXCEPTION 'allocator_failed';
    END IF;
    IF (result->>'created')::boolean IS DISTINCT FROM true OR result->>'attempt_id' IS DISTINCT FROM aid::text THEN RAISE EXCEPTION 'allocator_failed';END IF;
    UPDATE public.gateway_requests SET state='ADMITTED',execution_budget=jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id') WHERE id=qid;
    UPDATE public.attempts SET binding_id=(ref->>'binding_id')::uuid WHERE id=aid;
    ELSE
     admission=jsonb_build_object('sequence',(SELECT count(*) FROM jsonb_each(pending->'admissions')),'customer',allocation,'binding_id',ref->>'binding_id','context_id',ctx.id,'owner_session_id',p->>'session_id','authorization',auth,'authority_times',authority_times,'command_deadline',p->>'deadline_at','package_digest',ref->>'package_digest','execution',projection->'execution','execution_budget',jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'));
    END IF;
   ELSE
    IF qid IS DISTINCT FROM (d->>'request_id')::uuid THEN RAISE EXCEPTION 'request_mismatch' USING ERRCODE='P0409';END IF;
    SELECT * INTO q FROM public.gateway_requests WHERE id=qid FOR UPDATE NOWAIT;
    PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
    PERFORM id FROM public.attempts WHERE request_id=qid ORDER BY attempts.ordinal,attempts.id FOR NO KEY UPDATE NOWAIT;
    PERFORM ep.attempt_id FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid ORDER BY ep.attempt_id FOR NO KEY UPDATE OF ep NOWAIT;
    PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=(d->>'previous_attempt_id')::uuid FOR NO KEY UPDATE NOWAIT;
    ordinal=coalesce((SELECT max(attempts.ordinal) FROM public.attempts WHERE request_id=qid),0)+1;
    IF q.execution_closed_at IS NOT NULL THEN RAISE EXCEPTION 'execution_closed' USING ERRCODE='P0409';END IF;
    IF (computed->>'token_count')::numeric+coalesce((SELECT sum((l.descriptor->>'tokens')::numeric) FROM public.execution_quota_leases l WHERE request_id=qid AND scope_kind='channel'),0)>(q.execution_budget->>'token_budget')::numeric THEN RAISE EXCEPTION 'retry_quota_exceeded' USING ERRCODE='P0422';END IF;
    IF q.fingerprint IS DISTINCT FROM ctx.fingerprint OR q.operation IS DISTINCT FROM ctx.operation OR q.model IS DISTINCT FROM ctx.model OR q.execution_budget->>'incarnation_id' IS DISTINCT FROM p->>'incarnation_id' OR manifest->'resource_ids' IS DISTINCT FROM (SELECT ep.manifest->'resource_ids' FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid AND at.ordinal=1) THEN RAISE EXCEPTION 'request_mismatch' USING ERRCODE='P0409';END IF;
    IF q.key_id IS DISTINCT FROM ctx.key_id OR q.project_id IS DISTINCT FROM ctx.project_id OR q.execution_budget->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR q.selected_attempt_id IS NOT NULL OR q.billing_outcome NOT IN ('PENDING','UNKNOWN') OR ordinal IS DISTINCT FROM (d->>'expected_ordinal')::integer OR ordinal>(q.execution_budget->>'max_attempts')::integer OR (q.execution_budget->>'deadline_at')::timestamptz< (p->>'deadline_at')::timestamptz OR NOT EXISTS(SELECT 1 FROM public.attempts WHERE id=(d->>'previous_attempt_id')::uuid AND request_id=qid AND attempts.ordinal=execution.ordinal-1) OR d->>'visible_output' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'retry_conflict' USING ERRCODE='P0409';END IF;
    IF (q.billing_quote->'prices'->>(ref->>'channel_id')) IS DISTINCT FROM ref->>'customer_price_id' OR (quote->>'amount_atoms')::numeric>(q.billing_quote->>'amount_atoms')::numeric THEN RAISE EXCEPTION 'retry_quote_exceeded' USING ERRCODE='P0422';END IF;
    IF EXISTS(SELECT 1 FROM public.attempts WHERE id=aid) THEN RAISE EXCEPTION 'proposed_id_conflict' USING ERRCODE='P0409';END IF;
    UPDATE public.execution_permits SET state='UNKNOWN' WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='SEND_GRANTED';
    UPDATE public.upstream_holds SET state='UNKNOWN' WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='HELD' AND EXISTS(SELECT 1 FROM public.execution_permits WHERE attempt_id=(d->>'previous_attempt_id')::uuid AND state='UNKNOWN');
    INSERT INTO public.attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,binding_id,state) VALUES(aid,qid,ordinal,(ref->>'channel_id')::uuid,c->>'upstream_model',(ref->>'cost_version_id')::uuid,(ref->>'customer_price_id')::uuid,(ref->>'binding_id')::uuid,'DISPATCHING');
    quote=q.billing_quote;
   END IF;
   plan=jsonb_build_object('version',2,'owner',jsonb_build_object('service','gateway','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'attempt_id',aid,'permit_epoch','1','dispatched_digest',d->>'dispatched_digest','dispatch_projection',projection);
   response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'ordinal',ordinal,'state','RESERVED','permit_epoch','1','deadline_at',p->>'deadline_at','customer_atoms',quote->>'amount_atoms','customer_currency',quote->>'currency','supplier_atoms',amount::text,'supplier_currency',c->>'cost_currency','authorization_plan',plan,'authorization_plan_digest',public.core_digest_v1(jsonb_build_array('stg.internal.host-authorization-plan','2',plan)));
   response=response||jsonb_build_object('quota_lease_ids',(SELECT jsonb_agg(lease_id ORDER BY lease_id) FROM (SELECT value->>'lease_id' AS lease_id FROM jsonb_array_elements(computed->'leases') UNION SELECT lease_id FROM public.execution_quota_leases WHERE request_id=qid AND scope_kind<>'channel') leases));
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
    admission=admission||jsonb_build_object('permit',jsonb_build_object('attempt_id',aid,'command_id',command_pk,'gateway_instance_id',p->>'instance_id','owner_session_id',p->>'session_id','owner_key_sha256',p->>'key_sha256','owner_service','gateway','certificate_sha256',p->>'certificate_sha256','incarnation_id',p->>'incarnation_id','permit_epoch',1,'binding_id',ref->>'binding_id','dispatched_digest',d->>'dispatched_digest','state','RESERVED','expires_at',p->>'deadline_at','route_id',ref->>'route_id','context_id',ctx.id,'snapshot',plan,'manifest',manifest,'manifest_digest',projection->>'billable_manifest_sha256','candidate_digest',ref->>'candidate_digest','authorization_plan_digest',response->>'authorization_plan_digest','prep_session_id',d->>'prep_session_id','prep_binding_digest',d->>'prep_binding_digest'),'hold',jsonb_build_object('attempt_id',aid,'account_id',account,'channel_id',ref->>'channel_id','currency',c->>'cost_currency','estimated_atoms',amount,'cost_version_id',ref->>'cost_version_id','state','HELD','estimated_quantities',estimated,'estimated_quantities_digest',public.core_digest_v1(estimated),'estimate_manifest_digest',projection->>'billable_manifest_sha256','estimate_algorithm_version',1,'funding_cell',funding_cell,'current_reserved_atoms',amount),'leases','[]'::jsonb);
    pending=jsonb_set(pending,ARRAY['commands'],(pending->'commands')||jsonb_build_array(jsonb_build_object('id',command_pk,'command_id',e->>'command_id','kind',kind,'caller_identity',caller,'owner_session_id',p->>'session_id','caller_incarnation',p->>'incarnation_id','body_digest',e->>'body_digest','deadline_at',e->>'deadline_at','trace_id',e->>'trace_id','contract_version',1,'state','COMMITTED','request_id',qid,'attempt_id',aid,'context_id',ctx.id,'receipt',response)));
    pending=jsonb_set(pending,ARRAY['keys','command:'||caller||':'||kind||':'||((e->>'command_id')::uuid)::text],'true'::jsonb);
    pending=jsonb_set(pending,ARRAY['keys','request:'||qid::text],'true'::jsonb);
    IF d->>'intent_digest' IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['keys','intent:'||ctx.project_id::text||':'||(d->>'intent_digest')],'true'::jsonb);END IF;
    journal=gen_random_uuid();funding=pending->'funding';
    funding=jsonb_set(funding,ARRAY['journals'],(funding->'journals')||jsonb_build_array(jsonb_build_object('id',journal,'account_id',account,'channel_id',ref->>'channel_id','attempt_id',aid,'currency',c->>'cost_currency','kind','RESERVE','reference','reserve:'||aid)));
    funding=jsonb_set(funding,ARRAY['entries'],(funding->'entries')||jsonb_build_array(jsonb_build_object('journal_id',journal,'ordinal',0,'currency',c->>'cost_currency','account','upstream:available','atoms',-amount),jsonb_build_object('journal_id',journal,'ordinal',1,'currency',c->>'cost_currency','account','upstream:reserved','atoms',amount)));
    pending=jsonb_set(pending,ARRAY['funding'],funding);
   ELSE
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
   INSERT INTO public.execution_permits(attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest) VALUES(aid,command_pk,p->>'instance_id',(p->>'session_id')::uuid,p->>'key_sha256','gateway',p->>'certificate_sha256',(p->>'incarnation_id')::uuid,1,(ref->>'binding_id')::uuid,d->>'dispatched_digest','RESERVED',(p->>'deadline_at')::timestamptz,(ref->>'route_id')::uuid,ctx.id,plan,manifest,projection->>'billable_manifest_sha256',ref->>'candidate_digest',response->>'authorization_plan_digest',(d->>'prep_session_id')::uuid,d->>'prep_binding_digest');
   INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version,funding_cell,current_reserved_atoms) VALUES(aid,account,(ref->>'channel_id')::uuid,c->>'cost_currency',amount,(ref->>'cost_version_id')::uuid,'HELD',estimated,public.core_digest_v1(estimated),projection->>'billable_manifest_sha256',1,funding_cell,amount);
   PERFORM public.core_upstream_journal_v1(account,c->>'cost_currency',(ref->>'channel_id')::uuid,aid,'RESERVE',amount);
   END IF;
   IF jsonb_array_length(computed->'leases')<>(CASE WHEN kind='execution_admit_v1' THEN jsonb_array_length(auth->'scopes')+1 ELSE 1 END) THEN RAISE EXCEPTION 'quota_invalid';END IF;
   FOR descriptor IN SELECT value FROM jsonb_array_elements(computed->'leases') LOOP
    IF descriptor->>'command_id' IS DISTINCT FROM e->>'command_id' OR descriptor->>'body_digest' IS DISTINCT FROM e->>'body_digest' OR descriptor->>'caller' IS DISTINCT FROM caller OR descriptor->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR descriptor->>'state' IS DISTINCT FROM 'DRAFT' OR NOT (EXISTS(SELECT 1 FROM jsonb_array_elements(auth->'scopes')s WHERE s->>'kind'=descriptor->>'scope_kind' AND s->>'id'=descriptor->>'scope_id') OR descriptor->>'scope_kind'='channel' AND descriptor->>'scope_id'=ref->>'channel_id') THEN RAISE EXCEPTION 'quota_invalid';END IF;
    IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
     admission=jsonb_set(admission,ARRAY['leases'],(admission->'leases')||jsonb_build_array(jsonb_build_object('lease_id',descriptor->>'lease_id','attempt_id',aid,'command_id',command_pk,'scope_kind',descriptor->>'scope_kind','scope_id',descriptor->>'scope_id','request_id',qid,'state','ADOPTION_PENDING','descriptor',descriptor)));
    ELSE
    INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,descriptor) VALUES(descriptor->>'lease_id',aid,command_pk,descriptor->>'scope_kind',descriptor->>'scope_id',qid,'ADOPTION_PENDING',descriptor);
    END IF;
   END LOOP;
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['admissions',aid::text],admission);END IF;
  ELSIF kind IN ('execution_begin_v1','execution_abort_v1') THEN
   aid=(d->>'attempt_id')::uuid;
   IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.permit_epoch::text IS DISTINCT FROM d->>'permit_epoch' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
   IF kind='execution_begin_v1' THEN
    catalog=core_funding.catalog_v1(cache,ctx.model,ctx.operation,auth->>'customer_currency');cache=catalog->'cache';candidates=catalog->'frame'->'candidates';allowed=catalog->'frame'->'runtimes';
    SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=permit.snapshot->'dispatch_projection'->'candidate_ref';
    IF c IS NOT NULL AND allowed IS NULL THEN allowed=core_execution_runtime.allowed_v1(jsonb_build_array(c));END IF;
    IF c IS NULL OR permit.snapshot->'dispatch_projection'->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
    IF permit.snapshot->'version' IS DISTINCT FROM '2'::jsonb OR permit.snapshot->'dispatch_projection'->'version' IS DISTINCT FROM '2'::jsonb OR (allowed->(c->'ref'->>'package_digest') @> jsonb_build_array(permit.snapshot->'dispatch_projection'->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   ELSE
    PERFORM id FROM public.channels WHERE id=(SELECT provider_id FROM public.attempts WHERE id=aid) FOR SHARE NOWAIT;
    PERFORM id FROM public.upstream_accounts WHERE id=(SELECT account_id FROM public.upstream_holds WHERE attempt_id=aid) FOR SHARE NOWAIT;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=aid;
   PERFORM core_funding.share_v1(ARRAY[h.account_id]);
   SELECT id INTO wallet FROM public.wallets WHERE id=(SELECT wallet_id FROM public.billing_holds WHERE request_id=(SELECT request_id FROM public.attempts WHERE id=aid)) FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO q FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=aid) FOR UPDATE NOWAIT;qid=q.id;
   PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
   SELECT * INTO a FROM public.attempts WHERE id=aid FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
   IF permit.state<>'RESERVED' THEN RAISE EXCEPTION 'permit_not_reserved' USING ERRCODE='P0409';END IF;
   IF kind='execution_begin_v1' THEN
    IF q.execution_closed_at IS NOT NULL OR permit.expires_at<=clock_timestamp() OR permit.authorization_plan_digest IS DISTINCT FROM d->>'authorization_plan_digest' OR permit.dispatched_digest IS DISTINCT FROM d->>'dispatched_digest' OR permit.prep_session_id::text IS DISTINCT FROM d->>'prep_session_id' OR permit.prep_binding_digest IS DISTINCT FROM d->>'prep_binding_digest' OR h.state<>'HELD' OR EXISTS(SELECT 1 FROM public.wallets WHERE id=wallet AND (freeze_reason IS NOT NULL OR public.wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) IS NOT NULL)) OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=qid AND (l.scope_kind<>'channel' OR l.attempt_id=aid) AND (l.state<>'ACTIVE' OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(computed->'proofs') proof WHERE proof->>'lease_id'=l.lease_id AND (proof->>'revision')::numeric=l.revision))) OR (computed->>'proof_remaining_ms')::numeric-extract(epoch FROM (clock_timestamp()-started))*1000<=1000 OR jsonb_array_length(computed->'proofs')<>(SELECT count(*) FROM public.execution_quota_leases WHERE request_id=qid AND (scope_kind<>'channel' OR attempt_id=aid)) THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
    rebuilt='{}'::jsonb;
    FOR dim IN SELECT jsonb_object_keys(c->'cost_rates') LOOP rebuilt=rebuilt||jsonb_build_object(dim,coalesce(permit.manifest->'quantities'->>dim,permit.manifest->'ceilings'->>dim,c->'cost_ceilings'->>dim));END LOOP;
    IF rebuilt IS DISTINCT FROM h.estimated_quantities OR h.estimated_quantities IS NULL OR h.estimated_quantities_digest IS DISTINCT FROM public.core_digest_v1(h.estimated_quantities) OR h.estimate_manifest_digest IS DISTINCT FROM permit.manifest_digest OR h.estimated_atoms IS DISTINCT FROM core_funding.cost_v1(h.cost_version_id,h.estimated_quantities) THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
    IF pending IS NULL THEN UPDATE public.execution_permits SET state='SEND_GRANTED',granted_at=clock_timestamp() WHERE attempt_id=aid AND state='RESERVED';fresh=FOUND;
    ELSE
     fresh=true;
     pending=jsonb_set(pending,ARRAY['grants',aid::text],jsonb_build_object('permit',jsonb_build_object('owner_service',permit.owner_service,'gateway_instance_id',permit.gateway_instance_id,'owner_session_id',permit.owner_session_id,'owner_key_sha256',permit.owner_key_sha256,'certificate_sha256',permit.certificate_sha256,'incarnation_id',permit.incarnation_id,'permit_epoch',permit.permit_epoch,'context_id',permit.context_id,'authorization_plan_digest',permit.authorization_plan_digest,'dispatched_digest',permit.dispatched_digest,'prep_session_id',permit.prep_session_id,'prep_binding_digest',permit.prep_binding_digest),'key_id',ctx.key_id,'ip',ctx.ip,'operation',ctx.operation,'model',ctx.model,'authorization',auth,'package_digest',c->'ref'->>'package_digest','execution',permit.snapshot->'dispatch_projection'->'execution','authority_times',authority_times,'command_deadline',p->>'deadline_at','proof_deadline_ms',computed->'proof_deadline_ms'));
    END IF;
    response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'state','SEND_GRANTED','permit_epoch',permit.permit_epoch::text,'authorization_plan_digest',permit.authorization_plan_digest,'deadline_at',to_char(permit.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
   ELSE
    UPDATE public.execution_permits SET state='ABORTED' WHERE attempt_id=aid AND state='RESERVED';
    UPDATE public.attempts SET state='NOT_EXECUTED',finished_at=clock_timestamp() WHERE id=aid;
    PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
    UPDATE public.upstream_holds SET state='RELEASED',final_cost_atoms=0 WHERE attempt_id=aid;
    PERFORM core_funding.apply_v1(aid,0,0,true);
    PERFORM public.core_upstream_journal_v1(h.account_id,h.currency,h.channel_id,aid,'RELEASE',h.current_reserved_atoms);
    IF d->>'close_request'='true' THEN
     IF EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=qid AND (ep.state<>'ABORTED' OR ep.granted_at IS NOT NULL)) THEN RAISE EXCEPTION 'abort_closure_conflict' USING ERRCODE='P0409';END IF;
     UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()) WHERE id=qid;
    END IF;
    IF (SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=qid) AND q.selected_attempt_id IS NULL AND NOT EXISTS(SELECT 1 FROM public.attempts WHERE request_id=qid AND state<>'NOT_EXECUTED') THEN
     UPDATE public.gateway_requests SET state='FAILED',billing_outcome='FAILED' WHERE id=qid;
     result=public.settle_requests(ARRAY[qid]);
     IF result->qid::text->>'state'='ERROR' THEN RAISE EXCEPTION 'settlement_failed';END IF;
    END IF;
    UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id=qid AND (scope_kind='channel' AND attempt_id=aid OR (SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=qid)) AND state IN ('ADOPTION_PENDING','ACTIVE');
    response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'state','ABORTED','permit_epoch',permit.permit_epoch::text);
   END IF;
   IF kind='execution_begin_v1' AND pending IS NOT NULL THEN
    pending=jsonb_set(pending,ARRAY['commands'],(pending->'commands')||jsonb_build_array(jsonb_build_object('id',command_pk,'command_id',e->>'command_id','kind',kind,'caller_identity',caller,'owner_session_id',p->>'session_id','caller_incarnation',p->>'incarnation_id','body_digest',e->>'body_digest','deadline_at',e->>'deadline_at','trace_id',e->>'trace_id','contract_version',1,'state','COMMITTED','request_id',qid,'attempt_id',aid,'context_id',ctx.id,'receipt',response)));
    pending=jsonb_set(pending,ARRAY['keys','command:'||caller||':'||kind||':'||((e->>'command_id')::uuid)::text],'true'::jsonb);
   ELSE
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
   END IF;
  ELSE RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
  IF kind IN('execution_begin_v1','execution_admit_v1') AND pending IS NOT NULL THEN
   pending=jsonb_set(pending,ARRAY['audits'],(pending->'audits')||jsonb_build_array(jsonb_build_object('id',gen_random_uuid(),'action',kind,'resource_id',aid::text,'actor',caller,'details',jsonb_build_object('command_id',e->>'command_id','request_id',qid,'attempt_id',aid,'state',response->>'state'))));
  ELSE
  INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),kind,aid::text,caller,jsonb_build_object('command_id',e->>'command_id','request_id',qid,'attempt_id',aid,'state',response->>'state'));
  END IF;
 EXCEPTION WHEN SQLSTATE 'P0402' OR SQLSTATE 'P0409' OR SQLSTATE 'P0422' THEN
  GET STACKED DIAGNOSTICS rejection=MESSAGE_TEXT;
  cache=saved_cache;pending=saved_pending;
  IF rejection NOT IN ('insufficient_funds','upstream_funds_unavailable','already_accepted','idempotency_conflict','proposed_id_conflict','context_stale','context_invalid','candidate_stale','cost_unbounded','cost_ceiling','estimate_invalid','quote_invalid','request_mismatch','execution_closed','retry_quota_exceeded','retry_conflict','retry_quote_exceeded','permit_not_reserved','begin_precondition','internal_request_invalid','abort_closure_conflict','prep_binding_invalid','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request') THEN RAISE;END IF;
  response=jsonb_build_object('command_state','REJECTED','code',rejection);
  IF rejection IN ('already_accepted','idempotency_conflict') THEN response=response||jsonb_build_object('request_id',qid);END IF;
  INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,rejection_code,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'REJECTED',rejection,ctx.id,response);
  fresh=false;
 END;
 RETURN jsonb_build_object('result',jsonb_build_object('receipt',response,'fresh_transition',fresh)||CASE WHEN fresh THEN jsonb_build_object('credential_ciphertext',(SELECT secret FROM public.channels WHERE id=a.provider_id),'channel_id',a.provider_id,'snapshot',permit.snapshot) ELSE '{}'::jsonb END,'cache',cache,'pending',pending);
END $$;

CREATE OR REPLACE FUNCTION core_funding.customer_authorities_v1(items jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE
 before_keys public.api_keys[];
 before_projects public.projects[];
 ks public.api_keys[];
 ps public.projects[];
 os public.organizations[];
 us public.principals[];
 result jsonb;
BEGIN
 IF jsonb_array_length(items)<2 THEN RETURN '{"customers":{},"catalog":{},"memo":false}'::jsonb;END IF;
 WITH inputs AS MATERIALIZED (
  SELECT value->'projection'->>'command_type' kind,value->'data' data FROM jsonb_array_elements(items)
 ), attempt_keys AS (
  SELECT DISTINCT (data->>'attempt_id')::uuid id FROM inputs WHERE kind IN('execution_begin_v1','execution_abort_v1')
 ), contexts AS (
  SELECT (data->>'context_id')::uuid id FROM inputs WHERE kind IN('execution_admit_v1','execution_next_v1')
  UNION
  SELECT ep.context_id FROM attempt_keys t JOIN LATERAL (
   SELECT context_id FROM public.execution_permits WHERE attempt_id=t.id OFFSET 0
  ) ep ON true
 ), keys AS (
  SELECT DISTINCT c.key_id FROM contexts x JOIN LATERAL (
   SELECT key_id FROM public.execution_contexts WHERE id=x.id OFFSET 0
  ) c ON true
  UNION
  SELECT k.id FROM jsonb_array_elements(items) i JOIN public.api_keys k ON k.verifier=i->'computed'->'route'->>'verifier'
   WHERE i->'projection'->>'command_type' IN('execution_admit_v1','execution_next_v1')
 )
 SELECT coalesce(array_agg(k.row ORDER BY x.key_id),'{}'::public.api_keys[]) INTO before_keys
 FROM keys x JOIN LATERAL (SELECT a AS row FROM public.api_keys a WHERE a.id=x.key_id OFFSET 0) k ON true;

 IF cardinality(before_keys)=0 THEN RETURN '{"customers":{},"catalog":{}}'::jsonb;END IF;
 SELECT coalesce(array_agg(p ORDER BY p.id),'{}'::public.projects[]) INTO before_projects FROM public.projects p WHERE p.id=ANY(ARRAY(SELECT DISTINCT k.project_id FROM unnest(before_keys) k));
 SELECT coalesce(array_agg(x.r),'{}'::public.organizations[]) INTO os FROM(SELECT o r FROM public.organizations o WHERE o.id=ANY(ARRAY(SELECT DISTINCT p.organization_id FROM unnest(before_projects) p)) ORDER BY o.id FOR SHARE OF o NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.principals[]) INTO us FROM(SELECT u r FROM public.principals u WHERE u.id=ANY(ARRAY(SELECT DISTINCT k.principal_id FROM unnest(before_keys) k)) ORDER BY u.id FOR SHARE OF u NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.projects[]) INTO ps FROM(SELECT p r FROM public.projects p WHERE p.id=ANY(ARRAY(SELECT DISTINCT k.project_id FROM unnest(before_keys) k)) ORDER BY p.id FOR SHARE OF p NOWAIT)x;
 SELECT coalesce(array_agg(x.r),'{}'::public.api_keys[]) INTO ks FROM(SELECT k r FROM public.api_keys k WHERE k.id=ANY(ARRAY(SELECT b.id FROM unnest(before_keys) b)) ORDER BY k.id FOR SHARE OF k NOWAIT)x;
 IF EXISTS(SELECT 1 FROM unnest(ks) k JOIN unnest(before_keys) b ON b.id=k.id WHERE (k.project_id,k.principal_id) IS DISTINCT FROM (b.project_id,b.principal_id))
  OR EXISTS(SELECT 1 FROM unnest(ps) p JOIN unnest(before_projects) b ON b.id=p.id WHERE p.organization_id IS DISTINCT FROM b.organization_id) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 SELECT jsonb_build_object('customers',coalesce(jsonb_object_agg(k.id::text,jsonb_build_object('key',to_jsonb(k),'project',to_jsonb(p),'organization',to_jsonb(o),'principal',to_jsonb(u))),'{}'::jsonb),'catalog','{}'::jsonb)
 INTO result FROM unnest(ks) k LEFT JOIN unnest(ps) p ON p.id=k.project_id LEFT JOIN unnest(os) o ON o.id=p.organization_id LEFT JOIN unnest(us) u ON u.id=k.principal_id;
 IF pg_column_size(result)>8388608 THEN RETURN '{"customers":{},"catalog":{}}'::jsonb;END IF;
 RETURN result;
END $$;
