REVOKE ALL ON SCHEMA core_execution_runtime FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core_execution_runtime FROM PUBLIC;
DO $$ DECLARE r record;BEGIN
 FOR r IN SELECT DISTINCT grantee FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a WHERE n.nspname='core_execution_runtime' AND grantee<>0 AND grantee<>n.nspowner LOOP
  EXECUTE format('REVOKE ALL ON SCHEMA core_execution_runtime FROM %I',pg_get_userbyid(r.grantee));
 END LOOP;
END $$;
CREATE FUNCTION core_execution_runtime.approve_v1(package text,runtime jsonb,proof jsonb,enabled boolean,who text,why text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE existing jsonb;BEGIN
 IF package IS NULL OR who IS NULL OR why IS NULL OR package!~'^[a-f0-9]{64}$' OR jsonb_typeof(runtime) IS DISTINCT FROM 'object' OR runtime-ARRAY['version','kind','artifact_digest']<>'{}'::jsonb OR runtime->'version' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(runtime->'kind') IS DISTINCT FROM 'string' OR jsonb_typeof(runtime->'artifact_digest') IS DISTINCT FROM 'string' OR runtime->>'kind' NOT IN ('wasm','builtin-worker') OR runtime->>'artifact_digest'!~'^[a-f0-9]{64}$' OR enabled IS NULL OR length(who) NOT BETWEEN 1 AND 100 OR length(why) NOT BETWEEN 1 AND 1000 OR jsonb_typeof(proof) IS DISTINCT FROM 'object' OR proof-ARRAY['version','module_digest','manifest_digest','config_schema_digest','fixture_digest','policy_digest','signer_digest','runtime_digest','worker_digest','registry_digest','verified_at','trust_until','approval_deadline','validation']<>'{}'::jsonb OR proof->'version' IS DISTINCT FROM '1'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['module_digest','manifest_digest','config_schema_digest','fixture_digest','policy_digest','signer_digest','runtime_digest']) f WHERE jsonb_typeof(proof->f) IS DISTINCT FROM 'string' OR proof->>f!~'^[a-f0-9]{64}$') OR jsonb_typeof(proof->'approval_deadline') IS DISTINCT FROM 'string' OR jsonb_typeof(proof->'trust_until') IS DISTINCT FROM 'string' OR jsonb_typeof(proof->'verified_at') IS DISTINCT FROM 'string' OR (proof->>'verified_at')::timestamptz>clock_timestamp()+interval '5 seconds' OR jsonb_typeof(proof->'validation') IS DISTINCT FROM 'object' OR (proof->'validation')-ARRAY['cases','steps','cleanup_confirmed']<>'{}'::jsonb OR proof->'validation'->'cleanup_confirmed' IS DISTINCT FROM 'true'::jsonb OR jsonb_typeof(proof->'validation'->'cases') IS DISTINCT FROM 'number' OR jsonb_typeof(proof->'validation'->'steps') IS DISTINCT FROM 'number' OR (proof->'validation'->>'cases')::integer NOT BETWEEN 1 AND 10000 OR (proof->'validation'->>'steps')::integer NOT BETWEEN 1 AND 100000 THEN RAISE EXCEPTION 'runtime_approval_invalid';END IF;
 IF runtime->>'kind'='wasm' THEN
  IF runtime->>'artifact_digest' IS DISTINCT FROM proof->>'runtime_digest' OR proof ? 'worker_digest' OR proof ? 'registry_digest' THEN RAISE EXCEPTION 'runtime_approval_invalid';END IF;
 ELSE
  IF jsonb_typeof(proof->'worker_digest') IS DISTINCT FROM 'string' OR proof->>'worker_digest'!~'^[a-f0-9]{64}$' OR jsonb_typeof(proof->'registry_digest') IS DISTINCT FROM 'string' OR proof->>'registry_digest'!~'^[a-f0-9]{64}$' OR runtime->>'artifact_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.builtin-worker-artifact','1',proof->>'worker_digest',proof->>'runtime_digest','1','1')) THEN RAISE EXCEPTION 'runtime_approval_invalid';END IF;
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('stg.execution.runtime.approval.v1:'||package,0));
 IF NOT EXISTS(SELECT 1 FROM public.plugin_packages WHERE digest=package) THEN RAISE EXCEPTION 'runtime_package_missing';END IF;
 SELECT facts INTO existing FROM core_execution_runtime.approvals WHERE package_digest=package AND kind=runtime->>'kind' AND artifact_digest=runtime->>'artifact_digest';
 IF FOUND THEN
  IF existing-ARRAY['verified_at','trust_until','approval_deadline','policy_digest','validation','registry_digest'] IS DISTINCT FROM proof-ARRAY['verified_at','trust_until','approval_deadline','policy_digest','validation','registry_digest'] THEN RAISE EXCEPTION 'runtime_approval_conflict';END IF;
  UPDATE core_execution_runtime.approvals SET active=enabled,facts=proof,actor=who,reason=why,updated_at=clock_timestamp() WHERE package_digest=package AND kind=runtime->>'kind' AND artifact_digest=runtime->>'artifact_digest';
 ELSE
  IF (SELECT count(*) FROM core_execution_runtime.approvals WHERE package_digest=package)>=16 THEN RAISE EXCEPTION 'runtime_approval_budget';END IF;
  INSERT INTO core_execution_runtime.approvals(package_digest,kind,artifact_digest,active,facts,actor,reason) VALUES(package,runtime->>'kind',runtime->>'artifact_digest',enabled,proof,who,why);
 END IF;
 IF enabled AND (proof->>'approval_deadline')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'runtime_approval_deadline';END IF;
 IF enabled AND (proof->>'trust_until')::timestamptz<=clock_timestamp() THEN RAISE EXCEPTION 'runtime_trust_expired';END IF;
END $$;
REVOKE ALL ON FUNCTION core_execution_runtime.approve_v1(text,jsonb,jsonb,boolean,text,text) FROM PUBLIC;
CREATE FUNCTION core_execution_runtime.audit_v1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE details jsonb;resource text;BEGIN
 IF TG_OP='TRUNCATE' THEN
  details=jsonb_build_object('operation',TG_OP);resource='all';
 ELSE
  details=jsonb_build_object('operation',TG_OP,'before',CASE WHEN TG_OP<>'INSERT' THEN to_jsonb(OLD) END,'after',CASE WHEN TG_OP<>'DELETE' THEN to_jsonb(NEW) END);
  resource=coalesce(NEW.package_digest,OLD.package_digest);
 END IF;
 INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),'execution_runtime_approval',resource,session_user,details);
 RETURN NULL;
END $$;
REVOKE ALL ON FUNCTION core_execution_runtime.audit_v1() FROM PUBLIC;
CREATE TRIGGER execution_runtime_audit_v1 AFTER INSERT OR UPDATE OR DELETE ON core_execution_runtime.approvals FOR EACH ROW EXECUTE FUNCTION core_execution_runtime.audit_v1();
CREATE TRIGGER execution_runtime_audit_truncate_v1 AFTER TRUNCATE ON core_execution_runtime.approvals FOR EACH STATEMENT EXECUTE FUNCTION core_execution_runtime.audit_v1();
CREATE TRIGGER execution_runtime_epoch_v1 AFTER INSERT OR UPDATE OR DELETE ON core_execution_runtime.approvals FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER execution_runtime_epoch_truncate_v1 AFTER TRUNCATE ON core_execution_runtime.approvals FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE FUNCTION core_execution_runtime.allowed_v1(candidates jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE package text;allowed jsonb;result jsonb='{}';BEGIN
 LOCK TABLE core_execution_runtime.approvals,public.plugin_packages IN ACCESS SHARE MODE NOWAIT;
 IF jsonb_typeof(candidates) IS DISTINCT FROM 'array' OR jsonb_array_length(candidates)>128 THEN RAISE EXCEPTION 'runtime_candidates_invalid';END IF;
 FOR package IN SELECT DISTINCT value->'ref'->>'package_digest' FROM jsonb_array_elements(candidates) LOOP
  SELECT coalesce(jsonb_agg(jsonb_build_object('version',1,'kind',r.kind,'artifact_digest',r.artifact_digest) ORDER BY r.kind,r.artifact_digest),'[]') INTO allowed FROM (
   SELECT 'wasm'::text kind,p.release_descriptor->>'runtime_digest' artifact_digest FROM public.plugin_packages p WHERE p.digest=package AND p.release_descriptor->>'runtime_digest' ~ '^[a-f0-9]{64}$'
   UNION
   SELECT a.kind,a.artifact_digest FROM core_execution_runtime.approvals a WHERE a.package_digest=package AND a.active AND (a.facts->>'trust_until')::timestamptz>clock_timestamp()
  )r WHERE NOT EXISTS(SELECT 1 FROM core_execution_runtime.approvals a WHERE a.package_digest=package AND a.kind=r.kind AND a.artifact_digest=r.artifact_digest AND (NOT a.active OR (a.facts->>'trust_until')::timestamptz<=clock_timestamp()));
  IF jsonb_array_length(allowed)>17 THEN RAISE EXCEPTION 'runtime_approval_budget';END IF;
  result=result||jsonb_build_object(package,allowed);
 END LOOP;
 RETURN result;
END $$;
REVOKE ALL ON FUNCTION core_execution_runtime.allowed_v1(jsonb) FROM PUBLIC;
DO $$ DECLARE function_owner name;BEGIN
 FOR function_owner IN SELECT DISTINCT pg_get_userbyid(proowner) FROM pg_proc WHERE oid IN ('public.core_context_v1(jsonb,uuid,jsonb)'::regprocedure,'public.core_execution_item_v2(jsonb,jsonb,jsonb,jsonb,jsonb)'::regprocedure) LOOP
  EXECUTE format('GRANT USAGE ON SCHEMA core_execution_runtime TO %I',function_owner);
  EXECUTE format('GRANT SELECT ON core_execution_runtime.approvals TO %I',function_owner);
  EXECUTE format('GRANT EXECUTE ON FUNCTION core_execution_runtime.allowed_v1(jsonb) TO %I',function_owner);
 END LOOP;
END $$;
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
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) VALUES(id,(auth->>'project_id')::uuid,k,(p->>'session_id')::uuid,input->>'operation',input->>'model',input->>'ip',input->>'fingerprint',snapshot,expiry);
 RETURN snapshot||jsonb_build_object('context_id',id,'expires_at',to_char(expiry AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE OR REPLACE FUNCTION public.core_execution_item_v2(p jsonb,e jsonb,d jsonb,computed jsonb,authority_times jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
<<execution>>
DECLARE ctx public.execution_contexts%ROWTYPE;cmd public.execution_commands%ROWTYPE;q public.gateway_requests%ROWTYPE;permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;h public.upstream_holds%ROWTYPE;balance public.upstream_account_balances%ROWTYPE;
 kind text=p->>'command_type';caller text=p->>'service'||':'||(p->>'instance_id');keys text[];k text;auth jsonb;candidates jsonb;c jsonb;projection jsonb;ref jsonb;manifest jsonb;quote jsonb;estimated jsonb='{}';dim text;bound numeric;qty numeric;amount numeric;response jsonb;result jsonb;plan jsonb;command_pk uuid=gen_random_uuid();qid uuid;aid uuid;ordinal integer=1;account uuid;wallet uuid;descriptor jsonb;failure text;rejection text;fresh boolean=false;started timestamptz=clock_timestamp();rebuilt jsonb;
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
  SELECT * INTO ctx FROM public.execution_contexts WHERE id=(d->>'context_id')::uuid;
 ELSE
  IF kind='execution_query_v1' THEN
   SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=d->>'kind' AND command_id=(d->>'command_id')::uuid;
   IF NOT FOUND THEN RETURN jsonb_build_object('receipt',jsonb_build_object('command_state','NOT_FOUND'));END IF;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=cmd.attempt_id;
  ELSE SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid;END IF;
  SELECT * INTO ctx FROM public.execution_contexts WHERE id=CASE WHEN kind='execution_query_v1' THEN cmd.context_id ELSE permit.context_id END;
 END IF;
 IF ctx.id IS NULL THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF NOT (kind='execution_query_v1' AND cmd.kind='execution_abort_recovery_v1') THEN auth=public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
 IF kind='execution_query_v1' THEN
  RETURN jsonb_build_object('receipt',cmd.receipt||CASE WHEN permit.attempt_id IS NOT NULL THEN jsonb_build_object('state',permit.state) ELSE '{}'::jsonb END);
 END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=execution.kind AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('receipt',cmd.receipt);
 END IF;
 BEGIN
  IF kind IN ('execution_admit_v1','execution_next_v1') THEN
   IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR ctx.expires_at<=clock_timestamp() OR ctx.snapshot->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' OR ctx.snapshot->'scopes' IS DISTINCT FROM auth->'scopes' THEN RAISE EXCEPTION 'context_stale' USING ERRCODE='P0422';END IF;
   IF computed->>'rejection' IN ('candidate_stale','cost_unbounded','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request','cost_ceiling') THEN RAISE EXCEPTION '%',computed->>'rejection' USING ERRCODE='P0422';END IF;
   candidates=public.core_candidates_v1(ctx.model,ctx.operation,auth->>'customer_currency');
   projection=d->'dispatch_projection';ref=projection->'candidate_ref';manifest=d->'manifest';
   SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=ref;
   IF projection->'version' IS DISTINCT FROM '2'::jsonb OR jsonb_typeof(projection) IS DISTINCT FROM 'object' OR projection-ARRAY['version','proposed_request_id','proposed_attempt_id','origin','method','operation','request_target_sha256','final_headers_digest','final_body_sha256','candidate_ref','auth','credential_ciphertext_sha256','billable_manifest_sha256','deadline_at','execution']<>'{}'::jsonb OR (SELECT count(*) FROM jsonb_object_keys(projection))<>15 OR jsonb_typeof(manifest) IS DISTINCT FROM 'object' OR manifest-ARRAY['version','operation','model','upstream_model','stream','input_bytes','ceilings','quantities','input_characters','images','resource_ids']<>'{}'::jsonb OR jsonb_typeof(manifest->'quantities') IS DISTINCT FROM 'object' OR jsonb_typeof(manifest->'resource_ids') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0422';END IF;
   IF c IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ctx.snapshot->'candidates') WHERE value=c) OR projection->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'model' IS DISTINCT FROM ctx.model OR manifest->>'upstream_model' IS DISTINCT FROM c->>'upstream_model' OR projection->>'origin' IS DISTINCT FROM c->>'origin' OR projection->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' OR projection->>'billable_manifest_sha256' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.billable-manifest','1',manifest)) OR d->>'dispatched_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.dispatch-projection','2',projection)) OR projection->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR (projection->'auth'->>'type') IS DISTINCT FROM c->'config'->>'auth' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF jsonb_typeof(projection->'execution') IS DISTINCT FROM 'object' OR (projection->'execution')-ARRAY['version','kind','artifact_digest']<>'{}'::jsonb OR projection->'execution'->'version' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(projection->'execution'->'kind') IS DISTINCT FROM 'string' OR projection->'execution'->>'kind' NOT IN ('wasm','builtin-worker') OR jsonb_typeof(projection->'execution'->'artifact_digest') IS DISTINCT FROM 'string' OR projection->'execution'->>'artifact_digest'!~'^[a-f0-9]{64}$' OR (ctx.snapshot->'execution_runtimes'->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true OR (core_execution_runtime.allowed_v1(jsonb_build_array(c))->(ref->>'package_digest') @> jsonb_build_array(projection->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
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
   amount=public.exact_billing_price(estimated,c->'cost_rates');
   IF computed->'estimated_quantities' IS DISTINCT FROM estimated OR computed->>'supplier_atoms' IS DISTINCT FROM amount::text THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
   quote=computed->'quote';
   IF quote->>'currency' IS DISTINCT FROM auth->>'customer_currency' OR NOT quote->'prices'? (ref->>'channel_id') OR quote->'prices'->>(ref->>'channel_id') IS DISTINCT FROM ref->>'customer_price_id' THEN RAISE EXCEPTION 'quote_invalid' USING ERRCODE='P0422';END IF;
   qid=(projection->>'proposed_request_id')::uuid;aid=(projection->>'proposed_attempt_id')::uuid;
   SELECT account_id INTO account FROM public.channels WHERE id=(ref->>'channel_id')::uuid;
   SELECT id INTO wallet FROM public.wallets WHERE project_id=ctx.project_id AND currency=auth->>'customer_currency' FOR NO KEY UPDATE NOWAIT;
   IF wallet IS NULL THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
   SELECT * INTO balance FROM public.upstream_account_balances WHERE account_id=account AND currency=c->>'cost_currency' FOR NO KEY UPDATE NOWAIT;
   IF NOT FOUND OR balance.balance_atoms-balance.reserved_atoms<amount THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
   IF kind='execution_admit_v1' THEN
    IF d->>'project_id' IS DISTINCT FROM ctx.project_id::text OR d->>'key_id' IS DISTINCT FROM ctx.key_id::text OR d->>'fingerprint' IS DISTINCT FROM ctx.fingerprint OR d->>'customer_currency' IS DISTINCT FROM auth->>'customer_currency' THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0422';END IF;
    IF d->>'intent_digest' IS NOT NULL THEN
     SELECT * INTO q FROM public.gateway_requests WHERE project_id=ctx.project_id AND intent_digest=d->>'intent_digest';
     IF FOUND THEN qid=q.id;aid=NULL;RAISE EXCEPTION '%',CASE WHEN q.fingerprint=ctx.fingerprint THEN 'already_accepted' ELSE 'idempotency_conflict' END USING ERRCODE='P0409';END IF;
    END IF;
    IF EXISTS(SELECT 1 FROM public.gateway_requests WHERE id=qid) OR EXISTS(SELECT 1 FROM public.attempts WHERE id=aid) THEN RAISE EXCEPTION 'proposed_id_conflict' USING ERRCODE='P0409';END IF;
    result=public.allocate_customer_batch_v1(jsonb_build_array(jsonb_build_object('id',qid,'attempt_id',aid,'project_id',ctx.project_id,'key_id',ctx.key_id,'operation',ctx.operation,'model',ctx.model,'digest',d->>'intent_digest','fingerprint',ctx.fingerprint,'quote',quote,'route_id',ref->>'route_id','cost_pricing_id',ref->>'cost_version_id')))->'0';
    IF result?'error' THEN
     IF result->>'error'='P0402' THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
     IF result->>'error'='55P03' THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
     RAISE EXCEPTION 'allocator_failed';
    END IF;
    IF (result->>'created')::boolean IS DISTINCT FROM true OR result->>'attempt_id' IS DISTINCT FROM aid::text THEN RAISE EXCEPTION 'allocator_failed';END IF;
    UPDATE public.gateway_requests SET state='ADMITTED',execution_budget=jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id') WHERE id=qid;
    UPDATE public.attempts SET binding_id=(ref->>'binding_id')::uuid WHERE id=aid;
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
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
   INSERT INTO public.execution_permits(attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest) VALUES(aid,command_pk,p->>'instance_id',(p->>'session_id')::uuid,p->>'key_sha256','gateway',p->>'certificate_sha256',(p->>'incarnation_id')::uuid,1,(ref->>'binding_id')::uuid,d->>'dispatched_digest','RESERVED',(p->>'deadline_at')::timestamptz,(ref->>'route_id')::uuid,ctx.id,plan,manifest,projection->>'billable_manifest_sha256',ref->>'candidate_digest',response->>'authorization_plan_digest',(d->>'prep_session_id')::uuid,d->>'prep_binding_digest');
   INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version) VALUES(aid,account,(ref->>'channel_id')::uuid,c->>'cost_currency',amount,(ref->>'cost_version_id')::uuid,'HELD',estimated,public.core_digest_v1(estimated),projection->>'billable_manifest_sha256',1);
   UPDATE public.upstream_account_balances SET reserved_atoms=reserved_atoms+amount,revision=revision+1 WHERE account_id=account AND currency=c->>'cost_currency';
   PERFORM public.core_upstream_journal_v1(account,c->>'cost_currency',(ref->>'channel_id')::uuid,aid,'RESERVE',amount);
   IF jsonb_array_length(computed->'leases')<>(CASE WHEN kind='execution_admit_v1' THEN jsonb_array_length(auth->'scopes')+1 ELSE 1 END) THEN RAISE EXCEPTION 'quota_invalid';END IF;
   FOR descriptor IN SELECT value FROM jsonb_array_elements(computed->'leases') LOOP
    IF descriptor->>'command_id' IS DISTINCT FROM e->>'command_id' OR descriptor->>'body_digest' IS DISTINCT FROM e->>'body_digest' OR descriptor->>'caller' IS DISTINCT FROM caller OR descriptor->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR descriptor->>'state' IS DISTINCT FROM 'DRAFT' OR NOT (EXISTS(SELECT 1 FROM jsonb_array_elements(auth->'scopes')s WHERE s->>'kind'=descriptor->>'scope_kind' AND s->>'id'=descriptor->>'scope_id') OR descriptor->>'scope_kind'='channel' AND descriptor->>'scope_id'=ref->>'channel_id') THEN RAISE EXCEPTION 'quota_invalid';END IF;
    INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,descriptor) VALUES(descriptor->>'lease_id',aid,command_pk,descriptor->>'scope_kind',descriptor->>'scope_id',qid,'ADOPTION_PENDING',descriptor);
   END LOOP;
  ELSIF kind IN ('execution_begin_v1','execution_abort_v1') THEN
   aid=(d->>'attempt_id')::uuid;
   IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.permit_epoch::text IS DISTINCT FROM d->>'permit_epoch' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
   IF kind='execution_begin_v1' THEN
    candidates=public.core_candidates_v1(ctx.model,ctx.operation,auth->>'customer_currency');
    SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=permit.snapshot->'dispatch_projection'->'candidate_ref';
    IF c IS NULL OR permit.snapshot->'dispatch_projection'->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
    IF permit.snapshot->'version' IS DISTINCT FROM '2'::jsonb OR permit.snapshot->'dispatch_projection'->'version' IS DISTINCT FROM '2'::jsonb OR (core_execution_runtime.allowed_v1(jsonb_build_array(c))->(c->'ref'->>'package_digest') @> jsonb_build_array(permit.snapshot->'dispatch_projection'->'execution')) IS DISTINCT FROM true THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   ELSE
    PERFORM id FROM public.channels WHERE id=(SELECT provider_id FROM public.attempts WHERE id=aid) FOR SHARE NOWAIT;
    PERFORM id FROM public.upstream_accounts WHERE id=(SELECT account_id FROM public.upstream_holds WHERE attempt_id=aid) FOR SHARE NOWAIT;
   END IF;
   SELECT id INTO wallet FROM public.wallets WHERE id=(SELECT wallet_id FROM public.billing_holds WHERE request_id=(SELECT request_id FROM public.attempts WHERE id=aid)) FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=aid;
   SELECT * INTO balance FROM public.upstream_account_balances WHERE account_id=h.account_id AND currency=h.currency FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO q FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=aid) FOR UPDATE NOWAIT;qid=q.id;
   PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
   SELECT * INTO a FROM public.attempts WHERE id=aid FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
   IF permit.state<>'RESERVED' THEN RAISE EXCEPTION 'permit_not_reserved' USING ERRCODE='P0409';END IF;
   IF kind='execution_begin_v1' THEN
    IF q.execution_closed_at IS NOT NULL OR permit.expires_at<=clock_timestamp() OR permit.authorization_plan_digest IS DISTINCT FROM d->>'authorization_plan_digest' OR permit.dispatched_digest IS DISTINCT FROM d->>'dispatched_digest' OR permit.prep_session_id::text IS DISTINCT FROM d->>'prep_session_id' OR permit.prep_binding_digest IS DISTINCT FROM d->>'prep_binding_digest' OR h.state<>'HELD' OR balance.balance_atoms<balance.reserved_atoms OR EXISTS(SELECT 1 FROM public.wallets WHERE id=wallet AND (freeze_reason IS NOT NULL OR public.wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) IS NOT NULL)) OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=qid AND (l.scope_kind<>'channel' OR l.attempt_id=aid) AND (l.state<>'ACTIVE' OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(computed->'proofs') proof WHERE proof->>'lease_id'=l.lease_id AND (proof->>'revision')::numeric=l.revision))) OR (computed->>'proof_remaining_ms')::numeric-extract(epoch FROM (clock_timestamp()-started))*1000<=1000 OR jsonb_array_length(computed->'proofs')<>(SELECT count(*) FROM public.execution_quota_leases WHERE request_id=qid AND (scope_kind<>'channel' OR attempt_id=aid)) THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
    rebuilt='{}'::jsonb;
    FOR dim IN SELECT jsonb_object_keys(c->'cost_rates') LOOP rebuilt=rebuilt||jsonb_build_object(dim,coalesce(permit.manifest->'quantities'->>dim,permit.manifest->'ceilings'->>dim,c->'cost_ceilings'->>dim));END LOOP;
    IF rebuilt IS DISTINCT FROM h.estimated_quantities OR h.estimated_quantities IS NULL OR h.estimated_quantities_digest IS DISTINCT FROM public.core_digest_v1(h.estimated_quantities) OR h.estimate_manifest_digest IS DISTINCT FROM permit.manifest_digest OR h.estimated_atoms IS DISTINCT FROM public.exact_billing_price(h.estimated_quantities,c->'cost_rates') THEN RAISE EXCEPTION 'estimate_invalid' USING ERRCODE='P0422';END IF;
    UPDATE public.execution_permits SET state='SEND_GRANTED',granted_at=clock_timestamp() WHERE attempt_id=aid AND state='RESERVED';fresh=FOUND;
    response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'state','SEND_GRANTED','permit_epoch',permit.permit_epoch::text,'authorization_plan_digest',permit.authorization_plan_digest,'deadline_at',to_char(permit.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
   ELSE
    UPDATE public.execution_permits SET state='ABORTED' WHERE attempt_id=aid AND state='RESERVED';
    UPDATE public.attempts SET state='NOT_EXECUTED',finished_at=clock_timestamp() WHERE id=aid;
    PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
    UPDATE public.upstream_holds SET state='RELEASED',final_cost_atoms=0 WHERE attempt_id=aid;
    UPDATE public.upstream_account_balances SET reserved_atoms=reserved_atoms-h.estimated_atoms,revision=revision+1 WHERE account_id=h.account_id AND currency=h.currency;
    PERFORM public.core_upstream_journal_v1(h.account_id,h.currency,h.channel_id,aid,'RELEASE',h.estimated_atoms);
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
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,response);
  ELSE RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
  INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),kind,aid::text,caller,jsonb_build_object('command_id',e->>'command_id','request_id',qid,'attempt_id',aid,'state',response->>'state'));
 EXCEPTION WHEN SQLSTATE 'P0402' OR SQLSTATE 'P0409' OR SQLSTATE 'P0422' THEN
  GET STACKED DIAGNOSTICS rejection=MESSAGE_TEXT;
  IF rejection NOT IN ('insufficient_funds','upstream_funds_unavailable','already_accepted','idempotency_conflict','proposed_id_conflict','context_stale','context_invalid','candidate_stale','cost_unbounded','cost_ceiling','estimate_invalid','quote_invalid','request_mismatch','execution_closed','retry_quota_exceeded','retry_conflict','retry_quote_exceeded','permit_not_reserved','begin_precondition','internal_request_invalid','abort_closure_conflict','prep_binding_invalid','amount_overflow','billing_ceiling','tariff_unavailable','invalid_tariff','quota_budget_exceeded','output_bound','invalid_request') THEN RAISE;END IF;
  response=jsonb_build_object('command_state','REJECTED','code',rejection);
  IF rejection IN ('already_accepted','idempotency_conflict') THEN response=response||jsonb_build_object('request_id',qid);END IF;
  INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,rejection_code,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'REJECTED',rejection,ctx.id,response);
  fresh=false;
 END;
 RETURN jsonb_build_object('receipt',response,'fresh_transition',fresh)||CASE WHEN fresh THEN jsonb_build_object('credential_ciphertext',(SELECT secret FROM public.channels WHERE id=a.provider_id),'channel_id',a.provider_id,'snapshot',permit.snapshot) ELSE '{}'::jsonb END;
END $$;
REVOKE ALL ON FUNCTION public.core_execution_item_v2(jsonb,jsonb,jsonb,jsonb,jsonb) FROM PUBLIC;
