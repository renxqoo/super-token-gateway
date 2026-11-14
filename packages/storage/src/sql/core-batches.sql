ALTER TABLE public.resources ADD COLUMN batch_ceilings jsonb CONSTRAINT resource_batch_ceilings CHECK (batch_ceilings IS NULL OR jsonb_typeof(batch_ceilings)='object');
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
 snapshot=auth||jsonb_build_object('owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'max_output_tokens',(SELECT max_output_tokens FROM public.models WHERE name=input->>'model'),'routing_strategy',(SELECT routing_strategy FROM public.models WHERE name=input->>'model'),'candidates',candidates,'operation',input->>'operation','model',input->>'model','execution_budget',jsonb_build_object('max_attempts',(input->>'max_attempts')::integer))||CASE WHEN input ? 'resource_ids' THEN jsonb_build_object('resources',refs) ELSE '{}'::jsonb END||CASE WHEN batch IS NOT NULL THEN jsonb_build_object('batch',batch) ELSE '{}'::jsonb END;
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) VALUES(id,(auth->>'project_id')::uuid,k,(p->>'session_id')::uuid,input->>'operation',input->>'model',input->>'ip',input->>'fingerprint',snapshot,expiry);
 RETURN snapshot||jsonb_build_object('context_id',id,'expires_at',to_char(expiry AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_commit_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;q public.gateway_requests%ROWTYPE;rid text;remote text=d->>'remote_id';found_id text;bf public.resources%ROWTYPE;manifest jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_commit_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['attempt_id','permit_epoch','kind','remote_id','state','batch','input_file_id']<>'{}'::jsonb OR d ? 'batch' AND (d->>'kind'<>'file' OR jsonb_typeof(d->'batch') IS DISTINCT FROM 'object' OR (d->'batch')-ARRAY['endpoint','upstream_model','items','ids_sha256','ceilings','images']<>'{}'::jsonb OR NOT d->'batch' ?& ARRAY['endpoint','upstream_model','items','ids_sha256','ceilings'] OR d->'batch'->>'endpoint' NOT IN ('/v1/chat/completions','/v1/responses','/v1/embeddings','/v1/completions','/v1/moderations','/v1/images/generations','/v1/images/edits') OR jsonb_typeof(d->'batch'->'upstream_model') IS DISTINCT FROM 'string' OR length(d->'batch'->>'upstream_model') NOT BETWEEN 1 AND 200 OR jsonb_typeof(d->'batch'->'items') IS DISTINCT FROM 'number' OR d->'batch'->>'items' !~ '^[1-9][0-9]{0,4}$' OR (d->'batch'->>'items')::integer>50000 OR jsonb_typeof(d->'batch'->'ids_sha256') IS DISTINCT FROM 'string' OR d->'batch'->>'ids_sha256' !~ '^[a-f0-9]{64}$' OR jsonb_typeof(d->'batch'->'ceilings') IS DISTINCT FROM 'object' OR EXISTS(SELECT 1 FROM jsonb_each(d->'batch'->'ceilings') e WHERE jsonb_typeof(e.value) IS DISTINCT FROM 'string' OR e.value#>>'{}' !~ '^(0|[1-9][0-9]{0,37})([.][0-9]{1,18})?$') OR d->'batch' ? 'images' AND (jsonb_typeof(d->'batch'->'images') IS DISTINCT FROM 'number' OR d->'batch'->>'images' !~ '^[1-9][0-9]{0,15}$')) OR (d->>'kind'='batch') IS DISTINCT FROM (d ? 'input_file_id') OR d ? 'input_file_id' AND NOT (d->>'input_file_id' ~ '^file_[0-9a-f]{32}$') OR d->>'kind' NOT IN ('response','file','batch','video') OR d->>'state' NOT IN ('ACTIVE','COMPLETED','FAILED','CANCELLED') OR remote IS NULL OR length(remote) NOT BETWEEN 1 AND 500 OR remote!~'^[A-Za-z0-9_:/.-]+$' OR remote LIKE '%://%' OR EXISTS(SELECT 1 FROM unnest(string_to_array(remote,'/')) part WHERE part IN ('.','..')) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid FOR SHARE;
 IF permit.attempt_id IS NULL OR permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.permit_epoch IS DISTINCT FROM (d->>'permit_epoch')::numeric OR permit.state NOT IN ('SEND_GRANTED','UNKNOWN','TERMINAL') THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 SELECT * INTO a FROM public.attempts WHERE id=permit.attempt_id;
 SELECT * INTO q FROM public.gateway_requests WHERE id=a.request_id;
 IF q.operation IS DISTINCT FROM public.core_resource_operation_v1(d->>'kind') OR a.binding_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 rid=(d->>'kind')||'_'||replace(q.id::text,'-','');
 IF d ? 'input_file_id' THEN
  SELECT * INTO bf FROM public.resources WHERE id=d->>'input_file_id' AND kind='file' AND project_id=q.project_id AND provider_id=a.provider_id AND batch_manifest IS NOT NULL;
  IF bf.id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 ELSIF d ? 'batch' THEN
  bf.batch_endpoint=d->'batch'->>'endpoint';bf.batch_upstream_model=d->'batch'->>'upstream_model';bf.batch_ceilings=d->'batch'->'ceilings';
  bf.batch_manifest=jsonb_build_object('items',d->'batch'->'items','idsSha256',d->'batch'->'ids_sha256')||CASE WHEN d->'batch' ? 'images' THEN jsonb_build_object('images',d->'batch'->'images') ELSE '{}'::jsonb END;
 END IF;
 INSERT INTO public.resources(id,project_id,provider_id,remote_id,kind,model,attempt_id,binding_id,state,batch_endpoint,batch_upstream_model,batch_manifest,batch_ceilings) VALUES(rid,q.project_id,a.provider_id,remote,d->>'kind',q.model,a.id,a.binding_id,d->>'state',bf.batch_endpoint,bf.batch_upstream_model,bf.batch_manifest,bf.batch_ceilings) ON CONFLICT DO NOTHING;
 SELECT id INTO found_id FROM public.resources WHERE project_id=q.project_id AND provider_id=a.provider_id AND kind=d->>'kind' AND remote_id=remote;
 IF found_id IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 RETURN jsonb_build_object('resource_id',found_id);
END $$;
CREATE FUNCTION public.core_batch_files_v1(r public.resources,remotes jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE mapped jsonb='{}';remote text;fid text;BEGIN
 IF jsonb_typeof(remotes) IS DISTINCT FROM 'array' OR jsonb_array_length(remotes) NOT BETWEEN 1 AND 3 OR EXISTS(SELECT 1 FROM jsonb_array_elements(remotes) x WHERE jsonb_typeof(x) IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR remote IN SELECT DISTINCT x FROM jsonb_array_elements_text(remotes) x ORDER BY x LOOP
  IF length(remote) NOT BETWEEN 1 AND 500 OR remote!~'^[A-Za-z0-9_:/.-]+$' OR remote LIKE '%://%' OR EXISTS(SELECT 1 FROM unnest(string_to_array(remote,'/')) part WHERE part IN ('.','..')) THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
  INSERT INTO public.resources(id,project_id,provider_id,remote_id,kind,model,binding_id,state) VALUES('file_'||md5(r.id||':'||remote),r.project_id,r.provider_id,remote,'file',r.model,r.binding_id,'COMPLETED') ON CONFLICT DO NOTHING;
  SELECT id INTO fid FROM public.resources WHERE project_id=r.project_id AND provider_id=r.provider_id AND kind='file' AND remote_id=remote;
  IF fid IS NULL THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
  mapped=mapped||jsonb_build_object(remote,fid);
 END LOOP;
 RETURN mapped;
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_permit_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;r public.resources%ROWTYPE;b public.channel_plugin_bindings%ROWTYPE;v public.plugin_versions%ROWTYPE;c public.channels%ROWTYPE;manifest jsonb;rkind text=split_part(d->>'resource_id','_',1);act text=d->>'action';resolution jsonb;outcome text='RESOLVED';caller text=(p->>'service')||':'||(p->>'instance_id');BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['verifier','ip','resource_id','action','auth','granted_command_id','remote_ids']<>'{}'::jsonb OR (act='files') IS DISTINCT FROM (d ? 'remote_ids') OR act='files' AND (d ? 'auth' OR d ? 'granted_command_id') OR NOT (d->>'resource_id' ~ '^(response|file|batch|video)_[0-9a-f]{32}$') OR act NOT IN ('get','delete','cancel','content','input_items','files') OR d ? 'granted_command_id' AND (act<>'delete' OR d ? 'auth') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 k=public.core_resource_key_v1(d->>'verifier',d->>'ip',public.core_resource_operation_v1(rkind));
 SELECT * INTO r FROM public.resources WHERE id=d->>'resource_id' AND kind=split_part(d->>'resource_id','_',1) AND project_id=k.project_id AND state<>'DELETED' AND binding_id IS NOT NULL FOR SHARE;
 IF r.id IS NULL OR cardinality(k.models)>0 AND (r.model IS NULL OR NOT r.model=ANY(k.models)) OR NOT (rkind='response' AND act IN ('get','delete','cancel','input_items') OR rkind='file' AND act IN ('get','delete','content') OR rkind='batch' AND act IN ('get','cancel','files') OR rkind='video' AND act IN ('get','delete','content')) THEN RAISE EXCEPTION 'resource_not_found' USING ERRCODE='P0404';END IF;
 PERFORM public.core_auth_v1(k.id,d->>'ip',public.core_resource_operation_v1(rkind),r.model);
 SELECT * INTO b FROM public.channel_plugin_bindings WHERE id=r.binding_id;
 SELECT * INTO v FROM public.plugin_versions WHERE id=b.plugin_version_id;
 SELECT * INTO c FROM public.channels WHERE id=b.channel_id;
 SELECT pk.manifest INTO manifest FROM public.plugin_packages pk WHERE pk.digest=v.package_digest;
 IF v.quarantined OR v.state NOT IN ('PUBLISHED','DISABLED') OR NOT c.enabled OR (manifest->'capabilities'->>'resources')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'no_route' USING ERRCODE='P0503';END IF;
 resolution=jsonb_build_object('resource_id',r.id,'kind',r.kind,'remote_id',r.remote_id,'model',r.model,'state',r.state,'channel_id',c.id,'package_digest',v.package_digest,'config',b.config,'origin',regexp_replace(c.base_url,'^(https://[^/]+).*$','\1'),'deadline_at',to_char(least((p->>'deadline_at')::timestamptz,clock_timestamp()+interval '30 seconds') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
 IF act='files' THEN
  RETURN jsonb_build_object('resolution',resolution,'state','RESOLVED','files',public.core_batch_files_v1(r,d->'remote_ids'),'channel_id',c.id,'key_id',k.id,'rpm',jsonb_build_object('key',coalesce(k.rpm,(SELECT rpm FROM public.projects WHERE id=k.project_id)),'channel',c.rpm));
 END IF;
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
CREATE OR REPLACE FUNCTION public.core_resource_resolution_v1(r public.resources,deadline timestamptz) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE b public.channel_plugin_bindings%ROWTYPE;v public.plugin_versions%ROWTYPE;c public.channels%ROWTYPE;BEGIN
 SELECT * INTO b FROM public.channel_plugin_bindings WHERE id=r.binding_id;
 SELECT * INTO v FROM public.plugin_versions WHERE id=b.plugin_version_id;
 SELECT * INTO c FROM public.channels WHERE id=b.channel_id;
 RETURN jsonb_build_object('batch',CASE WHEN r.kind='batch' THEN jsonb_build_object('endpoint',r.batch_endpoint,'items',(r.batch_manifest->>'items')::integer,'ids_sha256',r.batch_manifest->>'idsSha256') END,'resource_id',r.id,'kind',r.kind,'remote_id',r.remote_id,'model',r.model,'state',r.state,'channel_id',c.id,'package_digest',v.package_digest,'config',b.config,'origin',regexp_replace(c.base_url,'^(https://[^/]+).*$','\1'),'deadline_at',to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_claim_v1(p jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.resources%ROWTYPE;leases jsonb='[]';BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_claim_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['limit']<>'{}'::jsonb OR jsonb_typeof(d->'limit') IS DISTINCT FROM 'number' OR (d->>'limit')!~'^[0-9]{1,2}$' OR (d->>'limit')::integer NOT BETWEEN 1 AND 16 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR r IN WITH due AS (
  SELECT x.id FROM public.resources x JOIN public.attempts a ON a.id=x.attempt_id JOIN public.execution_permits ep ON ep.attempt_id=a.id
  WHERE (x.kind='response' OR x.kind='batch' AND x.batch_manifest IS NOT NULL) AND x.binding_id IS NOT NULL AND x.attempt_id IS NOT NULL AND a.state='PARTIAL' AND x.next_poll_at<=clock_timestamp() AND (x.poll_until IS NULL OR x.poll_until<clock_timestamp())
  ORDER BY x.next_poll_at,x.id LIMIT (d->>'limit')::integer FOR UPDATE OF x SKIP LOCKED
 ) UPDATE public.resources x SET lease_session_id=(p->>'session_id')::uuid,lease_epoch=x.lease_epoch+1,poll_until=clock_timestamp()+CASE WHEN x.kind='batch' THEN interval '900 seconds' ELSE interval '60 seconds' END FROM due WHERE x.id=due.id RETURNING x.*
 LOOP
  leases=leases||jsonb_build_array(jsonb_build_object('resource_id',r.id,'attempt_id',r.attempt_id,'permit_epoch',(SELECT permit_epoch::text FROM public.execution_permits WHERE attempt_id=r.attempt_id),'lease_epoch',r.lease_epoch::text,'resolution',public.core_resource_resolution_v1(r,r.poll_until)));
 END LOOP;
 RETURN jsonb_build_object('leases',leases);
END $$;
CREATE OR REPLACE FUNCTION public.core_resource_lease_v1(p jsonb,d jsonb,live boolean) RETURNS public.resources LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r public.resources%ROWTYPE;BEGIN
 IF NOT (d->>'resource_id' ~ '^(response|batch)_[0-9a-f]{32}$') OR jsonb_typeof(d->'lease_epoch') IS DISTINCT FROM 'string' OR d->>'lease_epoch' !~ '^[1-9][0-9]{0,37}$' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO r FROM public.resources WHERE id=d->>'resource_id' FOR UPDATE;
 IF r.id IS NULL OR r.lease_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR r.lease_epoch IS DISTINCT FROM (d->>'lease_epoch')::numeric OR live AND (r.poll_until IS NULL OR r.poll_until<=clock_timestamp()) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 RETURN r;
END $$;
CREATE OR REPLACE FUNCTION public.core_evidence_group_v1(g jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];permit public.execution_permits%ROWTYPE;h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;auth text[];sess public.internal_startup_sessions%ROWTYPE;revoked boolean;basis text;seen_final boolean;first_copy integer;BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') e(value) ORDER BY value->>'attempt_id',value->>'origin_identity',value->>'source',lpad(value->>'source_revision',38,'0'),value->>'event_id' LOOP
  IF jsonb_typeof(ev) IS DISTINCT FROM 'object' OR jsonb_typeof(ev->'signature') IS DISTINCT FROM 'string' OR ev->>'signature' !~ '^[a-f0-9]{128}$'
   OR jsonb_typeof(ev->'signature_payload_digest') IS DISTINCT FROM 'string' OR ev->>'signature_payload_digest' !~ '^[a-f0-9]{64}$'
   OR jsonb_typeof(ev->'producer_key_sha256') IS DISTINCT FROM 'string' OR ev->>'producer_key_sha256' !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
  PERFORM public.core_evidence_validate_v1(ev-ARRAY['signature','signature_payload_digest','producer_key_sha256']);
  events=events||(ev||jsonb_build_object('body_digest',public.core_digest_v1(ev-ARRAY['event_id','signature','signature_payload_digest','producer_key_sha256'])));
 END LOOP;
 n=cardinality(events);
 FOR k IN SELECT DISTINCT key FROM unnest(events) e,LATERAL (VALUES('core-event:'||(e->>'origin_identity')||':'||(e->>'event_id')),('core-revision:'||(e->>'attempt_id')||':'||(e->>'origin_identity')||':'||(e->>'source')||':'||(e->>'source_revision'))) l(key) ORDER BY key LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended(k,81442607)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM unnest(events) e WHERE NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=(e->>'origin_session_id')::uuid)) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
 PERFORM 1 FROM public.internal_service_instances WHERE (service,instance_id) IN(SELECT s.service,s.instance_id FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY service COLLATE "C",instance_id COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_service_certificates WHERE certificate_sha256 IN(SELECT s.certificate_sha256 FROM public.internal_startup_sessions s WHERE s.session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e)) ORDER BY certificate_sha256 COLLATE "C" FOR SHARE NOWAIT;
 PERFORM 1 FROM public.internal_startup_sessions WHERE session_id IN(SELECT (e->>'origin_session_id')::uuid FROM unnest(events) e) ORDER BY session_id FOR SHARE NOWAIT;
 attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
 request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.wallets WHERE id IN(SELECT wallet_id FROM public.billing_holds WHERE request_id=ANY(request_ids)) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_account_balances WHERE (account_id,currency) IN(SELECT account_id,currency FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY account_id,currency COLLATE "C" FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.gateway_requests WHERE id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.billing_holds WHERE request_id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.attempts WHERE id=ANY(attempt_ids) ORDER BY request_id,ordinal,id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.execution_permits WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR UPDATE NOWAIT;
 mode=array_fill('NEW'::text,ARRAY[n]);contested=array_fill(false,ARRAY[n]);auth=array_fill('NORMAL'::text,ARRAY[n]);
 FOR i IN 1..n LOOP
  ev=events[i];
  SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(ev->>'attempt_id')::uuid;
  SELECT * INTO sess FROM public.internal_startup_sessions WHERE session_id=(ev->>'origin_session_id')::uuid;
  IF permit.attempt_id IS NULL OR NOT (permit.state IN ('SEND_GRANTED','UNKNOWN','TERMINAL') OR (permit.state IN ('RESERVED','ABORTED') AND permit.granted_at IS NULL))
   OR sess.service||':'||sess.instance_id IS DISTINCT FROM ev->>'origin_identity' OR sess.incarnation_id IS DISTINCT FROM (ev->>'origin_incarnation')::uuid OR sess.public_key_sha256 IS DISTINCT FROM ev->>'producer_key_sha256'
   OR permit.permit_epoch IS DISTINCT FROM (ev->>'permit_epoch')::numeric
   OR ((permit.owner_service||':'||permit.gateway_instance_id IS DISTINCT FROM ev->>'origin_identity' OR permit.owner_session_id IS DISTINCT FROM sess.session_id OR permit.incarnation_id IS DISTINCT FROM sess.incarnation_id
     OR (permit.owner_key_sha256 IS NOT NULL AND permit.owner_key_sha256<>sess.public_key_sha256) OR (permit.certificate_sha256 IS NOT NULL AND permit.certificate_sha256<>sess.certificate_sha256))
    AND NOT (ev->>'source' IN ('async_partial','async_final','batch_completed','batch_cancelled','batch_failed') AND EXISTS(SELECT 1 FROM public.resources rs WHERE rs.attempt_id=permit.attempt_id AND rs.lease_session_id=sess.session_id AND rs.lease_epoch=(ev->>'source_revision')::numeric)))
   OR NOT EXISTS(SELECT 1 FROM public.upstream_holds WHERE attempt_id=permit.attempt_id AND estimated_quantities IS NOT NULL)
   OR EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.attempt_id<>permit.attempt_id) THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
  revoked=sess.security_revoked_at IS NOT NULL OR EXISTS(SELECT 1 FROM public.internal_service_instances x WHERE x.service=sess.service AND x.instance_id=sess.instance_id AND x.security_revoked_at IS NOT NULL) OR EXISTS(SELECT 1 FROM public.internal_service_certificates c WHERE c.certificate_sha256=sess.certificate_sha256 AND c.security_revoked_at IS NOT NULL);
  auth[i]=CASE WHEN revoked THEN 'SECURITY_REVOKED' WHEN permit.state IN ('RESERVED','ABORTED') THEN 'UNGRANTED' ELSE 'NORMAL' END;
 END LOOP;
 FOR i IN 1..n LOOP
  ev=events[i];
  IF ev->>'finality'='NOT_EXECUTED' THEN
   SELECT v.rates INTO rates FROM public.upstream_holds hh JOIN public.cost_versions v ON v.id=hh.cost_version_id WHERE hh.attempt_id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(p.rates,'{}') INTO prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=(ev->>'attempt_id')::uuid;
   SELECT coalesce(jsonb_object_agg(key,'0'),'{}') INTO usage FROM (SELECT key FROM jsonb_each(rates) UNION SELECT key FROM jsonb_each(prices)) dims;
   events[i]=ev||jsonb_build_object('normalized_usage',usage,'normalized_finals',(SELECT coalesce(jsonb_agg(key ORDER BY key),'[]') FROM jsonb_each(usage)));
  ELSE
   events[i]=ev||jsonb_build_object('normalized_usage',ev->'usage','normalized_finals',ev->'final_dimensions');
  END IF;
 END LOOP;
 FOR i IN 1..n LOOP
  ev=events[i];
  IF EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest') OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'=ev->>'body_digest') THEN mode[i]='DUPLICATE';CONTINUE;END IF;
  IF auth[i]<>'NORMAL' THEN mode[i]='QUARANTINED';CONTINUE;END IF;
  SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=(ev->>'attempt_id')::uuid AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
  IF EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim) OR (rev_prior.id IS NOT NULL AND rev_prior.body_digest<>ev->>'body_digest')
   OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND ((events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'<>ev->>'body_digest') OR (events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND events[j]->>'body_digest'<>ev->>'body_digest')))
  THEN mode[i]='CONFLICT';contested[i]=true;CONTINUE;END IF;
  IF rev_prior.id IS NOT NULL OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND mode[j] IN ('NEW','REPLAY')) THEN mode[i]='REPLAY';CONTINUE;END IF;
  a=(ev->>'attempt_id')::uuid;
  IF NOT claims?a::text THEN
   SELECT i2.final_dimensions,p.usage INTO head FROM public.usage_event_index i2 JOIN public.usage_payloads p ON p.id=i2.id AND p.created_at=i2.created_at WHERE i2.attempt_id=a AND i2.record_kind='CANONICAL' ORDER BY i2.snapshot_revision DESC LIMIT 1;
   claims=claims||jsonb_build_object(a::text,coalesce((SELECT jsonb_object_agg(f,jsonb_build_object('value',head.usage->>f,'owner',0)) FROM unnest(head.final_dimensions) f),'{}'));
  END IF;
  FOR d,q IN SELECT f,ev->'normalized_usage'->>f FROM jsonb_array_elements_text(ev->'normalized_finals') f LOOP
   claim=claims->a::text->d;
   IF claim IS NULL THEN claims=jsonb_set(claims,ARRAY[a::text,d],jsonb_build_object('value',q,'owner',i));
   ELSIF (claim->>'value')::numeric<>q::numeric OR coalesce((claim->>'contested')::boolean,false) THEN
    contested[i]=true;owner=(claim->>'owner')::integer;
    IF owner>0 THEN contested[owner]=true;claims=jsonb_set(claims,ARRAY[a::text,d,'contested'],'true');END IF;
   END IF;
  END LOOP;
 END LOOP;
 FOR i IN 1..n LOOP
  CONTINUE WHEN mode[i]<>'REPLAY';
  SELECT min(j2) INTO first_copy FROM generate_series(1,i-1) j2 WHERE events[j2]->>'origin_identity'=events[i]->>'origin_identity' AND events[j2]->>'attempt_id'=events[i]->>'attempt_id' AND events[j2]->>'source'=events[i]->>'source' AND events[j2]->>'source_revision'=events[i]->>'source_revision' AND mode[j2] IN ('NEW','REPLAY');
  IF first_copy IS NOT NULL AND contested[first_copy] THEN contested[i]=true;mode[i]='NEW';END IF;
 END LOOP;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=(contested[i] OR mode[i]='DUPLICATE');
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    IF prior.signature_payload_digest<>ev->>'signature_payload_digest' OR prior.producer_key_sha256<>ev->>'producer_key_sha256' THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT p.basis,coalesce(p.rates,'{}') INTO basis,prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=a;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    IF rev_prior.id IS NULL OR rev_prior.body_digest<>ev->>'body_digest' THEN RAISE EXCEPTION 'evidence_replay_unanchored' USING ERRCODE='P0503';END IF;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,rev_prior.canonical_evidence_id,'{}','VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);
   IF mode[i]='QUARANTINED' THEN
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'QUARANTINED',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,'{}','QUARANTINED',auth[i],ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','QUARANTINED','duplicate',false));CONTINUE;
   END IF;
   IF ev->>'finality'='PARTIAL' THEN
    risk=ARRAY(SELECT d2 FROM jsonb_object_keys(rates) d2 WHERE usage?d2 AND NOT d2=ANY(finals) AND NOT d2=ANY(merged_finals) AND (usage->>d2)::numeric>(h.estimated_quantities->>d2)::numeric ORDER BY d2);
   END IF;
   IF contested[i] THEN
    outcome='CONFLICT';merged=merged||usage;merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
    existing=coalesce(cur.id,(SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND (r.event_id=(ev->>'event_id')::uuid OR (r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric)) ORDER BY r.created_at LIMIT 1),
     (SELECT r.raw_evidence_id FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='CONFLICT' ORDER BY r.created_at LIMIT 1),
     (SELECT i3.id FROM public.usage_event_index i3 WHERE i3.attempt_id=a AND i3.record_kind='RAW' AND i3.id<>(raw->>'id')::uuid ORDER BY i3.snapshot_revision DESC LIMIT 1),(raw->>'id')::uuid);
    BEGIN x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,merged,merged_finals,'{}');
    EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
     WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
    snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));
    disputed=(snap->>'id')::uuid;conflict=gen_random_uuid();
    INSERT INTO public.usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint,incoming_evidence_id,disputed_evidence_id,incoming_receipt_id)
    VALUES(conflict,a,existing,ev->>'source',finality,usage,public.core_digest_v1(jsonb_build_object('attempt_id',a,'existing',existing,'incoming',raw->>'id','usage',usage,'final_dimensions',to_jsonb(finals))),(raw->>'id')::uuid,disputed,receipt);
    UPDATE public.attempts SET metering_conflict=true WHERE id=a;
   ELSIF ev->>'finality'='ESTIMATED' THEN outcome='ESTIMATED';risk='{}';
   ELSE
    stale=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.outcome='ACCEPTED' AND r.source_revision>(ev->>'source_revision')::numeric);
    IF stale THEN outcome='STALE';
    ELSE
     merged=merged||(usage-merged_finals);merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
     BEGIN x=public.core_supplier_exposure_v1(rates,h.estimated_quantities,merged,merged_finals,'{}');
     EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
      WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      seen_final=finality='FINAL' OR EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='ACCEPTED' AND r.finality IN ('FINAL','NOT_EXECUTED'));
      snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);
      canonical=(snap->>'id')::uuid;
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
     END IF;
    END IF;
   END IF;
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault,'VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
   touched=jsonb_set(touched,ARRAY[a::text],jsonb_build_object('canonical',CASE WHEN outcome='ACCEPTED' THEN to_jsonb(canonical) ELSE coalesce(touched->a::text->'canonical','null'::jsonb) END,'revision',CASE WHEN outcome='ACCEPTED' THEN snap->>'revision' ELSE coalesce(touched->a::text->>'revision',raw->>'revision') END,'money',coalesce((touched->a::text->>'money')::boolean,false) OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0));
   receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome',outcome,'duplicate',false));
  END LOOP;
 END LOOP;
 FOR row IN SELECT key::uuid attempt,value FROM jsonb_each(touched) WHERE (value->>'money')::boolean ORDER BY key LOOP
  PERFORM public.core_supplier_settle_v1(row.attempt,(row.value->>'canonical')::uuid,CASE WHEN row.value->>'canonical' IS NULL THEN 'risk:' ELSE 'cost:' END||row.attempt||':'||(row.value->>'revision'));
 END LOOP;
 request_ids=ARRAY(SELECT q.id FROM public.gateway_requests q WHERE q.id=ANY(request_ids) AND q.billing_outcome IN ('SUCCESS','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM public.billing_charges c WHERE c.request_id=q.id) ORDER BY q.id);
 IF cardinality(request_ids)>0 THEN
  billing=public.settle_requests(request_ids);
  IF EXISTS(SELECT 1 FROM jsonb_each(billing) b WHERE b.value->>'state'='ERROR') THEN RAISE EXCEPTION 'customer_settlement_failed' USING ERRCODE='P0500';END IF;
 END IF;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt,balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt,balanced_upstream_journal,balanced_upstream_entries DEFERRED;
 RETURN jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing);
END $$;
CREATE OR REPLACE FUNCTION public.core_execution_v1(p jsonb,e jsonb,d jsonb,computed jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
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
 PERFORM public.core_caller_v1(p);
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
   IF jsonb_typeof(projection) IS DISTINCT FROM 'object' OR projection-ARRAY['version','proposed_request_id','proposed_attempt_id','origin','method','operation','request_target_sha256','final_headers_digest','final_body_sha256','candidate_ref','auth','credential_ciphertext_sha256','billable_manifest_sha256','deadline_at']<>'{}'::jsonb OR (SELECT count(*) FROM jsonb_object_keys(projection))<>14 OR jsonb_typeof(manifest) IS DISTINCT FROM 'object' OR manifest-ARRAY['version','operation','model','upstream_model','stream','input_bytes','ceilings','quantities','input_characters','images','resource_ids']<>'{}'::jsonb OR jsonb_typeof(manifest->'quantities') IS DISTINCT FROM 'object' OR jsonb_typeof(manifest->'resource_ids') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0422';END IF;
   IF c IS NULL OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ctx.snapshot->'candidates') WHERE value=c) OR projection->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'operation' IS DISTINCT FROM ctx.operation OR manifest->>'model' IS DISTINCT FROM ctx.model OR manifest->>'upstream_model' IS DISTINCT FROM c->>'upstream_model' OR projection->>'origin' IS DISTINCT FROM c->>'origin' OR projection->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256' OR projection->>'billable_manifest_sha256' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.billable-manifest','1',manifest)) OR d->>'dispatched_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.dispatch-projection','1',projection)) OR projection->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR (projection->'auth'->>'type') IS DISTINCT FROM c->'config'->>'auth' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   IF d->>'prep_binding_digest' IS DISTINCT FROM public.core_digest_v1(jsonb_build_array('stg.internal.prep-binding','1',jsonb_build_object('session',jsonb_build_object('id',d->>'prep_session_id','tenant',ctx.project_id,'attempt',projection->>'proposed_attempt_id','incarnation',p->>'incarnation_id','plugin',ref->>'package_digest','config',public.core_digest_v1(c->'config')),'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id'),'request_id',projection->>'proposed_request_id','operation',projection->>'operation','binding_id',ref->>'binding_id','config_revision',ref->>'config_revision','config_digest',ref->>'config_digest'))) THEN RAISE EXCEPTION 'prep_binding_invalid' USING ERRCODE='P0422';END IF;
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
   plan=jsonb_build_object('version',1,'owner',jsonb_build_object('service','gateway','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'attempt_id',aid,'permit_epoch','1','dispatched_digest',d->>'dispatched_digest','dispatch_projection',projection);
   response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'ordinal',ordinal,'state','RESERVED','permit_epoch','1','deadline_at',p->>'deadline_at','customer_atoms',quote->>'amount_atoms','customer_currency',quote->>'currency','supplier_atoms',amount::text,'supplier_currency',c->>'cost_currency','authorization_plan',plan,'authorization_plan_digest',public.core_digest_v1(jsonb_build_array('stg.internal.host-authorization-plan','1',plan)));
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
REVOKE ALL ON FUNCTION public.core_batch_files_v1(public.resources,jsonb) FROM PUBLIC;
