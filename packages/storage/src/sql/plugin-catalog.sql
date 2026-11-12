CREATE FUNCTION public.guard_plugin_version() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW.id<>OLD.id OR NEW.name<>OLD.name OR NEW.version<>OLD.version OR NEW.package_digest<>OLD.package_digest OR NEW.abi_version<>OLD.abi_version OR NEW.config_schema_digest<>OLD.config_schema_digest OR NEW.created_at<>OLD.created_at THEN RAISE EXCEPTION 'plugin identity is immutable' USING ERRCODE='P0409';END IF;
 IF current_setting('gateway.catalog_mutation',true) IS DISTINCT FROM 'on' THEN RAISE EXCEPTION 'controlled plugin transition required' USING ERRCODE='P0403';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_plugin_identity BEFORE UPDATE OR DELETE ON public.plugin_versions FOR EACH ROW EXECUTE FUNCTION public.guard_plugin_version();
CREATE TRIGGER immutable_plugin_publications BEFORE UPDATE OR DELETE ON public.plugin_publication_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_catalog_receipts BEFORE UPDATE OR DELETE ON public.plugin_catalog_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE FUNCTION public.validate_catalog_audit() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
BEGIN
 IF NEW.action LIKE 'plugin_%' AND (NEW.actor IS NULL OR length(NEW.actor) NOT BETWEEN 1 AND 200 OR NEW.details IS NULL OR jsonb_typeof(NEW.details)<>'object' OR octet_length(NEW.details::text)>8192 OR NOT(NEW.details ?& ARRAY['command_id','old_state','new_state','reason']) OR coalesce(length(NEW.details->>'reason'),0) NOT BETWEEN 1 AND 500) THEN RAISE EXCEPTION 'catalog audit context required' USING ERRCODE='P0422';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER catalog_audit_context BEFORE INSERT ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.validate_catalog_audit();
CREATE FUNCTION public.verify_platform_credential_v1(p_verifier text) RETURNS TABLE(actor text,permissions text[],credential_id uuid,administrator_id uuid) LANGUAGE sql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
 SELECT 'technical-admin:'||a.id::text,a.permissions,c.id,a.id FROM public.administrator_credentials c JOIN public.technical_administrators a ON a.id=c.administrator_id WHERE c.verifier=p_verifier AND c.enabled AND a.enabled AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())
$$;
CREATE FUNCTION public.plugin_catalog_command_v1(p_kind text,p_actor text,p_command uuid,p_body jsonb,p_owner uuid DEFAULT NULL,p_trust_expires_at timestamptz DEFAULT NULL,p_credential uuid DEFAULT NULL,p_administrator uuid DEFAULT NULL,p_verifier text DEFAULT NULL,p_bootstrap boolean DEFAULT false) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE body_digest_value text; cmd public.execution_commands%ROWTYPE; v public.plugin_versions%ROWTYPE; pkg public.plugin_packages%ROWTYPE; run public.plugin_validation_runs%ROWTYPE; channel public.channels%ROWTYPE; publication public.plugin_publication_receipts%ROWTYPE; receipt jsonb; target uuid; old_state text; action text; detail jsonb; next_config_revision numeric; old_binding_digest text; raw bytea; credential public.administrator_credentials%ROWTYPE; administrator public.technical_administrators%ROWTYPE; permission text; authorization_expires_at timestamptz;
BEGIN
 IF p_kind IS NULL OR p_actor IS NULL OR p_command IS NULL OR p_kind NOT IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1') OR length(p_actor) NOT BETWEEN 1 AND 200 OR p_actor !~ '^(technical-admin:|bootstrap:)' OR p_body IS NULL OR jsonb_typeof(p_body)<>'object' OR octet_length(p_body::text)>65000000 THEN RAISE EXCEPTION 'invalid catalog command' USING ERRCODE='P0422';END IF;
 body_digest_value=encode(sha256(convert_to(p_body::text,'UTF8')),'hex');
 PERFORM pg_advisory_xact_lock(hashtextextended(p_kind||':'||p_actor||':'||p_command::text,0));
 SELECT * INTO cmd FROM execution_commands WHERE kind=p_kind AND caller_identity=p_actor AND command_id=p_command FOR UPDATE;
 IF FOUND THEN
  IF cmd.body_digest<>body_digest_value THEN RAISE EXCEPTION 'catalog command body conflict' USING ERRCODE='P0409';END IF;
  SELECT r.receipt INTO STRICT receipt FROM plugin_catalog_receipts r WHERE r.command_id=cmd.id;
  RETURN receipt;
 END IF;
 IF p_kind NOT IN ('plugin_finish_v1','plugin_resources_v1') THEN
  permission=CASE p_kind WHEN 'plugin_register_v1' THEN 'plugins:install-bundled' WHEN 'plugin_validate_v1' THEN 'plugins:validate' WHEN 'plugin_publish_v1' THEN 'plugins:publish' WHEN 'plugin_bind_v1' THEN 'plugins:bind' WHEN 'plugin_recover_v1' THEN 'plugins:recover' END;
  IF p_bootstrap IS TRUE THEN
   IF p_actor IS DISTINCT FROM 'bootstrap:ADMIN_KEY' OR p_credential IS NOT NULL OR p_administrator IS NOT NULL OR p_verifier IS NOT NULL THEN RAISE EXCEPTION 'invalid bootstrap authority' USING ERRCODE='P0403';END IF;
  ELSE
   SELECT * INTO credential FROM administrator_credentials WHERE id=p_credential FOR SHARE;
   IF NOT FOUND THEN RAISE EXCEPTION 'platform credential denied' USING ERRCODE='P0403';END IF;
   SELECT * INTO administrator FROM technical_administrators WHERE id=p_administrator FOR SHARE;
   IF NOT FOUND OR credential.administrator_id IS DISTINCT FROM administrator.id OR p_actor IS DISTINCT FROM 'technical-admin:'||administrator.id::text OR credential.verifier IS DISTINCT FROM p_verifier OR NOT credential.enabled OR NOT administrator.enabled OR (permission=ANY(administrator.permissions)) IS NOT TRUE OR (credential.expires_at IS NOT NULL AND credential.expires_at<=clock_timestamp()) THEN RAISE EXCEPTION 'platform credential denied' USING ERRCODE='P0403';END IF;
   authorization_expires_at=credential.expires_at;
  END IF;
 END IF;
 IF jsonb_typeof(p_body->'reason') IS DISTINCT FROM 'string' OR length(p_body->>'reason') NOT BETWEEN 1 AND 500 THEN RAISE EXCEPTION 'catalog reason required' USING ERRCODE='P0422';END IF;
 IF p_kind IN ('plugin_validate_v1','plugin_publish_v1','plugin_bind_v1') AND (jsonb_typeof(p_body->'expected_revision') IS DISTINCT FROM 'string' OR p_body->>'expected_revision' !~ '^(0|[1-9][0-9]{0,37})$') THEN RAISE EXCEPTION 'catalog revision required' USING ERRCODE='P0422';END IF;
 IF p_kind IN ('plugin_validate_v1','plugin_finish_v1','plugin_resources_v1','plugin_recover_v1') AND p_owner IS NULL THEN RAISE EXCEPTION 'lease owner required' USING ERRCODE='P0409';END IF;
 IF p_kind IN ('plugin_finish_v1','plugin_resources_v1','plugin_recover_v1') AND (jsonb_typeof(p_body->'lease_epoch') IS DISTINCT FROM 'string' OR p_body->>'lease_epoch' !~ '^[1-9][0-9]{0,37}$') THEN RAISE EXCEPTION 'lease epoch required' USING ERRCODE='P0409';END IF;
 IF p_kind<>'plugin_register_v1' AND (jsonb_typeof(p_body->'version_id') IS DISTINCT FROM 'string' OR p_body->>'version_id' !~ '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$') THEN RAISE EXCEPTION 'version identity required' USING ERRCODE='P0422';END IF;
 IF p_kind IN ('plugin_finish_v1','plugin_resources_v1','plugin_recover_v1') AND (jsonb_typeof(p_body->'run_id') IS DISTINCT FROM 'string' OR p_body->>'run_id' !~ '^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$') THEN RAISE EXCEPTION 'run identity required' USING ERRCODE='P0422';END IF;
 INSERT INTO execution_commands(id,command_id,kind,caller_identity,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state) VALUES(gen_random_uuid(),p_command,p_kind,p_actor,'00000000-0000-4000-8000-000000000001',body_digest_value,clock_timestamp()+interval '90 seconds',p_command::text,1,'COMMITTED') RETURNING * INTO cmd;
 PERFORM set_config('gateway.catalog_mutation','on',true);
 IF p_kind='plugin_register_v1' THEN
  PERFORM pg_advisory_xact_lock(hashtextextended((p_body->'manifest'->>'id')||':'||(p_body->'manifest'->>'version'),1));
  IF p_trust_expires_at IS NULL OR p_trust_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'release trust expired while awaiting lock' USING ERRCODE='P0409';END IF;
  IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
  raw=decode(p_body->>'package_base64','base64');
  IF p_body->>'config_schema_digest' IS DISTINCT FROM p_body->'release_descriptor'->>'config_schema_digest' OR encode(sha256(raw),'hex') IS DISTINCT FROM p_body->'release_descriptor'->>'envelope_digest' OR p_body->'release_descriptor'->>'package_digest' IS DISTINCT FROM p_body->>'package_digest' OR p_body->'release_descriptor'->>'name' IS DISTINCT FROM p_body->'manifest'->>'id' OR p_body->'release_descriptor'->>'version' IS DISTINCT FROM p_body->'manifest'->>'version' OR (p_body->'release_descriptor'->>'abi_version')::int IS DISTINCT FROM (p_body->'manifest'->>'abi_version')::int THEN RAISE EXCEPTION 'release content mismatch' USING ERRCODE='P0422';END IF;
  INSERT INTO plugin_packages(digest,package_bytes,manifest,signature,signing_key_id,uploaded_by,release_descriptor,release_signature,release_key_id) VALUES(p_body->>'package_digest',raw,p_body->'manifest',p_body->>'signature',p_body->>'signing_key_id',p_actor,p_body->'release_descriptor',p_body->>'release_signature',p_body->>'release_key_id') ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT pkg FROM plugin_packages WHERE digest=p_body->>'package_digest';
  IF pkg.package_bytes IS DISTINCT FROM raw OR pkg.manifest IS DISTINCT FROM p_body->'manifest' OR pkg.signature IS DISTINCT FROM p_body->>'signature' OR pkg.signing_key_id IS DISTINCT FROM p_body->>'signing_key_id' OR pkg.release_descriptor IS DISTINCT FROM p_body->'release_descriptor' OR pkg.release_signature IS DISTINCT FROM p_body->>'release_signature' OR pkg.release_key_id IS DISTINCT FROM p_body->>'release_key_id' THEN RAISE EXCEPTION 'immutable package conflict' USING ERRCODE='P0409';END IF;
  SELECT * INTO v FROM plugin_versions WHERE name=p_body->'manifest'->>'id' AND version=p_body->'manifest'->>'version' FOR UPDATE;
  IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
  IF FOUND THEN
   IF v.package_digest<>pkg.digest OR v.abi_version IS DISTINCT FROM (p_body->'manifest'->>'abi_version')::int OR v.config_schema_digest IS DISTINCT FROM p_body->>'config_schema_digest' THEN RAISE EXCEPTION 'immutable plugin version conflict' USING ERRCODE='P0409';END IF;
  ELSE
   INSERT INTO plugin_versions(id,name,version,package_digest,abi_version,config_schema_digest) VALUES(gen_random_uuid(),p_body->'manifest'->>'id',p_body->'manifest'->>'version',pkg.digest,(p_body->'manifest'->>'abi_version')::int,p_body->>'config_schema_digest') RETURNING * INTO v;
   action='plugin_uploaded'; old_state='ABSENT';
  END IF;
  receipt=jsonb_build_object('version_id',v.id,'package_digest',v.package_digest,'state',v.state,'revision',v.revision::text);
 ELSE
  SELECT * INTO STRICT v FROM plugin_versions WHERE id=(p_body->>'version_id')::uuid FOR UPDATE;
  IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
  target=v.id;old_state=v.state;
  SELECT * INTO STRICT pkg FROM plugin_packages WHERE digest=v.package_digest;
  IF p_kind IN ('plugin_validate_v1','plugin_publish_v1','plugin_bind_v1') AND (p_trust_expires_at IS NULL OR p_trust_expires_at<=clock_timestamp()) THEN RAISE EXCEPTION 'release trust expired while awaiting lock' USING ERRCODE='P0409';END IF;
  IF p_kind IN ('plugin_validate_v1','plugin_publish_v1') AND (p_body->>'package_digest' IS DISTINCT FROM v.package_digest OR p_body->>'policy_digest' IS DISTINCT FROM pkg.release_descriptor->>'validation_policy_digest' OR p_body->>'runtime_digest' IS DISTINCT FROM pkg.release_descriptor->>'runtime_digest' OR p_body->>'fixture_digest' IS DISTINCT FROM pkg.release_descriptor->>'fixture_digest' OR p_body->>'config_schema_digest' IS DISTINCT FROM v.config_schema_digest) THEN RAISE EXCEPTION 'fixed validation inputs mismatch' USING ERRCODE='P0409';END IF;
  IF p_kind='plugin_validate_v1' THEN
   IF v.state NOT IN ('UPLOADED','REJECTED') OR v.quarantined OR v.revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric OR p_owner IS NULL THEN RAISE EXCEPTION 'plugin validation state conflict' USING ERRCODE='P0409';END IF;
   INSERT INTO plugin_validation_runs(id,version_id,command_id,package_digest,fixture_digest,policy_digest,runtime_digest,actor,state,lease_owner,lease_epoch,lease_expires_at) VALUES(gen_random_uuid(),v.id,cmd.id,v.package_digest,p_body->>'fixture_digest',p_body->>'policy_digest',p_body->>'runtime_digest',p_actor,'RUNNING',p_owner,1,clock_timestamp()+interval '90 seconds') RETURNING * INTO run;
   UPDATE plugin_versions SET state='VALIDATING',revision=revision+1,validation_result=NULL WHERE id=v.id RETURNING * INTO v;
   receipt=jsonb_build_object('version_id',v.id,'run_id',run.id,'state',run.state,'lease_owner',run.lease_owner,'lease_epoch',run.lease_epoch::text,'revision',v.revision::text);action='plugin_validation_started';
  ELSIF p_kind='plugin_finish_v1' THEN
   SELECT * INTO STRICT run FROM plugin_validation_runs WHERE id=(p_body->>'run_id')::uuid AND version_id=v.id FOR UPDATE;
   IF run.state NOT IN ('RUNNING','CLEANUP_PENDING') OR run.lease_owner IS DISTINCT FROM p_owner OR run.lease_epoch IS DISTINCT FROM (p_body->>'lease_epoch')::numeric OR run.lease_expires_at<=clock_timestamp() OR run.package_digest<>v.package_digest OR v.state NOT IN ('VALIDATING','REJECTED') THEN RAISE EXCEPTION 'stale validation lease' USING ERRCODE='P0409';END IF;
   detail=p_body->'result';
   IF jsonb_typeof(detail) IS DISTINCT FROM 'object' OR octet_length(detail::text)>4096 OR NOT(detail ?& ARRAY['cases','steps','cleanup_confirmed','error_code']) OR jsonb_typeof(detail->'cases') IS DISTINCT FROM 'number' OR detail->>'cases' !~ '^(0|[1-9][0-9]{0,3})$' OR jsonb_typeof(detail->'steps') IS DISTINCT FROM 'number' OR detail->>'steps' !~ '^(0|[1-9][0-9]{0,6})$' OR jsonb_typeof(detail->'cleanup_confirmed') IS DISTINCT FROM 'boolean' OR (detail->'error_code'<>'null'::jsonb AND jsonb_typeof(detail->'error_code') IS DISTINCT FROM 'string') OR (detail->>'cases')::int NOT BETWEEN 0 AND 1000 OR (detail->>'steps')::int NOT BETWEEN 0 AND 1000000 OR (detail->>'cleanup_confirmed')::boolean IS NULL OR detail-ARRAY['cases','steps','cleanup_confirmed','error_code']<>'{}'::jsonb OR (detail->>'error_code' IS NULL AND ((detail->>'cases')::int<1 OR (detail->>'steps')::int<(detail->>'cases')::int)) OR (detail->>'error_code' IS NOT NULL AND detail->>'error_code' !~ '^[A-Z][A-Z0-9_]{0,79}$') THEN RAISE EXCEPTION 'invalid validation result' USING ERRCODE='P0422';END IF;
   IF (detail->>'cleanup_confirmed')::boolean IS NOT TRUE THEN
    UPDATE plugin_validation_runs SET state='CLEANUP_PENDING',result=detail,error_code='CLEANUP_PENDING' WHERE id=run.id;
    UPDATE plugin_versions SET state='REJECTED',revision=revision+1 WHERE id=v.id RETURNING * INTO v;
   ELSE
    IF run.state='CLEANUP_PENDING' AND detail->>'error_code' IS NULL THEN RAISE EXCEPTION 'unknown validation cannot become successful' USING ERRCODE='P0409';END IF;
    UPDATE plugin_validation_runs SET state=CASE WHEN detail->>'error_code' IS NULL THEN 'SUCCEEDED' ELSE 'FAILED' END,result=detail,result_digest=encode(sha256(convert_to(detail::text,'UTF8')),'hex'),error_code=detail->>'error_code',finished_at=clock_timestamp() WHERE id=run.id;
    UPDATE plugin_versions SET state=CASE WHEN detail->>'error_code' IS NULL THEN 'VALIDATED' ELSE 'REJECTED' END,revision=revision+1,validation_result=detail WHERE id=v.id RETURNING * INTO v;
   END IF;
   receipt=jsonb_build_object('version_id',v.id,'run_id',run.id,'state',v.state,'revision',v.revision::text,'result',detail);action='plugin_validation_finished';
  ELSIF p_kind='plugin_resources_v1' THEN
   SELECT * INTO STRICT run FROM plugin_validation_runs WHERE id=(p_body->>'run_id')::uuid AND version_id=v.id FOR UPDATE;
   IF run.state<>'RUNNING' OR run.lease_owner IS DISTINCT FROM p_owner OR run.lease_epoch IS DISTINCT FROM (p_body->>'lease_epoch')::numeric OR run.lease_expires_at<=clock_timestamp() OR run.resource_pid IS NOT NULL OR jsonb_typeof(p_body->'pid') IS DISTINCT FROM 'number' OR p_body->>'pid' !~ '^[1-9][0-9]{0,9}$' OR (p_body->>'pid')::numeric NOT BETWEEN 2 AND 2147483647 OR jsonb_typeof(p_body->'connection') IS DISTINCT FROM 'string' OR length(p_body->>'connection') NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'resource ownership conflict' USING ERRCODE='P0409';END IF;
   UPDATE plugin_validation_runs SET resource_pid=(p_body->>'pid')::int,resource_connection=p_body->>'connection' WHERE id=run.id;
   receipt=jsonb_build_object('version_id',v.id,'run_id',run.id,'registered',true);action='plugin_validation_resources';
  ELSIF p_kind='plugin_recover_v1' THEN
   SELECT * INTO STRICT run FROM plugin_validation_runs WHERE id=(p_body->>'run_id')::uuid AND version_id=v.id FOR UPDATE;
   IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
   IF run.state NOT IN ('RUNNING','CLEANUP_PENDING') OR run.lease_expires_at>clock_timestamp() OR run.lease_epoch IS DISTINCT FROM (p_body->>'lease_epoch')::numeric OR p_owner IS NULL THEN RAISE EXCEPTION 'recovery lease conflict' USING ERRCODE='P0409';END IF;
   UPDATE plugin_validation_runs SET state='CLEANUP_PENDING',lease_owner=p_owner,lease_epoch=lease_epoch+1,lease_expires_at=clock_timestamp()+interval '90 seconds',error_code='OWNER_LOST' WHERE id=run.id RETURNING * INTO run;
   UPDATE plugin_versions SET state='REJECTED',revision=revision+1 WHERE id=v.id RETURNING * INTO v;
   receipt=jsonb_build_object('version_id',v.id,'run_id',run.id,'state',run.state,'lease_owner',run.lease_owner,'lease_epoch',run.lease_epoch::text,'revision',v.revision::text);action='plugin_validation_recovery';
  ELSIF p_kind='plugin_publish_v1' THEN
   SELECT * INTO publication FROM plugin_publication_receipts WHERE version_id=v.id;
   IF FOUND AND v.state='PUBLISHED' AND NOT v.quarantined THEN
    IF v.revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric THEN RAISE EXCEPTION 'stale publication revision' USING ERRCODE='P0409';END IF;
    receipt=publication.receipt;
   ELSE
    IF v.state<>'VALIDATED' OR v.quarantined OR v.revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric THEN RAISE EXCEPTION 'plugin publication state conflict' USING ERRCODE='P0409';END IF;
    SELECT * INTO STRICT run FROM plugin_validation_runs WHERE version_id=v.id ORDER BY started_at DESC,id DESC LIMIT 1 FOR UPDATE;
    IF run.state<>'SUCCEEDED' OR run.finished_at IS NULL OR run.package_digest<>v.package_digest OR run.policy_digest<>p_body->>'policy_digest' OR run.runtime_digest<>p_body->>'runtime_digest' OR run.fixture_digest<>p_body->>'fixture_digest' OR (run.result->>'cleanup_confirmed')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'successful reaped validation required' USING ERRCODE='P0409';END IF;
    IF p_trust_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'release trust expired while awaiting validation lock' USING ERRCODE='P0409';END IF;
    IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
    UPDATE plugin_versions SET state='PUBLISHED',revision=revision+1 WHERE id=v.id RETURNING * INTO v;
    receipt=jsonb_build_object('publication_id',gen_random_uuid(),'version_id',v.id,'package_digest',v.package_digest,'validation_run_id',run.id,'result_digest',run.result_digest,'policy_digest',run.policy_digest,'config_schema_digest',v.config_schema_digest,'abi_version',v.abi_version,'state',v.state,'revision',v.revision::text,'actor',p_actor,'command_id',p_command);
    INSERT INTO plugin_publication_receipts(id,version_id,command_id,validation_run_id,package_digest,result_digest,policy_digest,actor,receipt) VALUES((receipt->>'publication_id')::uuid,v.id,cmd.id,run.id,v.package_digest,run.result_digest,run.policy_digest,p_actor,receipt);action='plugin_published';
   END IF;
  ELSIF p_kind='plugin_bind_v1' THEN
   SELECT * INTO STRICT channel FROM channels WHERE id=(p_body->>'channel_id')::uuid FOR UPDATE;
   PERFORM 1 FROM upstream_accounts WHERE id=channel.account_id AND brand_id=channel.brand_id FOR UPDATE;
   IF NOT FOUND OR channel.account_id IS DISTINCT FROM (p_body->>'account_id')::uuid OR channel.brand_id IS DISTINCT FROM (p_body->>'brand_id')::uuid OR p_body->>'secret_reference' IS DISTINCT FROM 'channel:'||channel.id::text THEN RAISE EXCEPTION 'channel config reference ownership mismatch' USING ERRCODE='P0403';END IF;
   IF channel.config_revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric OR v.state<>'PUBLISHED' OR v.quarantined OR NOT EXISTS(SELECT 1 FROM plugin_publication_receipts WHERE version_id=v.id AND package_digest=v.package_digest) THEN RAISE EXCEPTION 'channel binding revision or publication conflict' USING ERRCODE='P0409';END IF;
   IF jsonb_typeof(p_body->'config') IS DISTINCT FROM 'object' OR octet_length((p_body->'config')::text)>65536 THEN RAISE EXCEPTION 'invalid binding config' USING ERRCODE='P0422';END IF;
   SELECT config_digest INTO old_binding_digest FROM channel_plugin_bindings WHERE id=channel.binding_id;
   IF p_trust_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'release trust expired while awaiting channel lock' USING ERRCODE='P0409';END IF;
   IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
   next_config_revision=channel.config_revision+1;target=gen_random_uuid();
   INSERT INTO channel_plugin_bindings(id,channel_id,plugin_version_id,config_revision,config,config_digest,actor,account_id,brand_id,secret_reference) VALUES(target,channel.id,v.id,next_config_revision,p_body->'config',encode(sha256(convert_to((p_body->'config')::text,'UTF8')),'hex'),p_actor,channel.account_id,channel.brand_id,p_body->>'secret_reference');
   UPDATE channels SET binding_id=target,config_revision=next_config_revision WHERE id=channel.id;
   receipt=jsonb_build_object('binding_id',target,'channel_id',channel.id,'version_id',v.id,'package_digest',v.package_digest,'config_revision',next_config_revision::text,'config_digest',encode(sha256(convert_to((p_body->'config')::text,'UTF8')),'hex'),'previous_binding_id',channel.binding_id,'previous_config_digest',old_binding_digest,'previous_config_revision',channel.config_revision::text,'account_id',channel.account_id,'brand_id',channel.brand_id,'actor',p_actor,'command_id',p_command);action='plugin_bound';
  END IF;
 END IF;
 IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
 IF action IS NOT NULL THEN
  INSERT INTO audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),action,coalesce(target,v.id)::text,p_actor,jsonb_build_object('command_id',p_command,'old_state',old_state,'new_state',v.state,'reason',p_body->>'reason','package_digest',v.package_digest,'receipt',receipt));
 END IF;
 INSERT INTO plugin_catalog_receipts(command_id,receipt) VALUES(cmd.id,receipt);
 IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
 RETURN receipt;
END $$;
REVOKE ALL ON FUNCTION public.guard_plugin_version(),public.validate_catalog_audit(),public.verify_platform_credential_v1(text),public.plugin_catalog_command_v1(text,text,uuid,jsonb,uuid,timestamptz,uuid,uuid,text,boolean) FROM PUBLIC;
CREATE FUNCTION public.plugin_catalog_receipt_v1(p_kind text,p_actor text,p_command uuid,p_body jsonb DEFAULT NULL) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE command public.execution_commands%ROWTYPE; receipt jsonb;
BEGIN
 IF p_kind NOT IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1') OR p_actor !~ '^(technical-admin:|bootstrap:)' OR length(p_actor) NOT BETWEEN 1 AND 200 THEN RAISE EXCEPTION 'invalid catalog receipt scope' USING ERRCODE='P0403';END IF;
 SELECT * INTO command FROM execution_commands WHERE kind=p_kind AND caller_identity=p_actor AND command_id=p_command AND state='COMMITTED' AND request_id IS NULL AND attempt_id IS NULL;
 IF NOT FOUND THEN RETURN NULL;END IF;
 IF p_body IS NOT NULL AND command.body_digest<>encode(sha256(convert_to(p_body::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'catalog command body conflict' USING ERRCODE='P0409';END IF;
 SELECT r.receipt INTO STRICT receipt FROM plugin_catalog_receipts r WHERE r.command_id=command.id;
 RETURN receipt;
END $$;
REVOKE ALL ON FUNCTION public.plugin_catalog_receipt_v1(text,text,uuid,jsonb) FROM PUBLIC;
