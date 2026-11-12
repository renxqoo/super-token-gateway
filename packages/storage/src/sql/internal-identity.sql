DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_identity_owner') THEN CREATE ROLE core_identity_owner NOLOGIN;END IF;
END $$;
REVOKE ALL ON internal_service_instances,internal_service_certificates,internal_startup_registrations,internal_startup_sessions FROM PUBLIC;
DO $$ DECLARE r text;BEGIN
 FOREACH r IN ARRAY ARRAY['gateway_runtime','gateway_worker','core_catalog'] LOOP
  IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=r) THEN
   EXECUTE format('REVOKE ALL ON internal_service_instances,internal_service_certificates,internal_startup_registrations,internal_startup_sessions FROM %I',r);
  END IF;
 END LOOP;
END $$;
GRANT USAGE ON SCHEMA public TO core_identity_owner;
GRANT INSERT ON audit_events TO core_identity_owner;
GRANT SELECT,INSERT,UPDATE ON internal_service_instances,internal_service_certificates,internal_startup_registrations,internal_startup_sessions TO core_identity_owner;
CREATE FUNCTION internal_identity_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 IF OLD.security_revoked_at IS NOT NULL AND NEW.security_revoked_at IS DISTINCT FROM OLD.security_revoked_at THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 IF TG_TABLE_NAME='internal_service_instances' THEN
  IF (to_jsonb(NEW)-ARRAY['enabled','security_revoked_at','security_reason','authorization_revision']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['enabled','security_revoked_at','security_reason','authorization_revision']) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 ELSIF TG_TABLE_NAME='internal_service_certificates' THEN
  IF (to_jsonb(NEW)-ARRAY['retired_at','security_revoked_at','authorization_revision']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['retired_at','security_revoked_at','authorization_revision']) OR (OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 ELSE
  IF (to_jsonb(NEW)-ARRAY['disabled_at','security_revoked_at','authorization_revision']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['disabled_at','security_revoked_at','authorization_revision']) OR (OLD.disabled_at IS NOT NULL AND NEW.disabled_at IS DISTINCT FROM OLD.disabled_at) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 END IF;
 IF NEW.authorization_revision<>OLD.authorization_revision+1 THEN RAISE EXCEPTION 'identity_revision' USING ERRCODE='P0403';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER identity_instance_guard BEFORE UPDATE OR DELETE ON internal_service_instances FOR EACH ROW EXECUTE FUNCTION internal_identity_guard_v1();
CREATE TRIGGER identity_certificate_guard BEFORE UPDATE OR DELETE ON internal_service_certificates FOR EACH ROW EXECUTE FUNCTION internal_identity_guard_v1();
CREATE TRIGGER identity_session_guard BEFORE UPDATE OR DELETE ON internal_startup_sessions FOR EACH ROW EXECUTE FUNCTION internal_identity_guard_v1();
CREATE FUNCTION internal_identity_registration_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE s public.internal_startup_sessions%ROWTYPE;BEGIN
 IF TG_OP='DELETE' OR OLD.state='REGISTERED' OR (to_jsonb(NEW)-ARRAY['state','session_id','receipt']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','session_id','receipt']) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 IF NEW.state='REGISTERED' THEN
  SELECT * INTO s FROM public.internal_startup_sessions WHERE session_id=NEW.session_id;
  IF NOT FOUND OR s.service<>NEW.service OR s.instance_id<>NEW.instance_id OR s.public_key_spki<>NEW.public_key_spki OR s.boot_nonce<>NEW.boot_nonce OR s.certificate_sha256<>NEW.certificate_sha256 OR NEW.receipt IS DISTINCT FROM jsonb_build_object('session_id',s.session_id,'incarnation_id',s.incarnation_id,'service',s.service,'instance_id',s.instance_id,'certificate_sha256',s.certificate_sha256,'public_key_sha256',s.public_key_sha256,'expires_at',to_char(s.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER identity_registration_guard BEFORE UPDATE OR DELETE ON internal_startup_registrations FOR EACH ROW EXECUTE FUNCTION internal_identity_registration_guard_v1();
CREATE TRIGGER identity_command_immutable BEFORE UPDATE OR DELETE ON execution_commands FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE FUNCTION internal_identity_command_owner_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_TABLE_NAME='execution_commands' THEN
  IF NEW.kind NOT IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1') OR NEW.owner_session_id IS NOT NULL THEN RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
 ELSE
  IF TG_OP='UPDATE' AND (NEW.owner_session_id,NEW.owner_key_sha256,NEW.owner_service,NEW.certificate_sha256,NEW.gateway_instance_id,NEW.incarnation_id) IS DISTINCT FROM (OLD.owner_session_id,OLD.owner_key_sha256,OLD.owner_service,OLD.certificate_sha256,OLD.gateway_instance_id,OLD.incarnation_id) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
  IF NEW.owner_session_id IS NULL OR NEW.owner_key_sha256 IS NULL OR NEW.owner_service<>'gateway' OR NEW.certificate_sha256 IS NULL OR NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=NEW.owner_session_id AND s.public_key_sha256=NEW.owner_key_sha256 AND s.service=NEW.owner_service AND s.instance_id=NEW.gateway_instance_id AND s.incarnation_id=NEW.incarnation_id AND s.certificate_sha256=NEW.certificate_sha256) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER identity_command_owner BEFORE INSERT ON execution_commands FOR EACH ROW EXECUTE FUNCTION internal_identity_command_owner_v1();
CREATE TRIGGER identity_permit_owner BEFORE INSERT OR UPDATE OF owner_session_id,owner_key_sha256,owner_service,certificate_sha256,gateway_instance_id,incarnation_id ON execution_permits FOR EACH ROW EXECUTE FUNCTION internal_identity_command_owner_v1();
CREATE FUNCTION internal_identity_locks_v1(keys text[]) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE k text;BEGIN
 FOR k IN SELECT q.k FROM (SELECT DISTINCT unnest(keys) AS k) q ORDER BY q.k COLLATE "C" LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.identity.v1:'||k,0)) THEN RAISE EXCEPTION 'identity_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
END $$;
CREATE FUNCTION internal_identity_projection_v1(p jsonb,endpoint text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE fields text[]:=ARRAY['service','instance_id','certificate_sha256','instance_revision','certificate_revision','raw_body_sha256'];k text;deadline timestamptz;BEGIN
 IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR endpoint IS NULL OR endpoint NOT IN ('challenges','registrations','current') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 IF endpoint='current' THEN
  fields:=fields||ARRAY['session_id','incarnation_id','key_sha256','session_revision','command_type','command_id','method','normalized_path','deadline_at','trace_id'];
 ELSE fields:=fields||ARRAY['endpoint'];END IF;
 IF p-fields<>'{}'::jsonb THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOREACH k IN ARRAY fields LOOP
  IF jsonb_typeof(p->k) IS DISTINCT FROM 'string' OR p->>k='' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 END LOOP;
 IF ((p->>'service') IN ('gateway','core-api','core-worker') AND (p->>'instance_id') ~ '^[A-Za-z0-9_-]{1,100}$' AND (p->>'certificate_sha256') ~ '^[a-f0-9]{64}$' AND (p->>'instance_revision') ~ '^[1-9][0-9]{0,37}$' AND (p->>'certificate_revision') ~ '^[1-9][0-9]{0,37}$' AND (p->>'raw_body_sha256') ~ '^[a-f0-9]{64}$') IS NOT TRUE THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 IF endpoint='current' THEN
  IF ((p->>'session_id') ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND (p->>'incarnation_id') ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND (p->>'command_id') ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND (p->>'key_sha256') ~ '^[a-f0-9]{64}$' AND (p->>'session_revision') ~ '^[1-9][0-9]{0,37}$' AND (p->>'trace_id') ~ '^[A-Za-z0-9_-]{1,100}$' AND (p->>'deadline_at') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$') IS NOT TRUE THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
  IF p->>'command_type' IS DISTINCT FROM 'identity_current_v1' OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/identity/current-session' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
  BEGIN deadline:=(p->>'deadline_at')::timestamptz;
  EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END;
  IF to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') IS DISTINCT FROM p->>'deadline_at' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 ELSIF p->>'endpoint' IS DISTINCT FROM endpoint THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
END $$;
CREATE FUNCTION internal_identity_peer_v1(p jsonb) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE i public.internal_service_instances%ROWTYPE;c public.internal_service_certificates%ROWTYPE;BEGIN
 SELECT * INTO i FROM public.internal_service_instances WHERE service=p->>'service' AND instance_id=p->>'instance_id' FOR SHARE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.internal_service_certificates WHERE certificate_sha256=p->>'certificate_sha256' FOR SHARE NOWAIT;
 IF NOT FOUND OR (c.service,c.instance_id) IS DISTINCT FROM (i.service,i.instance_id) THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF i.security_revoked_at IS NOT NULL OR c.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION 'internal_identity_revoked' USING ERRCODE='P0403';END IF;
 IF NOT i.enabled OR c.retired_at IS NOT NULL OR c.not_before>clock_timestamp() OR c.not_after<=clock_timestamp() OR (i.authorization_revision::text,c.authorization_revision::text) IS DISTINCT FROM (p->>'instance_revision',p->>'certificate_revision') THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 RETURN jsonb_build_object('service',i.service,'instance_id',i.instance_id,'certificate_sha256',c.certificate_sha256,'not_after',to_char(c.not_after AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE FUNCTION internal_identity_read_peer_v1(hash text) RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
 SELECT to_jsonb(c)||jsonb_build_object('instance_enabled',i.enabled,'instance_security_revoked_at',i.security_revoked_at,'instance_revision',i.authorization_revision::text,'certificate_revision',c.authorization_revision::text) FROM public.internal_service_certificates c JOIN public.internal_service_instances i USING(service,instance_id) WHERE c.certificate_sha256=hash
$$;
CREATE FUNCTION internal_identity_read_session_v1(id uuid) RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
 SELECT to_jsonb(s)-'public_key_spki'-'boot_nonce'||jsonb_build_object('public_key_spki',encode(s.public_key_spki,'base64'),'session_revision',s.authorization_revision::text) FROM public.internal_startup_sessions s WHERE s.session_id=id
$$;
CREATE FUNCTION internal_identity_read_registration_v1(id uuid) RETURNS jsonb LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
 SELECT to_jsonb(r)-'public_key_spki'-'boot_nonce'-'server_nonce'||jsonb_build_object('public_key_spki',encode(r.public_key_spki,'base64'),'boot_nonce',rtrim(translate(encode(r.boot_nonce,'base64'),'+/','-_'),'='),'server_nonce',rtrim(translate(encode(r.server_nonce,'base64'),'+/','-_'),'='),'challenge_expires_at',to_char(r.challenge_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) FROM public.internal_startup_registrations r WHERE r.challenge_id=id
$$;
CREATE FUNCTION internal_identity_challenge_v1(p jsonb,reg uuid,spki bytea,boot bytea,aud text,digest text,nonce bytea,ttl integer) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE r public.internal_startup_registrations%ROWTYPE;peer jsonb;BEGIN
 PERFORM public.internal_identity_projection_v1(p,'challenges');
 IF (reg::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND ttl BETWEEN 1 AND 300 AND octet_length(spki)=44 AND octet_length(boot)=32 AND octet_length(nonce)=32 AND digest ~ '^[a-f0-9]{64}$' AND aud ~ '^[A-Za-z0-9_.-]{1,100}/core-api$') IS NOT TRUE THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 PERFORM public.internal_identity_locks_v1(ARRAY['registration:'||(p->>'service')||':'||(p->>'instance_id')||':'||reg::text,'key:'||encode(sha256(spki),'hex'),'boot:'||(p->>'service')||':'||(p->>'instance_id')||':'||encode(boot,'hex')]);
 peer:=public.internal_identity_peer_v1(p);
 SELECT * INTO r FROM public.internal_startup_registrations WHERE service=p->>'service' AND instance_id=p->>'instance_id' AND registration_id=reg FOR UPDATE NOWAIT;
 IF FOUND THEN
  IF (r.registration_digest,r.certificate_sha256,r.public_key_spki,r.boot_nonce,r.audience) IS DISTINCT FROM (digest,p->>'certificate_sha256',spki,boot,aud) THEN RAISE EXCEPTION 'registration_conflict' USING ERRCODE='P0409';END IF;
 ELSE
  INSERT INTO public.internal_startup_registrations(challenge_id,service,instance_id,registration_id,certificate_sha256,audience,public_key_spki,public_key_sha256,boot_nonce,server_nonce,registration_digest,challenge_expires_at) VALUES(gen_random_uuid(),p->>'service',p->>'instance_id',reg,p->>'certificate_sha256',aud,spki,encode(sha256(spki),'hex'),boot,nonce,digest,LEAST(clock_timestamp()+make_interval(secs=>ttl),(peer->>'not_after')::timestamptz)) RETURNING * INTO r;
 END IF;
 RETURN public.internal_identity_read_registration_v1(r.challenge_id);
END $$;
CREATE FUNCTION internal_identity_register_v1(p jsonb,id uuid,reg uuid,expected_digest text,ttl integer) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE r public.internal_startup_registrations%ROWTYPE;s public.internal_startup_sessions%ROWTYPE;peer jsonb;replayed boolean:=false;BEGIN
 PERFORM public.internal_identity_projection_v1(p,'registrations');
 IF (id::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND reg::text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' AND expected_digest ~ '^[a-f0-9]{64}$' AND ttl BETWEEN 1 AND 86400) IS NOT TRUE THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO r FROM public.internal_startup_registrations WHERE challenge_id=id;
 IF NOT FOUND THEN RAISE EXCEPTION 'internal_proof_invalid' USING ERRCODE='P0401';END IF;
 PERFORM public.internal_identity_locks_v1(ARRAY['registration:'||r.service||':'||r.instance_id||':'||reg::text,'key:'||r.public_key_sha256,'boot:'||r.service||':'||r.instance_id||':'||encode(r.boot_nonce,'hex')]);
 peer:=public.internal_identity_peer_v1(p);
 SELECT * INTO r FROM public.internal_startup_registrations WHERE challenge_id=id FOR UPDATE NOWAIT;
 IF (r.registration_id,r.registration_digest,r.certificate_sha256,r.service,r.instance_id) IS DISTINCT FROM (reg,expected_digest,p->>'certificate_sha256',p->>'service',p->>'instance_id') THEN RAISE EXCEPTION 'registration_conflict' USING ERRCODE='P0409';END IF;
 IF r.state='REGISTERED' THEN
  SELECT * INTO s FROM public.internal_startup_sessions WHERE session_id=r.session_id FOR SHARE NOWAIT;replayed:=true;
 ELSE
  IF r.state<>'PENDING' OR r.challenge_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'internal_deadline' USING ERRCODE='P0408';END IF;
  IF EXISTS(SELECT 1 FROM public.internal_startup_sessions WHERE public_key_sha256=r.public_key_sha256) THEN RAISE EXCEPTION 'startup_key_reused' USING ERRCODE='P0409';END IF;
  IF EXISTS(SELECT 1 FROM public.internal_startup_sessions WHERE service=r.service AND instance_id=r.instance_id AND boot_nonce=r.boot_nonce) THEN RAISE EXCEPTION 'startup_boot_conflict' USING ERRCODE='P0409';END IF;
  INSERT INTO public.internal_startup_sessions(session_id,incarnation_id,service,instance_id,certificate_sha256,public_key_spki,public_key_sha256,boot_nonce,registered_at,expires_at) VALUES(gen_random_uuid(),gen_random_uuid(),r.service,r.instance_id,r.certificate_sha256,r.public_key_spki,r.public_key_sha256,r.boot_nonce,clock_timestamp(),LEAST(clock_timestamp()+make_interval(secs=>ttl),(peer->>'not_after')::timestamptz)) RETURNING * INTO s;
  UPDATE public.internal_startup_registrations SET state='REGISTERED',session_id=s.session_id,receipt=jsonb_build_object('session_id',s.session_id,'incarnation_id',s.incarnation_id,'service',s.service,'instance_id',s.instance_id,'certificate_sha256',s.certificate_sha256,'public_key_sha256',s.public_key_sha256,'expires_at',to_char(s.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) WHERE challenge_id=id RETURNING * INTO r;
 END IF;
 RETURN jsonb_build_object('receipt',r.receipt,'replayed',replayed,'current',s.expires_at>clock_timestamp() AND s.disabled_at IS NULL AND s.security_revoked_at IS NULL);
END $$;
CREATE FUNCTION internal_identity_current_v1(p jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE s public.internal_startup_sessions%ROWTYPE;BEGIN
 PERFORM public.internal_identity_projection_v1(p,'current');
 PERFORM public.internal_identity_peer_v1(p);
 SELECT * INTO s FROM public.internal_startup_sessions WHERE session_id=(p->>'session_id')::uuid FOR SHARE NOWAIT;
 IF NOT FOUND OR (s.service,s.instance_id,s.incarnation_id::text,s.public_key_sha256,s.certificate_sha256) IS DISTINCT FROM (p->>'service',p->>'instance_id',p->>'incarnation_id',p->>'key_sha256',p->>'certificate_sha256') THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF s.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION 'internal_identity_revoked' USING ERRCODE='P0403';END IF;
 IF s.disabled_at IS NOT NULL OR s.expires_at<=clock_timestamp() OR s.authorization_revision::text IS DISTINCT FROM p->>'session_revision' THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
 IF (p->>'deadline_at')::timestamptz<=clock_timestamp() OR (p->>'deadline_at')::timestamptz>clock_timestamp()+interval '1 hour' THEN RAISE EXCEPTION 'internal_deadline' USING ERRCODE='P0408';END IF;
 RETURN jsonb_build_object('command_id',p->>'command_id','kind','identity_current_v1','receipt',jsonb_build_object('session_id',s.session_id,'incarnation_id',s.incarnation_id,'service',s.service,'instance_id',s.instance_id,'certificate_sha256',s.certificate_sha256,'public_key_sha256',s.public_key_sha256,'expires_at',to_char(s.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')),'current',true,'authorization_revision',s.authorization_revision::text);
END $$;
CREATE FUNCTION internal_identity_provision_v1(svc text,inst text,hash text,san text,exact_san text,valid_from timestamptz,valid_until timestamptz) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE i public.internal_service_instances%ROWTYPE;c public.internal_service_certificates%ROWTYPE;BEGIN
 IF (svc IN ('gateway','core-api','core-worker') AND inst ~ '^[A-Za-z0-9_-]{1,100}$' AND hash ~ '^[a-f0-9]{64}$' AND san ~ '^spiffe://[A-Za-z0-9_./-]{1,200}$' AND length(exact_san) BETWEEN 1 AND 2000 AND isfinite(valid_from) AND isfinite(valid_until) AND valid_until>valid_from) IS NOT TRUE THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 PERFORM public.internal_identity_locks_v1(ARRAY['provision:'||svc||':'||inst,'certificate:'||hash]);
 INSERT INTO public.internal_service_instances(service,instance_id) VALUES(svc,inst) ON CONFLICT DO NOTHING;
 SELECT * INTO i FROM public.internal_service_instances WHERE service=svc AND instance_id=inst FOR SHARE NOWAIT;
 IF i.security_revoked_at IS NOT NULL OR NOT i.enabled THEN RAISE EXCEPTION 'internal_identity_revoked' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.internal_service_certificates WHERE certificate_sha256=hash FOR SHARE NOWAIT;
 IF FOUND THEN
  IF (c.service,c.instance_id,c.uri_san,c.subject_altname_exact,c.not_before,c.not_after) IS DISTINCT FROM (svc,inst,san,exact_san,valid_from,valid_until) THEN RAISE EXCEPTION 'registration_conflict' USING ERRCODE='P0409';END IF;
 ELSE
  INSERT INTO public.internal_service_certificates(certificate_sha256,service,instance_id,uri_san,subject_altname_exact,not_before,not_after) VALUES(hash,svc,inst,san,exact_san,valid_from,valid_until);
 END IF;
 INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),'internal_identity_provision',svc||':'||inst,session_user,jsonb_build_object('certificate_sha256',hash));
END $$;
CREATE FUNCTION internal_identity_change_v1(svc text,inst text,hash text,sid uuid,action text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE i public.internal_service_instances%ROWTYPE;c public.internal_service_certificates%ROWTYPE;s public.internal_startup_sessions%ROWTYPE;BEGIN
 IF (svc IN ('gateway','core-api','core-worker') AND inst ~ '^[A-Za-z0-9_-]{1,100}$' AND action IN ('DISABLE','RETIRE','SECURITY_REVOKE')) IS NOT TRUE OR (hash IS NOT NULL AND hash !~ '^[a-f0-9]{64}$') OR (sid IS NOT NULL AND sid::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$') OR (sid IS NOT NULL AND action='RETIRE') OR (sid IS NULL AND hash IS NOT NULL AND action='DISABLE') OR (sid IS NULL AND hash IS NULL AND action='RETIRE') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO i FROM public.internal_service_instances WHERE service=svc AND instance_id=inst FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF hash IS NOT NULL THEN
  SELECT * INTO c FROM public.internal_service_certificates WHERE certificate_sha256=hash FOR UPDATE;
  IF NOT FOUND OR (c.service,c.instance_id) IS DISTINCT FROM (svc,inst) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 END IF;
 IF sid IS NOT NULL THEN
  SELECT * INTO s FROM public.internal_startup_sessions WHERE session_id=sid FOR SHARE;
  IF NOT FOUND OR (s.service,s.instance_id) IS DISTINCT FROM (svc,inst) OR (hash IS NOT NULL AND s.certificate_sha256<>hash) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
  UPDATE public.internal_startup_sessions SET disabled_at=CASE WHEN action='DISABLE' THEN COALESCE(disabled_at,clock_timestamp()) ELSE disabled_at END,security_revoked_at=CASE WHEN action='SECURITY_REVOKE' THEN COALESCE(security_revoked_at,clock_timestamp()) ELSE security_revoked_at END,authorization_revision=authorization_revision+1 WHERE session_id=sid AND action<>'RETIRE';
 ELSIF hash IS NOT NULL THEN
  UPDATE public.internal_service_certificates SET retired_at=CASE WHEN action='RETIRE' THEN COALESCE(retired_at,clock_timestamp()) ELSE retired_at END,security_revoked_at=CASE WHEN action='SECURITY_REVOKE' THEN COALESCE(security_revoked_at,clock_timestamp()) ELSE security_revoked_at END,authorization_revision=authorization_revision+1 WHERE certificate_sha256=hash AND action<>'DISABLE';
 ELSE
  UPDATE public.internal_service_instances SET enabled=CASE WHEN action='DISABLE' THEN false ELSE enabled END,security_revoked_at=CASE WHEN action='SECURITY_REVOKE' THEN COALESCE(security_revoked_at,clock_timestamp()) ELSE security_revoked_at END,security_reason=CASE WHEN action='SECURITY_REVOKE' THEN 'SECURITY_INCIDENT' ELSE security_reason END,authorization_revision=authorization_revision+1 WHERE service=svc AND instance_id=inst AND action<>'RETIRE';
 END IF;
 INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),'internal_identity_'||lower(action),svc||':'||inst,session_user,jsonb_build_object('certificate_sha256',hash,'session_id',sid));
END $$;
DO $$ DECLARE f record;BEGIN
 FOR f IN SELECT oid::regprocedure AS signature FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname LIKE 'internal_identity_%_v1' LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',f.signature);
  EXECUTE format('ALTER FUNCTION %s OWNER TO core_identity_owner',f.signature);
 END LOOP;
END $$;
