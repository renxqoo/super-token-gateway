DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM public.upstream_holds WHERE state IN ('HELD','UNKNOWN','PARTIAL')) THEN RAISE EXCEPTION 'Unmappable active upstream holds';END IF;
END $$;
CREATE FUNCTION public.core_utf16_sort_v1(value text) RETURNS bytea LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE result text='';point integer;char_index integer;BEGIN
 FOR char_index IN 1..char_length(value) LOOP
  point=ascii(substr(value,char_index,1));
  IF point>65535 THEN point=point-65536;result=result||lpad(to_hex(55296+point/1024),4,'0')||lpad(to_hex(56320+point%1024),4,'0');
  ELSE result=result||lpad(to_hex(point),4,'0');END IF;
 END LOOP;RETURN decode(result,'hex');
END $$;
CREATE FUNCTION public.core_canonical_v1(v jsonb) RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE r text;BEGIN
 IF jsonb_typeof(v)='object' THEN SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||public.core_canonical_v1(value),',' ORDER BY public.core_utf16_sort_v1(key)),'')||'}' INTO r FROM jsonb_each(v);
 ELSIF jsonb_typeof(v)='array' THEN SELECT '['||coalesce(string_agg(public.core_canonical_v1(value),',' ORDER BY n),'')||']' INTO r FROM jsonb_array_elements(v) WITH ORDINALITY a(value,n);
 ELSE r=v::text;END IF;RETURN r;
END $$;
CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $$ SELECT encode(sha256(convert_to(public.core_canonical_v1(v),'UTF8')),'hex') $$;
CREATE OR REPLACE FUNCTION public.internal_identity_command_owner_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_TABLE_NAME='execution_commands' THEN
  IF NEW.kind IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1') THEN
   IF NEW.owner_session_id IS NOT NULL THEN RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
  ELSIF NEW.kind='execution_abort_recovery_v1' THEN
   IF NEW.owner_session_id IS NULL OR NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=NEW.owner_session_id AND s.incarnation_id=NEW.caller_incarnation AND s.service IN ('core-api','core-worker') AND NEW.caller_identity=s.service||':'||s.instance_id) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
  ELSIF NEW.kind IN ('execution_admit_v1','execution_next_v1','execution_begin_v1','execution_abort_v1','execution_quota_v1') THEN
   IF NEW.owner_session_id IS NULL OR NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=NEW.owner_session_id AND s.incarnation_id=NEW.caller_incarnation AND s.service='gateway' AND NEW.caller_identity=s.service||':'||s.instance_id) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
  ELSE RAISE EXCEPTION 'unsupported_command' USING ERRCODE='P0403';END IF;
 ELSE
  IF TG_OP='UPDATE' AND (NEW.owner_session_id,NEW.owner_key_sha256,NEW.owner_service,NEW.certificate_sha256,NEW.gateway_instance_id,NEW.incarnation_id) IS DISTINCT FROM (OLD.owner_session_id,OLD.owner_key_sha256,OLD.owner_service,OLD.certificate_sha256,OLD.gateway_instance_id,OLD.incarnation_id) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
  IF NEW.owner_session_id IS NULL OR NEW.owner_key_sha256 IS NULL OR NEW.owner_service IS DISTINCT FROM 'gateway' OR NEW.certificate_sha256 IS NULL OR NOT EXISTS(SELECT 1 FROM public.internal_startup_sessions s WHERE s.session_id=NEW.owner_session_id AND s.public_key_sha256=NEW.owner_key_sha256 AND s.service=NEW.owner_service AND s.instance_id=NEW.gateway_instance_id AND s.incarnation_id=NEW.incarnation_id AND s.certificate_sha256=NEW.certificate_sha256) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 END IF;RETURN NEW;
END $$;
CREATE FUNCTION public.core_caller_v1(p jsonb) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE path text;BEGIN
 path=CASE p->>'command_type' WHEN 'execution_context_v1' THEN 'context' WHEN 'execution_admit_v1' THEN 'admissions' WHEN 'execution_next_v1' THEN 'attempts' WHEN 'execution_begin_v1' THEN 'begin' WHEN 'execution_abort_v1' THEN 'abort' WHEN 'execution_query_v1' THEN 'commands' WHEN 'execution_quota_v1' THEN 'quota' WHEN 'execution_quota_recovery_v1' THEN 'quota-recovery' WHEN 'execution_abort_recovery_v1' THEN 'abort-recovery' END;
 IF path IS NULL OR NOT (p->>'service'='gateway' AND p->>'command_type' NOT IN ('execution_quota_recovery_v1','execution_abort_recovery_v1') OR p->>'service' IN ('core-api','core-worker') AND p->>'command_type' IN ('execution_quota_recovery_v1','execution_abort_recovery_v1','execution_query_v1')) OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/'||path THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_current_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'));
END $$;
CREATE FUNCTION public.core_auth_v1(kid uuid,ip text,op text,model text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k public.api_keys%ROWTYPE;pr public.projects%ROWTYPE;o public.organizations%ROWTYPE;u public.principals%ROWTYPE;before_key public.api_keys%ROWTYPE;before_project public.projects%ROWTYPE;scopes jsonb='[]';BEGIN
 SELECT * INTO before_key FROM public.api_keys WHERE id=kid;
 SELECT * INTO before_project FROM public.projects WHERE id=before_key.project_id;
 IF before_project.organization_id IS NOT NULL THEN SELECT * INTO o FROM public.organizations WHERE id=before_project.organization_id FOR SHARE NOWAIT;END IF;
 IF before_key.principal_id IS NOT NULL THEN SELECT * INTO u FROM public.principals WHERE id=before_key.principal_id FOR SHARE NOWAIT;END IF;
 SELECT * INTO pr FROM public.projects WHERE id=before_key.project_id FOR SHARE NOWAIT;
 SELECT * INTO k FROM public.api_keys WHERE id=kid FOR SHARE NOWAIT;
 IF (k.project_id,k.principal_id,pr.organization_id) IS DISTINCT FROM (before_key.project_id,before_key.principal_id,before_project.organization_id) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 IF k.id IS NULL OR pr.id IS NULL OR (k.enabled AND pr.enabled AND k.role IN ('ADMIN','INVOKER') AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp()) AND op=ANY(k.operations) AND (cardinality(k.models)=0 OR model=ANY(k.models)) AND (pr.organization_id IS NULL OR o.enabled) AND (k.principal_id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN','MEMBER') AND u.organization_id=pr.organization_id)) IS NOT TRUE OR (cardinality(k.allowed_cidrs)>0 AND NOT EXISTS(SELECT 1 FROM unnest(k.allowed_cidrs) c WHERE ip::inet <<= c::inet)) THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 IF o.id IS NOT NULL THEN scopes=scopes||jsonb_build_array(jsonb_build_object('kind','organization','id',o.id,'rpm',o.rpm,'tpm',o.tpm,'concurrency',o.concurrency));END IF;
 IF u.id IS NOT NULL THEN scopes=scopes||jsonb_build_array(jsonb_build_object('kind','principal','id',u.id,'rpm',u.rpm,'tpm',u.tpm,'concurrency',u.concurrency));END IF;
 scopes=scopes||jsonb_build_array(jsonb_build_object('kind','project','id',pr.id,'rpm',pr.rpm,'tpm',pr.tpm,'concurrency',pr.concurrency),jsonb_build_object('kind','key','id',k.id,'rpm',coalesce(k.rpm,pr.rpm),'tpm',coalesce(k.tpm,pr.tpm),'concurrency',coalesce(k.concurrency,pr.concurrency)));
 RETURN jsonb_build_object('project_id',pr.id,'key_id',k.id,'customer_currency',pr.billing_currency,'scopes',scopes);
END $$;
CREATE FUNCTION public.core_candidates_v1(model text,op text,cur text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE m public.models%ROWTYPE;r record;semantic jsonb;ref jsonb;candidate jsonb;result jsonb='[]';BEGIN
 SELECT * INTO m FROM public.models WHERE name=model FOR SHARE NOWAIT;
 IF NOT FOUND OR NOT m.enabled OR (op=ANY(m.operations)) IS NOT TRUE THEN RETURN result;END IF;
 PERFORM id FROM public.routes WHERE model_name=model ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.billing_tariffs WHERE billing_tariffs.model=core_candidates_v1.model AND operation=op AND currency=cur ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.supplier_brands WHERE id IN(SELECT brand_id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.plugin_versions WHERE id IN(SELECT b.plugin_version_id FROM public.channel_plugin_bindings b JOIN public.channels c ON c.binding_id=b.id WHERE c.id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 FOR r IN SELECT rt,c,b,v,p,cv,bp,t.id AS tariff_id FROM public.routes rt JOIN public.channels c ON c.id=rt.provider_id JOIN public.supplier_brands br ON br.id=c.brand_id JOIN public.upstream_accounts a ON a.id=c.account_id JOIN public.channel_plugin_bindings b ON b.id=c.binding_id JOIN public.plugin_versions v ON v.id=b.plugin_version_id JOIN public.plugin_publication_receipts pub ON pub.version_id=v.id JOIN public.plugin_packages p ON p.digest=v.package_digest JOIN public.cost_versions cv ON cv.id=rt.pricing_id JOIN LATERAL(SELECT * FROM public.billing_tariffs t WHERE t.model=core_candidates_v1.model AND t.operation=op AND t.currency=cur AND t.enabled AND (t.provider_id IS NULL OR t.provider_id=c.id) ORDER BY (t.provider_id IS NOT NULL) DESC LIMIT 1)t ON true JOIN public.billing_prices bp ON bp.id=t.pricing_id WHERE rt.model_name=core_candidates_v1.model AND rt.enabled AND c.enabled AND br.enabled AND v.state='PUBLISHED' AND NOT v.quarantined AND a.mode='PREPAID' AND a.state='ACTIVE' AND op=ANY(rt.operations) AND b.channel_id=c.id AND b.account_id=c.account_id AND b.brand_id=c.brand_id AND b.config_revision=c.config_revision AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(p.manifest->'capabilities'->'operations') x WHERE x=op) ORDER BY rt.priority DESC,rt.id LIMIT 128 LOOP
  semantic=jsonb_build_object('model',to_jsonb(m),'route',to_jsonb(r.rt),'channel',to_jsonb(r.c)-'secret','ciphertext_sha256',encode(sha256(convert_to((r.c).secret,'UTF8')),'hex'),'binding',to_jsonb(r.b),'plugin',to_jsonb(r.v),'tariff_id',r.tariff_id,'price_id',(r.bp).id);
  ref=jsonb_build_object('route_id',(r.rt).id,'channel_id',(r.c).id,'binding_id',(r.b).id,'plugin_version_id',(r.v).id,'package_digest',(r.v).package_digest,'config_revision',(r.b).config_revision::text,'config_digest',(r.b).config_digest,'cost_version_id',(r.cv).id,'customer_price_id',(r.bp).id,'candidate_digest',public.core_digest_v1(semantic));
  candidate=jsonb_build_object('ref',ref,'origin',regexp_replace((r.c).base_url,'^(https://[^/]+).*$', '\1'),'upstream_model',(r.rt).upstream_model,'credential_ciphertext_sha256',encode(sha256(convert_to((r.c).secret,'UTF8')),'hex'),'config',(r.b).config,'cost_currency',(r.cv).currency,'cost_rates',(r.cv).rates,'cost_ceilings',(r.cv).ceilings,'customer_currency',(r.bp).currency,'customer_rates',(r.bp).rates,'customer_ceilings',(r.bp).ceilings,'customer_basis',(r.bp).basis,'request_policy',(r.rt).request_policy,'routing',jsonb_build_object('priority',(r.rt).priority,'weight',(r.rt).weight),'channel_limits',jsonb_build_object('rpm',(r.c).rpm,'concurrency',(r.c).concurrency));
  result=result||jsonb_build_array(candidate);
 END LOOP;RETURN result;
END $$;
CREATE FUNCTION public.core_context_v1(p jsonb,id uuid,input jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE k uuid;auth jsonb;candidates jsonb;snapshot jsonb;expiry timestamptz;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_context_v1' OR jsonb_typeof(input) IS DISTINCT FROM 'object' OR input-ARRAY['verifier','ip','operation','model','fingerprint','max_attempts']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['verifier','ip','operation','model','fingerprint'])f WHERE jsonb_typeof(input->f) IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT api_keys.id INTO k FROM public.api_keys WHERE verifier=input->>'verifier';
 IF jsonb_typeof(input->'max_attempts') IS DISTINCT FROM 'number' OR (input->>'max_attempts')::integer NOT BETWEEN 1 AND 5 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 auth=public.core_auth_v1(k,input->>'ip',input->>'operation',input->>'model');
 candidates=public.core_candidates_v1(input->>'model',input->>'operation',auth->>'customer_currency');
 expiry=least((p->>'deadline_at')::timestamptz,clock_timestamp()+interval '30 seconds');
 snapshot=auth||jsonb_build_object('owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'max_output_tokens',(SELECT max_output_tokens FROM public.models WHERE name=input->>'model'),'routing_strategy',(SELECT routing_strategy FROM public.models WHERE name=input->>'model'),'candidates',candidates,'operation',input->>'operation','model',input->>'model','execution_budget',jsonb_build_object('max_attempts',(input->>'max_attempts')::integer));
 INSERT INTO public.execution_contexts(id,project_id,key_id,owner_session_id,operation,model,ip,fingerprint,snapshot,expires_at) VALUES(id,(auth->>'project_id')::uuid,k,(p->>'session_id')::uuid,input->>'operation',input->>'model',input->>'ip',input->>'fingerprint',snapshot,expiry);
 RETURN snapshot||jsonb_build_object('context_id',id,'expires_at',to_char(expiry AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;
CREATE OR REPLACE FUNCTION public.allocate_customer_batch_v1(p_inputs jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'pg_temp'
AS $function$
DECLARE item record;p jsonb;quote jsonb;credential api_keys%ROWTYPE;route routes%ROWTYPE;old gateway_requests%ROWTYPE;
 wallet wallets%ROWTYPE;lot wallet_lots%ROWTYPE;wallet_rows wallets[];lot_rows wallet_lots[];local_lots wallet_lots[];
 wallet_index jsonb;lot_ranges jsonb;wallet_dirty boolean[];lot_dirty boolean[];
 staged_requests gateway_requests[]='{}';staged_holds billing_holds[]='{}';staged_allocations jsonb[]='{}';staged_journals jsonb[]='{}';staged_attempts attempts[]='{}';local_allocations jsonb;
 pending_request gateway_requests%ROWTYPE;pending_hold billing_holds%ROWTYPE;pending_attempt attempts%ROWTYPE;
 result jsonb;results jsonb='{}';outcomes jsonb[]='{}';expiry_errors jsonb='{}';legs jsonb;failure text;new_id uuid;attempt_id uuid;pricing uuid;
 wi integer;first_lot integer;last_lot integer;li integer;i integer;sequence integer;
 atoms numeric;remaining numeric;take numeric;credit_part numeric;funded_part numeric;asof timestamptz;locked_wallets uuid[];
 midpoint integer;left_inputs jsonb;right_inputs jsonb;left_results jsonb;right_results jsonb;
 max_atoms constant numeric=99999999999999999999999999999999999999;
BEGIN
 IF p_inputs IS NULL OR jsonb_typeof(p_inputs)<>'array' OR jsonb_array_length(p_inputs) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'invalid admission batch';END IF;
 SELECT coalesce(array_agg(id),'{}'::uuid[]) INTO locked_wallets FROM (SELECT w.id FROM (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target
 JOIN LATERAL(SELECT id FROM wallets WHERE project_id=target.project_id AND currency=target.currency OFFSET 0) found ON true
 JOIN wallets w ON w.id=found.id ORDER BY w.id FOR NO KEY UPDATE OF w SKIP LOCKED) acquired;
 FOR item IN SELECT w.id FROM wallets w JOIN (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target
  ON target.project_id=w.project_id AND target.currency=w.currency WHERE w.id=ANY(locked_wallets) AND w.has_expiring_lots ORDER BY w.id LOOP
  BEGIN PERFORM expire_wallet(item.id);
  EXCEPTION WHEN numeric_value_out_of_range THEN expiry_errors=jsonb_set(expiry_errors,ARRAY[item.id::text],to_jsonb('22003'::text));END;
 END LOOP;
 BEGIN
 SELECT array_agg(w ORDER BY w.id) INTO wallet_rows FROM wallets w JOIN (SELECT DISTINCT (input->>'project_id')::uuid AS project_id,input->'quote'->>'currency' AS currency FROM jsonb_array_elements(p_inputs) input) target ON target.project_id=w.project_id AND target.currency=w.currency WHERE w.id=ANY(locked_wallets);
 wallet_rows=coalesce(wallet_rows,'{}'::wallets[]);
 SELECT coalesce(jsonb_object_agg((wallet_rows[n]).project_id::text||':'||(wallet_rows[n]).currency,n),'{}') INTO wallet_index FROM generate_subscripts(wallet_rows,1) input(n);
 SELECT array_agg(l ORDER BY l.wallet_id,
  CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
  CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id)
 INTO lot_rows FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.id IN(SELECT (v).id FROM unnest(wallet_rows) v) AND l.available_atoms>0;
 lot_rows=coalesce(lot_rows,'{}'::wallet_lots[]);
 SELECT coalesce(jsonb_object_agg(wallet_id,jsonb_build_array(first_index,last_index)),'{}') INTO lot_ranges FROM(
  SELECT (lot_rows[n]).wallet_id AS wallet_id,min(n) AS first_index,max(n) AS last_index FROM generate_subscripts(lot_rows,1) input(n) GROUP BY (lot_rows[n]).wallet_id
 ) ranges;
 wallet_dirty=array_fill(false,ARRAY[cardinality(wallet_rows)]);lot_dirty=array_fill(false,ARRAY[cardinality(lot_rows)]);
 FOR item IN SELECT input.value,input.ordinality,k AS credential,project.enabled AS project_enabled,r AS route,
  m.enabled AS model_enabled,pr.enabled AS provider_enabled,m.operations AS model_operations,
  price.id AS billing_price,cost.id AS cost_price
  FROM jsonb_array_elements(p_inputs) WITH ORDINALITY input
  LEFT JOIN api_keys k ON k.id=(input.value->>'key_id')::uuid
  LEFT JOIN projects project ON project.id=k.project_id
  LEFT JOIN routes r ON r.id=(input.value->>'route_id')::uuid
  LEFT JOIN models m ON m.name=r.model_name LEFT JOIN channels pr ON pr.id=r.provider_id
  LEFT JOIN billing_prices price ON price.id=(input.value->'quote'->'prices'->>r.provider_id::text)::uuid AND price.currency=input.value->'quote'->>'currency'
  LEFT JOIN cost_versions cost ON cost.id=(input.value->>'cost_pricing_id')::uuid
  ORDER BY input.ordinality LOOP
  p=item.value;quote=p->'quote';
   new_id=(p->>'id')::uuid;attempt_id=(p->>'attempt_id')::uuid; IF attempt_id IS NULL THEN RAISE EXCEPTION 'attempt identity required';END IF;
   credential=item.credential;route=item.route;
   IF credential.id IS NULL OR NOT credential.enabled OR NOT coalesce(item.project_enabled,false) OR credential.expires_at<=clock_timestamp() THEN outcomes[item.ordinality]=jsonb_build_object('error','P0401');CONTINUE;END IF;
   IF credential.project_id<>(p->>'project_id')::uuid OR NOT p->>'operation'=ANY(credential.operations) OR (cardinality(credential.models)>0 AND NOT p->>'model'=ANY(credential.models)) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0403');CONTINUE;END IF;
   IF p->>'digest' IS NOT NULL THEN
    SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
    IF FOUND THEN
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   END IF;
   IF route.id IS NULL OR NOT route.enabled OR NOT coalesce(item.model_enabled,false) OR NOT coalesce(item.provider_enabled,false) OR route.model_name<>p->>'model' OR NOT p->>'operation'=ANY(route.operations) OR NOT p->>'operation'=ANY(item.model_operations) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   pricing=(quote->'prices'->>route.provider_id::text)::uuid;
   IF quote IS NULL OR item.billing_price IS NULL OR item.cost_price IS NULL THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   wi=(wallet_index->>(credential.project_id::text||':'||(quote->>'currency')))::integer;
   IF wi IS NULL THEN
    result=jsonb_build_object('error',CASE WHEN EXISTS(SELECT 1 FROM wallets WHERE project_id=credential.project_id AND currency=quote->>'currency') THEN '55P03' ELSE 'P0402' END);
    outcomes[item.ordinality]=result;CONTINUE;
   END IF;
   atoms=(quote->>'amount_atoms')::numeric;
   IF atoms IS NULL OR atoms<0 OR atoms<>trunc(atoms) OR atoms>max_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','P0422');CONTINUE;END IF;
   wallet=wallet_rows[wi];
   IF expiry_errors?wallet.id::text THEN outcomes[item.ordinality]=jsonb_build_object('error','22003');CONTINUE;END IF;
   IF wallet.freeze_reason IS NOT NULL OR wallet_freeze_reason(wallet.evidence_disputed,wallet.manual_frozen,wallet.debt_atoms,wallet.credit_reserved_atoms,wallet.credit_limit_atoms) IS NOT NULL OR EXISTS(SELECT 1 FROM wallet_disputes d WHERE d.wallet_id=wallet.id AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=d.request_id)) THEN outcomes[item.ordinality]=jsonb_build_object('error','P0402');CONTINUE;END IF;
   first_lot=coalesce((lot_ranges->wallet.id::text->>0)::integer,1);last_lot=coalesce((lot_ranges->wallet.id::text->>1)::integer,0);
   local_lots=lot_rows[first_lot:last_lot];remaining=atoms;sequence=0;legs='[]';local_allocations='[]';asof=clock_timestamp();
   FOR i IN 1..cardinality(local_lots) LOOP
    EXIT WHEN remaining=0;lot=local_lots[i];
    IF lot.available_atoms<=0 OR lot.expires_at<=asof OR cardinality(lot.models)>0 AND NOT p->>'model'=ANY(lot.models) OR cardinality(lot.operations)>0 AND NOT p->>'operation'=ANY(lot.operations) THEN CONTINUE;END IF;
    take=least(remaining,lot.available_atoms);remaining=remaining-take;sequence=sequence+1;
    lot.available_atoms=lot.available_atoms-take;lot.reserved_atoms=lot.reserved_atoms+take;
    local_lots[i]=lot;
    local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('hold_id',new_id,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));
    legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take),money_leg('lot:'||lot.id||':reserved',take));
   END LOOP;
   credit_part=remaining;funded_part=atoms-credit_part;
   IF wallet.debt_atoms+wallet.credit_reserved_atoms+credit_part>wallet.credit_limit_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','P0402');CONTINUE;END IF;
   IF wallet.reserved_atoms+funded_part>max_atoms OR wallet.credit_reserved_atoms+credit_part>max_atoms THEN outcomes[item.ordinality]=jsonb_build_object('error','22003');CONTINUE;END IF;
   IF p->>'digest' IS NOT NULL THEN
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','RUNNING',quote->>'currency',quote,'PENDING') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    IF NOT FOUND THEN
     SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   ELSE
    pending_request.id=new_id;pending_request.project_id=credential.project_id;pending_request.key_id=credential.id;
    pending_request.operation=p->>'operation';pending_request.model=p->>'model';pending_request.intent_digest=p->>'digest';pending_request.fingerprint=p->>'fingerprint';pending_request.billing_quote=quote;
    staged_requests=array_append(staged_requests,pending_request);
   END IF;
   wallet.available_atoms=wallet.available_atoms-funded_part;wallet.reserved_atoms=wallet.reserved_atoms+funded_part;wallet.credit_reserved_atoms=wallet.credit_reserved_atoms+credit_part;
   wallet_rows[wi]=wallet;wallet_dirty[wi]=atoms>0 OR wallet_dirty[wi];
   FOR i IN 1..cardinality(local_lots) LOOP
    li=first_lot+i-1;IF lot_rows[li] IS DISTINCT FROM local_lots[i] THEN lot_rows[li]=local_lots[i];lot_dirty[li]=true;END IF;
   END LOOP;
   pending_hold.id=new_id;pending_hold.wallet_id=wallet.id;pending_hold.amount_atoms=atoms;pending_hold.credit_atoms=credit_part;
   staged_holds=array_append(staged_holds,pending_hold);
   staged_allocations=array_append(staged_allocations,local_allocations);
   IF funded_part>0 THEN staged_journals=array_append(staged_journals,jsonb_build_object('id',ledger_uuid(),'wallet_id',wallet.id,'reference','hold:'||new_id,'kind','HOLD','currency',wallet.currency,'lines',legs));END IF;
   pending_attempt.id=attempt_id;pending_attempt.request_id=new_id;pending_attempt.provider_id=route.provider_id;pending_attempt.upstream_model=route.upstream_model;pending_attempt.pricing_id=(p->>'cost_pricing_id')::uuid;pending_attempt.billing_pricing_id=pricing;
   staged_attempts=array_append(staged_attempts,pending_attempt);
   result=jsonb_build_object('created',true,'record',jsonb_build_object('id',new_id,'state','RUNNING','fingerprint',p->>'fingerprint'),'attempt_id',attempt_id);
  outcomes[item.ordinality]=result;
 END LOOP;
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'RUNNING',q.billing_quote->>'currency',q.billing_quote,'PENDING'
 FROM unnest(staged_requests) q;
 INSERT INTO billing_holds(id,request_id,wallet_id,amount_atoms,credit_atoms,next_reconcile_at) SELECT h.id,h.id,h.wallet_id,h.amount_atoms,h.credit_atoms,now()+interval '30 seconds' FROM unnest(staged_holds) h;
 INSERT INTO hold_allocations(hold_id,lot_id,ordinal,amount_atoms) SELECT a.hold_id,a.lot_id,a.ordinal,a.amount_atoms
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(hold_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric);
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,credit_reserved_atoms=state.credit_reserved_atoms
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(to_jsonb(staged_journals));
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING' FROM unnest(staged_attempts) a;
 SELECT jsonb_object_agg((n-1)::text,outcomes[n]) INTO results FROM generate_subscripts(outcomes,1) input(n);
 RETURN results;
 EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
 END;
 IF jsonb_array_length(p_inputs)=1 THEN RETURN jsonb_build_object('0',jsonb_build_object('error',failure));END IF;
 midpoint=jsonb_array_length(p_inputs)/2;
 SELECT jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality<=midpoint),jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality>midpoint)
 INTO left_inputs,right_inputs FROM jsonb_array_elements(p_inputs) WITH ORDINALITY;
 left_results=allocate_customer_batch_v1(left_inputs);right_results=allocate_customer_batch_v1(right_inputs);
 SELECT left_results||jsonb_object_agg((key::integer+midpoint)::text,value) INTO results FROM jsonb_each(right_results);
 RETURN results;
END $function$;

CREATE OR REPLACE FUNCTION public.admit_gateway_batch(p_inputs jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE inputs jsonb;BEGIN
 SELECT jsonb_agg(value||jsonb_build_object('attempt_id',public.ledger_uuid()) ORDER BY n) INTO inputs FROM jsonb_array_elements(p_inputs) WITH ORDINALITY a(value,n);
 RETURN public.allocate_customer_batch_v1(inputs);
END $$;
REVOKE ALL ON FUNCTION public.allocate_customer_batch_v1(jsonb) FROM PUBLIC;
CREATE FUNCTION public.core_permit_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','granted_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','granted_at']) OR NOT (NEW.state=OLD.state OR OLD.state='RESERVED' AND NEW.state IN ('SEND_GRANTED','ABORTED') OR OLD.state='SEND_GRANTED' AND NEW.state IN ('UNKNOWN','TERMINAL') OR OLD.state='UNKNOWN' AND NEW.state='TERMINAL') THEN RAISE EXCEPTION 'permit_immutable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;
CREATE TRIGGER core_permit_guard BEFORE UPDATE OR DELETE ON public.execution_permits FOR EACH ROW EXECUTE FUNCTION public.core_permit_guard_v1();
CREATE TRIGGER core_context_immutable BEFORE UPDATE OR DELETE ON public.execution_contexts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE FUNCTION public.core_quota_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','revision','descriptor']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','revision','descriptor']) OR NEW.revision<OLD.revision OR OLD.state IN ('RELEASE_PENDING','RELEASED','EXPIRED') AND NEW.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;
CREATE TRIGGER core_quota_guard BEFORE UPDATE OR DELETE ON public.execution_quota_leases FOR EACH ROW EXECUTE FUNCTION public.core_quota_guard_v1();
CREATE FUNCTION public.core_upstream_journal_v1(account uuid,cur text,channel uuid,attempt uuid,kind text,amount numeric) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE journal uuid=gen_random_uuid();BEGIN
 IF kind NOT IN ('RESERVE','RELEASE') OR amount IS NULL OR amount<0 OR NOT EXISTS(SELECT 1 FROM public.upstream_holds h WHERE h.attempt_id=attempt AND h.account_id=account AND h.channel_id=channel AND h.currency=cur) THEN RAISE EXCEPTION 'journal_ownership';END IF;
 INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference) VALUES(journal,account,channel,attempt,cur,kind,lower(kind)||':'||attempt);
 INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms) VALUES(journal,0,cur,'upstream:available',CASE WHEN kind='RESERVE' THEN -amount ELSE amount END),(journal,1,cur,'upstream:reserved',CASE WHEN kind='RESERVE' THEN amount ELSE -amount END);
 SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
 SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
END $$;
CREATE FUNCTION public.core_execution_v1(p jsonb,e jsonb,d jsonb,computed jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
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
    IF qty<0 OR qty>bound OR dim='requests' AND qty<1 THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
    estimated=estimated||jsonb_build_object(dim,qty::text);
   END LOOP;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'input_tokens' THEN RAISE EXCEPTION 'cost_unbounded' USING ERRCODE='P0422';END IF;
   IF ctx.operation IN ('chat','responses','messages','gemini','completions') AND NOT manifest->'ceilings'?'output_tokens' OR (manifest->'ceilings'->>'output_tokens')::numeric>(SELECT max_output_tokens FROM public.models WHERE name=ctx.model) THEN RAISE EXCEPTION 'output_bound' USING ERRCODE='P0422';END IF;
   IF c->'config'?'max_output_tokens' AND (manifest->'ceilings'->>'output_tokens')::numeric>(c->'config'->>'max_output_tokens')::numeric OR c->'config'?'max_input_tokens' AND (manifest->'ceilings'->>'input_tokens')::numeric>(c->'config'->>'max_input_tokens')::numeric THEN RAISE EXCEPTION 'cost_ceiling' USING ERRCODE='P0422';END IF;
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
CREATE FUNCTION public.core_preflight_v1(p jsonb,context_id uuid,attempt_id uuid) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE ctx public.execution_contexts%ROWTYPE;permit public.execution_permits%ROWTYPE;channel public.channels%ROWTYPE;auth jsonb;leases jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF attempt_id IS NOT NULL THEN
  SELECT * INTO permit FROM public.execution_permits ep WHERE ep.attempt_id=core_preflight_v1.attempt_id;
  IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.owner_key_sha256 IS DISTINCT FROM p->>'key_sha256' OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.owner_service IS DISTINCT FROM p->>'service' OR permit.certificate_sha256 IS DISTINCT FROM p->>'certificate_sha256' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
  context_id=permit.context_id;
 END IF;
 SELECT * INTO ctx FROM public.execution_contexts ec WHERE ec.id=context_id;
 IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF p->>'command_type'<>'execution_quota_v1' THEN auth=public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('state',l.state,'attempt_id',l.attempt_id,'permit_epoch',ep.permit_epoch::text,'descriptor',l.descriptor) ORDER BY l.lease_id),'[]'::jsonb) INTO leases FROM public.execution_quota_leases l JOIN public.execution_permits ep ON ep.attempt_id=l.attempt_id WHERE l.request_id=(SELECT request_id FROM public.attempts WHERE id=core_preflight_v1.attempt_id);
 RETURN ctx.snapshot||jsonb_build_object('leases',leases,'permit_state',permit.state,'permit_epoch',permit.permit_epoch::text,'execution_closed',(SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=core_preflight_v1.attempt_id)));
END $$;
CREATE FUNCTION public.core_quota_v1(p jsonb,attempt uuid,action text,items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;item jsonb;l public.execution_quota_leases%ROWTYPE;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' NOT IN ('execution_admit_v1','execution_next_v1','execution_begin_v1','execution_abort_v1','execution_quota_v1') OR action IS NULL OR action NOT IN ('ACTIVE','RELEASED') OR jsonb_typeof(items) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=attempt FOR NO KEY UPDATE NOWAIT;
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR NOT (l.attempt_id=attempt OR l.scope_kind<>'channel' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt) OR action='RELEASED' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt)) OR (l.descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) IS DISTINCT FROM (item-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) OR (item->>'revision')::numeric<l.revision THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
  IF action='ACTIVE' THEN
   IF (SELECT execution_closed_at FROM public.gateway_requests WHERE id=l.request_id) IS NOT NULL OR l.state NOT IN ('ADOPTION_PENDING','ACTIVE') OR permit.state NOT IN ('RESERVED','SEND_GRANTED','UNKNOWN') OR item->>'state' IS DISTINCT FROM 'ADOPTED' OR item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text OR item->>'permit_epoch' IS DISTINCT FROM permit.permit_epoch::text OR (item->>'remaining_ms')::numeric<=1000 THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  ELSE
   IF l.state NOT IN ('RELEASE_PENDING','RELEASED') OR item->>'state' IS DISTINCT FROM 'RELEASED' THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  END IF;
  UPDATE public.execution_quota_leases SET state=action,revision=(item->>'revision')::numeric,descriptor=item WHERE lease_id=l.lease_id;
 END LOOP;RETURN jsonb_build_object('updated',jsonb_array_length(items));
END $$;
CREATE FUNCTION public.core_lookup_v1(p jsonb,e jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE cmd public.execution_commands%ROWTYPE;ctx public.execution_contexts%ROWTYPE;BEGIN
 PERFORM public.core_caller_v1(p);
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=p->>'service'||':'||(p->>'instance_id') AND kind=p->>'command_type' AND command_id=(e->>'command_id')::uuid;
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO ctx FROM public.execution_contexts WHERE id=cmd.context_id;
 PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);
 IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
 RETURN cmd.receipt;
END $$;
DO $$ DECLARE f record;BEGIN
 FOR f IN SELECT p.oid::regprocedure AS signature FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND (p.proname LIKE 'core_%_v1' OR p.proname='allocate_customer_batch_v1') LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',f.signature);END LOOP;
END $$;
CREATE FUNCTION public.core_platform_authorize_v1(proof jsonb,permission text) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE c public.administrator_credentials%ROWTYPE;a public.technical_administrators%ROWTYPE;BEGIN
 IF permission IS NULL OR permission NOT IN ('catalog:manage','funding:read','funding:recharge','customer:fund') OR jsonb_typeof(proof) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 IF proof=jsonb_build_object('identity','bootstrap:ADMIN_KEY','bootstrap',true) THEN RETURN 'bootstrap:ADMIN_KEY';END IF;
 IF proof-ARRAY['identity','credentialId','administratorId','verifier']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['identity','credentialId','administratorId','verifier'])f WHERE jsonb_typeof(proof->f) IS DISTINCT FROM 'string' OR proof->>f='') THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.administrator_credentials WHERE id=(proof->>'credentialId')::uuid FOR SHARE;
 SELECT * INTO a FROM public.technical_administrators WHERE id=(proof->>'administratorId')::uuid FOR SHARE;
 IF c.id IS NULL OR a.id IS NULL OR c.administrator_id IS DISTINCT FROM a.id OR proof->>'identity' IS DISTINCT FROM 'technical-admin:'||a.id::text OR c.verifier IS DISTINCT FROM proof->>'verifier' OR (c.enabled AND a.enabled AND permission=ANY(a.permissions) AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())) IS NOT TRUE THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 RETURN proof->>'identity';
END $$;
REVOKE ALL ON FUNCTION public.core_platform_authorize_v1(jsonb,text) FROM PUBLIC;

CREATE FUNCTION public.core_quota_close_v1(p jsonb,e jsonb,d jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE permit public.execution_permits%ROWTYPE;cmd public.execution_commands%ROWTYPE;qid uuid;caller text=p->>'service'||':'||(p->>'instance_id');receipt jsonb;BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.execution.v1:command:'||caller||':execution_quota_v1:'||(e->>'command_id'),0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_quota_v1' OR d->>'action' IS DISTINCT FROM 'CLOSE' OR (d->>'reason' IN ('COMPLETED','FAILED','CANCELLED','DRAINED')) IS NOT TRUE OR e->>'command_id' IS DISTINCT FROM p->>'command_id' OR e->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR public.core_digest_v1(jsonb_build_object('kind','execution_quota_v1','contract_version',1,'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'deadline_at',p->>'deadline_at','data',d)) IS DISTINCT FROM e->>'body_digest' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND kind='execution_quota_v1' AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;RETURN cmd.receipt;
 END IF;
 SELECT request_id INTO qid FROM public.attempts WHERE id=(d->>'attempt_id')::uuid;
 PERFORM id FROM public.gateway_requests WHERE id=qid FOR UPDATE NOWAIT;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid FOR NO KEY UPDATE NOWAIT;
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.permit_epoch::text IS DISTINCT FROM d->>'permit_epoch' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()) WHERE id=qid;
 UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id=qid AND state IN ('ADOPTION_PENDING','ACTIVE');
 receipt=jsonb_build_object('command_state','COMMITTED','code','execution_closed','request_id',qid,'attempt_id',permit.attempt_id);
 INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(gen_random_uuid(),(e->>'command_id')::uuid,'execution_quota_v1',caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,permit.attempt_id,permit.context_id,receipt);
 RETURN receipt;
END $$;
REVOKE ALL ON FUNCTION public.core_quota_close_v1(jsonb,jsonb,jsonb) FROM PUBLIC;
CREATE FUNCTION public.core_request_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF OLD.execution_budget IS NOT NULL AND NEW.execution_budget IS DISTINCT FROM OLD.execution_budget OR OLD.execution_closed_at IS NOT NULL AND NEW.execution_closed_at IS DISTINCT FROM OLD.execution_closed_at THEN RAISE EXCEPTION 'execution_budget_immutable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;
CREATE TRIGGER core_request_guard BEFORE UPDATE ON public.gateway_requests FOR EACH ROW EXECUTE FUNCTION public.core_request_guard_v1();
REVOKE ALL ON FUNCTION public.core_request_guard_v1() FROM PUBLIC;

CREATE FUNCTION public.core_hold_guard_v1() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','final_cost_atoms']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','final_cost_atoms']) THEN RAISE EXCEPTION 'hold_snapshot_immutable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;
CREATE TRIGGER core_hold_guard BEFORE UPDATE OR DELETE ON public.upstream_holds FOR EACH ROW EXECUTE FUNCTION public.core_hold_guard_v1();
REVOKE ALL ON FUNCTION public.core_hold_guard_v1() FROM PUBLIC;
CREATE FUNCTION public.core_execution_batch_v1(items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE item jsonb;result jsonb;results jsonb='[]';failure text;message text;BEGIN
 IF jsonb_typeof(items) IS DISTINCT FROM 'array' OR jsonb_array_length(items) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
  BEGIN
   result=public.core_execution_v1(item->'projection',item->'envelope',item->'data',item->'computed');
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
   result=jsonb_build_object('error',CASE WHEN failure IN ('55P03','P0503','40P01') THEN 'identity_busy' WHEN message IN ('command_conflict','customer_forbidden','internal_identity_revoked','internal_session_invalid','internal_peer_forbidden','internal_owner_mismatch','context_invalid','internal_deadline') THEN message ELSE 'identity_store_unavailable' END);
  END;
  results=results||jsonb_build_array(result);
 END LOOP;RETURN results;
END $$;
REVOKE ALL ON FUNCTION public.core_execution_batch_v1(jsonb) FROM PUBLIC;

CREATE FUNCTION public.core_quota_recovery_v1(p jsonb,items jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE item jsonb;l public.execution_quota_leases%ROWTYPE;result jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_quota_recovery_v1' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(items)='number' AND items::text::integer BETWEEN 1 AND 100 THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('attempt_id',attempt_id,'descriptor',descriptor)),'[]'::jsonb) INTO result FROM (SELECT attempt_id,descriptor FROM public.execution_quota_leases WHERE state='RELEASE_PENDING' ORDER BY lease_id LIMIT items::text::integer) pending;RETURN result;
 END IF;
 IF jsonb_typeof(items) IS DISTINCT FROM 'array' OR jsonb_array_length(items)>100 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR l.state NOT IN ('RELEASE_PENDING','RELEASED') OR item->>'state' IS DISTINCT FROM 'RELEASED' OR (l.descriptor-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) IS DISTINCT FROM (item-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) OR (item->>'revision')::numeric<l.revision OR (item->>'attempt_id' IS NOT NULL AND item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text) OR (item->>'permit_epoch' IS NOT NULL AND item->>'permit_epoch' IS DISTINCT FROM '1') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
  UPDATE public.execution_quota_leases SET state='RELEASED',descriptor=item WHERE lease_id=l.lease_id;
 END LOOP;RETURN jsonb_build_object('released',jsonb_array_length(items));
END $$;
REVOKE ALL ON FUNCTION public.core_quota_recovery_v1(jsonb,jsonb) FROM PUBLIC;
