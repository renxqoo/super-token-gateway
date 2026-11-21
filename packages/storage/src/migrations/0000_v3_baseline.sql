SET check_function_bodies = false;

DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'core_identity_owner') THEN CREATE ROLE core_identity_owner NOLOGIN; END IF; END $$;

CREATE SCHEMA core_catalog_cache;

CREATE SCHEMA core_execution_runtime;

CREATE SCHEMA core_funding;

CREATE TABLE public.upstream_holds (
    attempt_id uuid NOT NULL,
    account_id uuid NOT NULL,
    channel_id uuid NOT NULL,
    currency text NOT NULL,
    estimated_atoms numeric(38,0) NOT NULL,
    cost_version_id uuid NOT NULL,
    state text NOT NULL,
    final_cost_atoms numeric(38,0),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    estimated_quantities jsonb,
    estimated_quantities_digest text,
    estimate_manifest_digest text,
    estimate_algorithm_version integer,
    funding_cell smallint NOT NULL,
    current_reserved_atoms numeric(38,0) NOT NULL,
    CONSTRAINT upstream_hold_amounts CHECK (((estimated_atoms >= (0)::numeric) AND (current_reserved_atoms >= (0)::numeric) AND ((final_cost_atoms IS NULL) OR (final_cost_atoms >= (0)::numeric)))),
    CONSTRAINT upstream_hold_state CHECK ((state = ANY (ARRAY['HELD'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'SETTLED'::text, 'RELEASED'::text])))
);

CREATE TYPE core_funding.evidence_attempt_stage_v1 AS (
	id uuid,
	request_id uuid,
	project_id uuid,
	currency text,
	hold public.upstream_holds,
	rates jsonb,
	prices jsonb,
	basis text,
	revision numeric,
	head_id uuid,
	usage jsonb,
	finals text[],
	seen_final boolean,
	sources jsonb,
	state text,
	finished_at timestamp with time zone,
	error_code text,
	provider_request_id text,
	dirty boolean,
	touched_canonical uuid,
	touched_revision text,
	money boolean
);

CREATE TYPE core_funding.evidence_request_stage_v1 AS (
	id uuid,
	selected_attempt_id uuid,
	state text,
	billing_outcome text,
	updated_at timestamp with time zone,
	dirty boolean
);

CREATE TABLE public.usage_event_index (
    id uuid NOT NULL,
    seq bigint NOT NULL,
    project_id uuid NOT NULL,
    request_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    source text NOT NULL,
    finality text NOT NULL,
    cost_atoms numeric(38,0),
    currency text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    cost_complete boolean DEFAULT false NOT NULL,
    record_kind text DEFAULT 'GATEWAY'::text NOT NULL,
    snapshot_revision numeric(38,0),
    snapshot_digest text,
    final_dimensions text[],
    parent_evidence_id uuid,
    input_fact_id uuid,
    CONSTRAINT usage_events_cost_atoms_check CHECK ((cost_atoms >= (0)::numeric)),
    CONSTRAINT usage_events_finality_check CHECK ((finality = ANY (ARRAY['FINAL'::text, 'PARTIAL'::text, 'UNKNOWN'::text, 'ESTIMATED'::text]))),
    CONSTRAINT usage_record_kind CHECK (((record_kind = ANY (ARRAY['GATEWAY'::text, 'RAW'::text, 'CANONICAL'::text, 'DISPUTED'::text])) AND
CASE record_kind
    WHEN 'GATEWAY'::text THEN ((cost_atoms IS NOT NULL) AND (snapshot_revision IS NULL) AND (snapshot_digest IS NULL) AND (final_dimensions IS NULL) AND (parent_evidence_id IS NULL) AND (input_fact_id IS NULL))
    WHEN 'RAW'::text THEN ((cost_atoms IS NULL) AND (NOT cost_complete) AND (snapshot_revision >= (1)::numeric) AND (snapshot_digest ~ '^[a-f0-9]{64}$'::text) AND (final_dimensions IS NOT NULL) AND (parent_evidence_id IS NULL) AND (input_fact_id IS NULL))
    WHEN 'CANONICAL'::text THEN ((cost_atoms IS NOT NULL) AND (snapshot_revision >= (1)::numeric) AND (snapshot_digest ~ '^[a-f0-9]{64}$'::text) AND (final_dimensions IS NOT NULL) AND (input_fact_id IS NOT NULL))
    ELSE ((snapshot_revision >= (1)::numeric) AND (snapshot_digest ~ '^[a-f0-9]{64}$'::text) AND (final_dimensions IS NOT NULL) AND (input_fact_id IS NOT NULL) AND ((cost_atoms IS NOT NULL) OR (NOT cost_complete)))
END))
);

CREATE TYPE core_funding.evidence_snapshot_stage_v1 AS (
	fact public.usage_event_index,
	usage jsonb
);

CREATE TYPE core_funding.first_final_cost_v1 AS (
	hold public.upstream_holds,
	evidence_id uuid,
	snapshot_revision numeric,
	snapshot_digest text,
	final_dimensions text[],
	cost_atoms numeric,
	journal_id uuid
);

CREATE FUNCTION core_catalog_cache.compute_v2(model text, op text, cur text) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE m public.models%ROWTYPE;r record;semantic jsonb;ref jsonb;candidate jsonb;result jsonb='[]';BEGIN
 SELECT * INTO m FROM public.models WHERE name=model FOR SHARE NOWAIT;
 IF NOT FOUND OR NOT m.enabled OR (op=ANY(m.operations)) IS NOT TRUE THEN RETURN result;END IF;
 PERFORM id FROM public.routes WHERE model_name=model ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.billing_tariffs WHERE billing_tariffs.model=compute_v2.model AND operation=op AND currency=cur ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.supplier_brands WHERE id IN(SELECT brand_id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.plugin_versions WHERE id IN(SELECT b.plugin_version_id FROM public.channel_plugin_bindings b JOIN public.channels c ON c.binding_id=b.id WHERE c.id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 PERFORM id FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.channels WHERE id IN(SELECT provider_id FROM public.routes WHERE model_name=model)) ORDER BY id FOR SHARE NOWAIT;
 FOR r IN SELECT rt,c,b,v,cv,bp,t.id AS tariff_id FROM public.routes rt JOIN public.channels c ON c.id=rt.provider_id JOIN public.supplier_brands br ON br.id=c.brand_id JOIN public.upstream_accounts a ON a.id=c.account_id JOIN public.channel_plugin_bindings b ON b.id=c.binding_id JOIN public.plugin_versions v ON v.id=b.plugin_version_id JOIN public.plugin_publication_receipts pub ON pub.version_id=v.id JOIN public.plugin_packages p ON p.digest=v.package_digest JOIN public.cost_versions cv ON cv.id=rt.pricing_id JOIN LATERAL(SELECT * FROM public.billing_tariffs t WHERE t.model=compute_v2.model AND t.operation=op AND t.currency=cur AND t.enabled AND (t.provider_id IS NULL OR t.provider_id=c.id) ORDER BY (t.provider_id IS NOT NULL) DESC LIMIT 1)t ON true JOIN public.billing_prices bp ON bp.id=t.pricing_id WHERE rt.model_name=compute_v2.model AND rt.enabled AND c.enabled AND br.enabled AND v.state='PUBLISHED' AND NOT v.quarantined AND a.mode='PREPAID' AND a.state='ACTIVE' AND op=ANY(rt.operations) AND b.channel_id=c.id AND b.account_id=c.account_id AND b.brand_id=c.brand_id AND b.config_revision=c.config_revision AND EXISTS(SELECT 1 FROM jsonb_array_elements_text(p.manifest->'capabilities'->'operations') x WHERE x=op OR (op IN ('responses','messages','gemini') AND x='chat')) ORDER BY rt.priority DESC,rt.id LIMIT 128 LOOP
  semantic=jsonb_build_object('model',to_jsonb(m),'route',to_jsonb(r.rt),'channel',to_jsonb(r.c)-'secret','ciphertext_sha256',encode(sha256(convert_to((r.c).secret,'UTF8')),'hex'),'binding',to_jsonb(r.b),'plugin',to_jsonb(r.v),'tariff_id',r.tariff_id,'price_id',(r.bp).id);
  ref=jsonb_build_object('route_id',(r.rt).id,'channel_id',(r.c).id,'binding_id',(r.b).id,'plugin_version_id',(r.v).id,'package_digest',(r.v).package_digest,'config_revision',(r.b).config_revision::text,'config_digest',(r.b).config_digest,'cost_version_id',(r.cv).id,'customer_price_id',(r.bp).id,'candidate_digest',public.core_digest_v1(semantic));
  candidate=jsonb_build_object('ref',ref,'origin',regexp_replace((r.c).base_url,'^(https://[^/]+).*$', '\1'),'upstream_model',(r.rt).upstream_model,'credential_ciphertext_sha256',encode(sha256(convert_to((r.c).secret,'UTF8')),'hex'),'config',(r.b).config,'cost_currency',(r.cv).currency,'cost_rates',(r.cv).rates,'cost_ceilings',(r.cv).ceilings,'customer_currency',(r.bp).currency,'customer_rates',(r.bp).rates,'customer_ceilings',(r.bp).ceilings,'customer_basis',(r.bp).basis,'request_policy',(r.rt).request_policy,'routing',jsonb_build_object('priority',(r.rt).priority,'weight',(r.rt).weight),'channel_limits',jsonb_build_object('rpm',(r.c).rpm,'concurrency',(r.c).concurrency));
  result=result||jsonb_build_array(candidate);
 END LOOP;RETURN result;
END $_$;

CREATE FUNCTION core_catalog_cache.invalidate_v2() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE before_value jsonb;after_value jsonb;new_revision numeric;BEGIN
 IF TG_OP='UPDATE' THEN
  before_value=to_jsonb(OLD);after_value=to_jsonb(NEW);
  CASE TG_TABLE_NAME
   WHEN 'supplier_brands' THEN before_value=jsonb_build_array(OLD.id,OLD.enabled);after_value=jsonb_build_array(NEW.id,NEW.enabled);
   WHEN 'upstream_accounts' THEN before_value=jsonb_build_array(OLD.id,OLD.mode,OLD.state);after_value=jsonb_build_array(NEW.id,NEW.mode,NEW.state);
   WHEN 'cost_versions' THEN before_value=jsonb_build_array(OLD.id,OLD.currency,OLD.rates,OLD.ceilings);after_value=jsonb_build_array(NEW.id,NEW.currency,NEW.rates,NEW.ceilings);
   WHEN 'billing_prices' THEN before_value=jsonb_build_array(OLD.id,OLD.currency,OLD.rates,OLD.ceilings,OLD.basis);after_value=jsonb_build_array(NEW.id,NEW.currency,NEW.rates,NEW.ceilings,NEW.basis);
   WHEN 'plugin_packages' THEN before_value=jsonb_build_array(OLD.digest,OLD.manifest->'capabilities'->'operations');after_value=jsonb_build_array(NEW.digest,NEW.manifest->'capabilities'->'operations');
   WHEN 'plugin_publication_receipts' THEN before_value=to_jsonb(OLD.version_id);after_value=to_jsonb(NEW.version_id);
   ELSE NULL;
  END CASE;
  IF before_value IS NOT DISTINCT FROM after_value THEN RETURN NULL;END IF;
 END IF;
 UPDATE core_catalog_cache.epoch SET revision=revision+1 WHERE singleton RETURNING revision INTO STRICT new_revision;
 DELETE FROM core_catalog_cache.candidates;
 UPDATE core_catalog_cache.usage SET byte_count=0,entry_count=0 WHERE singleton;
 IF NOT FOUND THEN RAISE EXCEPTION 'catalog_cache_usage_missing';END IF;
 PERFORM pg_notify('stg_catalog',new_revision::text);
 RETURN NULL;
END $$;

CREATE FUNCTION core_execution_runtime.allowed_v1(candidates jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION core_execution_runtime.approve_v1(package text, runtime jsonb, proof jsonb, enabled boolean, who text, why text) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION core_execution_runtime.audit_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
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

CREATE FUNCTION core_funding.account_key_v1(account uuid) RETURNS bigint
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT hashtextextended('stg.funding.account.v1:'||account::text,0)
$$;

CREATE FUNCTION core_funding.apply_v1(attempt uuid, cost_delta numeric, new_reserve numeric, advance boolean) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE h public.upstream_holds%ROWTYPE;c core_funding.balance_cells%ROWTYPE;BEGIN
 SELECT * INTO STRICT h FROM public.upstream_holds WHERE attempt_id=attempt FOR NO KEY UPDATE NOWAIT;
 IF NOT core_funding.held_v1(h.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF cost_delta IS NULL OR new_reserve IS NULL OR advance IS NULL OR cost_delta<0 OR new_reserve<0 OR cost_delta<>trunc(cost_delta) OR new_reserve<>trunc(new_reserve) THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=h.account_id AND currency=h.currency AND cell=h.funding_cell FOR NO KEY UPDATE NOWAIT;
 IF c.spent_atoms+cost_delta>c.spent_limit OR c.revision+advance::integer>c.revision_limit OR c.balance_atoms-cost_delta<c.reserved_atoms+new_reserve-h.current_reserved_atoms OR NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=h.account_id AND state='ACTIVE') THEN
  IF NOT core_funding.held_v1(h.account_id,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  PERFORM core_funding.headroom_v1(h.account_id,h.currency,h.funding_cell,c.spent_atoms+cost_delta,c.revision+advance::integer);
 END IF;
 UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms-cost_delta,spent_atoms=spent_atoms+cost_delta,reserved_atoms=reserved_atoms+new_reserve-h.current_reserved_atoms,revision=revision+advance::integer WHERE account_id=h.account_id AND currency=h.currency AND cell=h.funding_cell;
 UPDATE public.upstream_holds SET current_reserved_atoms=new_reserve WHERE attempt_id=attempt;
END $$;

CREATE FUNCTION core_funding.audit_v1(account uuid, cur text) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE result jsonb;BEGIN
 WITH cells AS (
  SELECT c.*,coalesce(t.balance,0) budget,coalesce(t.spent_allocation,0) history,coalesce(t.spent_limit,0) allocated_spent_limit,coalesce(t.revision_limit,0) allocated_revision_limit
  FROM core_funding.balance_cells c LEFT JOIN LATERAL (
   SELECT sum(t.balance_delta) balance,sum(t.spent_allocation_atoms) spent_allocation,sum(t.spent_limit_delta) spent_limit,sum(t.revision_limit_delta) revision_limit
   FROM core_funding.budget_transfers t WHERE t.account_id=c.account_id AND t.currency=c.currency AND t.cell=c.cell
  ) t ON true WHERE c.account_id=account AND c.currency=cur
 ), totals AS (
  SELECT coalesce(sum(balance_atoms),0) balance,coalesce(sum(reserved_atoms),0) reserved,coalesce(sum(spent_atoms),0) spent,coalesce(sum(revision),0) revision,
   coalesce(bool_and(balance_atoms+spent_atoms=budget+history AND spent_limit=allocated_spent_limit AND revision_limit=allocated_revision_limit),true) conserved
  FROM cells
 ) SELECT jsonb_build_object('balance_atoms',(b.balance_atoms+t.balance)::text,'reserved_atoms',t.reserved::text,'spent_atoms',(b.spent_atoms+t.spent)::text,'revision',(b.revision+t.revision)::text,
  'available_atoms',(b.balance_atoms+t.balance-t.reserved)::text,'adjustment_atoms',b.adjustment_atoms::text,'root_reserved_atoms',b.reserved_atoms::text,'cells_conserved',t.conserved)
 INTO result FROM public.upstream_account_balances b CROSS JOIN totals t WHERE b.account_id=account AND b.currency=cur;
 IF result->>'root_reserved_atoms'<>'0' THEN RAISE EXCEPTION 'funding_root_not_migrated' USING ERRCODE='P0422';END IF;
 RETURN result;
END $$;

CREATE TABLE public.api_keys (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name text NOT NULL,
    verifier text NOT NULL,
    prefix text NOT NULL,
    models text[] NOT NULL,
    operations text[] NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    principal_id uuid,
    role text DEFAULT 'INVOKER'::text NOT NULL,
    allowed_cidrs text[] DEFAULT '{}'::text[] NOT NULL,
    rpm integer,
    tpm bigint,
    concurrency integer,
    CONSTRAINT api_keys_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT api_keys_role_check CHECK ((role = ANY (ARRAY['ADMIN'::text, 'INVOKER'::text, 'BILLING'::text, 'VIEWER'::text]))),
    CONSTRAINT api_keys_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT api_keys_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);

CREATE TABLE public.organizations (
    id uuid NOT NULL,
    external_ref text NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer DEFAULT 100000 NOT NULL,
    tpm bigint DEFAULT 1000000000 NOT NULL,
    concurrency integer DEFAULT 10000 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT organizations_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT organizations_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT organizations_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);

CREATE TABLE public.principals (
    id uuid NOT NULL,
    organization_id uuid NOT NULL,
    external_ref text NOT NULL,
    role text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer DEFAULT 10000 NOT NULL,
    tpm bigint DEFAULT 100000000 NOT NULL,
    concurrency integer DEFAULT 1000 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT principals_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT principals_role_check CHECK ((role = ANY (ARRAY['OWNER'::text, 'ADMIN'::text, 'MEMBER'::text, 'BILLING'::text, 'VIEWER'::text]))),
    CONSTRAINT principals_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT principals_tpm_check CHECK (((tpm > 0) AND (tpm <= '9007199254740991'::bigint)))
);

CREATE TABLE public.projects (
    id uuid NOT NULL,
    name text NOT NULL,
    external_ref text,
    enabled boolean DEFAULT true NOT NULL,
    rpm integer NOT NULL,
    tpm integer NOT NULL,
    concurrency integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    billing_currency text DEFAULT 'USD'::text NOT NULL,
    organization_id uuid,
    CONSTRAINT projects_billing_currency_check CHECK ((billing_currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT projects_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT projects_rpm_check CHECK ((rpm > 0)),
    CONSTRAINT projects_tpm_check CHECK ((tpm > 0))
);

CREATE FUNCTION core_funding.auth_decision_v1(k public.api_keys, pr public.projects, o public.organizations, u public.principals, ip text, op text, model text, asof timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE scopes jsonb='[]';BEGIN
 IF k.id IS NULL OR pr.id IS NULL OR (k.enabled AND pr.enabled AND k.role IN ('ADMIN','INVOKER') AND (k.expires_at IS NULL OR k.expires_at>asof) AND op=ANY(k.operations) AND (cardinality(k.models)=0 OR model=ANY(k.models)) AND (pr.organization_id IS NULL OR o.enabled) AND (k.principal_id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN','MEMBER') AND u.organization_id=pr.organization_id)) IS NOT TRUE OR (cardinality(k.allowed_cidrs)>0 AND NOT EXISTS(SELECT 1 FROM unnest(k.allowed_cidrs) c WHERE ip::inet <<= c::inet)) THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 IF o.id IS NOT NULL THEN scopes=scopes||jsonb_build_array(jsonb_build_object('kind','organization','id',o.id,'rpm',o.rpm,'tpm',o.tpm,'concurrency',o.concurrency));END IF;
 IF u.id IS NOT NULL THEN scopes=scopes||jsonb_build_array(jsonb_build_object('kind','principal','id',u.id,'rpm',u.rpm,'tpm',u.tpm,'concurrency',u.concurrency));END IF;
 scopes=scopes||jsonb_build_array(jsonb_build_object('kind','project','id',pr.id,'rpm',pr.rpm,'tpm',pr.tpm,'concurrency',pr.concurrency),jsonb_build_object('kind','key','id',k.id,'rpm',coalesce(k.rpm,pr.rpm),'tpm',coalesce(k.tpm,pr.tpm),'concurrency',coalesce(k.concurrency,pr.concurrency)));
 RETURN jsonb_build_object('project_id',pr.id,'key_id',k.id,'customer_currency',pr.billing_currency,'scopes',scopes);
END $$;

CREATE FUNCTION core_funding.bill_first_final_v1(p_requests uuid[], snapshots core_funding.evidence_snapshot_stage_v1[]) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE receipts jsonb;journals jsonb;
BEGIN
 WITH facts AS MATERIALIZED (
  SELECT (f).id,(f).attempt_id,(f).request_id,(f).source,(f).finality,x.usage
  FROM unnest(snapshots) x(f,usage) WHERE (f).record_kind='CANONICAL'
 ), targets AS MATERIALIZED (
  SELECT h.* FROM billing_holds h WHERE h.request_id=ANY(p_requests)
 ), candidates AS MATERIALIZED (
  SELECT h.id AS hold_id,h.request_id,h.wallet_id,h.amount_atoms AS held,w.currency,
   l.id AS lot_id,p.id AS pricing_id,e.id AS evidence_id,exact_billing_price(CASE p.basis WHEN 'REQUEST_COMPLETION' THEN '{"requests":"1"}'::jsonb ELSE e.usage END,p.rates) AS actual
  FROM targets h JOIN wallets w ON w.id=h.wallet_id
  JOIN LATERAL(SELECT id,selected_attempt_id,operation,billing_outcome FROM gateway_requests WHERE id=h.request_id OFFSET 0) q ON true
  JOIN LATERAL(SELECT id,evidence_id,billing_pricing_id,state FROM attempts WHERE id=q.selected_attempt_id OFFSET 0) a ON true
  JOIN facts e ON (e.id,e.attempt_id,e.request_id)=(a.evidence_id,a.id,q.id)
  JOIN billing_prices p ON p.id=a.billing_pricing_id
  JOIN LATERAL(SELECT * FROM hold_allocations WHERE hold_id=h.id AND ordinal=1 AND amount_atoms=h.amount_atoms OFFSET 0) s ON true
  JOIN wallet_lots l ON l.id=s.lot_id AND l.wallet_id=w.id AND l.expires_at IS NULL
  WHERE h.state IN ('HELD','RECONCILING') AND h.credit_atoms=0 AND w.debt_atoms=0 AND w.freeze_reason IS NULL AND NOT w.evidence_disputed
   AND q.operation IN ('chat','messages','gemini','embeddings','completions') AND q.billing_outcome='SUCCESS'
   AND a.state='SUCCEEDED' AND e.source='execution' AND e.finality='FINAL' AND p.basis IN ('PROVIDER_USAGE','REQUEST_COMPLETION') AND NOT w.has_expiring_lots AND p.currency=w.currency
   AND (p.basis='REQUEST_COMPLETION' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p.rates) dimension WHERE NOT e.usage?dimension))
   AND NOT EXISTS(SELECT 1 FROM hold_allocations extra WHERE extra.hold_id=h.id AND extra.ordinal<>1)
   AND NOT EXISTS(SELECT 1 FROM attempts x WHERE x.request_id=q.id AND x.metering_conflict)
   AND NOT EXISTS(SELECT 1 FROM billing_resolutions x WHERE x.request_id=q.id)
   AND NOT EXISTS(SELECT 1 FROM billing_charges x WHERE x.request_id=q.id)
 ), wallet_counts AS (
  SELECT wallet_id,count(*) AS requests FROM targets GROUP BY wallet_id
 ), covered AS (
  SELECT c.wallet_id FROM candidates c JOIN wallet_counts t ON t.wallet_id=c.wallet_id
  WHERE c.actual>0 AND c.actual<=c.held GROUP BY c.wallet_id,t.requests HAVING count(*)=t.requests
 ), entries AS MATERIALIZED (
  SELECT c.*,ledger_uuid() AS charge_id,ledger_uuid() AS journal_id FROM (
   SELECT candidate.* FROM candidates candidate JOIN covered w ON w.wallet_id=candidate.wallet_id
   ORDER BY array_position(p_requests,candidate.request_id) OFFSET 0
  ) c
 ), charges AS (
  INSERT INTO billing_charges(id,request_id,hold_id,pricing_id,evidence_id,currency,amount_atoms,funded_atoms,debt_atoms)
  SELECT charge_id,request_id,hold_id,pricing_id,evidence_id,currency,actual,actual,0 FROM entries RETURNING id
 ), allocations AS (
  INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms)
  SELECT c.id,e.lot_id,1,e.actual FROM entries e JOIN charges c ON c.id=e.charge_id
 ), lot_totals AS (
  SELECT lot_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY lot_id
 ), lots AS (
  UPDATE wallet_lots l SET reserved_atoms=l.reserved_atoms-t.held,available_atoms=l.available_atoms+t.held-t.actual,spent_atoms=l.spent_atoms+t.actual
  FROM lot_totals t WHERE l.id=t.lot_id
 ), wallet_totals AS (
  SELECT wallet_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY wallet_id
 ), balances AS (
  UPDATE wallets w SET reserved_atoms=w.reserved_atoms-t.held,available_atoms=w.available_atoms+t.held-t.actual,spent_atoms=w.spent_atoms+t.actual
  FROM wallet_totals t WHERE w.id=t.wallet_id
 ), holds AS (
  UPDATE billing_holds h SET state='SETTLED',closed_at=now(),next_reconcile_at=now()+interval '1 hour',reconcile_until=NULL,reconcile_reason=NULL
  FROM entries e WHERE h.id=e.hold_id
 )
 SELECT coalesce(jsonb_object_agg(e.request_id,jsonb_build_object('state','SETTLED','charge_id',e.charge_id,'charged_atoms',e.actual::text,'funded_atoms',e.actual::text,'debt_atoms','0','replayed',false)),'{}'::jsonb),
  coalesce(jsonb_agg(jsonb_build_object('id',e.journal_id,'wallet_id',e.wallet_id,'reference','settle:'||e.request_id,'kind','SETTLE','currency',e.currency,'lines',
   jsonb_build_array(money_leg('lot:'||e.lot_id||':reserved',-e.held))||
   CASE WHEN e.held>e.actual THEN jsonb_build_array(money_leg('lot:'||e.lot_id||':available',e.held-e.actual)) ELSE '[]'::jsonb END||
   jsonb_build_array(money_leg('system:revenue',e.actual))) ORDER BY array_position(p_requests,e.request_id)),'[]'::jsonb)
 INTO receipts,journals FROM entries e;
 PERFORM post_money_journals(journals);
 RETURN receipts;
EXCEPTION WHEN numeric_value_out_of_range OR check_violation OR raise_exception OR invalid_parameter_value OR insufficient_privilege THEN RETURN '{}'::jsonb;
END $$;

CREATE TABLE public.execution_contexts (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    key_id uuid NOT NULL,
    owner_session_id uuid NOT NULL,
    operation text NOT NULL,
    model text NOT NULL,
    ip text NOT NULL,
    fingerprint text NOT NULL,
    snapshot jsonb NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE FUNCTION core_funding.bind_chat_context_v1(p jsonb, d jsonb, route jsonb) RETURNS public.execution_contexts
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION core_funding.capture_price_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE rates jsonb;program record;source text;failure text;reason text;BEGIN
 IF TG_TABLE_NAME='cost_prices' THEN source='cost_versions';SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=NEW.price_id;
 ELSIF TG_TABLE_NAME='billing_prices' THEN source='billing_prices';SELECT v.rates INTO rates FROM public.billing_prices v WHERE v.id=NEW.price_id;
 ELSE RAISE EXCEPTION 'price_program_source_invalid';END IF;
 IF rates IS NULL THEN RAISE EXCEPTION 'price_program_source_invalid';END IF;
 BEGIN
  SELECT * INTO STRICT program FROM core_funding.compile_price_v1(rates);
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,reason=MESSAGE_TEXT;
  RAISE EXCEPTION 'price_program_compile_failed:%:%:%',source,NEW.price_id,left(reason,256) USING ERRCODE=failure;
 END;
 NEW.compiler_version=1;
 NEW.source_digest=encode(sha256(convert_to(rates::text,'UTF8')),'hex');
 NEW.dimensions=program.dimensions;NEW.weights=program.weights;NEW.denominator=program.denominator;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.catalog_held_v1() RETURNS boolean
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks l WHERE l.locktype='advisory' AND l.pid=pg_backend_pid() AND l.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
  AND l.classid::bigint=((hashtextextended('stg.catalog.control.v1',0)>>32)&4294967295) AND l.objid::bigint=(hashtextextended('stg.catalog.control.v1',0)&4294967295) AND l.objsubid=1 AND l.granted AND l.mode='ExclusiveLock')
$$;

CREATE FUNCTION core_funding.catalog_v1(cache jsonb, model text, op text, cur text) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k text=jsonb_build_array(model,op,cur)::text;frame jsonb=cache->'catalog'->k;candidates jsonb;allowed jsonb;observed timestamptz=clock_timestamp();expiry timestamptz;size integer;BEGIN
 IF cache->'memo'='false'::jsonb THEN RETURN jsonb_build_object('cache',cache,'frame',jsonb_build_object('candidates',public.core_candidates_v1(model,op,cur)));END IF;
 IF frame IS NOT NULL AND (frame->>'observed_at')::timestamptz<=observed AND (frame->>'valid_until' IS NULL OR (frame->>'valid_until')::timestamptz>observed) THEN RETURN jsonb_build_object('cache',cache,'frame',frame);END IF;
 IF frame IS NULL THEN candidates=public.core_candidates_v1(model,op,cur);ELSE candidates=frame->'candidates';END IF;
 allowed=core_execution_runtime.allowed_v1(candidates);
 SELECT min((a.facts->>'trust_until')::timestamptz) INTO expiry FROM core_execution_runtime.approvals a WHERE a.active AND (a.facts->>'trust_until')::timestamptz>observed AND a.package_digest IN(SELECT c->'ref'->>'package_digest' FROM jsonb_array_elements(candidates)c);
 frame=jsonb_build_object('candidates',candidates,'runtimes',allowed,'observed_at',observed,'valid_until',expiry);
 size=pg_column_size(frame);
 IF size<=8388608 AND pg_column_size(cache)<=8388608-size THEN cache=jsonb_set(cache,ARRAY['catalog',k],frame);END IF;
 RETURN jsonb_build_object('cache',cache,'frame',frame);
END $$;

CREATE FUNCTION core_funding.check_account_trigger_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN PERFORM core_funding.check_account_v1(NEW.id);RETURN NULL;END $$;

CREATE FUNCTION core_funding.check_account_v1(account uuid) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE') AND
  (EXISTS(SELECT 1 FROM public.upstream_account_balances WHERE account_id=account AND balance_atoms<0) OR EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND balance_atoms<reserved_atoms)) THEN RAISE EXCEPTION 'funding_coverage_invalid' USING ERRCODE='P0403';END IF;
END $$;

CREATE FUNCTION core_funding.check_coverage_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM public.upstream_accounts a WHERE a.id=NEW.account_id AND a.state='ACTIVE')
  AND (EXISTS(SELECT 1 FROM public.upstream_account_balances b WHERE b.account_id=NEW.account_id AND b.balance_atoms<0)
   OR EXISTS(SELECT 1 FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.balance_atoms<c.reserved_atoms)) THEN RAISE EXCEPTION 'funding_coverage_invalid' USING ERRCODE='P0403';END IF;
 RETURN NULL;
END $$;

CREATE FUNCTION core_funding.check_root_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE b record;BEGIN
 SELECT r.balance_atoms+coalesce(c.balance,0) balance,r.spent_atoms+coalesce(c.spent_limit,0) spent_limit,r.revision+coalesce(c.revision_limit,0) revision_limit,coalesce(c.reserved,0) reserved
 INTO b FROM public.upstream_account_balances r LEFT JOIN LATERAL(
  SELECT sum(c.balance_atoms) balance,sum(c.reserved_atoms) reserved,sum(c.spent_limit) spent_limit,sum(c.revision_limit) revision_limit FROM core_funding.balance_cells c WHERE c.account_id=r.account_id AND c.currency=r.currency
 )c ON true WHERE r.account_id=NEW.account_id AND r.currency=NEW.currency;
 IF abs(b.balance)>99999999999999999999999999999999999999 OR greatest(b.spent_limit,b.revision_limit,b.reserved)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 PERFORM core_funding.check_account_v1(NEW.account_id);
 RETURN NULL;
END $$;

CREATE FUNCTION core_funding.coalesce_groups_v1(groups jsonb, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
 events jsonb;
 attempt_owners jsonb;
 event_owners jsonb;
 request_owners jsonb;
 applied jsonb;
 result jsonb;
 results jsonb='[]';
 shared_wallet boolean;
 expires boolean;
 segment jsonb;
 current_group jsonb;
 segment_size integer;
 group_size integer;
BEGIN
 IF EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) g
  WHERE jsonb_typeof(g) IS DISTINCT FROM 'object' OR g-'events'<>'{}'::jsonb
   OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array'
   OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64
 ) THEN RETURN NULL; END IF;

 SELECT jsonb_agg(e.value ORDER BY g.n,e.n),
  jsonb_object_agg(((e.value->>'attempt_id')::uuid)::text,g.n),
  jsonb_object_agg(((e.value->>'event_id')::uuid)::text,g.n)
 INTO events,attempt_owners,event_owners
 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
 CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') WITH ORDINALITY e(value,n);
 IF jsonb_array_length(events)>64 THEN
  IF pending IS DISTINCT FROM '{"cells":{},"journals":[],"entries":[]}'::jsonb THEN RETURN NULL;END IF;
  segment='[]';segment_size=0;
  FOR current_group IN SELECT value FROM jsonb_array_elements(groups) LOOP
   group_size=jsonb_array_length(current_group->'events');
   IF segment_size+group_size>64 THEN
    results=results||core_funding.persist_v1(segment,true);
    segment='[]';segment_size=0;
   END IF;
   segment=segment||jsonb_build_array(current_group);
   segment_size=segment_size+group_size;
  END LOOP;
  results=results||core_funding.persist_v1(segment,true);
  IF EXISTS(SELECT 1 FROM jsonb_array_elements(results) item WHERE item->>'status'='REJECTED') THEN
   RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
  END IF;
  RETURN jsonb_build_object('results',results,'pending',pending);
 END IF;

 IF EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
  CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e
  GROUP BY (e->>'attempt_id')::uuid HAVING count(DISTINCT g.n)>1
 ) OR EXISTS (
  SELECT 1 FROM jsonb_array_elements(groups) WITH ORDINALITY g(value,n)
  CROSS JOIN LATERAL jsonb_array_elements(g.value->'events') e
  GROUP BY (e->>'event_id')::uuid HAVING count(DISTINCT g.n)>1
 ) THEN RETURN NULL; END IF;

 SELECT bool_or(wallet.groups>1),bool_or(wallet.expires) INTO shared_wallet,expires
 FROM (
  SELECT w.wallet_id,count(DISTINCT o.group_number) AS groups,bool_or(w.has_expiring_lots) AS expires
  FROM jsonb_each_text(attempt_owners) o(attempt_id,group_number)
  CROSS JOIN LATERAL (
   SELECT h.wallet_id,w.has_expiring_lots FROM public.attempts t
   JOIN public.billing_holds h ON h.request_id=t.request_id
   JOIN public.wallets w ON w.id=h.wallet_id
   WHERE t.id=o.attempt_id::uuid OFFSET 0
  ) w GROUP BY w.wallet_id
 ) wallet;
 IF expires THEN RETURN NULL;END IF;
 IF shared_wallet THEN
  IF EXISTS(
   SELECT 1 FROM jsonb_array_elements(groups) g
   CROSS JOIN LATERAL (SELECT g->'events'->0 AS usage,g->'events'->1 AS terminal) facts
   WHERE jsonb_array_length(g->'events')<>2
    OR (usage->>'source',usage->>'source_revision',usage->>'finality') IS DISTINCT FROM ('execution','1','FINAL')
    OR usage?'attempt_state' OR usage?'request_state'
    OR (terminal->>'source',terminal->>'source_revision',terminal->>'finality',terminal->>'attempt_state',terminal->>'request_state')
      IS DISTINCT FROM ('execution','2','FINAL','SUCCEEDED','SUCCEEDED')
    OR (terminal->>'attempt_id',terminal->>'origin_identity',terminal->'usage',terminal->'final_dimensions')
      IS DISTINCT FROM (usage->>'attempt_id',usage->>'origin_identity',usage->'usage',usage->'final_dimensions')
  ) THEN RETURN NULL;END IF;
  pending=pending||jsonb_build_object('shared_prepaid',true);
 END IF;

 applied=core_funding.evidence_group_v1(
  jsonb_build_object('events',events),pending||jsonb_build_object('partition',attempt_owners)
 );
 result=applied->'result';
 SELECT jsonb_object_agg(t.request_id::text,o.group_number) INTO request_owners
 FROM jsonb_each_text(attempt_owners) o(attempt_id,group_number)
 CROSS JOIN LATERAL (SELECT request_id FROM public.attempts WHERE id=o.attempt_id::uuid OFFSET 0) t;

 FOR i IN 1..jsonb_array_length(groups) LOOP
  IF applied->'blocked_groups' @> jsonb_build_array(i) THEN
   results=results||jsonb_build_array(jsonb_build_object('status','ERROR','error','55P03'));
   CONTINUE;
  END IF;
  results=results||jsonb_build_array(jsonb_build_object(
   'status','COMMITTED',
   'receipts',coalesce((
    SELECT jsonb_agg(r.value ORDER BY r.n)
    FROM jsonb_array_elements(result->'receipts') WITH ORDINALITY r(value,n)
    WHERE (event_owners->>(((r.value->>'event_id')::uuid)::text))::integer=i
   ),'[]'),
   'billing',coalesce((
    SELECT jsonb_object_agg(b.key,b.value) FROM jsonb_each(result->'billing') b
    WHERE (request_owners->>b.key)::integer=i
   ),'{}')
  ));
 END LOOP;
 RETURN jsonb_build_object('results',results,'pending',(applied->'pending')-ARRAY['partition','shared_prepaid']);
END $$;

CREATE FUNCTION core_funding.collect_v1(account uuid, cur text, needed numeric, exclude_cell integer DEFAULT NULL::integer) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE root numeric;c record;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF needed IS NULL OR needed<0 OR needed>99999999999999999999999999999999999999 OR needed<>trunc(needed) THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 FOR c IN SELECT cell,balance_atoms-reserved_atoms free FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms>reserved_atoms AND (exclude_cell IS NULL OR cell<>exclude_cell) ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN root>=needed;
  take=least(c.free,needed-root,99999999999999999999999999999999999999-root);
  IF take>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,-take,0,0,'collect:'||gen_random_uuid());root=root+take;END IF;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.compile_inserted_price_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_TABLE_NAME='cost_versions' THEN INSERT INTO core_funding.cost_prices(price_id) VALUES(NEW.id);
 ELSIF TG_TABLE_NAME='billing_prices' THEN INSERT INTO core_funding.billing_prices(price_id) VALUES(NEW.id);
 ELSE RAISE EXCEPTION 'price_program_source_invalid';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.compile_price_v1(rates jsonb) RETURNS TABLE(dimensions text[], weights numeric[], denominator numeric)
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE part record;numerators numeric[]='{}';denominators numeric[]='{}';numerator numeric;divisor numeric;a numeric;b numeric;remainder numeric;i integer;BEGIN
 IF jsonb_typeof(rates) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'invalid price input';END IF;
 IF rates?'cache_write_tokens' AND (rates?'cache_write_5m_tokens' OR rates?'cache_write_1h_tokens') THEN RAISE EXCEPTION 'duplicate cache write rate';END IF;
 dimensions='{}';weights='{}';denominator=1;
 FOR part IN SELECT key,value FROM jsonb_each(rates) ORDER BY key COLLATE "C" LOOP
  IF (part.value->>'numerator' ~ '^(0|[1-9][0-9]{0,37})$') IS NOT TRUE OR (part.value->>'denominator' ~ '^[1-9][0-9]{0,37}$') IS NOT TRUE THEN RAISE EXCEPTION 'invalid price input';END IF;
  dimensions=array_append(dimensions,part.key);
  numerator=(part.value->>'numerator')::numeric;
  divisor=(part.value->>'denominator')::numeric;
  a=numerator;b=divisor;
  WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  numerator=div(numerator,a);divisor=div(divisor,a);
  numerators=array_append(numerators,numerator);
  denominators=array_append(denominators,divisor);
  a=denominator;b=divisor;
  WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  denominator=div(denominator,a)*divisor;
 END LOOP;
 FOR i IN 1..cardinality(dimensions) LOOP weights=array_append(weights,numerators[i]*div(denominator,denominators[i]));END LOOP;
 RETURN NEXT;
END $_$;

CREATE FUNCTION core_funding.cost_v1(price uuid, usage jsonb) RETURNS numeric
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE p core_funding.cost_prices%ROWTYPE;BEGIN
 SELECT * INTO p FROM core_funding.cost_prices WHERE price_id=price;
 IF NOT FOUND THEN RAISE EXCEPTION 'supplier_price_missing' USING ERRCODE='P0422';END IF;
 RETURN core_funding.price_v1(usage,p.dimensions,p.weights,p.denominator);
END $$;

CREATE FUNCTION core_funding.customer_authorities_v1(items jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
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

CREATE FUNCTION core_funding.evidence_attempt_stages_v1(ids uuid[]) RETURNS core_funding.evidence_attempt_stage_v1[]
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT ARRAY(
  SELECT ROW(
   t.id,t.request_id,q.project_id,v.currency,h,v.rates,coalesce(bp.rates,'{}'),bp.basis,
   coalesce(rev.revision,0),ci.id,coalesce(cp.usage,'{}'),coalesce(ci.final_dimensions,'{}'),
   EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=t.id AND r.outcome='ACCEPTED' AND r.finality IN('FINAL','NOT_EXECUTED')),
   coalesce(src.sources,'{}'),t.state,t.finished_at,t.error_code,t.provider_request_id,false,NULL,NULL,false
  )::core_funding.evidence_attempt_stage_v1
  FROM public.attempts t
  JOIN public.gateway_requests q ON q.id=t.request_id
  JOIN public.upstream_holds h ON h.attempt_id=t.id
  JOIN public.cost_versions v ON v.id=h.cost_version_id
  LEFT JOIN public.billing_prices bp ON bp.id=t.billing_pricing_id
  LEFT JOIN LATERAL (
   SELECT max(i.snapshot_revision) revision FROM public.usage_event_index i WHERE i.attempt_id=t.id AND i.snapshot_revision IS NOT NULL
  ) rev ON true
  LEFT JOIN LATERAL (
   SELECT i.id,i.created_at,i.final_dimensions FROM public.usage_event_index i
   WHERE i.attempt_id=t.id AND i.record_kind='CANONICAL' ORDER BY i.snapshot_revision DESC LIMIT 1
  ) ci ON true
  LEFT JOIN public.usage_payloads cp ON cp.id=ci.id AND cp.created_at=ci.created_at
  LEFT JOIN LATERAL (
   SELECT jsonb_object_agg(jsonb_build_array(s.origin_identity,s.source)::text,s.revision) sources
   FROM (
    SELECT r.origin_identity,r.source,max(r.source_revision) revision FROM public.core_evidence_receipts r
    WHERE r.attempt_id=t.id AND r.outcome='ACCEPTED' GROUP BY r.origin_identity,r.source
   ) s
  ) src ON true
  WHERE t.id=ANY(ids)
  ORDER BY t.id
 )
$$;

CREATE FUNCTION core_funding.evidence_group_v1(g jsonb, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE ev jsonb;events jsonb[]='{}';n integer;i integer;pass integer;k text;attempt_ids uuid[];request_ids uuid[];h public.upstream_holds%ROWTYPE;
 prior public.core_evidence_receipts%ROWTYPE;rev_prior public.core_evidence_receipts%ROWTYPE;head record;cur public.usage_event_index%ROWTYPE;
 mode text[];contested boolean[];claims jsonb='{}';claim jsonb;d text;q text;owner integer;
 delivered boolean[];event_claims boolean[];revision_ids uuid[];revision_digests text[];blocked_groups integer[]='{}';
 rates jsonb;prices jsonb;usage jsonb;finals text[];merged jsonb;merged_finals text[];raw jsonb;snap jsonb;x jsonb;outcome text;fault text;canonical uuid;disputed uuid;conflict uuid;receipt uuid;risk text[];stale boolean;event_claimed boolean;existing uuid;finality text;
 touched jsonb='{}';receipts jsonb='[]';billing jsonb='{}';settled jsonb;a uuid;row record;auth text[];invalid_subject boolean;basis text;seen_final boolean;first_copy integer;snapshot core_funding.evidence_snapshot_stage_v1;
 stages core_funding.evidence_attempt_stage_v1[];
 requests core_funding.evidence_request_stage_v1[];
 stage core_funding.evidence_attempt_stage_v1;
 request_row core_funding.evidence_request_stage_v1;
 staged_receipt public.core_evidence_receipts%ROWTYPE;
 staged_receipts public.core_evidence_receipts[]='{}';
 staged_snapshots core_funding.evidence_snapshot_stage_v1[]='{}';
 exposures jsonb[]='{}';
 first_costs core_funding.first_final_cost_v1[]='{}';
 first_cost core_funding.first_final_cost_v1;
 first_final boolean=false; close_together boolean=false; closed_count integer;
 stage_index integer;request_index integer;wallet_ids uuid[];previous_timeout text;cap_wait boolean;remaining_wait_ms integer;
BEGIN
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
 PERFORM core_funding.share_v1(ARRAY(SELECT DISTINCT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY account_id));
 PERFORM 1 FROM public.upstream_accounts WHERE id IN(SELECT account_id FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids)) ORDER BY id FOR SHARE NOWAIT;
 wallet_ids=ARRAY(SELECT DISTINCT bh.wallet_id FROM public.billing_holds bh WHERE bh.request_id=ANY(request_ids) ORDER BY bh.wallet_id);
 IF cardinality(wallet_ids)=1 THEN
  PERFORM 1 FROM public.wallets WHERE id=wallet_ids[1] FOR NO KEY UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
   remaining_wait_ms=floor(extract(epoch FROM statement_timestamp()+interval '12 milliseconds'-clock_timestamp())*1000);
   IF remaining_wait_ms<=0 THEN
    PERFORM 1 FROM public.wallets WHERE id=wallet_ids[1] FOR NO KEY UPDATE NOWAIT;
   ELSE
    previous_timeout=current_setting('lock_timeout');
    cap_wait=previous_timeout::interval=interval '0' OR previous_timeout::interval>remaining_wait_ms*interval '1 millisecond';
    IF cap_wait THEN PERFORM set_config('lock_timeout',remaining_wait_ms::text,true);END IF;
    PERFORM 1 FROM public.wallets WHERE id=wallet_ids[1] FOR NO KEY UPDATE;
    IF cap_wait THEN PERFORM set_config('lock_timeout',previous_timeout,true);END IF;
   END IF;
  END IF;
 ELSE
  PERFORM 1 FROM public.wallets WHERE id=ANY(wallet_ids) ORDER BY id FOR NO KEY UPDATE NOWAIT;
 END IF;
 PERFORM 1 FROM public.gateway_requests WHERE id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.billing_holds WHERE request_id=ANY(request_ids) ORDER BY id FOR UPDATE NOWAIT;
 PERFORM 1 FROM public.attempts WHERE id=ANY(attempt_ids) ORDER BY request_id,ordinal,id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.execution_permits WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR NO KEY UPDATE NOWAIT;
 PERFORM 1 FROM public.upstream_holds WHERE attempt_id=ANY(attempt_ids) ORDER BY attempt_id FOR UPDATE NOWAIT;
 WITH event_keys AS MATERIALIZED (
  SELECT data,ordinal,(data->>'attempt_id')::uuid AS attempt_id,(data->>'origin_session_id')::uuid AS session_id
  FROM unnest(events) WITH ORDINALITY e(data,ordinal)
 ), producers AS MATERIALIZED (
  SELECT s.* FROM (SELECT DISTINCT session_id FROM event_keys) k
  JOIN LATERAL (
   SELECT s.session_id,s.service,s.instance_id,s.incarnation_id,s.public_key_sha256,s.certificate_sha256,
    s.security_revoked_at IS NOT NULL OR i.security_revoked_at IS NOT NULL OR c.security_revoked_at IS NOT NULL AS revoked
   FROM public.internal_startup_sessions s
   LEFT JOIN public.internal_service_instances i ON (i.service,i.instance_id)=(s.service,s.instance_id)
   LEFT JOIN public.internal_service_certificates c ON c.certificate_sha256=s.certificate_sha256
   WHERE s.session_id=k.session_id OFFSET 0
  ) s ON true
 ), permits AS MATERIALIZED (
  SELECT ep.* FROM unnest(attempt_ids) a(id) JOIN LATERAL (
   SELECT p.attempt_id,p.state,p.granted_at,p.permit_epoch,p.owner_service,p.gateway_instance_id,
    p.owner_session_id,p.incarnation_id,p.owner_key_sha256,p.certificate_sha256,
    EXISTS (SELECT 1 FROM public.upstream_holds uh WHERE uh.attempt_id=p.attempt_id AND uh.estimated_quantities IS NOT NULL) AS estimated
   FROM public.execution_permits p WHERE p.attempt_id=a.id OFFSET 0
  ) ep ON true
 )
 SELECT array_agg(CASE WHEN s.revoked THEN 'SECURITY_REVOKED' WHEN p.state IN ('RESERVED','ABORTED') THEN 'UNGRANTED' ELSE 'NORMAL' END ORDER BY e.ordinal),
  array_agg(EXISTS (
   SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.body_digest=e.data->>'body_digest'
  ) ORDER BY e.ordinal),
  array_agg(EXISTS (
   SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.event_claim
  ) ORDER BY e.ordinal),
  array_agg(revision.id ORDER BY e.ordinal),array_agg(revision.body_digest ORDER BY e.ordinal),
  bool_or(
   p.attempt_id IS NULL OR NOT (p.state IN ('SEND_GRANTED','UNKNOWN','TERMINAL') OR (p.state IN ('RESERVED','ABORTED') AND p.granted_at IS NULL))
   OR s.service||':'||s.instance_id IS DISTINCT FROM e.data->>'origin_identity'
   OR s.incarnation_id IS DISTINCT FROM (e.data->>'origin_incarnation')::uuid
   OR s.public_key_sha256 IS DISTINCT FROM e.data->>'producer_key_sha256'
   OR p.permit_epoch IS DISTINCT FROM (e.data->>'permit_epoch')::numeric
   OR ((p.owner_service||':'||p.gateway_instance_id IS DISTINCT FROM e.data->>'origin_identity'
    OR p.owner_session_id IS DISTINCT FROM s.session_id OR p.incarnation_id IS DISTINCT FROM s.incarnation_id
    OR (p.owner_key_sha256 IS NOT NULL AND p.owner_key_sha256<>s.public_key_sha256)
    OR (p.certificate_sha256 IS NOT NULL AND p.certificate_sha256<>s.certificate_sha256))
    AND NOT (e.data->>'source' IN ('async_partial','async_final','batch_completed','batch_cancelled','batch_failed') AND EXISTS (
     SELECT 1 FROM public.resources r WHERE r.attempt_id=p.attempt_id AND r.lease_session_id=s.session_id AND r.lease_epoch=(e.data->>'source_revision')::numeric
    )))
   OR NOT p.estimated
   OR EXISTS (
    SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=e.data->>'origin_identity' AND r.event_id=(e.data->>'event_id')::uuid AND r.attempt_id<>p.attempt_id
   )
  ) INTO auth,delivered,event_claims,revision_ids,revision_digests,invalid_subject
 FROM event_keys e
 LEFT JOIN permits p ON p.attempt_id=e.attempt_id
 LEFT JOIN producers s ON s.session_id=e.session_id
 LEFT JOIN LATERAL (
  SELECT r.id,r.body_digest FROM public.core_evidence_receipts r
  WHERE r.attempt_id=e.attempt_id AND r.origin_identity=e.data->>'origin_identity' AND r.source=e.data->>'source' AND r.source_revision=(e.data->>'source_revision')::numeric AND r.revision_claim
  OFFSET 0
 ) revision ON true;
 IF invalid_subject THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;

 IF pending?'partition' AND EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE auth[j]<>'NORMAL' OR delivered[j] OR event_claims[j] OR revision_ids[j] IS NOT NULL) THEN
  RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
 END IF;
 mode=array_fill('NEW'::text,ARRAY[n]);contested=array_fill(false,ARRAY[n]);
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
  IF delivered[i] OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'=ev->>'body_digest') THEN mode[i]='DUPLICATE';CONTINUE;END IF;
  IF auth[i]<>'NORMAL' THEN mode[i]='QUARANTINED';CONTINUE;END IF;
  IF event_claims[i] OR (revision_ids[i] IS NOT NULL AND revision_digests[i]<>ev->>'body_digest')
   OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND ((events[j]->>'event_id'=ev->>'event_id' AND events[j]->>'body_digest'<>ev->>'body_digest') OR (events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND events[j]->>'body_digest'<>ev->>'body_digest')))
  THEN mode[i]='CONFLICT';contested[i]=true;CONTINUE;END IF;
  IF revision_ids[i] IS NOT NULL OR EXISTS(SELECT 1 FROM generate_series(1,i-1) j WHERE events[j]->>'origin_identity'=ev->>'origin_identity' AND events[j]->>'attempt_id'=ev->>'attempt_id' AND events[j]->>'source'=ev->>'source' AND events[j]->>'source_revision'=ev->>'source_revision' AND mode[j] IN ('NEW','REPLAY')) THEN mode[i]='REPLAY';CONTINUE;END IF;
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
 IF pending?'partition' THEN
  IF EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL')
   OR EXISTS(
    SELECT 1 FROM unnest(attempt_ids) ids(attempt_id)
    LEFT JOIN LATERAL(SELECT request_id FROM public.attempts WHERE id=ids.attempt_id OFFSET 0) t ON true
    LEFT JOIN LATERAL(SELECT wallet_id FROM public.billing_holds WHERE request_id=t.request_id OFFSET 0) bh ON true
    LEFT JOIN LATERAL(SELECT has_expiring_lots FROM public.wallets WHERE id=bh.wallet_id OFFSET 0) w ON true
    GROUP BY bh.wallet_id HAVING bh.wallet_id IS NULL OR bool_or(w.has_expiring_lots) OR (NOT pending?'shared_prepaid' AND count(DISTINCT pending->'partition'->>ids.attempt_id::text)>1)
   ) THEN RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';END IF;
 END IF;
 IF pending IS NOT NULL AND NOT EXISTS(SELECT 1 FROM generate_series(1,n) j WHERE mode[j]<>'NEW' OR contested[j] OR auth[j]<>'NORMAL') THEN
  IF pending?'partition' THEN
   WITH required AS MATERIALIZED (
    SELECT DISTINCT (pending->'partition'->>hh.attempt_id::text)::integer group_number,hh.account_id,hh.currency,hh.funding_cell
    FROM unnest(events) e JOIN LATERAL (
     SELECT attempt_id,account_id,currency,funding_cell FROM public.upstream_holds WHERE attempt_id=(e->>'attempt_id')::uuid OFFSET 0
    ) hh ON true
    WHERE e->>'finality'<>'ESTIMATED'
   ), acquired AS MATERIALIZED (
    SELECT c.account_id,c.currency,c.cell FROM core_funding.balance_cells c
    WHERE (c.account_id,c.currency,c.cell) IN(SELECT r.account_id,r.currency,r.funding_cell FROM required r)
    ORDER BY c.account_id,c.currency COLLATE "C",c.cell FOR NO KEY UPDATE SKIP LOCKED
   )
   SELECT coalesce(array_agg(DISTINCT r.group_number),'{}') INTO blocked_groups FROM required r
   LEFT JOIN acquired ac ON (ac.account_id,ac.currency,ac.cell)=(r.account_id,r.currency,r.funding_cell)
   WHERE ac.account_id IS NULL;
   IF pending?'shared_prepaid' AND (cardinality(blocked_groups)>0 OR cardinality(request_ids)<>cardinality(attempt_ids)) THEN
    RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
   END IF;
   IF cardinality(blocked_groups)>0 THEN
    SELECT coalesce(array_agg(e.value ORDER BY e.ordinal),'{}') INTO events FROM unnest(events) WITH ORDINALITY e(value,ordinal)
    WHERE NOT ((pending->'partition'->>((e.value->>'attempt_id')::uuid)::text)::integer=ANY(blocked_groups));
    n=cardinality(events);
    IF n=0 THEN
     RETURN jsonb_build_object('result',jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing),'pending',pending,'blocked_groups',to_jsonb(blocked_groups));
    END IF;
    attempt_ids=ARRAY(SELECT DISTINCT (e->>'attempt_id')::uuid FROM unnest(events) e ORDER BY 1);
    request_ids=ARRAY(SELECT DISTINCT t.request_id FROM public.attempts t WHERE t.id=ANY(attempt_ids) ORDER BY 1);
   END IF;
  ELSE
   PERFORM 1 FROM core_funding.balance_cells c
    WHERE (c.account_id,c.currency,c.cell) IN(SELECT hh.account_id,hh.currency,hh.funding_cell FROM public.upstream_holds hh WHERE hh.attempt_id IN(SELECT (e->>'attempt_id')::uuid FROM unnest(events) e WHERE e->>'finality'<>'ESTIMATED'))
    ORDER BY c.account_id,c.currency COLLATE "C",c.cell FOR NO KEY UPDATE NOWAIT;
  END IF;
  stages=core_funding.evidence_attempt_stages_v1(attempt_ids);
  IF cardinality(stages)<>cardinality(attempt_ids) THEN RAISE EXCEPTION 'evidence_stage_missing' USING ERRCODE='P0500';END IF;
  first_final=NOT EXISTS(SELECT 1 FROM unnest(events) e WHERE e->>'source'<>'execution' OR e->>'finality'<>'FINAL')
   AND NOT EXISTS(SELECT 1 FROM unnest(stages) st JOIN public.upstream_accounts ac ON ac.id=(st.hold).account_id
     WHERE st.revision<>0 OR (st.hold).state<>'HELD' OR ac.state<>'ACTIVE')
   AND NOT EXISTS(SELECT 1 FROM public.upstream_costs c WHERE c.attempt_id=ANY(attempt_ids))
   AND NOT EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=ANY(attempt_ids));
  requests=ARRAY(
   SELECT ROW(q.id,q.selected_attempt_id,q.state,q.billing_outcome,NULL,false)::core_funding.evidence_request_stage_v1
   FROM public.gateway_requests q WHERE q.id=ANY(request_ids) ORDER BY q.id
  );
  IF cardinality(requests)<>cardinality(request_ids) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
 END IF;
 FOR pass IN 1..2 LOOP
  FOR i IN 1..n LOOP
   ev=events[i];a=(ev->>'attempt_id')::uuid;
   CONTINUE WHEN (pass=1)=(contested[i] OR mode[i]='DUPLICATE');
   IF mode[i]='DUPLICATE' THEN
    SELECT * INTO prior FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.body_digest=ev->>'body_digest';
    IF prior.signature_payload_digest<>ev->>'signature_payload_digest' OR prior.producer_key_sha256<>ev->>'producer_key_sha256' THEN RAISE EXCEPTION 'evidence_subject_invalid' USING ERRCODE='P0403';END IF;
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',prior.id,'outcome',prior.outcome,'duplicate',true));CONTINUE;
   END IF;
   IF stages IS NULL THEN
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=a;
   SELECT v.rates INTO rates FROM public.cost_versions v WHERE v.id=h.cost_version_id;
   SELECT p.basis,coalesce(p.rates,'{}') INTO basis,prices FROM public.attempts t LEFT JOIN public.billing_prices p ON p.id=t.billing_pricing_id WHERE t.id=a;
   SELECT * INTO cur FROM public.usage_event_index WHERE attempt_id=a AND record_kind='CANONICAL' ORDER BY snapshot_revision DESC LIMIT 1;
   SELECT p.usage INTO merged FROM public.usage_payloads p WHERE p.id=cur.id AND p.created_at=cur.created_at;
   ELSE
    stage_index=array_position(attempt_ids,a);
    stage=stages[stage_index];
    h=stage.hold;rates=stage.rates;basis=stage.basis;prices=stage.prices;
    cur.id=stage.head_id;cur.final_dimensions=stage.finals;merged=stage.usage;
   END IF;
   merged=coalesce(merged,'{}');merged_finals=coalesce(cur.final_dimensions,'{}');
   usage=ev->'normalized_usage';finals=ARRAY(SELECT jsonb_array_elements_text(ev->'normalized_finals'));
   IF stages IS NULL THEN event_claimed=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.origin_identity=ev->>'origin_identity' AND r.event_id=(ev->>'event_id')::uuid AND r.event_claim);ELSE event_claimed=false;END IF;
   receipt=gen_random_uuid();canonical=cur.id;disputed=NULL;conflict=NULL;fault=NULL;risk='{}';
   IF mode[i]='REPLAY' THEN
    SELECT * INTO rev_prior FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.source_revision=(ev->>'source_revision')::numeric AND r.revision_claim;
    IF rev_prior.id IS NULL OR rev_prior.body_digest<>ev->>'body_digest' THEN RAISE EXCEPTION 'evidence_replay_unanchored' USING ERRCODE='P0503';END IF;
    INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,risk_dimensions,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
    VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,false,'REVISION_REPLAY',ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',rev_prior.raw_evidence_id,rev_prior.canonical_evidence_id,'{}','VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
    receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome','REVISION_REPLAY','duplicate',false));CONTINUE;
   END IF;
   finality=CASE ev->>'finality' WHEN 'NOT_EXECUTED' THEN 'FINAL' ELSE ev->>'finality' END;
   IF stages IS NULL THEN raw=public.core_evidence_snapshot_v1(a,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'RAW',ev->>'source',finality,usage,finals,NULL,NULL,NULL,false);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     raw=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
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
    BEGIN x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
    EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
     WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
    IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,coalesce((x->>'complete')::boolean,false));
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
    disputed=(snap->>'id')::uuid;conflict=gen_random_uuid();
    INSERT INTO public.usage_conflicts(id,attempt_id,existing_event_id,source,finality,usage,fingerprint,incoming_evidence_id,disputed_evidence_id,incoming_receipt_id)
    VALUES(conflict,a,existing,ev->>'source',finality,usage,public.core_digest_v1(jsonb_build_object('attempt_id',a,'existing',existing,'incoming',raw->>'id','usage',usage,'final_dimensions',to_jsonb(finals))),(raw->>'id')::uuid,disputed,receipt);
    UPDATE public.attempts SET metering_conflict=true WHERE id=a;
   ELSIF ev->>'finality'='ESTIMATED' THEN outcome='ESTIMATED';risk='{}';
   ELSE
    IF stages IS NULL THEN stale=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.origin_identity=ev->>'origin_identity' AND r.source=ev->>'source' AND r.outcome='ACCEPTED' AND r.source_revision>(ev->>'source_revision')::numeric);ELSE stale=coalesce((stage.sources->>jsonb_build_array(ev->>'origin_identity',ev->>'source')::text)::numeric,0)>(ev->>'source_revision')::numeric;END IF;
    IF stale THEN outcome='STALE';
    ELSE
     merged=merged||(usage-merged_finals);merged_finals=ARRAY(SELECT DISTINCT unnest(merged_finals||finals) ORDER BY 1);
     BEGIN
      IF stages IS NOT NULL AND exposures[stage_index] IS NOT NULL AND stage.usage=merged AND stage.finals=merged_finals THEN
       x=exposures[stage_index];
      ELSE
       x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,merged,merged_finals,'{}');
      END IF;
     EXCEPTION WHEN numeric_value_out_of_range THEN x=NULL;fault='COST_OVERFLOW';
      WHEN raise_exception THEN IF SQLERRM IS DISTINCT FROM 'price exceeds amount range' THEN RAISE;END IF;x=NULL;fault='COST_OVERFLOW';END;
     IF x IS NULL THEN
      outcome='OVERFLOW';IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'DISPUTED',ev->>'source',finality,merged,merged_finals,cur.id,(raw->>'id')::uuid,NULL,false);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;disputed=(snap->>'id')::uuid;
     ELSE
      outcome='ACCEPTED';
      seen_final=finality='FINAL' OR CASE WHEN stages IS NULL THEN EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=a AND r.outcome='ACCEPTED' AND r.finality IN ('FINAL','NOT_EXECUTED')) ELSE stage.seen_final END;
      IF stages IS NULL THEN snap=public.core_evidence_snapshot_v1(a,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);ELSE
     stage.revision=stage.revision+1;
     snapshot=core_funding.make_evidence_snapshot_v1(a,stage.project_id,stage.request_id,stage.currency,stage.revision,'CANONICAL',ev->>'source',CASE WHEN seen_final AND (basis IS DISTINCT FROM 'PROVIDER_USAGE' OR NOT EXISTS(SELECT 1 FROM jsonb_object_keys(prices) p2 WHERE NOT p2=ANY(merged_finals))) THEN 'FINAL' WHEN cardinality(merged_finals)>0 OR merged<>'{}'::jsonb THEN 'PARTIAL' ELSE 'UNKNOWN' END,merged,merged_finals,cur.id,(raw->>'id')::uuid,(x->>'cost_atoms')::numeric,(x->>'complete')::boolean);
     staged_snapshots=array_append(staged_snapshots,snapshot);
     snap=jsonb_build_object('id',(snapshot.fact).id,'revision',stage.revision::text);
    END IF;
      canonical=(snap->>'id')::uuid;
      IF stages IS NULL THEN
      UPDATE public.attempts SET evidence_id=canonical,state=CASE WHEN state IN ('SUCCEEDED','FAILED','NOT_EXECUTED') OR ev->>'attempt_state' IS NULL THEN state ELSE ev->>'attempt_state' END,
       finished_at=CASE WHEN ev->>'attempt_state' IN ('SUCCEEDED','FAILED','NOT_EXECUTED') THEN coalesce(finished_at,clock_timestamp()) ELSE finished_at END,error_code=coalesce(ev->>'error_code',error_code),provider_request_id=coalesce(ev->>'provider_request_id',provider_request_id) WHERE id=a;
      UPDATE public.gateway_requests SET selected_attempt_id=CASE WHEN ev->>'request_state' IN ('SUCCEEDED','CANCELLED') THEN a END,
       state=ev->>'request_state',billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END,updated_at=clock_timestamp()
       WHERE id=(SELECT request_id FROM public.attempts WHERE id=a) AND ev->>'request_state' IN ('SUCCEEDED','CANCELLED','FAILED') AND selected_attempt_id IS NULL AND billing_outcome IN ('PENDING','UNKNOWN');
      ELSE
       exposures[stage_index]=x;
       IF first_final THEN
        IF (x->>'complete')::boolean AND NOT (x->>'overrun')::boolean AND (x->>'reserve_atoms')::numeric=0
         AND (x->>'cost_atoms')::numeric<=h.current_reserved_atoms THEN
         first_cost=ROW(h,canonical,stage.revision,(snapshot.fact).snapshot_digest,merged_finals,(x->>'cost_atoms')::numeric,
          CASE WHEN h.current_reserved_atoms<>0 OR (x->>'cost_atoms')::numeric<>0 THEN gen_random_uuid() END)::core_funding.first_final_cost_v1;
         first_costs[stage_index]=first_cost;
        ELSE first_final=false;END IF;
       END IF;
       stage.head_id=canonical;stage.usage=merged;stage.finals=merged_finals;stage.dirty=true;
       IF stage.state NOT IN('SUCCEEDED','FAILED','NOT_EXECUTED') AND ev->>'attempt_state' IS NOT NULL THEN stage.state=ev->>'attempt_state';END IF;
       IF ev->>'attempt_state' IN('SUCCEEDED','FAILED','NOT_EXECUTED') THEN stage.finished_at=coalesce(stage.finished_at,clock_timestamp());END IF;
       stage.error_code=coalesce(ev->>'error_code',stage.error_code);
       stage.provider_request_id=coalesce(ev->>'provider_request_id',stage.provider_request_id);
       stage.seen_final=seen_final;
       stage.sources=jsonb_set(stage.sources,ARRAY[jsonb_build_array(ev->>'origin_identity',ev->>'source')::text],to_jsonb((ev->>'source_revision')::numeric));
       IF ev->>'request_state' IN('SUCCEEDED','CANCELLED','FAILED') THEN
        request_index=array_position(request_ids,stage.request_id);
        request_row=requests[request_index];
        IF request_row.selected_attempt_id IS NULL AND request_row.billing_outcome IN('PENDING','UNKNOWN') THEN
         request_row.selected_attempt_id=CASE WHEN ev->>'request_state' IN('SUCCEEDED','CANCELLED') THEN a END;
         request_row.state=ev->>'request_state';
         request_row.billing_outcome=CASE ev->>'request_state' WHEN 'SUCCEEDED' THEN 'SUCCESS' ELSE ev->>'request_state' END;
         request_row.updated_at=clock_timestamp();request_row.dirty=true;
         requests[request_index]=request_row;
        END IF;
       END IF;
      END IF;
     END IF;
    END IF;
   END IF;
   IF stages IS NULL THEN
   INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
   VALUES(receipt,ev->>'origin_identity',(ev->>'origin_incarnation')::uuid,(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,a,ev->>'source',(ev->>'source_revision')::numeric,ev->>'body_digest',NOT event_claimed,outcome<>'CONFLICT',outcome,ev->>'finality',ev->>'attempt_state',ev->>'request_state',ev->>'error_code',ev->>'provider_request_id',(raw->>'id')::uuid,canonical,disputed,conflict,risk,fault,'VERIFIED','NORMAL',ev->>'producer_key_sha256',1,decode(ev->>'signature','hex'),ev->>'signature_payload_digest');
   ELSE
    staged_receipt.id=receipt;staged_receipt.origin_identity=ev->>'origin_identity';
    staged_receipt.origin_incarnation=(ev->>'origin_incarnation')::uuid;staged_receipt.origin_session_id=(ev->>'origin_session_id')::uuid;
    staged_receipt.event_id=(ev->>'event_id')::uuid;staged_receipt.attempt_id=a;
    staged_receipt.source=ev->>'source';staged_receipt.source_revision=(ev->>'source_revision')::numeric;
    staged_receipt.body_digest=ev->>'body_digest';staged_receipt.event_claim=NOT event_claimed;staged_receipt.revision_claim=outcome<>'CONFLICT';
    staged_receipt.outcome=outcome;staged_receipt.finality=ev->>'finality';
    staged_receipt.attempt_state=ev->>'attempt_state';staged_receipt.request_state=ev->>'request_state';
    staged_receipt.error_code=ev->>'error_code';staged_receipt.provider_request_id=ev->>'provider_request_id';
    staged_receipt.raw_evidence_id=(raw->>'id')::uuid;staged_receipt.canonical_evidence_id=canonical;
    staged_receipt.disputed_evidence_id=disputed;staged_receipt.conflict_id=conflict;staged_receipt.risk_dimensions=risk;staged_receipt.numeric_fault=fault;
    staged_receipt.authentication_disposition='VERIFIED';staged_receipt.authentication_reason='NORMAL';
    staged_receipt.producer_key_sha256=ev->>'producer_key_sha256';staged_receipt.signature_version=1;
    staged_receipt.signature=decode(ev->>'signature','hex');staged_receipt.signature_payload_digest=ev->>'signature_payload_digest';
    staged_receipts=array_append(staged_receipts,staged_receipt);
   END IF;
   IF outcome<>'ACCEPTED' THEN first_final=false;END IF;
   IF stages IS NULL THEN
    touched=jsonb_set(touched,ARRAY[a::text],jsonb_build_object('canonical',CASE WHEN outcome='ACCEPTED' THEN to_jsonb(canonical) ELSE coalesce(touched->a::text->'canonical','null'::jsonb) END,'revision',CASE WHEN outcome='ACCEPTED' THEN snap->>'revision' ELSE coalesce(touched->a::text->>'revision',raw->>'revision') END,'money',coalesce((touched->a::text->>'money')::boolean,false) OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0));
   ELSE
    IF outcome='ACCEPTED' THEN
     stage.touched_canonical=canonical;stage.touched_revision=snap->>'revision';
    ELSE
     stage.touched_revision=coalesce(stage.touched_revision,raw->>'revision');
    END IF;
    stage.money=stage.money OR outcome IN ('ACCEPTED','CONFLICT','OVERFLOW') OR cardinality(risk)>0;
    stages[stage_index]=stage;
   END IF;
   receipts=receipts||jsonb_build_array(jsonb_build_object('event_id',ev->>'event_id','receipt_id',receipt,'outcome',outcome,'duplicate',false));
  END LOOP;
 END LOOP;
 IF first_final THEN
  close_together=cardinality(requests)>0 AND NOT EXISTS(
   SELECT 1 FROM unnest(requests) r WHERE NOT r.dirty OR r.state<>'SUCCEEDED' OR r.billing_outcome<>'SUCCESS'
    OR NOT EXISTS(SELECT 1 FROM unnest(stages) t WHERE t.id=r.selected_attempt_id AND t.request_id=r.id AND t.state='SUCCEEDED')
    OR NOT EXISTS(SELECT 1 FROM unnest(staged_receipts) f WHERE f.attempt_id=r.selected_attempt_id AND f.outcome='ACCEPTED' AND f.attempt_state='SUCCEEDED' AND f.request_state='SUCCEEDED')
  );
 END IF;
 IF pending?'shared_prepaid' AND (NOT close_together OR cardinality(first_costs)<>cardinality(stages)
  OR EXISTS(SELECT 1 FROM unnest(first_costs) c WHERE c.evidence_id IS NULL)) THEN
  RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
 END IF;
 IF stages IS NOT NULL THEN
  PERFORM core_funding.flush_evidence_stages_v1(staged_snapshots,staged_receipts,stages,CASE WHEN close_together THEN '{}'::core_funding.evidence_request_stage_v1[] ELSE requests END);
 END IF;
 IF close_together THEN
  WITH targets AS MATERIALIZED (
   SELECT n.*,q.execution_closed_at FROM unnest(requests) n JOIN public.gateway_requests q ON q.id=n.id
  ), updated AS (
   UPDATE public.gateway_requests q SET selected_attempt_id=n.selected_attempt_id,state=n.state,billing_outcome=n.billing_outcome,
    updated_at=n.updated_at,execution_closed_at=coalesce(n.execution_closed_at,clock_timestamp())
   FROM targets n WHERE q.id=n.id RETURNING q.id,n.execution_closed_at IS NULL AS closed
  ), released AS (
   UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id IN(SELECT id FROM updated WHERE closed) AND state IN('ADOPTION_PENDING','ACTIVE')
  ) SELECT count(*) INTO closed_count FROM updated;
  IF closed_count<>cardinality(requests) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
 ELSE
 WITH closed AS (
  UPDATE public.gateway_requests q SET execution_closed_at=clock_timestamp()
  WHERE q.id=ANY(request_ids) AND q.execution_closed_at IS NULL AND q.state='SUCCEEDED' AND q.billing_outcome='SUCCESS'
   AND EXISTS(SELECT 1 FROM public.attempts t WHERE t.id=q.selected_attempt_id AND t.request_id=q.id AND t.state='SUCCEEDED')
   AND EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=q.selected_attempt_id AND r.outcome='ACCEPTED' AND r.attempt_state='SUCCEEDED' AND r.request_state='SUCCEEDED')
  RETURNING q.id
 ) UPDATE public.execution_quota_leases l SET state='RELEASE_PENDING' WHERE l.request_id IN(SELECT id FROM closed) AND l.state IN('ADOPTION_PENDING','ACTIVE');
 END IF;
 IF first_final AND cardinality(first_costs)=cardinality(stages)
  AND NOT EXISTS(SELECT 1 FROM unnest(first_costs) c WHERE c.evidence_id IS NULL) THEN
  pending=core_funding.flush_first_final_costs_v1(first_costs,pending);
 ELSE
 FOR row IN
  SELECT attempt,snapshot_id,revision FROM (
   SELECT key::uuid attempt,(value->>'canonical')::uuid snapshot_id,value->>'revision' revision
   FROM jsonb_each(touched) WHERE (value->>'money')::boolean
   UNION ALL
   SELECT s.id,s.touched_canonical,s.touched_revision FROM unnest(stages) s WHERE s.money
  ) work ORDER BY attempt::text
 LOOP
  settled=core_funding.supplier_settle_v1(row.attempt,row.snapshot_id,CASE WHEN row.snapshot_id IS NULL THEN 'risk:' ELSE 'cost:' END||row.attempt||':'||row.revision,pending);
  IF pending IS NOT NULL THEN pending=settled->'pending';END IF;
 END LOOP;
 pending=core_funding.flush_costs_v1(pending);
 END IF;
 IF pending?'shared_prepaid' THEN
  request_ids=ARRAY(SELECT s.request_id FROM unnest(stages) s ORDER BY (pending->'partition'->>s.id::text)::integer);
 ELSE
 request_ids=ARRAY(SELECT q.id FROM public.gateway_requests q WHERE q.id=ANY(request_ids) AND q.billing_outcome IN ('SUCCESS','CANCELLED','FAILED') AND NOT EXISTS(SELECT 1 FROM public.billing_charges c WHERE c.request_id=q.id) ORDER BY q.id);
 END IF;
 IF first_final AND cardinality(request_ids)>0 THEN
  billing=core_funding.bill_first_final_v1(request_ids,staged_snapshots);
  IF pending?'shared_prepaid' AND EXISTS(SELECT 1 FROM unnest(request_ids) id WHERE NOT billing?id::text) THEN
   RAISE EXCEPTION 'evidence_batch_dependency' USING ERRCODE='P0E01';
  END IF;
  request_ids=ARRAY(SELECT id FROM unnest(request_ids) id WHERE NOT billing?id::text ORDER BY id);
 END IF;
 IF cardinality(request_ids)>0 THEN
  billing=billing||public.settle_requests(request_ids);
  IF EXISTS(SELECT 1 FROM jsonb_each(billing) b WHERE b.value->>'state'='ERROR') THEN RAISE EXCEPTION 'customer_settlement_failed' USING ERRCODE='P0500';END IF;
 END IF;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt IMMEDIATE;
 SET CONSTRAINTS usage_index_payload,core_receipt_conflict,usage_conflict_incoming_receipt DEFERRED;
 RETURN jsonb_build_object('result',jsonb_build_object('status','COMMITTED','receipts',receipts,'billing',billing),'pending',pending,'blocked_groups',to_jsonb(blocked_groups));
END $_$;

CREATE FUNCTION core_funding.exclusive_v1(accounts uuid[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE account uuid;BEGIN
 IF accounts IS NULL OR cardinality(accounts)>128 OR array_position(accounts,NULL) IS NOT NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 FOR account IN SELECT DISTINCT id FROM unnest(accounts) id ORDER BY id LOOP
  IF core_funding.held_v1(account) AND NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  PERFORM pg_advisory_xact_lock(core_funding.account_key_v1(account));
 END LOOP;
END $$;

CREATE TABLE public.execution_commands (
    id uuid NOT NULL,
    command_id uuid NOT NULL,
    kind text NOT NULL,
    caller_identity text NOT NULL,
    caller_incarnation uuid NOT NULL,
    body_digest text NOT NULL,
    deadline_at timestamp with time zone NOT NULL,
    trace_id text NOT NULL,
    contract_version integer NOT NULL,
    state text NOT NULL,
    request_id uuid,
    attempt_id uuid,
    rejection_code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    owner_session_id uuid,
    receipt jsonb DEFAULT '{}'::jsonb NOT NULL,
    context_id uuid,
    CONSTRAINT command_digest CHECK (((body_digest ~ '^[a-f0-9]{64}$'::text) AND (contract_version > 0))),
    CONSTRAINT command_state CHECK ((state = ANY (ARRAY['COMMITTED'::text, 'REJECTED'::text])))
);

CREATE FUNCTION core_funding.execution_command_receipt_v1(cmd public.execution_commands) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE plan jsonb;
BEGIN
 IF cmd.kind<>'execution_admit_v1' OR cmd.state<>'COMMITTED'
  OR cmd.receipt->>'state' IS DISTINCT FROM 'SEND_GRANTED' OR cmd.receipt?'authorization_plan' THEN
  RETURN cmd.receipt;
 END IF;
 SELECT snapshot INTO STRICT plan FROM public.execution_permits
 WHERE attempt_id=cmd.attempt_id AND command_id=cmd.id
  AND authorization_plan_digest=cmd.receipt->>'authorization_plan_digest';
 RETURN cmd.receipt||jsonb_build_object('authorization_plan',plan);
END $$;

CREATE FUNCTION core_funding.execution_item_v1(p jsonb, e jsonb, d jsonb, computed jsonb, authority_times jsonb, cache jsonb, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
<<execution>>
DECLARE ctx public.execution_contexts%ROWTYPE;cmd public.execution_commands%ROWTYPE;q public.gateway_requests%ROWTYPE;permit public.execution_permits%ROWTYPE;a public.attempts%ROWTYPE;h public.upstream_holds%ROWTYPE;funding_cell smallint;
 kind text=p->>'command_type';caller text=p->>'service'||':'||(p->>'instance_id');keys text[];k text;auth jsonb;candidates jsonb;c jsonb;projection jsonb;ref jsonb;manifest jsonb;quote jsonb;estimated jsonb='{}';dim text;bound numeric;qty numeric;amount numeric;response jsonb;result jsonb;plan jsonb;command_pk uuid=gen_random_uuid();qid uuid;aid uuid;ordinal integer=1;account uuid;wallet uuid;descriptor jsonb;failure text;rejection text;fresh boolean=false;started timestamptz=clock_timestamp();rebuilt jsonb;
 route jsonb=computed->'route'; expected_context public.execution_contexts%ROWTYPE;
 saved_cache jsonb=cache;saved_pending jsonb=pending;customer jsonb;catalog jsonb;allowed jsonb;allocation jsonb;funding jsonb;admission jsonb;journal uuid;
 lease public.execution_quota_leases%ROWTYPE;adopted jsonb;lease_count integer=0;candidate_invalid boolean;combined boolean=kind='execution_admit_v1' AND d->'commit_send'='true'::jsonb; lease_state text;lease_revision numeric;
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
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',core_funding.execution_command_receipt_v1(cmd)||CASE WHEN permit.attempt_id IS NOT NULL THEN jsonb_build_object('state',permit.state) ELSE '{}'::jsonb END),'cache',cache,'pending',pending);
 END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND execution_commands.kind=execution.kind AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('result',jsonb_build_object('receipt',core_funding.execution_command_receipt_v1(cmd)),'cache',cache,'pending',pending);
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
    allocation=allocation||jsonb_build_object('execution_budget',jsonb_build_object('max_attempts',(ctx.snapshot->'execution_budget'->>'max_attempts')::integer,'token_budget',((computed->>'token_count')::numeric*(ctx.snapshot->'execution_budget'->>'max_attempts')::integer)::text,'deadline_at',p->>'deadline_at','owner_session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'binding_id',ref->>'binding_id');
    IF pending IS NULL THEN
     result=public.allocate_customer_rows_v1(jsonb_build_array(allocation),true)->'0';
    IF result?'error' THEN
     IF result->>'error'='P0402' THEN RAISE EXCEPTION 'insufficient_funds' USING ERRCODE='P0402';END IF;
     IF result->>'error'='55P03' THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
     RAISE EXCEPTION 'allocator_failed';
    END IF;
    IF (result->>'created')::boolean IS DISTINCT FROM true OR result->>'attempt_id' IS DISTINCT FROM aid::text THEN RAISE EXCEPTION 'allocator_failed';END IF;
    IF result->'record'->>'state'='RUNNING' THEN
     UPDATE public.gateway_requests SET state='ADMITTED',execution_budget=allocation->'execution_budget' WHERE id=qid;
     UPDATE public.attempts SET binding_id=(ref->>'binding_id')::uuid WHERE id=aid;
    END IF;
    ELSE
     admission=jsonb_build_object('sequence',(SELECT count(*) FROM jsonb_each(pending->'admissions')),'customer',allocation,'binding_id',ref->>'binding_id','context_id',ctx.id,'owner_session_id',p->>'session_id','authorization',auth,'authority_times',authority_times,'command_deadline',p->>'deadline_at','package_digest',ref->>'package_digest','execution',projection->'execution','execution_budget',allocation->'execution_budget');
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
   IF combined AND (route IS NULL OR ctx.operation IS DISTINCT FROM 'chat' OR jsonb_typeof(manifest->'stream') IS DISTINCT FROM 'boolean' OR manifest->'resource_ids' IS DISTINCT FROM '[]'::jsonb
    OR NULLIF(current_setting('stg.short_chat_funding_cell',true),'')::integer IS DISTINCT FROM get_byte(uuid_send(ctx.project_id),15)%16
    OR coalesce((computed->>'proof_deadline_ms')::numeric-extract(epoch FROM clock_timestamp())*1000,-1)<=1000
    OR jsonb_typeof(computed->'adopted_leases') IS DISTINCT FROM 'array'
    OR EXISTS(SELECT 1 FROM public.wallets WHERE id=wallet AND (freeze_reason IS NOT NULL OR public.wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) IS NOT NULL)))
   THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
   IF combined THEN
    SELECT jsonb_object_agg(value->>'lease_id',value) INTO adopted FROM jsonb_array_elements(computed->'adopted_leases');
    IF jsonb_array_length(computed->'leases') IS DISTINCT FROM jsonb_array_length(computed->'adopted_leases')
     OR jsonb_array_length(computed->'leases') IS DISTINCT FROM (SELECT count(*)::integer FROM jsonb_object_keys(adopted))
    THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
   END IF;
   plan=jsonb_build_object('version',2,'owner',jsonb_build_object('service','gateway','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'attempt_id',aid,'permit_epoch','1','dispatched_digest',d->>'dispatched_digest','dispatch_projection',projection);
   response=jsonb_build_object('command_state','COMMITTED','request_id',qid,'attempt_id',aid,'ordinal',ordinal,'state',CASE WHEN combined THEN 'SEND_GRANTED' ELSE 'RESERVED' END,'permit_epoch','1','deadline_at',p->>'deadline_at','customer_atoms',quote->>'amount_atoms','customer_currency',quote->>'currency','supplier_atoms',amount::text,'supplier_currency',c->>'cost_currency','authorization_plan',plan,'authorization_plan_digest',public.core_digest_v1(jsonb_build_array('stg.internal.host-authorization-plan','2',plan)));
   response=response||jsonb_build_object('quota_lease_ids',(SELECT jsonb_agg(lease_id ORDER BY lease_id) FROM (SELECT value->>'lease_id' AS lease_id FROM jsonb_array_elements(computed->'leases') UNION SELECT lease_id FROM public.execution_quota_leases WHERE request_id=qid AND scope_kind<>'channel') leases));
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
    admission=admission||jsonb_build_object('permit',jsonb_build_object('attempt_id',aid,'command_id',command_pk,'gateway_instance_id',p->>'instance_id','owner_session_id',p->>'session_id','owner_key_sha256',p->>'key_sha256','owner_service','gateway','certificate_sha256',p->>'certificate_sha256','incarnation_id',p->>'incarnation_id','permit_epoch',1,'binding_id',ref->>'binding_id','dispatched_digest',d->>'dispatched_digest','state',response->>'state','expires_at',p->>'deadline_at','route_id',ref->>'route_id','context_id',ctx.id,'snapshot',plan,'manifest',manifest,'manifest_digest',projection->>'billable_manifest_sha256','candidate_digest',ref->>'candidate_digest','authorization_plan_digest',response->>'authorization_plan_digest','prep_session_id',d->>'prep_session_id','prep_binding_digest',d->>'prep_binding_digest'),'hold',jsonb_build_object('attempt_id',aid,'account_id',account,'channel_id',ref->>'channel_id','currency',c->>'cost_currency','estimated_atoms',amount,'cost_version_id',ref->>'cost_version_id','state','HELD','estimated_quantities',estimated,'estimated_quantities_digest',public.core_digest_v1(estimated),'estimate_manifest_digest',projection->>'billable_manifest_sha256','estimate_algorithm_version',1,'funding_cell',funding_cell,'current_reserved_atoms',amount),'leases','[]'::jsonb);
    pending=jsonb_set(pending,ARRAY['commands'],(pending->'commands')||jsonb_build_array(jsonb_build_object('id',command_pk,'command_id',e->>'command_id','kind',kind,'caller_identity',caller,'owner_session_id',p->>'session_id','caller_incarnation',p->>'incarnation_id','body_digest',e->>'body_digest','deadline_at',e->>'deadline_at','trace_id',e->>'trace_id','contract_version',1,'state','COMMITTED','request_id',qid,'attempt_id',aid,'context_id',ctx.id,'receipt',CASE WHEN combined THEN response-'authorization_plan' ELSE response END)));
    pending=jsonb_set(pending,ARRAY['keys','command:'||caller||':'||kind||':'||((e->>'command_id')::uuid)::text],'true'::jsonb);
    pending=jsonb_set(pending,ARRAY['keys','request:'||qid::text],'true'::jsonb);
    IF d->>'intent_digest' IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['keys','intent:'||ctx.project_id::text||':'||(d->>'intent_digest')],'true'::jsonb);END IF;
    journal=gen_random_uuid();funding=pending->'funding';
    funding=jsonb_set(funding,ARRAY['journals'],(funding->'journals')||jsonb_build_array(jsonb_build_object('id',journal,'account_id',account,'channel_id',ref->>'channel_id','attempt_id',aid,'currency',c->>'cost_currency','kind','RESERVE','reference','reserve:'||aid)));
    funding=jsonb_set(funding,ARRAY['entries'],(funding->'entries')||jsonb_build_array(jsonb_build_object('journal_id',journal,'ordinal',0,'currency',c->>'cost_currency','account','upstream:available','atoms',-amount),jsonb_build_object('journal_id',journal,'ordinal',1,'currency',c->>'cost_currency','account','upstream:reserved','atoms',amount)));
    pending=jsonb_set(pending,ARRAY['funding'],funding);
   ELSE
   INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt) VALUES(command_pk,(e->>'command_id')::uuid,kind,caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,'COMMITTED',qid,aid,ctx.id,CASE WHEN combined THEN response-'authorization_plan' ELSE response END);
   INSERT INTO public.execution_permits(granted_at,attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest) VALUES(CASE WHEN combined THEN clock_timestamp() END,aid,command_pk,p->>'instance_id',(p->>'session_id')::uuid,p->>'key_sha256','gateway',p->>'certificate_sha256',(p->>'incarnation_id')::uuid,1,(ref->>'binding_id')::uuid,d->>'dispatched_digest',response->>'state',(p->>'deadline_at')::timestamptz,(ref->>'route_id')::uuid,ctx.id,plan,manifest,projection->>'billable_manifest_sha256',ref->>'candidate_digest',response->>'authorization_plan_digest',(d->>'prep_session_id')::uuid,d->>'prep_binding_digest');
   INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version,funding_cell,current_reserved_atoms) VALUES(aid,account,(ref->>'channel_id')::uuid,c->>'cost_currency',amount,(ref->>'cost_version_id')::uuid,'HELD',estimated,public.core_digest_v1(estimated),projection->>'billable_manifest_sha256',1,funding_cell,amount);
   PERFORM public.core_upstream_journal_v1(account,c->>'cost_currency',(ref->>'channel_id')::uuid,aid,'RESERVE',amount);
   END IF;
   IF jsonb_array_length(computed->'leases')<>(CASE WHEN kind='execution_admit_v1' THEN jsonb_array_length(auth->'scopes')+1 ELSE 1 END) THEN RAISE EXCEPTION 'quota_invalid';END IF;
   FOR descriptor IN SELECT value FROM jsonb_array_elements(computed->'leases') LOOP
    IF descriptor->>'command_id' IS DISTINCT FROM e->>'command_id' OR descriptor->>'body_digest' IS DISTINCT FROM e->>'body_digest' OR descriptor->>'caller' IS DISTINCT FROM caller OR descriptor->>'owner_session_id' IS DISTINCT FROM p->>'session_id' OR descriptor->>'state' IS DISTINCT FROM 'DRAFT' OR NOT (EXISTS(SELECT 1 FROM jsonb_array_elements(auth->'scopes')s WHERE s->>'kind'=descriptor->>'scope_kind' AND s->>'id'=descriptor->>'scope_id') OR descriptor->>'scope_kind'='channel' AND descriptor->>'scope_id'=ref->>'channel_id') THEN RAISE EXCEPTION 'quota_invalid';END IF;
    lease_state='ADOPTION_PENDING';lease_revision=0;
    IF combined THEN
     result=adopted->(descriptor->>'lease_id');
     IF result->>'state' IS DISTINCT FROM 'ADOPTED' OR result->>'attempt_id' IS DISTINCT FROM aid::text
      OR result->>'permit_epoch' IS DISTINCT FROM '1' OR result->>'revision' IS DISTINCT FROM '1'
      OR (descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision']) IS DISTINCT FROM (result-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision'])
      OR (CASE WHEN jsonb_typeof(result->'remaining_ms')='number' AND result->>'remaining_ms'~'^[0-9]+$' THEN (result->>'remaining_ms')::numeric>1000 END) IS DISTINCT FROM true
     THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
     descriptor=result;lease_state='ACTIVE';lease_revision=1;
    END IF;
    IF kind='execution_admit_v1' AND pending IS NOT NULL THEN
     admission=jsonb_set(admission,ARRAY['leases'],(admission->'leases')||jsonb_build_array(jsonb_build_object('lease_id',descriptor->>'lease_id','attempt_id',aid,'command_id',command_pk,'scope_kind',descriptor->>'scope_kind','scope_id',descriptor->>'scope_id','request_id',qid,'state',lease_state,'revision',lease_revision,'descriptor',descriptor)));
    ELSE
    INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,revision,descriptor) VALUES(descriptor->>'lease_id',aid,command_pk,descriptor->>'scope_kind',descriptor->>'scope_id',qid,lease_state,lease_revision,descriptor);
    END IF;
   END LOOP;
   IF combined THEN
    fresh=true;
    IF pending IS NOT NULL THEN admission=admission||jsonb_build_object('short_chat_proof_deadline_ms',computed->'proof_deadline_ms');END IF;
   END IF;
   IF kind='execution_admit_v1' AND pending IS NOT NULL THEN pending=jsonb_set(pending,ARRAY['admissions',aid::text],admission);END IF;
  ELSIF kind IN ('execution_begin_v1','execution_abort_v1') THEN
   aid=(d->>'attempt_id')::uuid;
   IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id' OR permit.permit_epoch::text IS DISTINCT FROM d->>'permit_epoch' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
   IF kind='execution_begin_v1' THEN
    catalog=core_funding.catalog_v1(cache,ctx.model,ctx.operation,auth->>'customer_currency');cache=catalog->'cache';candidates=catalog->'frame'->'candidates';allowed=catalog->'frame'->'runtimes';
    SELECT value INTO c FROM jsonb_array_elements(candidates) WHERE value->'ref'=permit.snapshot->'dispatch_projection'->'candidate_ref';
    IF c IS NOT NULL AND allowed IS NULL THEN allowed=core_execution_runtime.allowed_v1(jsonb_build_array(c));END IF;
    candidate_invalid=c IS NULL OR permit.snapshot->'dispatch_projection'->>'credential_ciphertext_sha256' IS DISTINCT FROM c->>'credential_ciphertext_sha256'
     OR permit.snapshot->'version' IS DISTINCT FROM '2'::jsonb OR permit.snapshot->'dispatch_projection'->'version' IS DISTINCT FROM '2'::jsonb
     OR (allowed->(c->'ref'->>'package_digest') @> jsonb_build_array(permit.snapshot->'dispatch_projection'->'execution')) IS DISTINCT FROM true;
    IF candidate_invalid AND NOT computed?'adopted_leases' THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
   ELSE
    PERFORM id FROM public.channels WHERE id=(SELECT provider_id FROM public.attempts WHERE id=aid) FOR SHARE NOWAIT;
    PERFORM id FROM public.upstream_accounts WHERE id=(SELECT account_id FROM public.upstream_holds WHERE attempt_id=aid) FOR SHARE NOWAIT;
   END IF;
   SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=aid;
   PERFORM core_funding.share_v1(ARRAY[h.account_id]);
   IF kind='execution_begin_v1' AND current_setting('transaction_isolation') IN ('read committed','read uncommitted') THEN
    SELECT bh.wallet_id INTO STRICT wallet FROM public.billing_holds bh JOIN public.attempts t ON t.request_id=bh.request_id WHERE t.id=aid;
    IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('stg.wallet.send.v1:'||wallet::text,0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
   ELSE
    SELECT id INTO wallet FROM public.wallets WHERE id=(SELECT wallet_id FROM public.billing_holds WHERE request_id=(SELECT request_id FROM public.attempts WHERE id=aid)) FOR NO KEY UPDATE NOWAIT;
   END IF;
   SELECT * INTO q FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=aid) FOR UPDATE NOWAIT;qid=q.id;
   PERFORM id FROM public.billing_holds WHERE request_id=qid FOR UPDATE NOWAIT;
   SELECT * INTO a FROM public.attempts WHERE id=aid FOR NO KEY UPDATE NOWAIT;
   SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=aid FOR NO KEY UPDATE NOWAIT;
   IF permit.state<>'RESERVED' THEN RAISE EXCEPTION 'permit_not_reserved' USING ERRCODE='P0409';END IF;
   IF kind='execution_begin_v1' THEN
    IF candidate_invalid THEN RAISE EXCEPTION 'candidate_stale' USING ERRCODE='P0422';END IF;
    IF computed?'adopted_leases' THEN
     IF a.ordinal<>1 OR ctx.operation IS DISTINCT FROM 'chat' OR permit.manifest->'stream' IS DISTINCT FROM 'false'::jsonb OR permit.manifest->'resource_ids' IS DISTINCT FROM '[]'::jsonb
      OR jsonb_typeof(computed->'adopted_leases') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
     IF EXISTS(SELECT 1 FROM jsonb_array_elements(computed->'adopted_leases') item WHERE jsonb_typeof(item->'lease_id') IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
     SELECT jsonb_object_agg(item->>'lease_id',item) INTO adopted FROM jsonb_array_elements(computed->'adopted_leases') item;
     FOR lease IN SELECT * FROM public.execution_quota_leases l WHERE l.request_id=qid AND (l.scope_kind<>'channel' OR l.attempt_id=aid) ORDER BY l.lease_id FOR NO KEY UPDATE NOWAIT LOOP
      descriptor=adopted->lease.lease_id;
      IF lease.state<>'ADOPTION_PENDING' OR lease.attempt_id<>aid OR descriptor->>'state' IS DISTINCT FROM 'ADOPTED'
       OR descriptor->>'attempt_id' IS DISTINCT FROM aid::text OR descriptor->>'permit_epoch' IS DISTINCT FROM permit.permit_epoch::text
       OR descriptor->'revision' IS DISTINCT FROM to_jsonb((lease.revision+1)::text)
       OR (lease.descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision']) IS DISTINCT FROM (descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision'])
       OR (CASE WHEN jsonb_typeof(descriptor->'remaining_ms')='number' AND descriptor->>'remaining_ms' ~ '^[0-9]+$' THEN (descriptor->>'remaining_ms')::numeric>1000 END) IS DISTINCT FROM true
      THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
      lease_count=lease_count+1;
     END LOOP;
     IF lease_count=0 OR lease_count<>jsonb_array_length(computed->'adopted_leases') THEN RAISE EXCEPTION 'begin_precondition' USING ERRCODE='P0422';END IF;
     UPDATE public.execution_quota_leases l SET state='ACTIVE',revision=l.revision+1,descriptor=items.value
      FROM jsonb_each(adopted) items WHERE l.lease_id=items.key;
    END IF;
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
 RETURN jsonb_build_object('result',jsonb_build_object('receipt',response,'fresh_transition',fresh)||CASE WHEN fresh AND combined THEN jsonb_build_object('credential_ciphertext',(SELECT secret FROM public.channels WHERE id=(ref->>'channel_id')::uuid),'channel_id',ref->>'channel_id','snapshot',plan,'command_row_id',command_pk) WHEN fresh THEN jsonb_build_object('credential_ciphertext',(SELECT secret FROM public.channels WHERE id=a.provider_id),'channel_id',a.provider_id,'snapshot',permit.snapshot) WHEN combined AND response->>'command_state'='REJECTED' THEN jsonb_build_object('command_row_id',command_pk) ELSE '{}'::jsonb END,'cache',cache,'pending',pending);
END $_$;

CREATE FUNCTION core_funding.exposure_v1(price uuid, estimated jsonb, usage jsonb, finals text[], risk jsonb) RETURNS jsonb
    LANGUAGE plpgsql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE program core_funding.cost_prices%ROWTYPE;i integer;dimension text;estimated_text text;quantity_text text;e numeric;q numeric;term numeric;known_n numeric=0;total_n numeric=0;base_n numeric=0;ceiling_n numeric=0;divisor numeric;cost numeric;total numeric;base_total numeric;ceiling numeric;complete boolean=true;overrun boolean=false;risky text[]='{}';BEGIN
 SELECT * INTO program FROM core_funding.cost_prices WHERE price_id=price;
 IF program.price_id IS NULL THEN RAISE EXCEPTION 'supplier_price_missing' USING ERRCODE='P0422';END IF;
 IF jsonb_typeof(estimated) IS DISTINCT FROM 'object' OR jsonb_typeof(usage) IS DISTINCT FROM 'object' OR finals IS NULL OR jsonb_typeof(risk) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'supplier_exposure_invalid' USING ERRCODE='P0422';END IF;
 FOR i IN 1..cardinality(program.dimensions) LOOP
  dimension=program.dimensions[i];estimated_text=estimated->>dimension;
  IF estimated_text IS NULL THEN RAISE EXCEPTION 'supplier_estimate_missing' USING ERRCODE='P0422';END IF;
  e=core_funding.quantity_v1(estimated_text);term=e*program.weights[i];ceiling_n=ceiling_n+term;
  IF dimension=ANY(finals) THEN
   quantity_text=usage->>dimension;
   IF quantity_text IS NULL THEN RAISE EXCEPTION 'supplier_final_quantity_missing' USING ERRCODE='P0422';END IF;
   q=core_funding.quantity_v1(quantity_text);
   IF q>e THEN overrun=true;END IF;
   term=q*program.weights[i];known_n=known_n+term;total_n=total_n+term;base_n=base_n+term;
  ELSE
   complete=false;base_n=base_n+term;
   IF coalesce((risk->>dimension)::numeric,0)>estimated_text::numeric THEN
    risky=array_append(risky,dimension);term=core_funding.quantity_v1(risk->>dimension)*program.weights[i];
   END IF;
   total_n=total_n+term;
  END IF;
 END LOOP;
 divisor=program.denominator*1000000000000000000;
 cost=div(known_n,divisor)+CASE WHEN mod(known_n,divisor)>0 THEN 1 ELSE 0 END;
 total=div(total_n,divisor)+CASE WHEN mod(total_n,divisor)>0 THEN 1 ELSE 0 END;
 base_total=div(base_n,divisor)+CASE WHEN mod(base_n,divisor)>0 THEN 1 ELSE 0 END;
 ceiling=div(ceiling_n,divisor)+CASE WHEN mod(ceiling_n,divisor)>0 THEN 1 ELSE 0 END;
 IF greatest(cost,total,base_total,ceiling)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range';END IF;
 IF cardinality(risky)>1 THEN risky=ARRAY(SELECT d FROM unnest(risky) d ORDER BY d);END IF;
 RETURN jsonb_build_object('cost_atoms',cost::text,'reserve_atoms',(total-cost)::text,'base_reserve_atoms',(base_total-cost)::text,'complete',complete,'overrun',overrun OR base_total>ceiling,'risk_dimensions',to_jsonb(risky));
END $$;

CREATE FUNCTION core_funding.fence_wallet_send_change_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF (OLD.freeze_reason IS NOT NULL OR public.wallet_freeze_reason(OLD.evidence_disputed,OLD.manual_frozen,OLD.debt_atoms,OLD.credit_reserved_atoms,OLD.credit_limit_atoms) IS NOT NULL)
  IS DISTINCT FROM
  (NEW.freeze_reason IS NOT NULL OR public.wallet_freeze_reason(NEW.evidence_disputed,NEW.manual_frozen,NEW.debt_atoms,NEW.credit_reserved_atoms,NEW.credit_limit_atoms) IS NOT NULL) THEN
  PERFORM pg_advisory_xact_lock(hashtextextended('stg.wallet.send.v1:'||NEW.id::text,0));
 END IF;
 RETURN NULL;
END $$;

CREATE FUNCTION core_funding.flush_admit_v1(pending jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE inputs jsonb;results jsonb;allowed jsonb;updated integer;BEGIN
 IF pending->'admissions'='{}'::jsonb THEN RETURN;END IF;
 SELECT jsonb_agg((a.value->'customer')||jsonb_build_object('execution_budget',a.value->'execution_budget','binding_id',a.value->'binding_id') ORDER BY (a.value->>'sequence')::integer) INTO inputs FROM jsonb_each(pending->'admissions') a;
 results=public.allocate_customer_rows_v1(inputs,true);
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(inputs) WITH ORDINALITY i(value,n) WHERE results->((n-1)::text)->>'created' IS DISTINCT FROM 'true' OR results->((n-1)::text)->>'attempt_id' IS DISTINCT FROM i.value->>'attempt_id') THEN RAISE EXCEPTION 'admit_batch_allocation' USING ERRCODE='P0B01';END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'admissions') a
  JOIN public.execution_contexts c ON c.id=(a.value->>'context_id')::uuid
  LEFT JOIN public.api_keys k ON k.id=c.key_id LEFT JOIN public.projects p ON p.id=k.project_id LEFT JOIN public.organizations o ON o.id=p.organization_id LEFT JOIN public.principals u ON u.id=k.principal_id
  WHERE core_funding.auth_decision_v1(k,p,o,u,c.ip,c.operation,c.model,clock_timestamp()) IS DISTINCT FROM a.value->'authorization'
   OR c.expires_at<=clock_timestamp() OR c.owner_session_id IS DISTINCT FROM (a.value->>'owner_session_id')::uuid
   OR (a.value->>'command_deadline')::timestamptz<=clock_timestamp()
   OR (a.value?'short_chat_proof_deadline_ms' AND (a.value->>'short_chat_proof_deadline_ms')::numeric-extract(epoch FROM clock_timestamp())*1000<=1000)
   OR (a.value->'authority_times'->>'session_expires_at')::timestamptz<=clock_timestamp()
   OR (a.value->'authority_times'->>'certificate_not_before')::timestamptz>clock_timestamp()
   OR (a.value->'authority_times'->>'certificate_expires_at')::timestamptz<=clock_timestamp()) THEN RAISE EXCEPTION 'admit_batch_recheck' USING ERRCODE='P0B01';END IF;
 allowed=core_execution_runtime.allowed_v1((SELECT jsonb_agg(DISTINCT jsonb_build_object('ref',jsonb_build_object('package_digest',a.value->>'package_digest'))) FROM jsonb_each(pending->'admissions') a));
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'admissions') a WHERE (allowed->(a.value->>'package_digest') @> jsonb_build_array(a.value->'execution')) IS DISTINCT FROM true) THEN RAISE EXCEPTION 'admit_batch_recheck' USING ERRCODE='P0B01';END IF;
 IF results->'0'->'record'->>'state'='RUNNING' THEN
  UPDATE public.gateway_requests q SET state='ADMITTED',execution_budget=a.value->'execution_budget'
  FROM jsonb_each(pending->'admissions') a WHERE q.id=(a.value->'customer'->>'id')::uuid;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>jsonb_array_length(inputs) THEN RAISE EXCEPTION 'admit_batch_request_missing' USING ERRCODE='P0B01';END IF;
  UPDATE public.attempts t SET binding_id=(a.value->>'binding_id')::uuid
  FROM jsonb_each(pending->'admissions') a WHERE t.id=a.key::uuid AND t.request_id=(a.value->'customer'->>'id')::uuid;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>jsonb_array_length(inputs) THEN RAISE EXCEPTION 'admit_batch_attempt_missing' USING ERRCODE='P0B01';END IF;
 END IF;
 INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt)
  SELECT id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt FROM jsonb_to_recordset(pending->'commands') x(id uuid,command_id uuid,kind text,caller_identity text,owner_session_id uuid,caller_incarnation uuid,body_digest text,deadline_at timestamptz,trace_id text,contract_version integer,state text,request_id uuid,attempt_id uuid,context_id uuid,receipt jsonb);
 INSERT INTO public.execution_permits(granted_at,attempt_id,command_id,gateway_instance_id,owner_session_id,owner_key_sha256,owner_service,certificate_sha256,incarnation_id,permit_epoch,binding_id,dispatched_digest,state,expires_at,route_id,context_id,snapshot,manifest,manifest_digest,candidate_digest,authorization_plan_digest,prep_session_id,prep_binding_digest)
  SELECT CASE WHEN ep.state='SEND_GRANTED' THEN clock_timestamp() END,ep.attempt_id,ep.command_id,ep.gateway_instance_id,ep.owner_session_id,ep.owner_key_sha256,ep.owner_service,ep.certificate_sha256,ep.incarnation_id,ep.permit_epoch,ep.binding_id,ep.dispatched_digest,ep.state,ep.expires_at,ep.route_id,ep.context_id,ep.snapshot,ep.manifest,ep.manifest_digest,ep.candidate_digest,ep.authorization_plan_digest,ep.prep_session_id,ep.prep_binding_digest
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_populate_record(NULL::public.execution_permits,a.value->'permit') ep;
 INSERT INTO public.upstream_holds(attempt_id,account_id,channel_id,currency,estimated_atoms,cost_version_id,state,estimated_quantities,estimated_quantities_digest,estimate_manifest_digest,estimate_algorithm_version,funding_cell,current_reserved_atoms)
  SELECT h.attempt_id,h.account_id,h.channel_id,h.currency,h.estimated_atoms,h.cost_version_id,h.state,h.estimated_quantities,h.estimated_quantities_digest,h.estimate_manifest_digest,h.estimate_algorithm_version,h.funding_cell,h.current_reserved_atoms
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_populate_record(NULL::public.upstream_holds,a.value->'hold') h;
 PERFORM core_funding.flush_v1(pending->'funding');
 INSERT INTO public.execution_quota_leases(lease_id,attempt_id,command_id,scope_kind,scope_id,request_id,state,revision,descriptor)
  SELECT l.lease_id,l.attempt_id,l.command_id,l.scope_kind,l.scope_id,l.request_id,l.state,coalesce(l.revision,0),l.descriptor
  FROM jsonb_each(pending->'admissions') a CROSS JOIN LATERAL jsonb_to_recordset(a.value->'leases') l(lease_id text,attempt_id uuid,command_id uuid,scope_kind text,scope_id text,request_id uuid,state text,revision numeric,descriptor jsonb);
 INSERT INTO public.audit_events(id,action,resource_id,actor,details)
  SELECT id,action,resource_id,actor,details FROM jsonb_to_recordset(pending->'audits') x(id uuid,action text,resource_id text,actor text,details jsonb);
END $$;

CREATE FUNCTION core_funding.flush_begin_v1(pending jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE allowed jsonb;updated integer;BEGIN
 IF pending->'grants'='{}'::jsonb THEN RETURN;END IF;
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'grants') g
  LEFT JOIN public.api_keys k ON k.id=(g.value->>'key_id')::uuid LEFT JOIN public.projects p ON p.id=k.project_id LEFT JOIN public.organizations o ON o.id=p.organization_id LEFT JOIN public.principals u ON u.id=k.principal_id
  WHERE core_funding.auth_decision_v1(k,p,o,u,g.value->>'ip',g.value->>'operation',g.value->>'model',clock_timestamp()) IS DISTINCT FROM g.value->'authorization') THEN RAISE EXCEPTION 'begin_batch_recheck' USING ERRCODE='P0B01';END IF;
 allowed=core_execution_runtime.allowed_v1((SELECT jsonb_agg(DISTINCT jsonb_build_object('ref',jsonb_build_object('package_digest',g.value->>'package_digest'))) FROM jsonb_each(pending->'grants') g));
 IF EXISTS(SELECT 1 FROM jsonb_each(pending->'grants') g WHERE (allowed->(g.value->>'package_digest') @> jsonb_build_array(g.value->'execution')) IS DISTINCT FROM true) THEN RAISE EXCEPTION 'begin_batch_recheck' USING ERRCODE='P0B01';END IF;
 UPDATE public.execution_permits ep SET state='SEND_GRANTED',granted_at=clock_timestamp()
 FROM jsonb_each(pending->'grants') g JOIN public.api_keys k ON k.id=(g.value->>'key_id')::uuid
 WHERE ep.attempt_id=g.key::uuid AND ep.state='RESERVED'
  AND (ep.owner_service,ep.gateway_instance_id,ep.owner_session_id,ep.owner_key_sha256,ep.certificate_sha256,ep.incarnation_id,ep.permit_epoch,ep.context_id,ep.authorization_plan_digest,ep.dispatched_digest,ep.prep_session_id,ep.prep_binding_digest)
   IS NOT DISTINCT FROM ((g.value->'permit'->>'owner_service'),(g.value->'permit'->>'gateway_instance_id'),(g.value->'permit'->>'owner_session_id')::uuid,(g.value->'permit'->>'owner_key_sha256'),(g.value->'permit'->>'certificate_sha256'),(g.value->'permit'->>'incarnation_id')::uuid,(g.value->'permit'->>'permit_epoch')::numeric,(g.value->'permit'->>'context_id')::uuid,(g.value->'permit'->>'authorization_plan_digest'),(g.value->'permit'->>'dispatched_digest'),(g.value->'permit'->>'prep_session_id')::uuid,(g.value->'permit'->>'prep_binding_digest'))
  AND ep.expires_at>clock_timestamp() AND (g.value->>'command_deadline')::timestamptz>clock_timestamp()
  AND (g.value->'authority_times'->>'session_expires_at')::timestamptz>clock_timestamp()
  AND (g.value->'authority_times'->>'certificate_not_before')::timestamptz<=clock_timestamp()
  AND (g.value->'authority_times'->>'certificate_expires_at')::timestamptz>clock_timestamp()
  AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())
  AND (g.value->>'proof_deadline_ms')::numeric-extract(epoch FROM clock_timestamp())*1000>1000;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM jsonb_each(pending->'grants')) THEN RAISE EXCEPTION 'begin_batch_recheck' USING ERRCODE='P0B01';END IF;
 INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt)
  SELECT id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,receipt FROM jsonb_to_recordset(pending->'commands') x(id uuid,command_id uuid,kind text,caller_identity text,owner_session_id uuid,caller_incarnation uuid,body_digest text,deadline_at timestamptz,trace_id text,contract_version integer,state text,request_id uuid,attempt_id uuid,context_id uuid,receipt jsonb);
 INSERT INTO public.audit_events(id,action,resource_id,actor,details)
  SELECT id,action,resource_id,actor,details FROM jsonb_to_recordset(pending->'audits') x(id uuid,action text,resource_id text,actor text,details jsonb);
END $$;

CREATE FUNCTION core_funding.flush_costs_v1(pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE updated integer;BEGIN
 IF pending IS NULL THEN RETURN NULL;END IF;
 IF jsonb_array_length(coalesce(pending->'costs','[]'))>0 THEN
  INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id)
   SELECT c.attempt_id,c.revision,c.currency,c.cost_atoms,c.complete,c.disputed,c.evidence_digest,c.evidence_id,c.cost_version_id FROM jsonb_array_elements(pending->'costs') x CROSS JOIN LATERAL jsonb_populate_record(NULL::public.upstream_costs,x) c;
  INSERT INTO public.upstream_cost_heads(attempt_id,revision)
   SELECT (c->>'attempt_id')::uuid,(c->>'revision')::numeric FROM jsonb_array_elements(pending->'costs') c ON CONFLICT(attempt_id) DO UPDATE SET revision=excluded.revision;
 END IF;
 IF coalesce(pending->'holds','{}')<>'{}'::jsonb THEN
  UPDATE public.upstream_holds h SET current_reserved_atoms=(d.value->>'current_reserved_atoms')::numeric,state=d.value->>'state',final_cost_atoms=(d.value->>'final_cost_atoms')::numeric
   FROM jsonb_each(pending->'holds') d WHERE h.attempt_id=d.key::uuid AND h.current_reserved_atoms=(d.value->>'base_reserved_atoms')::numeric;
  GET DIAGNOSTICS updated=ROW_COUNT;
  IF updated<>(SELECT count(*) FROM jsonb_each(pending->'holds')) THEN RAISE EXCEPTION 'evidence_hold_base_changed' USING ERRCODE='P0500';END IF;
 END IF;
 RETURN pending-ARRAY['costs','holds'];
END $$;

CREATE TABLE public.core_evidence_receipts (
    id uuid NOT NULL,
    origin_identity text NOT NULL,
    origin_incarnation uuid NOT NULL,
    origin_session_id uuid NOT NULL,
    event_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    source text NOT NULL,
    source_revision numeric(38,0) NOT NULL,
    body_digest text NOT NULL,
    event_claim boolean NOT NULL,
    revision_claim boolean NOT NULL,
    outcome text NOT NULL,
    finality text NOT NULL,
    attempt_state text,
    request_state text,
    error_code text,
    provider_request_id text,
    raw_evidence_id uuid NOT NULL,
    canonical_evidence_id uuid,
    disputed_evidence_id uuid,
    conflict_id uuid,
    risk_dimensions text[] NOT NULL,
    numeric_fault text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    authentication_disposition text NOT NULL,
    authentication_reason text NOT NULL,
    producer_key_sha256 text NOT NULL,
    signature_version integer NOT NULL,
    signature bytea NOT NULL,
    signature_payload_digest text NOT NULL,
    CONSTRAINT core_receipt_authentication CHECK (((authentication_disposition = ANY (ARRAY['VERIFIED'::text, 'QUARANTINED'::text])) AND (authentication_reason = ANY (ARRAY['NORMAL'::text, 'SECURITY_REVOKED'::text, 'UNGRANTED'::text])) AND ((authentication_disposition = 'VERIFIED'::text) = (authentication_reason = 'NORMAL'::text)) AND ((authentication_disposition = 'QUARANTINED'::text) = (outcome = 'QUARANTINED'::text)) AND ((outcome <> 'QUARANTINED'::text) OR ((canonical_evidence_id IS NULL) AND (disputed_evidence_id IS NULL) AND (conflict_id IS NULL) AND (NOT revision_claim) AND (cardinality(risk_dimensions) = 0))) AND (producer_key_sha256 ~ '^[a-f0-9]{64}$'::text) AND (signature_version = 1) AND (octet_length(signature) = 64) AND (signature_payload_digest ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT core_receipt_outcome CHECK (((outcome = ANY (ARRAY['ACCEPTED'::text, 'STALE'::text, 'ESTIMATED'::text, 'REVISION_REPLAY'::text, 'CONFLICT'::text, 'OVERFLOW'::text, 'QUARANTINED'::text])) AND (finality = ANY (ARRAY['FINAL'::text, 'PARTIAL'::text, 'UNKNOWN'::text, 'ESTIMATED'::text, 'NOT_EXECUTED'::text])) AND ((outcome <> 'CONFLICT'::text) OR ((conflict_id IS NOT NULL) AND (disputed_evidence_id IS NOT NULL) AND (NOT revision_claim))) AND ((outcome <> 'OVERFLOW'::text) OR ((numeric_fault = 'COST_OVERFLOW'::text) AND (disputed_evidence_id IS NOT NULL))) AND ((outcome <> 'ACCEPTED'::text) OR (canonical_evidence_id IS NOT NULL)) AND ((outcome <> ALL (ARRAY['ACCEPTED'::text, 'REVISION_REPLAY'::text])) OR ((disputed_evidence_id IS NULL) AND (conflict_id IS NULL) AND (numeric_fault IS NULL))) AND ((numeric_fault IS NULL) OR (numeric_fault = 'COST_OVERFLOW'::text)) AND ((attempt_state IS NULL) OR (attempt_state = ANY (ARRAY['SUCCEEDED'::text, 'FAILED'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'NOT_EXECUTED'::text]))) AND ((request_state IS NULL) OR (request_state = ANY (ARRAY['SUCCEEDED'::text, 'FAILED'::text, 'CANCELLED'::text, 'UNKNOWN'::text]))) AND ((error_code IS NULL) OR (error_code ~ '^[a-z][a-z0-9_]{0,79}$'::text)) AND ((provider_request_id IS NULL) OR (provider_request_id ~ '^[A-Za-z0-9._:/-]{1,256}$'::text)) AND (cardinality(risk_dimensions) <= 16))),
    CONSTRAINT core_receipt_source CHECK (((source = ANY (ARRAY['execution'::text, 'execution_outcome'::text, 'recovery_interruption'::text, 'async_partial'::text, 'async_final'::text, 'batch_completed'::text, 'batch_cancelled'::text, 'batch_failed'::text, 'provider_invoice'::text])) AND (source_revision >= (1)::numeric) AND (body_digest ~ '^[a-f0-9]{64}$'::text) AND (origin_identity ~ '^gateway:[A-Za-z0-9_-]{1,100}$'::text)))
);

CREATE FUNCTION core_funding.flush_evidence_stages_v1(snapshots core_funding.evidence_snapshot_stage_v1[], receipts public.core_evidence_receipts[], attempts core_funding.evidence_attempt_stage_v1[], requests core_funding.evidence_request_stage_v1[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE updated integer;
BEGIN
 INSERT INTO public.usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete,record_kind,snapshot_revision,snapshot_digest,final_dimensions,parent_evidence_id,input_fact_id)
 SELECT (f).id,(f).project_id,(f).request_id,(f).attempt_id,(f).source,(f).finality,(f).cost_atoms,
  (f).currency,(f).cost_complete,(f).record_kind,(f).snapshot_revision,(f).snapshot_digest,
  (f).final_dimensions,(f).parent_evidence_id,(f).input_fact_id
 FROM unnest(snapshots) s(f,usage) ORDER BY (f).attempt_id,(f).snapshot_revision;
 INSERT INTO public.usage_payloads(id,created_at,usage)
 SELECT (f).id,(f).created_at,s.usage FROM unnest(snapshots) s(f,usage);
 INSERT INTO public.event_deliveries(consumer_id,event_id)
 SELECT c.id,(f).id FROM unnest(snapshots) s(f,usage) CROSS JOIN public.event_consumers c WHERE (f).record_kind<>'RAW';
 INSERT INTO public.core_evidence_receipts(id,origin_identity,origin_incarnation,origin_session_id,event_id,attempt_id,source,source_revision,body_digest,event_claim,revision_claim,outcome,finality,attempt_state,request_state,error_code,provider_request_id,raw_evidence_id,canonical_evidence_id,disputed_evidence_id,conflict_id,risk_dimensions,numeric_fault,authentication_disposition,authentication_reason,producer_key_sha256,signature_version,signature,signature_payload_digest)
 SELECT r.id,r.origin_identity,r.origin_incarnation,r.origin_session_id,r.event_id,r.attempt_id,r.source,r.source_revision,
  r.body_digest,r.event_claim,r.revision_claim,r.outcome,r.finality,r.attempt_state,r.request_state,r.error_code,
  r.provider_request_id,r.raw_evidence_id,r.canonical_evidence_id,r.disputed_evidence_id,r.conflict_id,r.risk_dimensions,
  r.numeric_fault,r.authentication_disposition,r.authentication_reason,r.producer_key_sha256,r.signature_version,
  r.signature,r.signature_payload_digest
 FROM unnest(receipts) r;
 UPDATE public.attempts a SET evidence_id=n.head_id,state=n.state,finished_at=n.finished_at,error_code=n.error_code,provider_request_id=n.provider_request_id
 FROM unnest(attempts) n WHERE n.dirty AND a.id=n.id;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM unnest(attempts) n WHERE n.dirty) THEN RAISE EXCEPTION 'evidence_attempt_missing' USING ERRCODE='P0500';END IF;
 UPDATE public.gateway_requests q SET selected_attempt_id=n.selected_attempt_id,state=n.state,billing_outcome=n.billing_outcome,updated_at=n.updated_at
 FROM unnest(requests) n WHERE n.dirty AND q.id=n.id;
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM unnest(requests) n WHERE n.dirty) THEN RAISE EXCEPTION 'evidence_request_missing' USING ERRCODE='P0500';END IF;
END $$;

CREATE FUNCTION core_funding.flush_first_final_costs_v1(items core_funding.first_final_cost_v1[], pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE
 delta record;
 cell_state core_funding.balance_cells%ROWTYPE;
 cell_key text;
 base jsonb;
 next_state jsonb;
 cells jsonb=jsonb_build_object('cells',pending->'cells');
 journals jsonb;
 entries jsonb;
 changed integer;
BEGIN
 IF EXISTS(
  SELECT 1 FROM (SELECT DISTINCT (s.hold).account_id AS id FROM unnest(items) s WHERE s.journal_id IS NOT NULL) a
  WHERE NOT core_funding.held_v1(a.id)
 ) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 FOR delta IN
  SELECT (s.hold).account_id AS account_id,(s.hold).currency AS currency,(s.hold).funding_cell AS cell,
   sum((s.hold).current_reserved_atoms) AS reserved,sum(s.cost_atoms) AS spent,count(*) AS revisions,
   bool_and(s.cost_atoms IS NOT NULL AND s.cost_atoms>=0 AND s.cost_atoms=trunc(s.cost_atoms)
    AND s.cost_atoms<=(s.hold).current_reserved_atoms) AS covered
  FROM unnest(items) s WHERE s.journal_id IS NOT NULL
  GROUP BY (s.hold).account_id,(s.hold).currency,(s.hold).funding_cell
  ORDER BY (s.hold).account_id,(s.hold).currency,(s.hold).funding_cell
 LOOP
  IF delta.covered IS DISTINCT FROM true THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
  cell_key=delta.account_id::text||':'||delta.currency||':'||delta.cell::text;
  base=cells->'cells'->cell_key->'base';
  IF base IS NULL THEN
   SELECT * INTO STRICT cell_state FROM core_funding.balance_cells c
   WHERE (c.account_id,c.currency,c.cell)=(delta.account_id,delta.currency,delta.cell) FOR NO KEY UPDATE NOWAIT;
   base=to_jsonb(cell_state);
  ELSE
   cell_state=jsonb_populate_record(NULL::core_funding.balance_cells,cells->'cells'->cell_key->'next');
  END IF;
  IF cell_state.reserved_atoms<delta.reserved THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
  IF cell_state.spent_atoms+delta.spent>cell_state.spent_limit OR cell_state.revision+delta.revisions>cell_state.revision_limit
   OR cell_state.balance_atoms<cell_state.reserved_atoms THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
  next_state=to_jsonb(cell_state)||jsonb_build_object('balance_atoms',cell_state.balance_atoms-delta.spent,
   'reserved_atoms',cell_state.reserved_atoms-delta.reserved,'spent_atoms',cell_state.spent_atoms+delta.spent,'revision',cell_state.revision+delta.revisions);
  cells=jsonb_set(cells,ARRAY['cells',cell_key],jsonb_build_object('base',base,'next',next_state));
 END LOOP;

 INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id)
 SELECT (s.hold).attempt_id,1,(s.hold).currency,s.cost_atoms,true,false,
  public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(s.final_dimensions),'cost_version_id',(s.hold).cost_version_id,'estimated_quantities_digest',(s.hold).estimated_quantities_digest,'cost_atoms',s.cost_atoms::text)),
  s.evidence_id,(s.hold).cost_version_id
 FROM unnest(items) s;
 INSERT INTO public.upstream_cost_heads(attempt_id,revision)
 SELECT (s.hold).attempt_id,1 FROM unnest(items) s;
 UPDATE public.upstream_holds h SET state='SETTLED',current_reserved_atoms=0,final_cost_atoms=s.cost_atoms
 FROM unnest(items) s WHERE h.attempt_id=(s.hold).attempt_id AND h.state='HELD'
 AND h.current_reserved_atoms=(s.hold).current_reserved_atoms;
 GET DIAGNOSTICS changed=ROW_COUNT;
 IF changed<>cardinality(items) THEN RAISE EXCEPTION 'evidence_hold_base_changed' USING ERRCODE='P0500';END IF;

 SELECT coalesce(jsonb_agg(jsonb_build_object('id',s.journal_id,'account_id',(s.hold).account_id,'channel_id',(s.hold).channel_id,
  'attempt_id',(s.hold).attempt_id,'currency',(s.hold).currency,'kind',CASE WHEN s.cost_atoms<>0 THEN 'COST' ELSE 'RISK' END,
  'reference','cost:'||(s.hold).attempt_id||':'||s.snapshot_revision)),'[]')
 INTO journals FROM unnest(items) s WHERE s.journal_id IS NOT NULL;
 SELECT coalesce(jsonb_agg(to_jsonb(leg)),'[]') INTO entries FROM (
  SELECT s.journal_id,row_number() OVER (PARTITION BY s.journal_id ORDER BY v.n)-1 AS ordinal,(s.hold).currency,v.account,v.atoms
  FROM unnest(items) s CROSS JOIN LATERAL (VALUES
   (0,'upstream:available',(s.hold).current_reserved_atoms-s.cost_atoms),
   (1,'upstream:reserved',-(s.hold).current_reserved_atoms),
   (2,'upstream:expense',s.cost_atoms)
  ) v(n,account,atoms) WHERE s.journal_id IS NOT NULL AND v.atoms<>0
 ) leg;
 RETURN pending||cells||jsonb_build_object('journals',(pending->'journals')||journals,'entries',(pending->'entries')||entries);
END $$;

CREATE FUNCTION core_funding.flush_v1(pending jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE updated integer;BEGIN
 IF jsonb_typeof(pending->'cells') IS DISTINCT FROM 'object' OR jsonb_typeof(pending->'journals') IS DISTINCT FROM 'array' OR jsonb_typeof(pending->'entries') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 IF jsonb_array_length(pending->'journals')>0 THEN
  INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference)
   SELECT id,account_id,channel_id,attempt_id,currency,kind,reference FROM jsonb_to_recordset(pending->'journals') j(id uuid,account_id uuid,channel_id uuid,attempt_id uuid,currency text,kind text,reference text);
  INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms)
   SELECT journal_id,ordinal,currency,account,atoms FROM jsonb_to_recordset(pending->'entries') e(journal_id uuid,ordinal integer,currency text,account text,atoms numeric);
  SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
  SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
 END IF;
 IF pending->'cells'='{}'::jsonb THEN RETURN;END IF;
 UPDATE core_funding.balance_cells c SET balance_atoms=n.balance_atoms,reserved_atoms=n.reserved_atoms,spent_atoms=n.spent_atoms,revision=n.revision
 FROM jsonb_each(pending->'cells') e CROSS JOIN LATERAL jsonb_populate_record(NULL::core_funding.balance_cells,e.value->'base') b CROSS JOIN LATERAL jsonb_populate_record(NULL::core_funding.balance_cells,e.value->'next') n
 WHERE (c.account_id,c.currency,c.cell)=(b.account_id,b.currency,b.cell) AND (n.account_id,n.currency,n.cell)=(b.account_id,b.currency,b.cell)
  AND (c.balance_atoms,c.reserved_atoms,c.spent_atoms,c.revision,c.spent_limit,c.revision_limit) IS NOT DISTINCT FROM (b.balance_atoms,b.reserved_atoms,b.spent_atoms,b.revision,b.spent_limit,b.revision_limit)
  AND (n.spent_limit,n.revision_limit) IS NOT DISTINCT FROM (b.spent_limit,b.revision_limit);
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>(SELECT count(*) FROM jsonb_each(pending->'cells')) THEN RAISE EXCEPTION 'funding_batch_base_changed' USING ERRCODE='P0500';END IF;
END $$;

CREATE FUNCTION core_funding.freeze_disputed_wallet_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 UPDATE public.wallets w SET evidence_disputed=true,freeze_reason='EVIDENCE_CONFLICT'
 WHERE w.id=NEW.wallet_id AND (NOT w.evidence_disputed OR w.freeze_reason IS DISTINCT FROM 'EVIDENCE_CONFLICT')
 AND NOT EXISTS(SELECT 1 FROM public.billing_resolutions r WHERE r.request_id=NEW.request_id);
 RETURN NULL;
END $$;

CREATE FUNCTION core_funding.guard_account_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' OR (NEW.id,NEW.state,NEW.mode) IS DISTINCT FROM (OLD.id,OLD.state,OLD.mode) THEN
  IF TG_OP='DELETE' OR NEW.id IS DISTINCT FROM OLD.id THEN RAISE EXCEPTION 'funding_account_immutable' USING ERRCODE='P0403';END IF;
  IF NOT core_funding.held_v1(NEW.id,true) OR NOT core_funding.catalog_held_v1() THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.guard_cell_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE exclusive boolean;old_balance numeric=0;old_reserved numeric=0;old_spent_limit numeric=0;old_revision_limit numeric=0;total_balance numeric;total_reserved numeric;total_spent_limit numeric;total_revision_limit numeric;BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'funding_cell_immutable' USING ERRCODE='P0403';END IF;
 exclusive=core_funding.held_v1(NEW.account_id,true);
 IF TG_OP='UPDATE' THEN
  IF (NEW.account_id,NEW.currency,NEW.cell) IS DISTINCT FROM (OLD.account_id,OLD.currency,OLD.cell) OR NEW.spent_atoms<OLD.spent_atoms OR NEW.revision<OLD.revision THEN RAISE EXCEPTION 'funding_cell_immutable' USING ERRCODE='P0403';END IF;
  old_balance=OLD.balance_atoms;old_reserved=OLD.reserved_atoms;old_spent_limit=OLD.spent_limit;old_revision_limit=OLD.revision_limit;
  IF NOT exclusive THEN
   IF NOT core_funding.held_v1(NEW.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
   IF NEW.spent_limit IS DISTINCT FROM OLD.spent_limit OR NEW.revision_limit IS DISTINCT FROM OLD.revision_limit OR NEW.balance_atoms-OLD.balance_atoms IS DISTINCT FROM -(NEW.spent_atoms-OLD.spent_atoms) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
   IF NEW.balance_atoms<NEW.reserved_atoms OR NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=NEW.account_id AND state='ACTIVE') THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
   RETURN NEW;
  END IF;
 ELSIF NOT exclusive THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 SELECT b.balance_atoms+coalesce((SELECT sum(c.balance_atoms) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.balance_atoms-old_balance,
  coalesce((SELECT sum(c.reserved_atoms) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.reserved_atoms-old_reserved,
  b.spent_atoms+coalesce((SELECT sum(c.spent_limit) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.spent_limit-old_spent_limit,
  b.revision+coalesce((SELECT sum(c.revision_limit) FROM core_funding.balance_cells c WHERE c.account_id=NEW.account_id AND c.currency=NEW.currency),0)+NEW.revision_limit-old_revision_limit
 INTO total_balance,total_reserved,total_spent_limit,total_revision_limit FROM public.upstream_account_balances b WHERE b.account_id=NEW.account_id AND b.currency=NEW.currency;
 IF total_balance IS NULL OR abs(total_balance)>99999999999999999999999999999999999999 OR total_reserved>99999999999999999999999999999999999999 OR total_spent_limit>99999999999999999999999999999999999999 OR total_revision_limit>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.guard_root_v1() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'funding_root_immutable' USING ERRCODE='P0403';END IF;
 IF TG_OP='UPDATE' AND ((NEW.account_id,NEW.currency) IS DISTINCT FROM (OLD.account_id,OLD.currency) OR NEW.spent_atoms<OLD.spent_atoms OR NEW.revision<OLD.revision) THEN RAISE EXCEPTION 'funding_root_immutable' USING ERRCODE='P0403';END IF;
 IF NEW.reserved_atoms<>0 THEN RAISE EXCEPTION 'funding_root_reservation_forbidden' USING ERRCODE='P0403';END IF;
 IF NOT core_funding.held_v1(NEW.account_id,true) AND NOT(TG_OP='INSERT' AND (NEW.balance_atoms,NEW.reserved_atoms,NEW.spent_atoms,NEW.adjustment_atoms,NEW.revision)=(0,0,0,0,0)) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION core_funding.headroom_v1(account uuid, cur text, target integer, needed_spent numeric, needed_revision numeric) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE r public.upstream_account_balances%ROWTYPE;t core_funding.balance_cells%ROWTYPE;c record;free_spent numeric;free_revision numeric;actual_spent numeric;actual_revision numeric;take_spent numeric;take_revision numeric;need_spent numeric;need_revision numeric;grant_spent numeric;grant_revision numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 SELECT * INTO STRICT r FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 SELECT * INTO STRICT t FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=target FOR NO KEY UPDATE NOWAIT;
 SELECT r.spent_atoms+coalesce(sum(spent_atoms),0),r.revision+coalesce(sum(revision),0),99999999999999999999999999999999999999-r.spent_atoms-coalesce(sum(spent_limit),0),99999999999999999999999999999999999999-r.revision-coalesce(sum(revision_limit),0)
 INTO actual_spent,actual_revision,free_spent,free_revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur;
 IF needed_spent<t.spent_atoms OR needed_revision<t.revision OR actual_spent+needed_spent-t.spent_atoms>99999999999999999999999999999999999999 OR actual_revision+needed_revision-t.revision>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 need_spent=greatest(needed_spent-t.spent_limit,0);need_revision=greatest(needed_revision-t.revision_limit,0);
 FOR c IN SELECT cell,spent_limit-spent_atoms unused_spent,revision_limit-revision unused_revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell<>target ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN free_spent>=need_spent AND free_revision>=need_revision;
  take_spent=least(c.unused_spent,greatest(need_spent-free_spent,0));take_revision=least(c.unused_revision,greatest(need_revision-free_revision,0));
  IF take_spent>0 OR take_revision>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,0,-take_spent,-take_revision,'headroom:return:'||gen_random_uuid());free_spent=free_spent+take_spent;free_revision=free_revision+take_revision;END IF;
 END LOOP;
 IF free_spent<need_spent OR free_revision<need_revision THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 grant_spent=CASE WHEN need_spent>0 THEN greatest(need_spent,least(free_spent,div(99999999999999999999999999999999999999-actual_spent,32))) ELSE 0 END;
 grant_revision=CASE WHEN need_revision>0 THEN greatest(need_revision,least(free_revision,div(99999999999999999999999999999999999999-actual_revision,32))) ELSE 0 END;
 IF grant_spent>0 OR grant_revision>0 THEN PERFORM core_funding.transfer_v1(account,cur,target,0,grant_spent,grant_revision,'headroom:grant:'||gen_random_uuid());END IF;
END $$;

CREATE FUNCTION core_funding.held_v1(account uuid, exclusive boolean DEFAULT false) RETURNS boolean
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT EXISTS(
  SELECT 1 FROM pg_catalog.pg_locks l
  WHERE l.locktype='advisory' AND l.pid=pg_backend_pid() AND l.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
   AND l.classid::bigint=((core_funding.account_key_v1(account)>>32)&4294967295)
   AND l.objid::bigint=(core_funding.account_key_v1(account)&4294967295) AND l.objsubid=1 AND l.granted
   AND (l.mode='ExclusiveLock' OR NOT exclusive AND l.mode='ShareLock')
 )
$$;

CREATE FUNCTION core_funding.make_evidence_snapshot_v1(attempt uuid, project uuid, request_id uuid, currency text, revision numeric, kind text, source text, finality text, usage jsonb, finals text[], parent uuid, input uuid, cost numeric, complete boolean) RETURNS core_funding.evidence_snapshot_stage_v1
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE fact public.usage_event_index%ROWTYPE;
BEGIN
 fact.id=gen_random_uuid();
 fact.created_at=transaction_timestamp();
 fact.project_id=project;
 fact.request_id=request_id;
 fact.attempt_id=attempt;
 fact.source=source;
 fact.finality=finality;
 fact.cost_atoms=cost;
 fact.currency=currency;
 fact.cost_complete=coalesce(complete,false);
 fact.record_kind=kind;
 fact.snapshot_revision=revision;
 fact.snapshot_digest=public.core_digest_v1(jsonb_build_object(
  'attempt_id',attempt,'record_kind',kind,'snapshot_revision',revision::text,'source',source,
  'finality',finality,'usage',usage,'final_dimensions',to_jsonb(finals),'parent',parent,'input',input,'cost_atoms',cost::text
 ));
 fact.final_dimensions=finals;
 fact.parent_evidence_id=parent;
 fact.input_fact_id=input;
 RETURN ROW(fact,usage)::core_funding.evidence_snapshot_stage_v1;
END $$;

CREATE FUNCTION core_funding.normalize_v1(account uuid, cur text) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE c record;root numeric;deficit numeric;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 PERFORM core_funding.collect_v1(account,cur,0);
 FOR c IN SELECT cell,reserved_atoms-balance_atoms deficit FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms<reserved_atoms ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  deficit=c.deficit;
  WHILE deficit>0 LOOP
   PERFORM core_funding.collect_v1(account,cur,least(deficit,99999999999999999999999999999999999999));
   SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
   EXIT WHEN root<=0;
   take=least(root,deficit);
   PERFORM core_funding.transfer_v1(account,cur,c.cell,take,0,0,'normalize:'||gen_random_uuid());
   deficit=deficit-take;
  END LOOP;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.offset_debt_v1(account uuid, cur text, needed numeric) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE root numeric;c record;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) OR EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE') THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 IF needed IS NULL OR needed<=0 OR needed>99999999999999999999999999999999999999 OR needed<>trunc(needed) OR root>=0 OR needed> -root THEN RAISE EXCEPTION 'funding_debt_offset_invalid' USING ERRCODE='P0400';END IF;
 FOR c IN SELECT cell,balance_atoms FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND balance_atoms>0 ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN needed=0;
  take=least(c.balance_atoms,needed);
  UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms+take WHERE account_id=account AND currency=cur;
  UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms-take WHERE account_id=account AND currency=cur AND cell=c.cell;
  INSERT INTO core_funding.budget_transfers(id,account_id,currency,cell,kind,reference,balance_delta) VALUES(gen_random_uuid(),account,cur,c.cell,'REBASE','debt-offset:'||gen_random_uuid(),-take);
  needed=needed-take;
 END LOOP;
 IF needed<>0 THEN RAISE EXCEPTION 'funding_debt_offset_inconsistent' USING ERRCODE='P0500';END IF;
END $$;

CREATE FUNCTION core_funding.persist_v1(p_groups jsonb, bulk boolean) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE g jsonb;pending jsonb=CASE WHEN bulk THEN '{"cells":{},"journals":[],"entries":[]}'::jsonb END;applied jsonb;results jsonb='[]';failure text;message text;batch uuid=gen_random_uuid();ordinal integer=0;reason text;ev jsonb;BEGIN
 IF jsonb_typeof(p_groups) IS DISTINCT FROM 'array' OR jsonb_array_length(p_groups) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
 IF bulk AND jsonb_array_length(p_groups)>1 THEN
  BEGIN
   applied=core_funding.coalesce_groups_v1(p_groups,pending);
   IF applied IS NOT NULL THEN
    PERFORM core_funding.flush_v1(applied->'pending');RETURN applied->'results';
   END IF;
  EXCEPTION WHEN OTHERS THEN pending='{"cells":{},"journals":[],"entries":[]}'::jsonb;results='[]';END;
 END IF;
 BEGIN
 FOR g IN SELECT value FROM jsonb_array_elements(p_groups) LOOP
  reason=NULL;
  IF jsonb_typeof(g) IS NOT DISTINCT FROM 'object' AND g?'rejected' THEN
   IF g->>'rejected' IS DISTINCT FROM 'evidence_signature_invalid' THEN RAISE EXCEPTION 'evidence_batch_invalid' USING ERRCODE='P0400';END IF;
   reason=g->>'rejected';results=results||jsonb_build_array(jsonb_build_object('status','REJECTED','error',reason));
  ELSE
   BEGIN
    applied=core_funding.evidence_group_v1(g,pending);
    results=results||jsonb_build_array(applied->'result');
    IF bulk THEN pending=applied->'pending';END IF;
   EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
    IF failure IN ('P0400','P0403') THEN reason=message;results=results||jsonb_build_array(jsonb_build_object('status','REJECTED','error',message));
    ELSE results=results||jsonb_build_array(jsonb_build_object('status','ERROR','error',failure));END IF;
   END;
  END IF;
  IF reason IS NOT NULL THEN
   FOR ev IN SELECT value FROM jsonb_array_elements(g->'events') LOOP
    INSERT INTO public.core_evidence_rejections(id,batch_id,ordinal,reason,origin_identity,origin_session_id,event_id,attempt_id)
    VALUES(gen_random_uuid(),batch,ordinal,reason,ev->>'origin_identity',(ev->>'origin_session_id')::uuid,(ev->>'event_id')::uuid,(ev->>'attempt_id')::uuid);
    ordinal=ordinal+1;
   END LOOP;
  END IF;
 END LOOP;
 IF bulk THEN PERFORM core_funding.flush_v1(pending);END IF;
 EXCEPTION WHEN OTHERS THEN
  IF NOT bulk THEN RAISE;END IF;
  RETURN core_funding.persist_v1(p_groups,false);
 END;
 RETURN results;
END $$;

CREATE FUNCTION core_funding.price_v1(usage jsonb, dimensions text[], weights numeric[], denominator numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE i integer;total numeric=0;divisor numeric;result numeric;BEGIN
 IF dimensions IS NULL OR weights IS NULL OR cardinality(dimensions)<>cardinality(weights) OR denominator IS NULL OR denominator<=0 OR denominator<>trunc(denominator) THEN RAISE EXCEPTION 'invalid compiled price';END IF;
 FOR i IN 1..cardinality(dimensions) LOOP
  total=total+core_funding.quantity_v1(coalesce(usage->>dimensions[i],'0'))*weights[i];
 END LOOP;
 divisor=denominator*1000000000000000000;
 result=div(total,divisor)+CASE WHEN mod(total,divisor)>0 THEN 1 ELSE 0 END;
 IF result>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range';END IF;
 RETURN result;
END $$;

CREATE FUNCTION core_funding.project_v1(h public.upstream_holds, cost_delta numeric, new_reserve numeric, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k text=h.account_id::text||':'||h.currency||':'||h.funding_cell::text;entry jsonb;c core_funding.balance_cells%ROWTYPE;b numeric;r numeric;s numeric;v numeric;BEGIN
 IF jsonb_typeof(pending->'cells') IS DISTINCT FROM 'object' OR cost_delta IS NULL OR new_reserve IS NULL OR cost_delta<0 OR new_reserve<0 OR cost_delta<>trunc(cost_delta) OR new_reserve<>trunc(new_reserve) THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 IF NOT core_funding.held_v1(h.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 entry=pending->'cells'->k;
 IF entry IS NULL THEN
  SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=h.account_id AND currency=h.currency AND cell=h.funding_cell FOR NO KEY UPDATE NOWAIT;
  entry=jsonb_build_object('base',to_jsonb(c),'next',to_jsonb(c));
 ELSE c=jsonb_populate_record(NULL::core_funding.balance_cells,entry->'next');END IF;
 b=c.balance_atoms-cost_delta;r=c.reserved_atoms+new_reserve-h.current_reserved_atoms;s=c.spent_atoms+cost_delta;v=c.revision+1;
 IF r<0 OR r>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 IF s>c.spent_limit OR v>c.revision_limit OR b<r THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 RETURN jsonb_set(pending,ARRAY['cells',k],jsonb_build_object('base',entry->'base','next',(entry->'next')||jsonb_build_object('balance_atoms',b,'reserved_atoms',r,'spent_atoms',s,'revision',v)));
END $$;

CREATE FUNCTION core_funding.quantity_v1(quantity text) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE fraction integer;BEGIN
 IF (quantity ~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') IS NOT TRUE THEN RAISE EXCEPTION 'invalid price input';END IF;
 fraction=CASE WHEN position('.' IN quantity)>0 THEN length(split_part(quantity,'.',2)) ELSE 0 END;
 RETURN replace(quantity,'.','')::numeric*power(10::numeric,18-fraction);
END $_$;

CREATE FUNCTION core_funding.reserve_batch_v1(account uuid, cur text, attempt uuid, amount numeric, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE c core_funding.balance_cells%ROWTYPE;entry jsonb;k text;owned smallint=NULLIF(current_setting('stg.short_chat_funding_cell',true),'')::smallint;preferred integer=coalesce(owned,get_byte(uuid_send(attempt),15)%16);BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(pending->'cells') IS DISTINCT FROM 'object' OR amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT state.* INTO c FROM core_funding.balance_cells state
 WHERE state.account_id=account AND state.currency=cur AND (owned IS NULL OR state.cell=owned)
  AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
  AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit
 ORDER BY (state.cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE OF state SKIP LOCKED;
 IF NOT FOUND THEN
  IF EXISTS(SELECT 1 FROM core_funding.balance_cells state
   WHERE state.account_id=account AND state.currency=cur AND (owned IS NULL OR state.cell=owned)
    AND state.balance_atoms-coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'reserved_atoms')::numeric,state.reserved_atoms)>=amount
    AND coalesce((pending->'cells'->(state.account_id::text||':'||state.currency||':'||state.cell::text)->'next'->>'revision')::numeric,state.revision)<state.revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
  RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';
 END IF;
 k=account::text||':'||cur||':'||c.cell::text;entry=pending->'cells'->k;
 IF entry IS NULL THEN entry=jsonb_build_object('base',to_jsonb(c),'next',to_jsonb(c));ELSE c=jsonb_populate_record(NULL::core_funding.balance_cells,entry->'next');END IF;
 pending=jsonb_set(pending,ARRAY['cells',k],jsonb_build_object('base',entry->'base','next',(entry->'next')||jsonb_build_object('reserved_atoms',c.reserved_atoms+amount,'revision',c.revision+1)));
 RETURN jsonb_build_object('cell',c.cell,'pending',pending);
END $$;

CREATE FUNCTION core_funding.reserve_v1(account uuid, cur text, attempt uuid, amount numeric) RETURNS smallint
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE target smallint;owned smallint=NULLIF(current_setting('stg.short_chat_funding_cell',true),'')::smallint;preferred integer=coalesce(owned,get_byte(uuid_send(attempt),15)%16);c core_funding.balance_cells%ROWTYPE;free numeric;root numeric;budget_grant numeric;seed record;slots integer=16;actual_spent numeric;BEGIN
 IF NOT core_funding.held_v1(account) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 IF amount IS NULL OR amount<0 OR amount<>trunc(amount) OR amount>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_budget_invalid' USING ERRCODE='P0400';END IF;
 IF NOT EXISTS(SELECT 1 FROM public.upstream_accounts WHERE id=account AND state='ACTIVE' AND mode='PREPAID') THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
 SELECT * INTO c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND (owned IS NULL OR cell=owned) AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit
 ORDER BY (cell-preferred+16)%16 LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED;
 IF NOT FOUND THEN
  IF NOT core_funding.held_v1(account,true) THEN
   IF EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND (owned IS NULL OR cell=owned) AND balance_atoms-reserved_atoms>=amount AND revision<revision_limit) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
   RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';
  END IF;
  SELECT balance_atoms-reserved_atoms INTO free FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
  IF free IS NULL OR free<amount THEN RAISE EXCEPTION 'upstream_funds_unavailable' USING ERRCODE='P0402';END IF;
  INSERT INTO core_funding.balance_cells(account_id,currency,cell,spent_limit,revision_limit) SELECT account,cur,i,0,0 FROM generate_series(0,15) i ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND revision<revision_limit) THEN
   SELECT spent_atoms INTO actual_spent FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
   FOR seed IN SELECT cell,balance_atoms,reserved_atoms,spent_atoms,revision FROM core_funding.balance_cells WHERE account_id=account AND currency=cur ORDER BY (cell-preferred+16)%16 FOR NO KEY UPDATE NOWAIT LOOP
    IF seed.cell=preferred THEN PERFORM core_funding.collect_v1(account,cur,greatest(amount-(seed.balance_atoms-seed.reserved_atoms),0),preferred);END IF;
    SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
    budget_grant=least(root,greatest(CASE WHEN seed.cell=preferred THEN amount-(seed.balance_atoms-seed.reserved_atoms) ELSE 0 END,div(root,slots)));
    IF budget_grant>0 THEN PERFORM core_funding.transfer_v1(account,cur,seed.cell,budget_grant,0,0,'initialize:'||attempt||':'||seed.cell);END IF;
    PERFORM core_funding.headroom_v1(account,cur,seed.cell,seed.spent_atoms+CASE WHEN actual_spent<99999999999999999999999999999999999999 THEN 1 ELSE 0 END,seed.revision+1);
    slots=slots-1;
   END LOOP;
  END IF;
  SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=preferred FOR NO KEY UPDATE NOWAIT;
  IF c.balance_atoms-c.reserved_atoms<amount THEN
   PERFORM core_funding.collect_v1(account,cur,amount-(c.balance_atoms-c.reserved_atoms),preferred);
   SELECT balance_atoms INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur;
   budget_grant=least(root,greatest(amount-(c.balance_atoms-c.reserved_atoms),div(free,16)));
   IF budget_grant>0 THEN PERFORM core_funding.transfer_v1(account,cur,preferred,budget_grant,0,0,'reserve:'||attempt);END IF;
  END IF;
  PERFORM core_funding.headroom_v1(account,cur,preferred,c.spent_atoms,c.revision+1);
  SELECT * INTO STRICT c FROM core_funding.balance_cells WHERE account_id=account AND currency=cur AND cell=preferred;
 END IF;
 target=c.cell;
 IF c.balance_atoms-c.reserved_atoms<amount OR c.revision>=c.revision_limit THEN RAISE EXCEPTION 'funding_allocation_inconsistent' USING ERRCODE='P0500';END IF;
 UPDATE core_funding.balance_cells SET reserved_atoms=reserved_atoms+amount,revision=revision+1 WHERE account_id=account AND currency=cur AND cell=target;
 RETURN target;
END $$;

CREATE FUNCTION core_funding.run_execution_v1(items jsonb, time_bounds jsonb[], group_indices integer[], item_errors jsonb[], group_errors jsonb[], bulk boolean, proof_bounds numeric[]) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE item jsonb;computed jsonb;applied jsonb;result jsonb;results jsonb='[]';cache jsonb;pending jsonb;group_index integer;item_index integer=0;failure text;message text;key text;attempt_key text;request_key text;intent_key text;kind text;BEGIN
 BEGIN
  BEGIN
   cache=core_funding.customer_authorities_v1((SELECT coalesce(jsonb_agg(value),'[]') FROM jsonb_array_elements(items) WITH ORDINALITY i(value,n) WHERE item_errors[n] IS NULL AND group_errors[group_indices[n]] IS NULL));
  EXCEPTION WHEN OTHERS THEN cache='{"customers":{},"catalog":{}}'::jsonb;END;
  pending=CASE WHEN bulk THEN '{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb END;
  FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
   item_index=item_index+1;group_index=group_indices[item_index];
   key=NULL;attempt_key=NULL;request_key=NULL;intent_key=NULL;kind=item->'projection'->>'command_type';
   BEGIN
    key='command:'||(item->'projection'->>'service')||':'||(item->'projection'->>'instance_id')||':'||(item->'projection'->>'command_type')||':'||((item->'envelope'->>'command_id')::uuid)::text;
    IF kind='execution_begin_v1' THEN attempt_key=((item->'data'->>'attempt_id')::uuid)::text;
    ELSIF kind='execution_admit_v1' THEN
     attempt_key=((item->'data'->'dispatch_projection'->>'proposed_attempt_id')::uuid)::text;
     request_key='request:'||((item->'data'->'dispatch_projection'->>'proposed_request_id')::uuid)::text;
     IF item->'data'->>'intent_digest' IS NOT NULL THEN intent_key='intent:'||((item->'data'->>'project_id')::uuid)::text||':'||(item->'data'->>'intent_digest');END IF;
    END IF;
   EXCEPTION WHEN invalid_text_representation THEN key=NULL;attempt_key=NULL;request_key=NULL;intent_key=NULL;END;
   IF bulk AND pending->'grants'<>'{}'::jsonb AND (item->'projection'->>'command_type' IS DISTINCT FROM 'execution_begin_v1' OR attempt_key IS NULL OR key IS NULL OR pending->'grants'?attempt_key OR pending->'keys'?key) THEN
    PERFORM core_funding.flush_begin_v1(pending);pending='{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb;
   END IF;
   IF bulk AND pending->'admissions'<>'{}'::jsonb AND (kind IS DISTINCT FROM 'execution_admit_v1' OR attempt_key IS NULL OR request_key IS NULL OR key IS NULL OR pending->'admissions'?attempt_key OR pending->'keys'?request_key OR pending->'keys'?key OR coalesce(pending->'keys'?intent_key,false)) THEN
    PERFORM core_funding.flush_admit_v1(pending);pending='{"grants":{},"admissions":{},"funding":{"cells":{},"journals":[],"entries":[]},"commands":[],"audits":[],"keys":{}}'::jsonb;
   END IF;
   result=coalesce(item_errors[item_index],group_errors[group_index]);
   IF result IS NULL THEN
    BEGIN
     computed=item->'computed';
     IF kind='execution_begin_v1' OR kind='execution_admit_v1' AND item->'data'->'commit_send'='true'::jsonb THEN computed=computed||jsonb_build_object('proof_remaining_ms',coalesce(floor(proof_bounds[item_index]-extract(epoch FROM clock_timestamp())*1000),-1),'proof_deadline_ms',proof_bounds[item_index]);END IF;
     applied=core_funding.execution_item_v1(item->'projection',item->'envelope',item->'data',computed,time_bounds[group_index],cache,pending);
     result=applied->'result';cache=applied->'cache';
     IF bulk THEN pending=applied->'pending';END IF;
    EXCEPTION WHEN OTHERS THEN
     GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
     result=jsonb_build_object('error',CASE WHEN failure='P0F01' THEN 'funding_slow' WHEN failure IN ('55P03','P0503','40P01') THEN 'identity_busy' WHEN message IN ('command_conflict','customer_forbidden','internal_identity_revoked','internal_session_invalid','internal_peer_forbidden','internal_owner_mismatch','context_invalid','internal_deadline') THEN message ELSE 'identity_store_unavailable' END);
    END;
   END IF;
   results=results||jsonb_build_array(result);
  END LOOP;
  IF bulk THEN PERFORM core_funding.flush_begin_v1(pending);PERFORM core_funding.flush_admit_v1(pending);END IF;
 EXCEPTION WHEN OTHERS THEN
  IF NOT bulk THEN RAISE;END IF;
  RETURN core_funding.run_execution_v1(items,time_bounds,group_indices,item_errors,group_errors,false,proof_bounds);
 END;
 RETURN results;
END $$;

CREATE FUNCTION core_funding.share_v1(accounts uuid[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE account uuid;BEGIN
 IF accounts IS NULL OR cardinality(accounts)>128 OR array_position(accounts,NULL) IS NOT NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 FOR account IN SELECT DISTINCT id FROM unnest(accounts) id ORDER BY id LOOP
  IF NOT pg_try_advisory_xact_lock_shared(core_funding.account_key_v1(account)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
END $$;

CREATE FUNCTION core_funding.supplier_settle_v1(attempt uuid, snapshot uuid, reference text, pending jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE h public.upstream_holds%ROWTYPE;acct public.upstream_accounts%ROWTYPE;head public.upstream_costs%ROWTYPE;s public.usage_event_index%ROWTYPE;rates jsonb;usage jsonb;finals text[];x jsonb;
 old_cost numeric;old_reserve numeric;new_cost numeric;new_reserve numeric;cost_delta numeric;reserve_delta numeric;cost_rev numeric;journal uuid;frozen boolean;disputed boolean;faulted boolean;reserve_floor numeric=0;d record;dx jsonb;BEGIN
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt;
 IF h.attempt_id IS NULL OR h.estimated_quantities IS NULL OR h.state='RELEASED' OR reference !~ '^(cost|risk):[0-9a-f-]{36}:[0-9]{1,38}$' THEN RAISE EXCEPTION 'supplier_settlement_invalid' USING ERRCODE='P0409';END IF;
 PERFORM core_funding.share_v1(ARRAY[h.account_id]);
 SELECT * INTO acct FROM public.upstream_accounts WHERE id=h.account_id FOR SHARE NOWAIT;
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=attempt FOR UPDATE;
 SELECT c.* INTO head FROM public.upstream_cost_heads hd JOIN public.upstream_costs c ON c.attempt_id=hd.attempt_id AND c.revision=hd.revision WHERE hd.attempt_id=attempt;
 faulted=EXISTS(SELECT 1 FROM public.core_evidence_receipts r WHERE r.attempt_id=attempt AND r.numeric_fault IS NOT NULL);
 disputed=EXISTS(SELECT 1 FROM public.usage_event_index i WHERE i.attempt_id=attempt AND i.record_kind='DISPUTED');
 IF snapshot IS NULL OR disputed OR faulted THEN
  IF head.attempt_id IS NULL THEN usage='{}';finals='{}';ELSE SELECT p.usage,i.final_dimensions INTO usage,finals FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.id=head.evidence_id;END IF;
 ELSE
  SELECT * INTO s FROM public.usage_event_index WHERE id=snapshot AND attempt_id=attempt;
  IF s.id IS NULL OR s.record_kind<>'CANONICAL' THEN RAISE EXCEPTION 'supplier_settlement_invalid' USING ERRCODE='P0409';END IF;
  SELECT p.usage INTO usage FROM public.usage_payloads p WHERE p.id=s.id AND p.created_at=s.created_at;finals=s.final_dimensions;
 END IF;
 x=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,usage,finals,public.core_supplier_risk_v1(attempt));
 old_cost=coalesce(head.cost_atoms,0);
 FOR d IN SELECT i.final_dimensions,p.usage FROM public.usage_event_index i JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at WHERE i.attempt_id=attempt AND i.record_kind='DISPUTED' AND i.cost_atoms IS NOT NULL LOOP
  dx=core_funding.exposure_v1(h.cost_version_id,h.estimated_quantities,d.usage,d.final_dimensions,'{}'::jsonb);
  reserve_floor=greatest(reserve_floor,(dx->>'cost_atoms')::numeric+(dx->>'reserve_atoms')::numeric-old_cost);
 END LOOP;
 old_reserve=h.current_reserved_atoms;
 new_cost=(x->>'cost_atoms')::numeric;new_reserve=greatest((x->>'reserve_atoms')::numeric,reserve_floor);
 cost_delta=new_cost-old_cost;reserve_delta=new_reserve-old_reserve;
 IF cost_delta<0 THEN RAISE EXCEPTION 'supplier_cost_regression' USING ERRCODE='P0409';END IF;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF (frozen OR acct.state<>'ACTIVE') AND (NOT core_funding.held_v1(h.account_id,true) OR NOT core_funding.catalog_held_v1()) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF snapshot IS NOT NULL AND NOT disputed AND NOT faulted THEN
  SELECT coalesce(max(c.revision),0)+1 INTO cost_rev FROM public.upstream_costs c WHERE c.attempt_id=attempt;
  IF pending IS NULL THEN
  INSERT INTO public.upstream_costs(attempt_id,revision,currency,cost_atoms,complete,disputed,evidence_digest,evidence_id,cost_version_id) VALUES(attempt,cost_rev,h.currency,new_cost,(x->>'complete')::boolean,false,public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(finals),'cost_version_id',h.cost_version_id,'estimated_quantities_digest',h.estimated_quantities_digest,'cost_atoms',new_cost::text)),snapshot,h.cost_version_id);
  INSERT INTO public.upstream_cost_heads(attempt_id,revision) VALUES(attempt,cost_rev) ON CONFLICT(attempt_id) DO UPDATE SET revision=excluded.revision;
  ELSE pending=jsonb_set(pending,ARRAY['costs'],coalesce(pending->'costs','[]')||jsonb_build_array(jsonb_build_object('attempt_id',attempt,'revision',cost_rev,'currency',h.currency,'cost_atoms',new_cost,'complete',(x->>'complete')::boolean,'disputed',false,'evidence_digest',public.core_digest_v1(jsonb_build_object('snapshot',s.snapshot_digest,'final_dimensions',to_jsonb(finals),'cost_version_id',h.cost_version_id,'estimated_quantities_digest',h.estimated_quantities_digest,'cost_atoms',new_cost::text)),'evidence_id',snapshot,'cost_version_id',h.cost_version_id)));END IF;
 END IF;
 IF cost_delta<>0 OR reserve_delta<>0 THEN
  journal=gen_random_uuid();
  IF pending IS NULL THEN
   INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference) VALUES(journal,h.account_id,h.channel_id,attempt,h.currency,CASE WHEN cost_delta<>0 THEN 'COST' ELSE 'RISK' END,reference);
   INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms)
    SELECT journal,(row_number() OVER (ORDER BY ord))-1,h.currency,account,atoms FROM (VALUES(0,'upstream:available',-(cost_delta+reserve_delta)),(1,'upstream:reserved',reserve_delta),(2,'upstream:expense',cost_delta)) leg(ord,account,atoms) WHERE atoms<>0;
   SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
   SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
   PERFORM core_funding.apply_v1(attempt,cost_delta,new_reserve,true);
  ELSE
   pending=jsonb_set(pending,ARRAY['journals'],(pending->'journals')||jsonb_build_array(jsonb_build_object('id',journal,'account_id',h.account_id,'channel_id',h.channel_id,'attempt_id',attempt,'currency',h.currency,'kind',CASE WHEN cost_delta<>0 THEN 'COST' ELSE 'RISK' END,'reference',reference)));
   pending=jsonb_set(pending,ARRAY['entries'],(pending->'entries')||(SELECT jsonb_agg(jsonb_build_object('journal_id',journal,'ordinal',ordinal,'currency',h.currency,'account',account,'atoms',atoms) ORDER BY ordinal) FROM (SELECT (row_number() OVER (ORDER BY ord))-1 ordinal,account,atoms FROM (VALUES(0,'upstream:available',-(cost_delta+reserve_delta)),(1,'upstream:reserved',reserve_delta),(2,'upstream:expense',cost_delta)) leg(ord,account,atoms) WHERE atoms<>0) entries));
   pending=core_funding.project_v1(h,cost_delta,new_reserve,pending);
  END IF;
 END IF;
 IF pending IS NULL THEN
 UPDATE public.upstream_holds SET current_reserved_atoms=new_reserve,state=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN 'SETTLED' ELSE 'PARTIAL' END,final_cost_atoms=CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN new_cost END WHERE attempt_id=attempt;
 ELSE pending=jsonb_set(pending,ARRAY['holds'],coalesce(pending->'holds','{}')||jsonb_build_object(attempt::text,jsonb_build_object('base_reserved_atoms',h.current_reserved_atoms,'current_reserved_atoms',new_reserve,'state',CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN 'SETTLED' ELSE 'PARTIAL' END,'final_cost_atoms',CASE WHEN (x->>'complete')::boolean AND new_reserve=0 AND NOT disputed AND NOT faulted THEN new_cost END)));END IF;
 frozen=(x->>'overrun')::boolean OR jsonb_array_length(x->'risk_dimensions')>0 OR disputed OR faulted;
 IF frozen THEN
  UPDATE public.upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=h.account_id AND state='ACTIVE';
  INSERT INTO public.upstream_audit_events(id,account_id,channel_id,actor,action,new_digest,reason) VALUES(gen_random_uuid(),h.account_id,h.channel_id,'core-settlement','RECONCILE_REQUIRED',public.core_digest_v1(jsonb_build_object('attempt',attempt,'reference',reference,'exposure',x)),CASE WHEN (x->>'overrun')::boolean THEN 'supplier_cost_overrun' WHEN jsonb_array_length(x->'risk_dimensions')>0 THEN 'supplier_partial_risk' WHEN faulted THEN 'supplier_cost_overflow' ELSE 'supplier_evidence_disputed' END);
 END IF;
 RETURN x||jsonb_build_object('cost_delta',cost_delta::text,'reserve_delta',reserve_delta::text,'reserve_atoms',new_reserve::text,'revision',cost_rev::text,'frozen',frozen,'pending',pending);
END $_$;

CREATE FUNCTION core_funding.transfer_v1(account uuid, cur text, target_cell integer, balance_delta numeric, spent_delta numeric, revision_delta numeric, ref text) RETURNS uuid
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE root public.upstream_account_balances%ROWTYPE;c core_funding.balance_cells%ROWTYPE;prior core_funding.budget_transfers%ROWTYPE;transfer uuid;transfer_kind text;BEGIN
 IF account IS NULL OR cur IS NULL OR target_cell IS NULL OR target_cell NOT BETWEEN 0 AND 15 OR ref IS NULL OR length(ref) NOT BETWEEN 1 AND 200
  OR balance_delta IS NULL OR spent_delta IS NULL OR revision_delta IS NULL
  OR EXISTS(SELECT 1 FROM unnest(ARRAY[balance_delta,spent_delta,revision_delta]) d WHERE abs(d)>99999999999999999999999999999999999999 OR d<>trunc(d))
  OR (balance_delta,spent_delta,revision_delta)=(0,0,0) THEN RAISE EXCEPTION 'funding_transfer_invalid' USING ERRCODE='P0400';END IF;
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 transfer_kind=CASE WHEN balance_delta>0 THEN 'GRANT' WHEN balance_delta<0 THEN 'RECLAIM' ELSE 'HEADROOM' END;
 SELECT * INTO prior FROM core_funding.budget_transfers t WHERE t.account_id=account AND t.currency=cur AND t.reference=ref;
 IF FOUND THEN
  IF (prior.cell,prior.kind,prior.balance_delta,prior.spent_allocation_atoms,prior.spent_limit_delta,prior.revision_limit_delta) IS DISTINCT FROM (target_cell,transfer_kind,balance_delta,0::numeric,spent_delta,revision_delta) THEN RAISE EXCEPTION 'funding_transfer_conflict' USING ERRCODE='P0409';END IF;
  RETURN prior.id;
 END IF;
 PERFORM 1 FROM public.upstream_accounts WHERE id=account FOR SHARE NOWAIT;
 SELECT * INTO root FROM public.upstream_account_balances b WHERE b.account_id=account AND b.currency=cur FOR NO KEY UPDATE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION 'funding_balance_missing' USING ERRCODE='P0404';END IF;
 IF root.reserved_atoms<>0 THEN RAISE EXCEPTION 'funding_root_not_migrated' USING ERRCODE='P0422';END IF;
 INSERT INTO core_funding.balance_cells(account_id,currency,cell,spent_limit,revision_limit) VALUES(account,cur,target_cell,0,0) ON CONFLICT DO NOTHING;
 SELECT * INTO c FROM core_funding.balance_cells b WHERE b.account_id=account AND b.currency=cur AND b.cell=target_cell FOR NO KEY UPDATE NOWAIT;
 IF balance_delta>0 AND balance_delta>root.balance_atoms OR balance_delta<0 AND -balance_delta>greatest(c.balance_atoms-c.reserved_atoms,0) THEN RAISE EXCEPTION 'funding_budget_unavailable' USING ERRCODE='P0422';END IF;
 IF c.spent_limit+spent_delta<c.spent_atoms OR c.revision_limit+revision_delta<c.revision THEN RAISE EXCEPTION 'funding_headroom_unavailable' USING ERRCODE='P0422';END IF;
 IF EXISTS(SELECT 1 FROM unnest(ARRAY[root.balance_atoms-balance_delta,c.balance_atoms+balance_delta,c.spent_limit+spent_delta,c.revision_limit+revision_delta]) d WHERE abs(d)>99999999999999999999999999999999999999) THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms-balance_delta WHERE account_id=account AND currency=cur;
 UPDATE core_funding.balance_cells SET balance_atoms=balance_atoms+balance_delta,spent_limit=spent_limit+spent_delta,revision_limit=revision_limit+revision_delta WHERE account_id=account AND currency=cur AND cell=target_cell;
 transfer=gen_random_uuid();
 INSERT INTO core_funding.budget_transfers(id,account_id,currency,cell,kind,reference,balance_delta,spent_limit_delta,revision_limit_delta) VALUES(transfer,account,cur,target_cell,transfer_kind,ref,balance_delta,spent_delta,revision_delta);
 RETURN transfer;
END $$;

CREATE FUNCTION public.adjust_billing_charge(p_charge uuid, p_target numeric, p_ref text, p_reason text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge billing_charges%ROWTYPE; req gateway_requests%ROWTYPE; existing billing_adjustments%ROWTYPE;lot wallet_lots%ROWTYPE;
 fingerprint text;delta numeric;remaining numeric;funded numeric=0;take numeric;sequence integer;legs jsonb='[]';operation uuid;asof timestamptz;
BEGIN
 IF p_target<0 OR p_target<>trunc(p_target) OR p_target>99999999999999999999999999999999999999 OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_reason NOT IN ('ADMIN_CORRECTION','PROVIDER_RECONCILIATION') THEN RAISE EXCEPTION 'invalid adjustment' USING ERRCODE='P0422';END IF;
 SELECT w.* INTO wallet FROM wallets w JOIN billing_holds h ON h.wallet_id=w.id JOIN billing_charges c ON c.hold_id=h.id WHERE c.id=p_charge FOR NO KEY UPDATE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'charge not found' USING ERRCODE='P0404';END IF;
 SELECT * INTO charge FROM billing_charges WHERE id=p_charge FOR UPDATE;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_charge,p_target::text,p_reason)::text,'UTF8')),'hex');
 SELECT * INTO existing FROM billing_adjustments WHERE charge_id=p_charge AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'adjustment identity conflict' USING ERRCODE='P0409';END IF;RETURN jsonb_build_object('id',existing.id,'replayed',true);END IF;
 operation=gen_random_uuid();delta=p_target-(charge.amount_atoms-charge.refunded_atoms);
 IF delta<0 THEN PERFORM refund_billing_charge(p_charge,-delta,'adjust:'||operation,p_reason);
 ELSIF delta>0 THEN
  SELECT * INTO req FROM gateway_requests WHERE id=charge.request_id;
  PERFORM expire_wallet(wallet.id);asof=clock_timestamp();remaining=delta;sequence=coalesce((SELECT max(ordinal) FROM charge_allocations WHERE charge_id=p_charge),0);
  FOR lot IN SELECT * FROM wallet_available_lots(wallet.id,req.model,req.operation,asof) LOOP
   EXIT WHEN remaining=0;take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;sequence=sequence+1;
   UPDATE wallet_lots SET available_atoms=available_atoms-take,spent_atoms=spent_atoms+take WHERE id=lot.id;
   INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms) VALUES(p_charge,lot.id,sequence,take) ON CONFLICT(charge_id,lot_id) DO UPDATE SET amount_atoms=charge_allocations.amount_atoms+excluded.amount_atoms;
   legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
  END LOOP;
  UPDATE billing_charges SET amount_atoms=amount_atoms+delta,funded_atoms=funded_atoms+funded,debt_atoms=debt_atoms+remaining WHERE id=p_charge;
  UPDATE wallets SET available_atoms=available_atoms-funded,spent_atoms=spent_atoms+funded,debt_atoms=debt_atoms+remaining,
   freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms+remaining,credit_reserved_atoms,credit_limit_atoms) WHERE id=wallet.id;
  legs=legs||jsonb_build_array(money_leg('system:revenue',delta));IF remaining>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',-remaining));END IF;
  INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'adjust:'||operation,'ADJUSTMENT',wallet.currency,legs);
 END IF;
 INSERT INTO billing_adjustments(id,charge_id,external_ref,fingerprint,target_atoms,delta_atoms,reason) VALUES(operation,p_charge,p_ref,fingerprint,p_target,delta,p_reason);
 RETURN jsonb_build_object('id',operation,'delta_atoms',delta::text,'replayed',false);
END $$;

CREATE FUNCTION public.allocate_customer_batch_v1(p_inputs jsonb) RETURNS jsonb
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT public.allocate_customer_rows_v1(p_inputs,false)
$$;

CREATE FUNCTION public.allocate_customer_rows_v1(p_inputs jsonb, bind_execution boolean) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
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
 bind_execution=bind_execution AND has_column_privilege(current_user,'public.gateway_requests','execution_budget','INSERT')
  AND has_column_privilege(current_user,'public.attempts','binding_id','INSERT');
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
    IF bind_execution THEN
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome,execution_budget)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','ADMITTED',quote->>'currency',quote,'PENDING',p->'execution_budget') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    ELSE
    INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
    VALUES(new_id,credential.project_id,credential.id,p->>'operation',p->>'model',p->>'digest',p->>'fingerprint','RUNNING',quote->>'currency',quote,'PENDING') ON CONFLICT(project_id,key_id,operation,intent_digest) DO NOTHING;
    END IF;
    IF NOT FOUND THEN
     SELECT * INTO old FROM gateway_requests WHERE project_id=credential.project_id AND key_id=credential.id AND operation=p->>'operation' AND intent_digest=p->>'digest';
     outcomes[item.ordinality]=CASE WHEN old.fingerprint<>p->>'fingerprint' THEN jsonb_build_object('error','P0409') ELSE jsonb_build_object('created',false,'record',jsonb_build_object('id',old.id,'state',old.state,'fingerprint',old.fingerprint)) END;CONTINUE;
    END IF;
   ELSE
    pending_request.id=new_id;pending_request.project_id=credential.project_id;pending_request.key_id=credential.id;
    pending_request.operation=p->>'operation';pending_request.model=p->>'model';pending_request.intent_digest=p->>'digest';pending_request.fingerprint=p->>'fingerprint';pending_request.billing_quote=quote;
    IF bind_execution THEN pending_request.execution_budget=p->'execution_budget';END IF;
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
   IF bind_execution THEN pending_attempt.binding_id=(p->>'binding_id')::uuid;END IF;
   staged_attempts=array_append(staged_attempts,pending_attempt);
   result=jsonb_build_object('created',true,'record',jsonb_build_object('id',new_id,'state',CASE WHEN bind_execution THEN 'ADMITTED' ELSE 'RUNNING' END,'fingerprint',p->>'fingerprint'),'attempt_id',attempt_id);
  outcomes[item.ordinality]=result;
 END LOOP;
 IF bind_execution THEN
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome,execution_budget)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'ADMITTED',q.billing_quote->>'currency',q.billing_quote,'PENDING',q.execution_budget
 FROM unnest(staged_requests) q;
 ELSE
 INSERT INTO gateway_requests(id,project_id,key_id,operation,model,intent_digest,fingerprint,state,billing_currency,billing_quote,billing_outcome)
 SELECT q.id,q.project_id,q.key_id,q.operation,q.model,q.intent_digest,q.fingerprint,'RUNNING',q.billing_quote->>'currency',q.billing_quote,'PENDING'
 FROM unnest(staged_requests) q;
 END IF;
 INSERT INTO billing_holds(id,request_id,wallet_id,amount_atoms,credit_atoms,next_reconcile_at) SELECT h.id,h.id,h.wallet_id,h.amount_atoms,h.credit_atoms,now()+interval '30 seconds' FROM unnest(staged_holds) h;
 INSERT INTO hold_allocations(hold_id,lot_id,ordinal,amount_atoms) SELECT a.hold_id,a.lot_id,a.ordinal,a.amount_atoms
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(hold_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric);
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,credit_reserved_atoms=state.credit_reserved_atoms
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(to_jsonb(staged_journals));
 IF bind_execution THEN
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state,binding_id)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING',a.binding_id FROM unnest(staged_attempts) a;
 ELSE
 INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,billing_pricing_id,state)
 SELECT a.id,a.request_id,1,a.provider_id,a.upstream_model,a.pricing_id,a.billing_pricing_id,'DISPATCHING' FROM unnest(staged_attempts) a;
 END IF;
 SELECT jsonb_object_agg((n-1)::text,outcomes[n]) INTO results FROM generate_subscripts(outcomes,1) input(n);
 RETURN results;
 EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
 END;
 IF jsonb_array_length(p_inputs)=1 THEN RETURN jsonb_build_object('0',jsonb_build_object('error',failure));END IF;
 midpoint=jsonb_array_length(p_inputs)/2;
 SELECT jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality<=midpoint),jsonb_agg(value ORDER BY ordinality) FILTER(WHERE ordinality>midpoint)
 INTO left_inputs,right_inputs FROM jsonb_array_elements(p_inputs) WITH ORDINALITY;
 left_results=public.allocate_customer_rows_v1(left_inputs,bind_execution);right_results=public.allocate_customer_rows_v1(right_inputs,bind_execution);
 SELECT left_results||jsonb_object_agg((key::integer+midpoint)::text,value) INTO results FROM jsonb_each(right_results);
 RETURN results;
END $$;

CREATE FUNCTION public.audit_configuration() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE identity text;
BEGIN
 identity=coalesce(to_jsonb(NEW)->>'id',to_jsonb(NEW)->>'name','unknown');
 INSERT INTO audit_events(id,action,resource_id) VALUES(gen_random_uuid(),CASE WHEN TG_TABLE_NAME='channels' THEN 'providers' ELSE TG_TABLE_NAME END||'.'||lower(TG_OP),encode(sha256(convert_to(identity,'UTF8')),'hex'));
 RETURN NEW;
END $$;

CREATE FUNCTION public.authorize_gateway_dispatch() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE allowed boolean;
BEGIN
 SELECT k.enabled AND p.enabled AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp()) AND k.role IN ('ADMIN','INVOKER')
 AND (o.id IS NULL OR o.enabled) AND (u.id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN','MEMBER')) INTO allowed
 FROM api_keys k JOIN projects p ON p.id=k.project_id LEFT JOIN organizations o ON o.id=p.organization_id LEFT JOIN principals u ON u.id=k.principal_id WHERE k.id=NEW.key_id AND k.project_id=NEW.project_id;
 IF NOT coalesce(allowed,false) THEN RAISE EXCEPTION 'dispatch permission unavailable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;

CREATE FUNCTION public.collect_wallet_debt(p_wallet uuid, p_limit integer DEFAULT 32) RETURNS integer
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge record; lot wallet_lots%ROWTYPE; take numeric; remaining numeric; legs jsonb; funded numeric; count integer=0; sequence integer;
BEGIN
 SELECT * INTO wallet FROM wallets WHERE id=p_wallet FOR NO KEY UPDATE;
 IF wallet.debt_atoms=0 THEN RETURN 0; END IF;
 FOR charge IN SELECT c.*,q.model,q.operation FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id JOIN gateway_requests q ON q.id=c.request_id WHERE h.wallet_id=p_wallet AND c.debt_atoms>0 AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id)) ORDER BY c.created_at,c.id LIMIT least(p_limit,128) LOOP
  remaining=charge.debt_atoms;funded=0;legs='[]';sequence=coalesce((SELECT max(ordinal) FROM charge_allocations WHERE charge_id=charge.id),0);
  FOR lot IN SELECT * FROM wallet_lots WHERE wallet_id=p_wallet AND kind='CASH' AND available_atoms>0 ORDER BY priority,created_at,id LOOP
   EXIT WHEN remaining=0;take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;sequence=sequence+1;
   UPDATE wallet_lots SET available_atoms=available_atoms-take,spent_atoms=spent_atoms+take WHERE id=lot.id;
   INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms) VALUES(charge.id,lot.id,sequence,take) ON CONFLICT(charge_id,lot_id) DO UPDATE SET amount_atoms=charge_allocations.amount_atoms+excluded.amount_atoms;
   legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
  END LOOP;
  IF funded>0 THEN
   UPDATE billing_charges SET funded_atoms=funded_atoms+funded,debt_atoms=debt_atoms-funded WHERE id=charge.id;
   UPDATE wallets SET available_atoms=available_atoms-funded,spent_atoms=spent_atoms+funded,debt_atoms=debt_atoms-funded WHERE id=p_wallet;
   legs=legs||jsonb_build_array(money_leg('system:receivable',funded));
   INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(gen_random_uuid(),p_wallet,'collect:'||gen_random_uuid(),'DEBT_COLLECTION',wallet.currency,legs);
   count=count+1;
  END IF;
  EXIT WHEN remaining>0;
 END LOOP;
 UPDATE wallets SET freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) WHERE id=p_wallet;
 RETURN count;
END $$;

CREATE FUNCTION public.configure_wallet_credit(p_project uuid, p_currency text, p_ref text, p_version numeric, p_limit numeric) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE wallet wallets%ROWTYPE;previous wallet_credit_policies%ROWTYPE;digest text;command uuid;next_version numeric;
BEGIN
 IF p_ref IS NULL OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_version IS NULL OR p_limit IS NULL OR p_version::text !~ '^(0|[1-9][0-9]{0,37})$' OR p_limit::text !~ '^(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'invalid credit policy' USING ERRCODE='P0422';END IF;
 SELECT * INTO wallet FROM wallets WHERE project_id=p_project AND currency=p_currency FOR NO KEY UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'wallet not found' USING ERRCODE='P0404';END IF;
 digest=encode(sha256(convert_to(jsonb_build_array(p_version::text,p_limit::text)::text,'UTF8')),'hex');
 SELECT * INTO previous FROM wallet_credit_policies WHERE wallet_id=wallet.id AND external_ref=p_ref;
 IF FOUND THEN
  IF previous.fingerprint<>digest THEN RAISE EXCEPTION 'credit policy identity conflict' USING ERRCODE='P0409';END IF;
  RETURN jsonb_build_object('id',previous.id,'credit_policy_version',previous.version::text,'credit_limit_atoms',previous.credit_limit_atoms::text,'replayed',true);
 END IF;
 IF p_version<>wallet.credit_policy_version OR p_limit<wallet.debt_atoms+wallet.credit_reserved_atoms THEN RAISE EXCEPTION 'credit policy exposure or version conflict' USING ERRCODE='P0409';END IF;
 IF p_version>=99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'credit policy version exhausted' USING ERRCODE='P0409';END IF;
 command=gen_random_uuid();next_version=p_version+1;
 UPDATE wallets SET credit_limit_atoms=p_limit,credit_policy_version=next_version,
  freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,p_limit) WHERE id=wallet.id;
 INSERT INTO wallet_credit_policies(id,wallet_id,external_ref,fingerprint,credit_limit_atoms,version) VALUES(command,wallet.id,p_ref,digest,p_limit,next_version);
 RETURN jsonb_build_object('id',command,'credit_policy_version',next_version::text,'credit_limit_atoms',p_limit::text,'replayed',false);
END $_$;

CREATE FUNCTION public.core_abort_recovery_slow_v1(p jsonb, e jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE account uuid;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_abort_recovery_v1' OR p->>'service' NOT IN('core-api','core-worker') THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 SELECT account_id INTO account FROM public.upstream_holds WHERE attempt_id=(d->>'attempt_id')::uuid;
 IF account IS NULL THEN RETURN public.core_abort_recovery_v1(p,e,d);END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 PERFORM public.core_catalog_write_v1();
 RETURN public.core_abort_recovery_v1(p,e,d);
END $$;

CREATE FUNCTION public.core_abort_recovery_v1(p jsonb, e jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE permit public.execution_permits%ROWTYPE;cmd public.execution_commands%ROWTYPE;q public.gateway_requests%ROWTYPE;h public.upstream_holds%ROWTYPE;ctx public.execution_contexts%ROWTYPE;
 k public.api_keys%ROWTYPE;pr public.projects%ROWTYPE;o public.organizations%ROWTYPE;u public.principals%ROWTYPE;original_key public.api_keys%ROWTYPE;original_project public.projects%ROWTYPE;
 owner_instance public.internal_service_instances%ROWTYPE;owner_certificate public.internal_service_certificates%ROWTYPE;owner_session public.internal_startup_sessions%ROWTYPE;channel public.channels%ROWTYPE;
 caller text=p->>'service'||':'||(p->>'instance_id');response jsonb;settled jsonb;rejection text;eligible boolean;BEGIN
 IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.execution.v1:command:'||caller||':execution_abort_recovery_v1:'||(e->>'command_id'),0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 IF jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['attempt_id']<>'{}'::jsonb OR jsonb_typeof(d->'attempt_id') IS DISTINCT FROM 'string' OR e->>'command_id' IS DISTINCT FROM p->>'command_id' OR e->>'deadline_at' IS DISTINCT FROM p->>'deadline_at' OR e->>'caller_incarnation' IS DISTINCT FROM p->>'incarnation_id' OR public.core_digest_v1(jsonb_build_object('kind','execution_abort_recovery_v1','contract_version',1,'owner',jsonb_build_object('service',p->>'service','instance_id',p->>'instance_id','session_id',p->>'session_id','incarnation_id',p->>'incarnation_id'),'deadline_at',p->>'deadline_at','data',d)) IS DISTINCT FROM e->>'body_digest' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid;
 IF permit.context_id IS NULL THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 PERFORM service FROM public.internal_service_instances WHERE (service,instance_id) IN ((p->>'service',p->>'instance_id'),(permit.owner_service,permit.gateway_instance_id)) ORDER BY service COLLATE "C",instance_id COLLATE "C" FOR SHARE NOWAIT;
 PERFORM certificate_sha256 FROM public.internal_service_certificates WHERE certificate_sha256=ANY(ARRAY[p->>'certificate_sha256',permit.certificate_sha256]) ORDER BY certificate_sha256 COLLATE "C" FOR SHARE NOWAIT;
 PERFORM session_id FROM public.internal_startup_sessions WHERE session_id=ANY(ARRAY[(p->>'session_id')::uuid,permit.owner_session_id]) ORDER BY session_id FOR SHARE NOWAIT;
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_abort_recovery_v1' OR p->>'service' NOT IN ('core-api','core-worker') THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=caller AND kind='execution_abort_recovery_v1' AND command_id=(e->>'command_id')::uuid;
 IF FOUND THEN
  IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;RETURN cmd.receipt;
 END IF;
 SELECT * INTO ctx FROM public.execution_contexts WHERE id=permit.context_id;
 SELECT * INTO original_key FROM public.api_keys WHERE id=ctx.key_id;
 SELECT * INTO original_project FROM public.projects WHERE id=ctx.project_id;
 IF original_project.organization_id IS NOT NULL THEN SELECT * INTO o FROM public.organizations WHERE id=original_project.organization_id FOR SHARE NOWAIT;END IF;
 IF original_key.principal_id IS NOT NULL THEN SELECT * INTO u FROM public.principals WHERE id=original_key.principal_id FOR SHARE NOWAIT;END IF;
 SELECT * INTO pr FROM public.projects WHERE id=ctx.project_id FOR SHARE NOWAIT;
 SELECT * INTO k FROM public.api_keys WHERE id=ctx.key_id FOR SHARE NOWAIT;
 IF (k.project_id,k.principal_id,pr.organization_id) IS DISTINCT FROM (original_key.project_id,original_key.principal_id,original_project.organization_id) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 SELECT * INTO h FROM public.upstream_holds WHERE attempt_id=permit.attempt_id;
 PERFORM core_funding.share_v1(ARRAY[h.account_id]);
 PERFORM id FROM public.supplier_brands WHERE id=(SELECT brand_id FROM public.channels WHERE id=h.channel_id) FOR SHARE NOWAIT;
 SELECT * INTO channel FROM public.channels WHERE id=h.channel_id FOR SHARE NOWAIT;
 PERFORM id FROM public.plugin_versions WHERE id=(SELECT plugin_version_id FROM public.channel_plugin_bindings WHERE id=permit.binding_id) FOR SHARE NOWAIT;
 PERFORM id FROM public.upstream_accounts WHERE id=h.account_id FOR SHARE NOWAIT;
 SELECT * INTO owner_instance FROM public.internal_service_instances WHERE service=permit.owner_service AND instance_id=permit.gateway_instance_id;
 SELECT * INTO owner_certificate FROM public.internal_service_certificates WHERE certificate_sha256=permit.certificate_sha256;
 SELECT * INTO owner_session FROM public.internal_startup_sessions WHERE session_id=permit.owner_session_id;
 BEGIN
  PERFORM id FROM public.wallets WHERE id=(SELECT wallet_id FROM public.billing_holds WHERE request_id=(SELECT request_id FROM public.attempts WHERE id=permit.attempt_id)) FOR NO KEY UPDATE NOWAIT;
  SELECT * INTO q FROM public.gateway_requests WHERE id=(SELECT request_id FROM public.attempts WHERE id=permit.attempt_id) FOR UPDATE NOWAIT;
  PERFORM id FROM public.billing_holds WHERE request_id=q.id FOR UPDATE NOWAIT;
  PERFORM id FROM public.attempts WHERE request_id=q.id ORDER BY ordinal,id FOR NO KEY UPDATE NOWAIT;
  PERFORM ep.attempt_id FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=q.id ORDER BY ep.attempt_id FOR NO KEY UPDATE OF ep NOWAIT;
  SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=(d->>'attempt_id')::uuid;
  PERFORM attempt_id FROM public.upstream_holds WHERE attempt_id=permit.attempt_id FOR NO KEY UPDATE NOWAIT;
  IF permit.state<>'RESERVED' OR permit.granted_at IS NOT NULL THEN RAISE EXCEPTION 'recovery_not_reserved' USING ERRCODE='P0409';END IF;
  eligible=permit.expires_at<=clock_timestamp() OR owner_instance.security_revoked_at IS NOT NULL OR NOT owner_instance.enabled OR owner_certificate.security_revoked_at IS NOT NULL OR owner_certificate.retired_at IS NOT NULL OR owner_certificate.not_after<=clock_timestamp() OR owner_session.disabled_at IS NOT NULL OR owner_session.security_revoked_at IS NOT NULL OR owner_session.expires_at<=clock_timestamp() OR NOT k.enabled OR k.expires_at<=clock_timestamp() OR NOT pr.enabled OR (o.id IS NOT NULL AND NOT o.enabled) OR (u.id IS NOT NULL AND (NOT u.enabled OR u.role NOT IN ('OWNER','ADMIN','MEMBER'))) OR k.role NOT IN ('ADMIN','INVOKER') OR (ctx.operation=ANY(k.operations)) IS NOT TRUE OR (cardinality(k.models)>0 AND NOT ctx.model=ANY(k.models)) OR NOT channel.enabled;
  IF eligible IS NOT TRUE THEN RAISE EXCEPTION 'recovery_not_eligible' USING ERRCODE='P0409';END IF;
  UPDATE public.execution_permits SET state='ABORTED' WHERE attempt_id=permit.attempt_id AND state='RESERVED' AND granted_at IS NULL;
  IF NOT FOUND THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
  UPDATE public.attempts SET state='NOT_EXECUTED',finished_at=clock_timestamp() WHERE id=permit.attempt_id;
  UPDATE public.upstream_holds SET state='RELEASED',final_cost_atoms=0 WHERE attempt_id=permit.attempt_id;
  PERFORM core_funding.apply_v1(permit.attempt_id,0,0,true);
  PERFORM public.core_upstream_journal_v1(h.account_id,h.currency,h.channel_id,permit.attempt_id,'RELEASE',h.current_reserved_atoms);
  UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE attempt_id=permit.attempt_id AND scope_kind='channel' AND state IN ('ADOPTION_PENDING','ACTIVE');
  IF NOT EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts at ON at.id=ep.attempt_id WHERE at.request_id=q.id AND (ep.state<>'ABORTED' OR ep.granted_at IS NOT NULL)) THEN
   UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()),state='FAILED',billing_outcome='FAILED' WHERE id=q.id;
   settled=public.settle_requests(ARRAY[q.id]);
   IF settled->q.id::text->>'state' IS DISTINCT FROM 'RELEASED' THEN RAISE EXCEPTION 'settlement_failed';END IF;
   UPDATE public.execution_quota_leases SET state='RELEASE_PENDING' WHERE request_id=q.id AND state IN ('ADOPTION_PENDING','ACTIVE');
  END IF;
  response=jsonb_build_object('command_state','COMMITTED','request_id',q.id,'attempt_id',permit.attempt_id,'state','ABORTED','permit_epoch',permit.permit_epoch::text);
  INSERT INTO public.audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),'execution_abort_recovery_v1',permit.attempt_id::text,caller,jsonb_build_object('command_id',e->>'command_id','request_id',q.id,'attempt_id',permit.attempt_id,'state','ABORTED'));
 EXCEPTION WHEN SQLSTATE 'P0409' THEN
  GET STACKED DIAGNOSTICS rejection=MESSAGE_TEXT;
  IF rejection NOT IN ('recovery_not_reserved','recovery_not_eligible') THEN RAISE;END IF;
  response=jsonb_build_object('command_state','REJECTED','code',rejection);
 END;
 INSERT INTO public.execution_commands(id,command_id,kind,caller_identity,owner_session_id,caller_incarnation,body_digest,deadline_at,trace_id,contract_version,state,request_id,attempt_id,context_id,rejection_code,receipt) VALUES(gen_random_uuid(),(e->>'command_id')::uuid,'execution_abort_recovery_v1',caller,(p->>'session_id')::uuid,(p->>'incarnation_id')::uuid,e->>'body_digest',(e->>'deadline_at')::timestamptz,e->>'trace_id',1,response->>'command_state',q.id,permit.attempt_id,ctx.id,rejection,response);
 RETURN response;
END $$;

CREATE FUNCTION public.core_account_auth_v1(proof text, ip text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k public.api_keys%ROWTYPE;pr public.projects%ROWTYPE;o public.organizations%ROWTYPE;u public.principals%ROWTYPE;BEGIN
 IF proof IS NULL OR proof='' THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 SELECT * INTO k FROM public.api_keys WHERE verifier=proof;
 IF k.id IS NULL THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 SELECT * INTO pr FROM public.projects WHERE id=k.project_id;
 IF pr.organization_id IS NOT NULL THEN SELECT * INTO o FROM public.organizations WHERE id=pr.organization_id;END IF;
 IF k.principal_id IS NOT NULL THEN SELECT * INTO u FROM public.principals WHERE id=k.principal_id;END IF;
 IF (k.enabled AND pr.enabled AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp()) AND (pr.organization_id IS NULL OR o.enabled) AND (k.principal_id IS NULL OR u.enabled AND u.organization_id=pr.organization_id AND u.role=ANY(CASE k.role WHEN 'ADMIN' THEN ARRAY['OWNER','ADMIN'] WHEN 'INVOKER' THEN ARRAY['OWNER','ADMIN','MEMBER'] WHEN 'BILLING' THEN ARRAY['OWNER','ADMIN','BILLING'] ELSE ARRAY['OWNER','ADMIN','MEMBER','BILLING','VIEWER'] END))) IS NOT TRUE THEN RETURN jsonb_build_object('status','unauthenticated');END IF;
 IF cardinality(k.allowed_cidrs)>0 AND (ip IS NULL OR NOT EXISTS(SELECT 1 FROM unnest(k.allowed_cidrs) c WHERE ip::inet <<= c::inet)) THEN RETURN jsonb_build_object('status','ip_denied');END IF;
 RETURN jsonb_build_object('status','authenticated','project_id',pr.id,'key_id',k.id,'role',k.role,'models',to_jsonb(k.models),'operations',to_jsonb(k.operations));
END $$;

CREATE FUNCTION public.core_auth_v1(kid uuid, ip text, op text, model text) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k public.api_keys%ROWTYPE;pr public.projects%ROWTYPE;o public.organizations%ROWTYPE;u public.principals%ROWTYPE;before_key public.api_keys%ROWTYPE;before_project public.projects%ROWTYPE;BEGIN
 SELECT * INTO before_key FROM public.api_keys WHERE id=kid;
 SELECT * INTO before_project FROM public.projects WHERE id=before_key.project_id;
 IF before_project.organization_id IS NOT NULL THEN SELECT * INTO o FROM public.organizations WHERE id=before_project.organization_id FOR SHARE NOWAIT;END IF;
 IF before_key.principal_id IS NOT NULL THEN SELECT * INTO u FROM public.principals WHERE id=before_key.principal_id FOR SHARE NOWAIT;END IF;
 SELECT * INTO pr FROM public.projects WHERE id=before_key.project_id FOR SHARE NOWAIT;
 SELECT * INTO k FROM public.api_keys WHERE id=kid FOR SHARE NOWAIT;
 IF (k.project_id,k.principal_id,pr.organization_id) IS DISTINCT FROM (before_key.project_id,before_key.principal_id,before_project.organization_id) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 RETURN core_funding.auth_decision_v1(k,pr,o,u,ip,op,model,clock_timestamp());
END $$;

CREATE TABLE public.resources (
    binding_id uuid,
    id text NOT NULL,
    project_id uuid NOT NULL,
    provider_id uuid NOT NULL,
    remote_id text NOT NULL,
    kind text NOT NULL,
    model text,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    attempt_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    next_poll_at timestamp with time zone DEFAULT now() NOT NULL,
    poll_token uuid,
    poll_until timestamp with time zone,
    batch_endpoint text,
    batch_upstream_model text,
    batch_manifest jsonb,
    poll_count integer DEFAULT 0 NOT NULL,
    upstream_state text,
    lease_session_id uuid,
    lease_epoch numeric(38,0) DEFAULT 0 NOT NULL,
    batch_ceilings jsonb,
    CONSTRAINT resource_batch_ceilings CHECK (((batch_ceilings IS NULL) OR (jsonb_typeof(batch_ceilings) = 'object'::text))),
    CONSTRAINT resource_batch_metadata CHECK (((batch_manifest IS NULL) OR (((jsonb_typeof(batch_manifest) = 'object'::text) AND ((batch_manifest - ARRAY['items'::text, 'idsSha256'::text, 'images'::text]) = '{}'::jsonb) AND (batch_manifest ?& ARRAY['items'::text, 'idsSha256'::text]) AND (jsonb_typeof((batch_manifest -> 'items'::text)) = 'number'::text) AND ((batch_manifest ->> 'items'::text) ~ '^[1-9][0-9]{0,15}$'::text) AND (((batch_manifest ->> 'items'::text))::numeric <= ('9007199254740991'::bigint)::numeric) AND (jsonb_typeof((batch_manifest -> 'idsSha256'::text)) = 'string'::text) AND ((batch_manifest ->> 'idsSha256'::text) ~ '^[a-f0-9]{64}$'::text) AND ((NOT (batch_manifest ? 'images'::text)) OR ((jsonb_typeof((batch_manifest -> 'images'::text)) = 'number'::text) AND ((batch_manifest ->> 'images'::text) ~ '^[1-9][0-9]{0,15}$'::text) AND (((batch_manifest ->> 'images'::text))::numeric <= ('9007199254740991'::bigint)::numeric)))) IS TRUE))),
    CONSTRAINT resource_opaque_identity CHECK ((((length(remote_id) >= 1) AND (length(remote_id) <= 500)) AND (remote_id ~ '^[A-Za-z0-9_:/.-]+$'::text) AND (remote_id !~ '://'::text) AND (remote_id !~ '(^|/)[.]{1,2}(/|$)'::text))),
    CONSTRAINT resources_kind_check CHECK ((kind = ANY (ARRAY['file'::text, 'batch'::text, 'video'::text, 'response'::text, 'operation'::text]))),
    CONSTRAINT resources_upstream_state_check CHECK ((upstream_state = ANY (ARRAY['queued'::text, 'pending'::text, 'in_progress'::text, 'running'::text, 'completed'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'canceled'::text, 'cancelling'::text, 'validating'::text, 'finalizing'::text, 'expired'::text, 'processed'::text])))
);

CREATE FUNCTION public.core_batch_files_v1(r public.resources, remotes jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION public.core_caller_projection_v2(p jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE path text;BEGIN
 path=CASE p->>'command_type' WHEN 'execution_context_v1' THEN 'context' WHEN 'execution_admit_v1' THEN 'admissions' WHEN 'execution_next_v1' THEN 'attempts' WHEN 'execution_begin_v1' THEN 'begin' WHEN 'execution_abort_v1' THEN 'abort' WHEN 'execution_query_v1' THEN 'commands' WHEN 'execution_quota_v1' THEN 'quota' WHEN 'execution_quota_recovery_v1' THEN 'quota-recovery' WHEN 'execution_abort_recovery_v1' THEN 'abort-recovery' WHEN 'execution_resource_commit_v1' THEN 'resources/commit' WHEN 'execution_resource_v1' THEN 'resource-permits' WHEN 'execution_resource_list_v1' THEN 'resources/list' WHEN 'execution_resource_claim_v1' THEN 'resources/claim' WHEN 'execution_resource_poll_v1' THEN 'resources/poll-permits' WHEN 'execution_resource_release_v1' THEN 'resources/release' END;
 IF path IS NULL OR NOT (p->>'service'='gateway' AND p->>'command_type' NOT IN ('execution_quota_recovery_v1','execution_abort_recovery_v1') OR p->>'service' IN ('core-api','core-worker') AND p->>'command_type' IN ('execution_quota_recovery_v1','execution_abort_recovery_v1','execution_query_v1')) OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/'||path THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_projection_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'),'current');
 IF (p->>'deadline_at')::timestamptz<=clock_timestamp() OR (p->>'deadline_at')::timestamptz>clock_timestamp()+interval '1 hour' THEN RAISE EXCEPTION 'internal_deadline' USING ERRCODE='P0408';END IF;
END $$;

CREATE FUNCTION public.core_caller_v1(p jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE path text;BEGIN
 path=CASE p->>'command_type' WHEN 'execution_context_v1' THEN 'context' WHEN 'execution_admit_v1' THEN 'admissions' WHEN 'execution_next_v1' THEN 'attempts' WHEN 'execution_begin_v1' THEN 'begin' WHEN 'execution_abort_v1' THEN 'abort' WHEN 'execution_query_v1' THEN 'commands' WHEN 'execution_quota_v1' THEN 'quota' WHEN 'execution_quota_recovery_v1' THEN 'quota-recovery' WHEN 'execution_abort_recovery_v1' THEN 'abort-recovery' WHEN 'execution_resource_commit_v1' THEN 'resources/commit' WHEN 'execution_resource_v1' THEN 'resource-permits' WHEN 'execution_resource_list_v1' THEN 'resources/list' WHEN 'execution_resource_claim_v1' THEN 'resources/claim' WHEN 'execution_resource_poll_v1' THEN 'resources/poll-permits' WHEN 'execution_resource_release_v1' THEN 'resources/release' END;
 IF path IS NULL OR NOT (p->>'service'='gateway' AND p->>'command_type' NOT IN ('execution_quota_recovery_v1','execution_abort_recovery_v1') OR p->>'service' IN ('core-api','core-worker') AND p->>'command_type' IN ('execution_quota_recovery_v1','execution_abort_recovery_v1','execution_query_v1')) OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/'||path THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_current_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'));
END $$;

CREATE FUNCTION public.core_candidates_v1(model text, op text, cur text) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE current_revision numeric;scope bytea;result jsonb;payload_size integer;used_bytes integer;used_entries integer;BEGIN
 IF NOT pg_try_advisory_xact_lock_shared(hashtextextended('stg.catalog.control.v1',0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 SELECT e.revision INTO STRICT current_revision FROM core_catalog_cache.epoch e WHERE singleton FOR SHARE NOWAIT;
 scope=sha256(convert_to(jsonb_build_array(model,op,cur)::text,'UTF8'));
 SELECT c.candidates INTO result FROM core_catalog_cache.candidates c WHERE c.catalog_revision=current_revision AND c.scope_digest=scope AND c.model=core_candidates_v1.model AND c.operation=op AND c.currency=cur;
 IF FOUND THEN RETURN result;END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.catalog.cache.v2:'||current_revision::text||':'||encode(scope,'hex'),0)) THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 SELECT c.candidates INTO result FROM core_catalog_cache.candidates c WHERE c.catalog_revision=current_revision AND c.scope_digest=scope AND c.model=core_candidates_v1.model AND c.operation=op AND c.currency=cur;
 IF FOUND THEN RETURN result;END IF;
 LOCK TABLE public.models,public.routes,public.billing_tariffs,public.supplier_brands,public.channels,public.plugin_versions,public.upstream_accounts,public.channel_plugin_bindings,public.plugin_publication_receipts,public.plugin_packages,public.cost_versions,public.billing_prices IN ACCESS SHARE MODE NOWAIT;
 result=core_catalog_cache.compute_v2(model,op,cur);
 IF NOT EXISTS(SELECT 1 FROM public.models m WHERE m.name=core_candidates_v1.model AND op=ANY(m.operations)) OR NOT EXISTS(SELECT 1 FROM public.billing_tariffs t WHERE t.model=core_candidates_v1.model AND t.operation=op AND t.currency=cur) THEN RETURN result;END IF;
 payload_size=pg_column_size(result);
 IF payload_size>67108864 THEN RETURN result;END IF;
 BEGIN
  SELECT u.byte_count,u.entry_count INTO STRICT used_bytes,used_entries FROM core_catalog_cache.usage u WHERE singleton FOR NO KEY UPDATE NOWAIT;
 EXCEPTION WHEN lock_not_available THEN RETURN result;
 END;
 IF used_bytes<=67108864-payload_size AND used_entries<4096 THEN
  INSERT INTO core_catalog_cache.candidates(catalog_revision,scope_digest,model,operation,currency,candidates,payload_bytes) VALUES(current_revision,scope,model,op,cur,result,payload_size);
  UPDATE core_catalog_cache.usage SET byte_count=byte_count+payload_size,entry_count=entry_count+1 WHERE singleton;
 END IF;
 RETURN result;
END $$;

CREATE FUNCTION public.core_canonical_v1(v jsonb) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE encoded text;ascii_keys boolean;
BEGIN
 CASE jsonb_typeof(v)
 WHEN 'object' THEN
  SELECT NOT EXISTS(SELECT 1 FROM jsonb_object_keys(v) k WHERE k COLLATE "C" ~ $ascii$[^\x01-\x7f]$ascii$) INTO ascii_keys;
  SELECT '{'||coalesce(string_agg(to_jsonb(key)::text||':'||CASE WHEN jsonb_typeof(value) IN ('object','array')
   THEN public.core_canonical_v1(value) ELSE value::text END,','
   ORDER BY CASE WHEN ascii_keys THEN key END COLLATE "C",CASE WHEN NOT ascii_keys THEN public.core_utf16_sort_v1(key) END),'')||'}'
  INTO encoded FROM jsonb_each(v);
 WHEN 'array' THEN
  SELECT '['||coalesce(string_agg(CASE WHEN jsonb_typeof(value) IN ('object','array') THEN public.core_canonical_v1(value)
   ELSE value::text END,',' ORDER BY n),'')||']' INTO encoded FROM jsonb_array_elements(v) WITH ORDINALITY a(value,n);
 ELSE encoded=v::text;
 END CASE;
 RETURN encoded;
END $_$;

CREATE FUNCTION public.core_catalog_write_v1() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended('stg.catalog.control.v1',0));
END $$;

CREATE FUNCTION public.core_context_v1(p jsonb, id uuid, input jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION public.core_digest_v1(v jsonb) RETURNS text
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$ SELECT encode(sha256(convert_to(public.core_canonical_v1(v),'UTF8')),'hex') $$;

CREATE FUNCTION public.core_evidence_snapshot_v1(attempt uuid, kind text, source text, finality text, usage jsonb, finals text[], parent uuid, input uuid, cost numeric, complete boolean) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE sid uuid=gen_random_uuid();rev numeric;a record;BEGIN
 SELECT q.project_id,q.id request_id,v.currency INTO a FROM public.attempts t JOIN public.gateway_requests q ON q.id=t.request_id JOIN public.upstream_holds h ON h.attempt_id=t.id JOIN public.cost_versions v ON v.id=h.cost_version_id WHERE t.id=attempt;
 SELECT coalesce(max(snapshot_revision),0)+1 INTO rev FROM public.usage_event_index WHERE attempt_id=attempt AND snapshot_revision IS NOT NULL;
 INSERT INTO public.usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,cost_complete,record_kind,snapshot_revision,snapshot_digest,final_dimensions,parent_evidence_id,input_fact_id)
 VALUES(sid,a.project_id,a.request_id,attempt,source,finality,cost,a.currency,coalesce(complete,false),kind,rev,public.core_digest_v1(jsonb_build_object('attempt_id',attempt,'record_kind',kind,'snapshot_revision',rev::text,'source',source,'finality',finality,'usage',usage,'final_dimensions',to_jsonb(finals),'parent',parent,'input',input,'cost_atoms',cost::text)),finals,parent,input);
 INSERT INTO public.usage_payloads(id,created_at,usage) SELECT i.id,i.created_at,usage FROM public.usage_event_index i WHERE i.id=sid;
 IF kind<>'RAW' THEN INSERT INTO public.event_deliveries(consumer_id,event_id) SELECT c.id,sid FROM public.event_consumers c;END IF;
 RETURN jsonb_build_object('id',sid,'revision',rev::text);
END $$;

CREATE FUNCTION public.core_evidence_validate_v1(ev jsonb) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE u constant text='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';d text;f text;BEGIN
 IF jsonb_typeof(ev) IS DISTINCT FROM 'object' OR ev-ARRAY['event_id','attempt_id','origin_identity','origin_incarnation','origin_session_id','permit_epoch','source','source_revision','finality','usage','final_dimensions','attempt_state','request_state','error_code','provider_request_id']<>'{}'::jsonb
  OR ev->>'event_id' !~ u OR ev->>'attempt_id' !~ u OR ev->>'origin_incarnation' !~ u OR ev->>'origin_session_id' !~ u
  OR jsonb_typeof(ev->'origin_identity') IS DISTINCT FROM 'string' OR ev->>'origin_identity' !~ '^gateway:[A-Za-z0-9_-]{1,100}$'
  OR jsonb_typeof(ev->'permit_epoch') IS DISTINCT FROM 'string' OR ev->>'permit_epoch' !~ '^[1-9][0-9]{0,37}$'
  OR jsonb_typeof(ev->'source_revision') IS DISTINCT FROM 'string' OR ev->>'source_revision' !~ '^[1-9][0-9]{0,37}$'
  OR jsonb_typeof(ev->'source') IS DISTINCT FROM 'string' OR ev->>'source' NOT IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice')
  OR jsonb_typeof(ev->'finality') IS DISTINCT FROM 'string' OR ev->>'finality' NOT IN ('FINAL','PARTIAL','UNKNOWN','ESTIMATED','NOT_EXECUTED')
  OR jsonb_typeof(ev->'usage') IS DISTINCT FROM 'object' OR jsonb_typeof(ev->'final_dimensions') IS DISTINCT FROM 'array' OR jsonb_array_length(ev->'final_dimensions')>15
  OR (ev?'attempt_state' AND (jsonb_typeof(ev->'attempt_state') IS DISTINCT FROM 'string' OR ev->>'attempt_state' NOT IN ('SUCCEEDED','FAILED','UNKNOWN','PARTIAL','NOT_EXECUTED')))
  OR (ev?'request_state' AND (jsonb_typeof(ev->'request_state') IS DISTINCT FROM 'string' OR ev->>'request_state' NOT IN ('SUCCEEDED','FAILED','CANCELLED','UNKNOWN')))
  OR (ev?'error_code' AND (jsonb_typeof(ev->'error_code') IS DISTINCT FROM 'string' OR ev->>'error_code' !~ '^[a-z][a-z0-9_]{0,79}$'))
  OR (ev?'provider_request_id' AND (jsonb_typeof(ev->'provider_request_id') IS DISTINCT FROM 'string' OR ev->>'provider_request_id' !~ '^[A-Za-z0-9._:/-]{1,256}$'))
 THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 FOR d IN SELECT key FROM jsonb_each(ev->'usage') LOOP
  IF jsonb_typeof(ev->'usage'->d)<>'string' OR d NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters')
   OR (d IN ('audio_seconds','video_seconds') AND ev->'usage'->>d !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR (d NOT IN ('audio_seconds','video_seconds') AND ev->'usage'->>d !~ '^(0|[1-9][0-9]{0,37})$') THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 END LOOP;
 IF ev->'usage'?&ARRAY['cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens'] AND (ev->'usage'->>'cache_write_tokens')::numeric<>(ev->'usage'->>'cache_write_5m_tokens')::numeric+(ev->'usage'->>'cache_write_1h_tokens')::numeric THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 FOR f IN SELECT value FROM jsonb_array_elements_text(ev->'final_dimensions') LOOP
  IF NOT ev->'usage'?f THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
 END LOOP;
 IF (SELECT count(DISTINCT value)<>count(*) FROM jsonb_array_elements_text(ev->'final_dimensions'))
  OR (ev->>'finality' IN ('UNKNOWN','ESTIMATED') AND jsonb_array_length(ev->'final_dimensions')>0)
  OR (ev->>'finality'='NOT_EXECUTED' AND (ev->'usage'<>'{}'::jsonb OR jsonb_array_length(ev->'final_dimensions')>0 OR coalesce(ev->>'attempt_state','NOT_EXECUTED')<>'NOT_EXECUTED'))
 THEN RAISE EXCEPTION 'evidence_event_invalid' USING ERRCODE='P0400';END IF;
END $_$;

CREATE FUNCTION public.core_execution_batch_v1(items jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE item jsonb;p jsonb;key jsonb;keys jsonb[]='{}';representatives jsonb[]='{}';time_bounds jsonb[]='{}';group_errors jsonb[]='{}';item_errors jsonb[]='{}';group_indices integer[]='{}';group_index integer;item_index integer=0;proof_bounds numeric[]='{}';proof_anchor numeric=extract(epoch FROM clock_timestamp())*1000;proof numeric;failure text;message text;times jsonb;
BEGIN
 IF jsonb_typeof(items) IS DISTINCT FROM 'array' OR jsonb_array_length(items) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) LOOP
  item_index=item_index+1;
  group_indices[item_index]=NULL;
  item_errors[item_index]=NULL;
  BEGIN
   p=item->'projection';
   PERFORM public.core_caller_projection_v2(p);
   IF p->>'command_type'='execution_begin_v1' OR p->>'command_type'='execution_admit_v1' AND item->'data'->'commit_send'='true'::jsonb THEN
    BEGIN
     proof=(item->'computed'->>'proof_remaining_ms')::numeric;
     IF proof::text NOT IN('Infinity','-Infinity','NaN') THEN proof_bounds[item_index]=proof_anchor+proof;END IF;
    EXCEPTION WHEN OTHERS THEN proof_bounds[item_index]=NULL;END;
   END IF;
   key=jsonb_build_array(p->>'service',p->>'instance_id',p->>'certificate_sha256',p->>'instance_revision',p->>'certificate_revision',p->>'session_id',p->>'incarnation_id',p->>'key_sha256',p->>'session_revision');
   group_index=array_position(keys,key);
   IF group_index IS NULL THEN
    keys=array_append(keys,key);
    group_index=cardinality(keys);
    representatives[group_index]=p;
   ELSIF (p->>'deadline_at')::timestamptz>(representatives[group_index]->>'deadline_at')::timestamptz THEN representatives[group_index]=p;
   END IF;
   group_indices[item_index]=group_index;
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
   item_errors[item_index]=jsonb_build_object('error',CASE WHEN failure='P0F01' THEN 'funding_slow' WHEN failure IN ('55P03','P0503','40P01') THEN 'identity_busy' WHEN message IN ('command_conflict','customer_forbidden','internal_identity_revoked','internal_session_invalid','internal_peer_forbidden','internal_owner_mismatch','context_invalid','internal_deadline') THEN message ELSE 'identity_store_unavailable' END);
  END;
 END LOOP;
 FOR group_index IN 1..cardinality(keys) LOOP
  group_errors[group_index]=NULL;
  time_bounds[group_index]=NULL;
  BEGIN
   p=representatives[group_index];
   PERFORM public.core_caller_v1(p);
   SELECT jsonb_build_object('session_expires_at',s.expires_at,'certificate_not_before',c.not_before,'certificate_expires_at',c.not_after) INTO STRICT times
   FROM public.internal_startup_sessions s JOIN public.internal_service_certificates c ON c.certificate_sha256=s.certificate_sha256
   WHERE s.session_id=(p->>'session_id')::uuid AND c.certificate_sha256=p->>'certificate_sha256';
   IF EXISTS(SELECT 1 FROM jsonb_each(times) f WHERE jsonb_typeof(f.value) IS DISTINCT FROM 'string') THEN RAISE EXCEPTION 'internal_session_invalid' USING ERRCODE='P0401';END IF;
   time_bounds[group_index]=times;
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE,message=MESSAGE_TEXT;
   group_errors[group_index]=jsonb_build_object('error',CASE WHEN failure='P0F01' THEN 'funding_slow' WHEN failure IN ('55P03','P0503','40P01') THEN 'identity_busy' WHEN message IN ('command_conflict','customer_forbidden','internal_identity_revoked','internal_session_invalid','internal_peer_forbidden','internal_owner_mismatch','context_invalid','internal_deadline') THEN message ELSE 'identity_store_unavailable' END);
   time_bounds[group_index]=NULL;
  END;
 END LOOP;
 RETURN core_funding.run_execution_v1(items,time_bounds,group_indices,item_errors,group_errors,jsonb_array_length(items)>1,proof_bounds);
END $$;

CREATE FUNCTION public.core_execution_slow_v1(item jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE p jsonb=item->'projection';account uuid;current_account uuid;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IN('execution_admit_v1','execution_next_v1') THEN SELECT account_id INTO account FROM public.channels WHERE id=(item->'data'->'dispatch_projection'->'candidate_ref'->>'channel_id')::uuid;
 ELSIF p->>'command_type' IN('execution_begin_v1','execution_abort_v1') THEN SELECT account_id INTO account FROM public.upstream_holds WHERE attempt_id=(item->'data'->>'attempt_id')::uuid;
 ELSE RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF account IS NULL THEN RETURN public.core_execution_batch_v1(jsonb_build_array(item))->0;END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 PERFORM public.core_catalog_write_v1();
 IF p->>'command_type' IN('execution_admit_v1','execution_next_v1') THEN
  SELECT account_id INTO current_account FROM public.channels WHERE id=(item->'data'->'dispatch_projection'->'candidate_ref'->>'channel_id')::uuid;
  IF current_account IS DISTINCT FROM account THEN RAISE EXCEPTION 'core_busy' USING ERRCODE='P0503';END IF;
 END IF;
 RETURN public.core_execution_batch_v1(jsonb_build_array(item))->0;
END $$;

CREATE FUNCTION public.core_funding_control_v1(account uuid, catalog boolean DEFAULT true) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF account IS NULL OR catalog IS NULL THEN RAISE EXCEPTION 'funding_accounts_invalid' USING ERRCODE='P0400';END IF;
 PERFORM core_funding.exclusive_v1(ARRAY[account]);
 IF catalog THEN PERFORM public.core_catalog_write_v1();END IF;
END $$;

CREATE FUNCTION public.core_hold_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_OP='UPDATE' AND (to_jsonb(NEW)-ARRAY['state','final_cost_atoms','current_reserved_atoms']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','final_cost_atoms','current_reserved_atoms']) THEN RAISE EXCEPTION 'hold_snapshot_immutable' USING ERRCODE='P0403';END IF;
 IF (TG_OP='INSERT' OR NEW.current_reserved_atoms IS DISTINCT FROM OLD.current_reserved_atoms) AND NOT core_funding.held_v1(NEW.account_id) THEN RAISE EXCEPTION 'funding_fence_required' USING ERRCODE='P0403';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.core_lookup_v1(p jsonb, e jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE cmd public.execution_commands%ROWTYPE;ctx public.execution_contexts%ROWTYPE;BEGIN
 PERFORM public.core_caller_v1(p);
 SELECT * INTO cmd FROM public.execution_commands WHERE caller_identity=p->>'service'||':'||(p->>'instance_id') AND kind=p->>'command_type' AND command_id=(e->>'command_id')::uuid;
 IF NOT FOUND THEN RETURN NULL;END IF;
 SELECT * INTO ctx FROM public.execution_contexts WHERE id=cmd.context_id;
 PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);
 IF cmd.body_digest IS DISTINCT FROM e->>'body_digest' OR cmd.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR cmd.caller_incarnation IS DISTINCT FROM (p->>'incarnation_id')::uuid THEN RAISE EXCEPTION 'command_conflict' USING ERRCODE='P0409';END IF;
 RETURN core_funding.execution_command_receipt_v1(cmd);
END $$;

CREATE FUNCTION public.core_permit_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' OR (to_jsonb(NEW)-ARRAY['state','granted_at']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','granted_at']) OR NOT (NEW.state=OLD.state OR OLD.state='RESERVED' AND NEW.state IN ('SEND_GRANTED','ABORTED') OR OLD.state='SEND_GRANTED' AND NEW.state IN ('UNKNOWN','TERMINAL') OR OLD.state='UNKNOWN' AND NEW.state='TERMINAL') THEN RAISE EXCEPTION 'permit_immutable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;

CREATE FUNCTION public.core_platform_authorize_v1(proof jsonb, permission text) RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE c public.administrator_credentials%ROWTYPE;a public.technical_administrators%ROWTYPE;BEGIN
 IF permission IS NULL OR permission NOT IN ('catalog:manage','funding:read','funding:recharge','customer:fund','customer:read','customer:adjust','funding:adjust','catalog:read','events:read','events:consume','operations:read','operations:manage') OR jsonb_typeof(proof) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 IF proof=jsonb_build_object('identity','bootstrap:ADMIN_KEY','bootstrap',true) THEN RETURN 'bootstrap:ADMIN_KEY';END IF;
 IF proof-ARRAY['identity','credentialId','administratorId','verifier']<>'{}'::jsonb OR EXISTS(SELECT 1 FROM unnest(ARRAY['identity','credentialId','administratorId','verifier'])f WHERE jsonb_typeof(proof->f) IS DISTINCT FROM 'string' OR proof->>f='') THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.administrator_credentials WHERE id=(proof->>'credentialId')::uuid FOR SHARE;
 SELECT * INTO a FROM public.technical_administrators WHERE id=(proof->>'administratorId')::uuid FOR SHARE;
 IF c.id IS NULL OR a.id IS NULL OR c.administrator_id IS DISTINCT FROM a.id OR proof->>'identity' IS DISTINCT FROM 'technical-admin:'||a.id::text OR c.verifier IS DISTINCT FROM proof->>'verifier' OR (c.enabled AND a.enabled AND permission=ANY(a.permissions) AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())) IS NOT TRUE THEN RAISE EXCEPTION 'platform_scope_denied' USING ERRCODE='P0403';END IF;
 RETURN proof->>'identity';
END $$;

CREATE FUNCTION public.core_plugin_package_v1(p jsonb, package_digest text) RETURNS bytea
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE result bytea;BEGIN
 IF p->>'service' IS DISTINCT FROM 'gateway' OR p->>'command_type' IS DISTINCT FROM 'plugin_package_v1' OR p->>'method' IS DISTINCT FROM 'POST' OR p->>'normalized_path' IS DISTINCT FROM '/_internal/core/v1/plugin-package' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.internal_identity_current_v1(p||jsonb_build_object('command_type','identity_current_v1','normalized_path','/_internal/core/v1/identity/current-session'));
 IF package_digest IS NULL OR package_digest !~ '^[a-f0-9]{64}$' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT k.package_bytes INTO result FROM public.plugin_packages k WHERE k.digest=package_digest AND EXISTS(SELECT 1 FROM public.plugin_versions v WHERE v.package_digest=k.digest AND v.state='PUBLISHED' AND NOT v.quarantined);
 IF result IS NULL THEN RAISE EXCEPTION 'plugin_package_unavailable' USING ERRCODE='P0404';END IF;
 RETURN result;
END $_$;

CREATE FUNCTION public.core_preflight_v1(p jsonb, context_id uuid, attempt_id uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE
 ctx public.execution_contexts%ROWTYPE;
 permit public.execution_permits%ROWTYPE;
 leases jsonb;
 requested_request uuid;
 execution_closed boolean;
 first_chat boolean;
 attempt_ordinal integer;
BEGIN
 PERFORM public.core_caller_v1(p);
 IF attempt_id IS NULL THEN
  EXECUTE 'SELECT * FROM public.execution_contexts WHERE id=$1' INTO ctx USING context_id;
  IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
  IF p->>'command_type'<>'execution_quota_v1' THEN PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
  RETURN ctx.snapshot||jsonb_build_object('leases','[]'::jsonb,'permit_state',NULL,'permit_epoch',NULL,'execution_closed',NULL);
 END IF;

 EXECUTE 'SELECT attempt_id,context_id,owner_session_id,incarnation_id,owner_key_sha256,gateway_instance_id,owner_service,certificate_sha256,permit_epoch,state,
  CASE WHEN $2=''execution_begin_v1'' THEN manifest->''stream''=''false''::jsonb AND manifest->''resource_ids''=''[]''::jsonb ELSE false END
  FROM public.execution_permits WHERE attempt_id=$1'
  INTO permit.attempt_id,permit.context_id,permit.owner_session_id,permit.incarnation_id,permit.owner_key_sha256,permit.gateway_instance_id,permit.owner_service,permit.certificate_sha256,permit.permit_epoch,permit.state,first_chat
  USING attempt_id,p->>'command_type';
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid
  OR permit.incarnation_id IS DISTINCT FROM (p->>'incarnation_id')::uuid
  OR permit.owner_key_sha256 IS DISTINCT FROM p->>'key_sha256'
  OR permit.gateway_instance_id IS DISTINCT FROM p->>'instance_id'
  OR permit.owner_service IS DISTINCT FROM p->>'service'
  OR permit.certificate_sha256 IS DISTINCT FROM p->>'certificate_sha256' THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 EXECUTE 'SELECT owner_session_id,key_id,ip,operation,model FROM public.execution_contexts WHERE id=$1'
  INTO ctx.owner_session_id,ctx.key_id,ctx.ip,ctx.operation,ctx.model USING permit.context_id;
 IF ctx.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'context_invalid' USING ERRCODE='P0403';END IF;
 IF p->>'command_type'<>'execution_quota_v1' THEN PERFORM public.core_auth_v1(ctx.key_id,ctx.ip,ctx.operation,ctx.model);END IF;
 EXECUTE 'SELECT request_id,ordinal FROM public.attempts WHERE id=$1' INTO requested_request,attempt_ordinal USING attempt_id;
 EXECUTE 'SELECT coalesce(jsonb_agg(jsonb_build_object(''state'',l.state,''attempt_id'',l.attempt_id,''permit_epoch'',ep.permit_epoch::text,''descriptor'',l.descriptor) ORDER BY l.lease_id),''[]''::jsonb)
  FROM public.execution_quota_leases l JOIN LATERAL(SELECT permit_epoch FROM public.execution_permits WHERE attempt_id=l.attempt_id OFFSET 0) ep ON true WHERE l.request_id=$1' INTO leases USING requested_request;
 EXECUTE 'SELECT execution_closed_at IS NOT NULL FROM public.gateway_requests WHERE id=$1' INTO execution_closed USING requested_request;
 RETURN jsonb_build_object('leases',leases,'permit_state',permit.state,'permit_epoch',permit.permit_epoch::text,'execution_closed',execution_closed,
  'first_chat',coalesce(first_chat AND ctx.operation='chat' AND attempt_ordinal=1,false));
END $_$;

CREATE FUNCTION public.core_quota_close_v1(p jsonb, e jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
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

CREATE FUNCTION public.core_quota_expire_v1(batch integer, after_key jsonb DEFAULT NULL::jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE request uuid;q public.gateway_requests%ROWTYPE;expired integer=0;changed integer;now_ms bigint=floor(extract(epoch FROM clock_timestamp())*1000)::bigint;due record;requests uuid[]='{}';scanned integer=0;next_key jsonb;BEGIN
 IF batch IS NULL OR batch NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 IF after_key IS NOT NULL AND after_key<>'null'::jsonb THEN
  IF jsonb_typeof(after_key) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
  IF jsonb_array_length(after_key)<>3 OR jsonb_typeof(after_key->0) IS DISTINCT FROM 'number' OR jsonb_typeof(after_key->1) IS DISTINCT FROM 'string' OR jsonb_typeof(after_key->2) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'quota_recovery_invalid' USING ERRCODE='P0400';END IF;
 END IF;
 FOR due IN
  SELECT l.request_id,l.lease_id,l.live_deadline_ms deadline FROM public.execution_quota_leases l
  WHERE l.live_deadline_ms<=now_ms
   AND (l.live_deadline_ms,l.request_id,l.lease_id)>(coalesce((after_key->>0)::bigint,-1),coalesce((after_key->>1)::uuid,'00000000-0000-0000-0000-000000000000'::uuid),coalesce(after_key->>2,''))
  ORDER BY l.live_deadline_ms,l.request_id,l.lease_id LIMIT batch
 LOOP
  requests=array_append(requests,due.request_id);scanned=scanned+1;next_key=jsonb_build_array(due.deadline,due.request_id,due.lease_id);
 END LOOP;
 FOR request IN SELECT DISTINCT id FROM unnest(requests) id ORDER BY id LOOP
  BEGIN
   SELECT * INTO q FROM public.gateway_requests WHERE id=request FOR UPDATE NOWAIT;
   IF q.id IS NULL OR ((q.execution_budget->>'deadline_at')::timestamptz<=clock_timestamp()) IS NOT TRUE THEN CONTINUE;END IF;
   PERFORM ep.attempt_id FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request ORDER BY ep.attempt_id FOR NO KEY UPDATE OF ep NOWAIT;
   PERFORM l.lease_id FROM public.execution_quota_leases l WHERE l.request_id=request ORDER BY l.lease_id FOR NO KEY UPDATE NOWAIT;
   IF NOT EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request)
    OR EXISTS(SELECT 1 FROM public.execution_permits ep JOIN public.attempts a ON a.id=ep.attempt_id WHERE a.request_id=request AND (ep.state='RESERVED' OR ep.expires_at>clock_timestamp()))
    OR EXISTS(SELECT 1 FROM public.execution_quota_leases l WHERE l.request_id=request AND l.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') AND (l.live_deadline_ms<=now_ms) IS NOT TRUE) THEN CONTINUE;END IF;
   UPDATE public.execution_quota_leases SET state='EXPIRED' WHERE request_id=request AND state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING');
   GET DIAGNOSTICS changed=ROW_COUNT;
   IF changed>0 THEN
    UPDATE public.gateway_requests SET execution_closed_at=coalesce(execution_closed_at,clock_timestamp()) WHERE id=request;
    expired=expired+1;
   END IF;
  EXCEPTION WHEN lock_not_available THEN CONTINUE;
  END;
 END LOOP;
 RETURN jsonb_build_object('expired',expired,'cursor',CASE WHEN scanned=batch THEN next_key ELSE NULL END,'more',scanned=batch AND expired>0);
END $$;

CREATE FUNCTION public.core_quota_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' OR TG_OP='UPDATE' AND (
  (NEW.lease_id,NEW.attempt_id,NEW.command_id,NEW.request_id,NEW.scope_kind,NEW.scope_id)
   IS DISTINCT FROM (OLD.lease_id,OLD.attempt_id,OLD.command_id,OLD.request_id,OLD.scope_kind,OLD.scope_id)
  OR NEW.revision<OLD.revision OR OLD.state IN ('RELEASE_PENDING','RELEASED','EXPIRED') AND NEW.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED')
 ) THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
 NEW.live_deadline_ms=CASE WHEN NEW.state IN ('ADOPTION_PENDING','ACTIVE','RELEASE_PENDING') THEN (NEW.descriptor->>'deadline_ms')::bigint END;
 NEW.release_pending=NEW.state='RELEASE_PENDING';
 RETURN NEW;
END $$;

CREATE FUNCTION public.core_quota_recovery_v1(p jsonb, items jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE item jsonb;l public.execution_quota_leases%ROWTYPE;result jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_quota_recovery_v1' THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF jsonb_typeof(items)='number' AND items::text::integer BETWEEN 1 AND 100 THEN
  SELECT coalesce(jsonb_agg(jsonb_build_object('attempt_id',attempt_id,'descriptor',descriptor)),'[]'::jsonb) INTO result FROM (SELECT attempt_id,descriptor FROM public.execution_quota_leases WHERE release_pending ORDER BY lease_id LIMIT items::text::integer) pending;
  RETURN result;
 END IF;
 IF jsonb_typeof(items)='object' AND items-ARRAY['limit','after']='{}'::jsonb AND jsonb_typeof(items->'limit')='number' AND items->>'limit' ~ '^[0-9]{1,3}$' AND (items->>'limit')::integer BETWEEN 1 AND 100 THEN
  RETURN public.core_quota_expire_v1((items->>'limit')::integer,items->'after');
 END IF;
 IF jsonb_typeof(items) IS DISTINCT FROM 'array' OR jsonb_array_length(items)>100 THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR l.state NOT IN ('RELEASE_PENDING','RELEASED','EXPIRED') OR item->>'state' IS DISTINCT FROM 'RELEASED' OR (l.descriptor-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) IS DISTINCT FROM (item-ARRAY['state','remaining_ms','revision','attempt_id','permit_epoch','tokens']) OR (item->>'revision')::numeric<l.revision OR (item->>'attempt_id' IS NOT NULL AND item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text) OR (item->>'permit_epoch' IS NOT NULL AND item->>'permit_epoch' IS DISTINCT FROM '1') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0403';END IF;
  IF l.state<>'EXPIRED' THEN UPDATE public.execution_quota_leases SET state='RELEASED',descriptor=item WHERE lease_id=l.lease_id;END IF;
 END LOOP;RETURN jsonb_build_object('released',jsonb_array_length(items));
END $_$;

CREATE FUNCTION public.core_quota_v1(p jsonb, attempt uuid, action text, items jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE permit public.execution_permits%ROWTYPE;item jsonb;l public.execution_quota_leases%ROWTYPE;
 activate_together boolean;ids text[];adopted jsonb='{}';BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' NOT IN ('execution_admit_v1','execution_next_v1','execution_begin_v1','execution_abort_v1','execution_quota_v1') OR action IS NULL OR action NOT IN ('ACTIVE','RELEASED') OR jsonb_typeof(items) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
 SELECT * INTO permit FROM public.execution_permits WHERE attempt_id=attempt FOR NO KEY UPDATE NOWAIT;
 IF permit.owner_session_id IS DISTINCT FROM (p->>'session_id')::uuid THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 activate_together=action='ACTIVE' AND permit.state='RESERVED' AND jsonb_array_length(items) BETWEEN 2 AND 3;
 IF activate_together THEN
  SELECT array_agg(DISTINCT entry->>'lease_id') INTO ids FROM jsonb_array_elements(items) entry;
  activate_together=cardinality(ids)=jsonb_array_length(items);
 END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(items) ORDER BY value->>'lease_id' LOOP
  IF jsonb_typeof(item->'revision') IS DISTINCT FROM 'string' OR item->>'revision' !~ '^(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
  IF action='ACTIVE' AND (jsonb_typeof(item->'remaining_ms') IS DISTINCT FROM 'number' OR item->>'remaining_ms' !~ '^[0-9]+$') THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  SELECT * INTO l FROM public.execution_quota_leases WHERE lease_id=item->>'lease_id' FOR NO KEY UPDATE NOWAIT;
  IF NOT FOUND OR NOT (l.attempt_id=attempt OR l.scope_kind<>'channel' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt) OR action='RELEASED' AND l.request_id=(SELECT request_id FROM public.attempts WHERE id=attempt)) OR (l.descriptor-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) IS DISTINCT FROM (item-ARRAY['state','attempt_id','permit_epoch','remaining_ms','revision','tokens']) OR (item->>'revision')::numeric<l.revision THEN RAISE EXCEPTION 'quota_invalid' USING ERRCODE='P0403';END IF;
  IF action='ACTIVE' THEN
   IF (SELECT execution_closed_at FROM public.gateway_requests WHERE id=l.request_id) IS NOT NULL OR l.state NOT IN ('ADOPTION_PENDING','ACTIVE') OR permit.state NOT IN ('RESERVED','SEND_GRANTED','UNKNOWN') OR item->>'state' IS DISTINCT FROM 'ADOPTED' OR item->>'attempt_id' IS DISTINCT FROM l.attempt_id::text OR item->>'permit_epoch' IS DISTINCT FROM permit.permit_epoch::text OR (item->>'remaining_ms')::numeric<=1000 THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  ELSE
   IF l.state NOT IN ('RELEASE_PENDING','RELEASED') OR item->>'state' IS DISTINCT FROM 'RELEASED' THEN RAISE EXCEPTION 'quota_barrier' USING ERRCODE='P0409';END IF;
  END IF;
  IF activate_together THEN adopted=jsonb_set(adopted,ARRAY[l.lease_id],item);
  ELSE UPDATE public.execution_quota_leases SET state=action,revision=(item->>'revision')::numeric,descriptor=item WHERE lease_id=l.lease_id;
  END IF;
 END LOOP;
 IF activate_together THEN
  UPDATE public.execution_quota_leases lease SET state=action,revision=(adopted->lease.lease_id->>'revision')::numeric,descriptor=adopted->lease.lease_id
  WHERE lease.lease_id=ANY(ids);
 END IF;
 RETURN jsonb_build_object('updated',jsonb_array_length(items));
END $_$;

CREATE FUNCTION public.core_reconcile_holds_v1(batch integer) RETURNS integer
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE token uuid=gen_random_uuid();requests uuid[];result jsonb;BEGIN
 IF batch IS NULL OR batch NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION 'reconcile_batch_invalid' USING ERRCODE='P0422';END IF;
 WITH candidates AS (
  SELECT h.id,h.next_reconcile_at,1 AS priority FROM public.billing_holds h
  WHERE h.state IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
  AND (EXISTS(SELECT 1 FROM public.attempts a WHERE a.request_id=h.request_id AND a.evidence_id IS NOT NULL) OR EXISTS(SELECT 1 FROM public.billing_resolutions r WHERE r.request_id=h.request_id))
  UNION ALL
  SELECT h.id,h.next_reconcile_at,0 AS priority FROM public.billing_charges c JOIN public.billing_holds h ON h.id=c.hold_id
  WHERE c.debt_atoms>0 AND h.state NOT IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
 ),due AS (
  SELECT h.id FROM candidates c JOIN public.billing_holds h ON h.id=c.id WHERE h.reconcile_until IS NULL OR h.reconcile_until<now()
  ORDER BY c.priority,c.next_reconcile_at,h.id LIMIT batch FOR UPDATE OF h SKIP LOCKED
 ),leased AS (
  UPDATE public.billing_holds h SET reconcile_token=token,reconcile_until=now()+interval '30 seconds' FROM due WHERE h.id=due.id RETURNING h.request_id
 ) SELECT array_agg(request_id) INTO requests FROM leased;
 IF requests IS NULL THEN RETURN 0;END IF;
 result=public.settle_requests(requests,token);
 RETURN (SELECT count(*)::integer FROM jsonb_each(result) e WHERE e.value->>'state' IS DISTINCT FROM 'STALE_LEASE' AND e.value->>'state' IS DISTINCT FROM 'ERROR');
END $$;

CREATE FUNCTION public.core_request_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF OLD.execution_budget IS NOT NULL AND NEW.execution_budget IS DISTINCT FROM OLD.execution_budget OR OLD.execution_closed_at IS NOT NULL AND NEW.execution_closed_at IS DISTINCT FROM OLD.execution_closed_at THEN RAISE EXCEPTION 'execution_budget_immutable' USING ERRCODE='P0403';END IF;RETURN NEW;
END $$;

CREATE FUNCTION public.core_resource_claim_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION public.core_resource_commit_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION public.core_resource_key_v1(verifier text, ip text, op text) RETURNS public.api_keys
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k public.api_keys%ROWTYPE;BEGIN
 SELECT * INTO k FROM public.api_keys WHERE api_keys.verifier=core_resource_key_v1.verifier;
 IF k.id IS NULL THEN RAISE EXCEPTION 'customer_forbidden' USING ERRCODE='P0403';END IF;
 PERFORM public.core_auth_v1(k.id,ip,op,CASE WHEN cardinality(k.models)>0 THEN k.models[1] END);
 RETURN k;
END $$;

CREATE FUNCTION public.core_resource_lease_v1(p jsonb, d jsonb, live boolean) RETURNS public.resources
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE r public.resources%ROWTYPE;BEGIN
 IF NOT (d->>'resource_id' ~ '^(response|batch|video)_[0-9a-f]{32}$') OR jsonb_typeof(d->'lease_epoch') IS DISTINCT FROM 'string' OR d->>'lease_epoch' !~ '^[1-9][0-9]{0,37}$' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT * INTO r FROM public.resources WHERE id=d->>'resource_id' FOR UPDATE;
 IF r.id IS NULL OR r.lease_session_id IS DISTINCT FROM (p->>'session_id')::uuid OR r.lease_epoch IS DISTINCT FROM (d->>'lease_epoch')::numeric OR live AND (r.poll_until IS NULL OR r.poll_until<=clock_timestamp()) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 RETURN r;
END $_$;

CREATE FUNCTION public.core_resource_list_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
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

CREATE FUNCTION public.core_resource_operation_v1(kind text) RETURNS text
    LANGUAGE sql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT CASE kind WHEN 'response' THEN 'responses' WHEN 'file' THEN 'files' WHEN 'batch' THEN 'batches' WHEN 'video' THEN 'videos' END
$$;

CREATE FUNCTION public.core_resource_permit_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
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
END $_$;

CREATE FUNCTION public.core_resource_poll_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE r public.resources%ROWTYPE;b public.channel_plugin_bindings%ROWTYPE;v public.plugin_versions%ROWTYPE;c public.channels%ROWTYPE;manifest jsonb;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_poll_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['resource_id','lease_epoch','auth']<>'{}'::jsonb OR jsonb_typeof(d->'auth') IS DISTINCT FROM 'object' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 r=public.core_resource_lease_v1(p,d,true);
 SELECT * INTO b FROM public.channel_plugin_bindings WHERE id=r.binding_id;
 SELECT * INTO v FROM public.plugin_versions WHERE id=b.plugin_version_id;
 SELECT * INTO c FROM public.channels WHERE id=b.channel_id;
 SELECT pk.manifest INTO manifest FROM public.plugin_packages pk WHERE pk.digest=v.package_digest;
 IF v.quarantined OR v.state NOT IN ('PUBLISHED','DISABLED') OR NOT c.enabled OR (manifest->'capabilities'->>'resources')::boolean IS NOT TRUE THEN RAISE EXCEPTION 'no_route' USING ERRCODE='P0503';END IF;
 IF d->'auth'->>'type' IS DISTINCT FROM b.config->>'auth' AND NOT (d->'auth'->>'type'='bearer' AND b.config->>'auth'='google-oauth') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 RETURN jsonb_build_object('channel_id',c.id,'credential_ciphertext',c.secret,'rpm',c.rpm,'deadline_at',to_char(r.poll_until AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;

CREATE FUNCTION public.core_resource_release_v1(p jsonb, d jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE r public.resources%ROWTYPE;BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' IS DISTINCT FROM 'execution_resource_release_v1' OR jsonb_typeof(d) IS DISTINCT FROM 'object' OR d-ARRAY['resource_id','lease_epoch','state','retry']<>'{}'::jsonb OR d->>'state' NOT IN ('ACTIVE','COMPLETED','FAILED','CANCELLED') OR jsonb_typeof(d->'retry') IS DISTINCT FROM 'boolean' THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 r=public.core_resource_lease_v1(p,d,false);
 UPDATE public.resources SET state=CASE WHEN state='DELETED' THEN state ELSE d->>'state' END,poll_until=NULL,poll_count=poll_count+1,updated_at=now(),
  next_poll_at=clock_timestamp()+CASE WHEN d->>'state'<>'ACTIVE' THEN interval '60 seconds' ELSE least(3600,greatest(CASE WHEN (d->>'retry')::boolean THEN 5 ELSE 1 END,power(2,least(poll_count,11))::integer))*interval '1 second' END
 WHERE id=r.id;
 RETURN jsonb_build_object('resource_id',r.id);
END $$;

CREATE FUNCTION public.core_resource_resolution_v1(r public.resources, deadline timestamp with time zone) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE b public.channel_plugin_bindings%ROWTYPE;v public.plugin_versions%ROWTYPE;c public.channels%ROWTYPE;BEGIN
 SELECT * INTO b FROM public.channel_plugin_bindings WHERE id=r.binding_id;
 SELECT * INTO v FROM public.plugin_versions WHERE id=b.plugin_version_id;
 SELECT * INTO c FROM public.channels WHERE id=b.channel_id;
 RETURN jsonb_build_object('batch',CASE WHEN r.kind='batch' THEN jsonb_build_object('endpoint',r.batch_endpoint,'items',(r.batch_manifest->>'items')::integer,'ids_sha256',r.batch_manifest->>'idsSha256') END,'resource_id',r.id,'kind',r.kind,'remote_id',r.remote_id,'model',r.model,'state',r.state,'channel_id',c.id,'package_digest',v.package_digest,'config',b.config,'origin',regexp_replace(c.base_url,'^(https://[^/]+).*$','\1'),'deadline_at',to_char(deadline AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $_$;

CREATE FUNCTION public.core_route_authority_v1(p jsonb, credential text, client_ip text, chat_model text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE k uuid;
BEGIN
 PERFORM public.core_caller_v1(p);
 IF p->>'command_type' NOT IN ('execution_admit_v1','execution_next_v1') THEN RAISE EXCEPTION 'internal_request_invalid' USING ERRCODE='P0400';END IF;
 SELECT id INTO k FROM public.api_keys WHERE verifier=credential;
 RETURN public.core_auth_v1(k,client_ip,'chat',chat_model);
END $$;

CREATE FUNCTION public.core_short_chat_funding_v1(group_id smallint, expected_epoch bigint) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 PERFORM 1 FROM core_funding.writer_groups WHERE cell=group_id AND epoch=expected_epoch FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'short_chat_funding_owner_changed' USING ERRCODE='P0403';END IF;
 PERFORM set_config('stg.short_chat_funding_cell',group_id::text,true);
END $$;

CREATE FUNCTION public.core_supplier_risk_v1(attempt uuid) RETURNS jsonb
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT coalesce(jsonb_object_agg(dimension,quantity::text),'{}'::jsonb) FROM (
  SELECT d dimension,max((p.usage->>d)::numeric) quantity FROM public.core_evidence_receipts r
  CROSS JOIN LATERAL unnest(r.risk_dimensions) d
  JOIN public.usage_event_index i ON i.id=r.raw_evidence_id
  JOIN public.usage_payloads p ON p.id=i.id AND p.created_at=i.created_at
  WHERE r.attempt_id=attempt AND p.usage ? d GROUP BY d) observed
$$;

CREATE FUNCTION public.core_upstream_fund_v1(account uuid, cur text, delta numeric, adjustment boolean) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE root public.upstream_account_balances%ROWTYPE;b record;c record;needed numeric;take numeric;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 IF delta IS NULL OR adjustment IS NULL OR delta<>trunc(delta) OR abs(delta)>99999999999999999999999999999999999999 OR NOT adjustment AND delta<=0 THEN RAISE EXCEPTION 'funding_delta_invalid' USING ERRCODE='P0400';END IF;
 INSERT INTO public.upstream_account_balances(account_id,currency) VALUES(account,cur) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT b FROM public.core_upstream_balances_v1 WHERE account_id=account AND currency=cur;
 IF abs(b.balance_atoms+delta)>99999999999999999999999999999999999999 OR b.revision>=99999999999999999999999999999999999999 OR adjustment AND abs(b.adjustment_atoms+delta)>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 IF delta>0 THEN PERFORM core_funding.normalize_v1(account,cur);
 ELSE PERFORM core_funding.collect_v1(account,cur,least(-delta,greatest(b.balance_atoms-b.reserved_atoms,0)));END IF;
 SELECT * INTO STRICT root FROM public.upstream_account_balances WHERE account_id=account AND currency=cur FOR NO KEY UPDATE NOWAIT;
 IF root.balance_atoms+delta< -99999999999999999999999999999999999999 THEN PERFORM core_funding.offset_debt_v1(account,cur,-99999999999999999999999999999999999999-root.balance_atoms-delta);END IF;
 SELECT greatest(root.revision+1+coalesce(sum(revision_limit),0)-99999999999999999999999999999999999999,0) INTO needed FROM core_funding.balance_cells WHERE account_id=account AND currency=cur;
 FOR c IN SELECT cell,revision_limit-revision unused FROM core_funding.balance_cells WHERE account_id=account AND currency=cur ORDER BY cell FOR NO KEY UPDATE NOWAIT LOOP
  EXIT WHEN needed=0;take=least(c.unused,needed);
  IF take>0 THEN PERFORM core_funding.transfer_v1(account,cur,c.cell,0,0,-take,'root:revision:'||gen_random_uuid());needed=needed-take;END IF;
 END LOOP;
 IF needed<>0 THEN RAISE EXCEPTION 'funding_counter_overflow' USING ERRCODE='22003';END IF;
 UPDATE public.upstream_account_balances SET balance_atoms=balance_atoms+delta,adjustment_atoms=adjustment_atoms+CASE WHEN adjustment THEN delta ELSE 0 END,revision=revision+1 WHERE account_id=account AND currency=cur;
END $$;

CREATE FUNCTION public.core_upstream_journal_v1(account uuid, cur text, channel uuid, attempt uuid, kind text, amount numeric) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE journal uuid=gen_random_uuid();BEGIN
 IF kind NOT IN ('RESERVE','RELEASE') OR amount IS NULL OR amount<0 OR NOT EXISTS(SELECT 1 FROM public.upstream_holds h WHERE h.attempt_id=attempt AND h.account_id=account AND h.channel_id=channel AND h.currency=cur) THEN RAISE EXCEPTION 'journal_ownership';END IF;
 INSERT INTO public.upstream_journals(id,account_id,channel_id,attempt_id,currency,kind,reference) VALUES(journal,account,channel,attempt,cur,kind,lower(kind)||':'||attempt);
 INSERT INTO public.upstream_entries(journal_id,ordinal,currency,account,atoms) VALUES(journal,0,cur,'upstream:available',CASE WHEN kind='RESERVE' THEN -amount ELSE amount END),(journal,1,cur,'upstream:reserved',CASE WHEN kind='RESERVE' THEN amount ELSE -amount END);
 SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE;
 SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED;
END $$;

CREATE FUNCTION public.core_upstream_normalize_v1(account uuid) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE cur text;BEGIN
 IF NOT core_funding.held_v1(account,true) THEN RAISE EXCEPTION 'funding_outer_transaction_required' USING ERRCODE='P0F01';END IF;
 FOR cur IN SELECT currency FROM public.upstream_account_balances WHERE account_id=account ORDER BY currency LOOP PERFORM core_funding.normalize_v1(account,cur);END LOOP;
END $$;

CREATE FUNCTION public.core_utf16_sort_v1(value text) RETURNS bytea
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE result text='';point integer;char_index integer;BEGIN
 IF value COLLATE "C" !~ $ascii$[^\x01-\x7f]$ascii$ THEN RETURN decode(regexp_replace(encode(convert_to(value,'UTF8'),'hex'),'(..)',E'00\\1','g'),'hex');END IF;
 FOR char_index IN 1..char_length(value) LOOP
  point=ascii(substr(value,char_index,1));
  IF point>65535 THEN point=point-65536;result=result||lpad(to_hex(55296+point/1024),4,'0')||lpad(to_hex(56320+point%1024),4,'0');
  ELSE result=result||lpad(to_hex(point),4,'0');END IF;
 END LOOP;RETURN decode(result,'hex');
END $_$;

CREATE FUNCTION public.credit_wallet(p_project uuid, p_currency text, p_ref text, p_atoms numeric, p_kind text, p_priority integer, p_expiry timestamp with time zone, p_models text[], p_operations text[]) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE wallet wallets%ROWTYPE; existing funding_operations%ROWTYPE; bucket text; fingerprint text; lot uuid; operation uuid;
BEGIN
 IF p_currency !~ '^[A-Z]{3,8}$' OR p_atoms<=0 OR p_atoms<>trunc(p_atoms) OR p_atoms>99999999999999999999999999999999999999 OR p_kind NOT IN ('CASH','GIFT') OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_models IS NULL OR p_operations IS NULL OR p_priority NOT BETWEEN 0 AND 100000 THEN RAISE EXCEPTION 'invalid funding command' USING ERRCODE='P0422'; END IF;
 IF p_kind='CASH' AND (p_expiry IS NOT NULL OR cardinality(p_models)>0 OR cardinality(p_operations)>0) THEN RAISE EXCEPTION 'cash cannot expire or be scoped' USING ERRCODE='P0422'; END IF;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_currency,p_atoms::text,p_kind,p_priority,p_expiry,p_models,p_operations)::text,'UTF8')),'hex');
 INSERT INTO wallets(id,project_id,currency) VALUES(gen_random_uuid(),p_project,p_currency) ON CONFLICT(project_id,currency) DO NOTHING;
 SELECT * INTO wallet FROM wallets WHERE project_id=p_project AND currency=p_currency FOR NO KEY UPDATE;
 SELECT * INTO existing FROM funding_operations WHERE wallet_id=wallet.id AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'funding identity conflict' USING ERRCODE='P0409'; END IF;RETURN jsonb_build_object('id',existing.id,'wallet_id',wallet.id,'lot_id',existing.lot_id,'replayed',true); END IF;
 IF p_expiry IS NOT NULL AND p_expiry<=clock_timestamp() THEN RAISE EXCEPTION 'grant is expired' USING ERRCODE='P0422'; END IF;
 PERFORM expire_wallet(wallet.id);
 bucket=encode(sha256(convert_to(jsonb_build_array(p_kind,p_priority,p_expiry,p_models,p_operations)::text,'UTF8')),'hex');
 SELECT id INTO lot FROM wallet_lots WHERE wallet_id=wallet.id AND bucket_digest=bucket;
 IF lot IS NULL THEN
  IF (SELECT count(*) FROM wallet_lots WHERE wallet_id=wallet.id AND (available_atoms>0 OR reserved_atoms>0 OR spent_atoms>0 AND (expires_at IS NULL OR expires_at>clock_timestamp())))>=256 THEN RAISE EXCEPTION 'refundable funding bucket limit' USING ERRCODE='P0422'; END IF;
  lot=gen_random_uuid();INSERT INTO wallet_lots(id,wallet_id,kind,bucket_digest,priority,expires_at,models,operations,issued_atoms,available_atoms) VALUES(lot,wallet.id,p_kind,bucket,p_priority,p_expiry,p_models,p_operations,p_atoms,p_atoms);
 ELSE UPDATE wallet_lots SET issued_atoms=issued_atoms+p_atoms,available_atoms=available_atoms+p_atoms WHERE id=lot; END IF;
 UPDATE wallets SET available_atoms=available_atoms+p_atoms,has_expiring_lots=has_expiring_lots OR p_expiry IS NOT NULL WHERE id=wallet.id;
 operation=gen_random_uuid();INSERT INTO funding_operations(id,wallet_id,external_ref,fingerprint,kind,amount_atoms,lot_id) VALUES(operation,wallet.id,p_ref,fingerprint,'CREDIT',p_atoms,lot);
 INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'credit:'||operation,'CREDIT',p_currency,jsonb_build_array(money_leg('system:funding',-p_atoms),money_leg('lot:'||lot||':available',p_atoms)));
 IF p_kind='CASH' AND wallet.debt_atoms>0 THEN PERFORM wake_wallet_debt(wallet.id);END IF;
 RETURN jsonb_build_object('id',operation,'wallet_id',wallet.id,'lot_id',lot,'replayed',false);
END $_$;

CREATE FUNCTION public.exact_billing_price(p_usage jsonb, p_rates jsonb) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE part record; quantity text; fraction integer; q numeric; d numeric; n numeric=0; denominator numeric=1; a numeric; b numeric; remainder numeric; result numeric;
BEGIN
 IF p_rates?'cache_write_tokens' AND (p_rates?'cache_write_5m_tokens' OR p_rates?'cache_write_1h_tokens') THEN RAISE EXCEPTION 'duplicate cache write rate';END IF;
 FOR part IN SELECT key,value FROM jsonb_each(p_rates) LOOP
  quantity=coalesce(p_usage->>part.key,'0');
  IF quantity !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$' OR part.value->>'numerator' !~ '^(0|[1-9][0-9]{0,37})$' OR part.value->>'denominator' !~ '^[1-9][0-9]{0,37}$' THEN RAISE EXCEPTION 'invalid price input'; END IF;
  fraction=CASE WHEN position('.' IN quantity)>0 THEN length(split_part(quantity,'.',2)) ELSE 0 END;
  q=replace(quantity,'.','')::numeric;d=(part.value->>'denominator')::numeric*power(10::numeric,fraction);
  IF d=1 THEN n=n+q*(part.value->>'numerator')::numeric*denominator;CONTINUE;END IF;
  n=n*d+q*(part.value->>'numerator')::numeric*denominator;denominator=denominator*d;
  a=n;b=denominator;WHILE b<>0 LOOP remainder=mod(a,b);a=b;b=remainder;END LOOP;
  IF a<>0 THEN n=div(n,a);denominator=div(denominator,a);END IF;
 END LOOP;
 result=div(n,denominator)+CASE WHEN mod(n,denominator)>0 THEN 1 ELSE 0 END;
 IF result>99999999999999999999999999999999999999 THEN RAISE EXCEPTION 'price exceeds amount range'; END IF;
 RETURN result;
END $_$;

CREATE FUNCTION public.expire_wallet(p_wallet uuid) RETURNS numeric
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE lot record; amount numeric=0; cur text;
BEGIN
 SELECT currency INTO cur FROM wallets WHERE id=p_wallet FOR NO KEY UPDATE;
 FOR lot IN SELECT * FROM wallet_lots WHERE wallet_id=p_wallet AND expires_at<=clock_timestamp() AND available_atoms>0 ORDER BY id LOOP
  UPDATE wallet_lots SET available_atoms=0,expired_atoms=expired_atoms+lot.available_atoms WHERE id=lot.id;
  INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(gen_random_uuid(),p_wallet,'expire:'||lot.id||':'||gen_random_uuid(),'EXPIRE',cur,jsonb_build_array(money_leg('lot:'||lot.id||':available',-lot.available_atoms),money_leg('lot:'||lot.id||':expired',lot.available_atoms)));
  amount=amount+lot.available_atoms;
 END LOOP;
 IF amount>0 THEN UPDATE wallets SET available_atoms=available_atoms-amount,expired_atoms=expired_atoms+amount WHERE id=p_wallet; END IF;
 RETURN amount;
END $$;

CREATE FUNCTION public.guard_plugin_version() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF TG_OP='DELETE' OR NEW.id<>OLD.id OR NEW.name<>OLD.name OR NEW.version<>OLD.version OR NEW.package_digest<>OLD.package_digest OR NEW.abi_version<>OLD.abi_version OR NEW.config_schema_digest<>OLD.config_schema_digest OR NEW.created_at<>OLD.created_at THEN RAISE EXCEPTION 'plugin identity is immutable' USING ERRCODE='P0409';END IF;
 IF current_setting('gateway.catalog_mutation',true) IS DISTINCT FROM 'on' THEN RAISE EXCEPTION 'controlled plugin transition required' USING ERRCODE='P0403';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.guard_upstream_account_identity() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN IF NEW.brand_id<>OLD.brand_id OR NEW.id<>OLD.id THEN RAISE EXCEPTION 'upstream account identity is immutable' USING ERRCODE='P0409';END IF;RETURN NEW;END $$;

CREATE FUNCTION public.immutable_row() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN
 RAISE EXCEPTION 'immutable record';
END $$;

CREATE FUNCTION public.insert_journal_view() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$ BEGIN
 INSERT INTO money_entries(id,wallet_id,reference,kind,currency,created_at) VALUES(NEW.id,NEW.wallet_id,NEW.reference,NEW.kind,NEW.currency,NEW.created_at);
 INSERT INTO journal_payloads VALUES(NEW.id,NEW.created_at,NEW.lines);RETURN NEW;
END $$;

CREATE FUNCTION public.insert_usage_view() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$ BEGIN
 IF NEW.seq IS NOT NULL THEN RAISE EXCEPTION 'usage sequence is generated always';END IF;
 INSERT INTO usage_event_index(id,project_id,request_id,attempt_id,source,finality,cost_atoms,currency,created_at,cost_complete)
 VALUES(NEW.id,NEW.project_id,NEW.request_id,NEW.attempt_id,NEW.source,NEW.finality,NEW.cost_atoms,NEW.currency,NEW.created_at,NEW.cost_complete) RETURNING seq INTO NEW.seq;
 INSERT INTO usage_payloads VALUES(NEW.id,NEW.created_at,NEW.usage);RETURN NEW;
END $$;

CREATE FUNCTION public.internal_identity_challenge_v1(p jsonb, reg uuid, spki bytea, boot bytea, aud text, digest text, nonce bytea, ttl integer) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
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
END $_$;

ALTER FUNCTION public.internal_identity_challenge_v1(p jsonb, reg uuid, spki bytea, boot bytea, aud text, digest text, nonce bytea, ttl integer) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_change_v1(svc text, inst text, hash text, sid uuid, action text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
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
END $_$;

ALTER FUNCTION public.internal_identity_change_v1(svc text, inst text, hash text, sid uuid, action text) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_command_owner_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
 IF TG_TABLE_NAME='execution_commands' THEN
  IF NEW.kind IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1','plugin_disable_v1','plugin_quarantine_v1') THEN
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

ALTER FUNCTION public.internal_identity_command_owner_v1() OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_current_v1(p jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $$
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

ALTER FUNCTION public.internal_identity_current_v1(p jsonb) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
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

ALTER FUNCTION public.internal_identity_guard_v1() OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_locks_v1(keys text[]) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE k text;BEGIN
 FOR k IN SELECT q.k FROM (SELECT DISTINCT unnest(keys) AS k) q ORDER BY q.k COLLATE "C" LOOP
  IF NOT pg_try_advisory_xact_lock(hashtextextended('stg.identity.v1:'||k,0)) THEN RAISE EXCEPTION 'identity_busy' USING ERRCODE='P0503';END IF;
 END LOOP;
END $$;

ALTER FUNCTION public.internal_identity_locks_v1(keys text[]) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_notify_v2() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
BEGIN
 PERFORM pg_notify('stg_identity','');
 RETURN NULL;
END $$;

ALTER FUNCTION public.internal_identity_notify_v2() OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_peer_v1(p jsonb) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE i public.internal_service_instances%ROWTYPE;c public.internal_service_certificates%ROWTYPE;BEGIN
 SELECT * INTO i FROM public.internal_service_instances WHERE service=p->>'service' AND instance_id=p->>'instance_id' FOR SHARE NOWAIT;
 IF NOT FOUND THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 SELECT * INTO c FROM public.internal_service_certificates WHERE certificate_sha256=p->>'certificate_sha256' FOR SHARE NOWAIT;
 IF NOT FOUND OR (c.service,c.instance_id) IS DISTINCT FROM (i.service,i.instance_id) THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 IF i.security_revoked_at IS NOT NULL OR c.security_revoked_at IS NOT NULL THEN RAISE EXCEPTION 'internal_identity_revoked' USING ERRCODE='P0403';END IF;
 IF NOT i.enabled OR c.retired_at IS NOT NULL OR c.not_before>clock_timestamp() OR c.not_after<=clock_timestamp() OR (i.authorization_revision::text,c.authorization_revision::text) IS DISTINCT FROM (p->>'instance_revision',p->>'certificate_revision') THEN RAISE EXCEPTION 'internal_peer_forbidden' USING ERRCODE='P0403';END IF;
 RETURN jsonb_build_object('service',i.service,'instance_id',i.instance_id,'certificate_sha256',c.certificate_sha256,'not_after',to_char(c.not_after AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'));
END $$;

ALTER FUNCTION public.internal_identity_peer_v1(p jsonb) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_projection_v1(p jsonb, endpoint text) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
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
END $_$;

ALTER FUNCTION public.internal_identity_projection_v1(p jsonb, endpoint text) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_provision_v1(svc text, inst text, hash text, san text, exact_san text, valid_from timestamp with time zone, valid_until timestamp with time zone) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
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
END $_$;

ALTER FUNCTION public.internal_identity_provision_v1(svc text, inst text, hash text, san text, exact_san text, valid_from timestamp with time zone, valid_until timestamp with time zone) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_read_peer_v1(hash text) RETURNS jsonb
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $$
 SELECT to_jsonb(c)||jsonb_build_object('instance_enabled',i.enabled,'instance_security_revoked_at',i.security_revoked_at,'instance_revision',i.authorization_revision::text,'certificate_revision',c.authorization_revision::text) FROM public.internal_service_certificates c JOIN public.internal_service_instances i USING(service,instance_id) WHERE c.certificate_sha256=hash
$$;

ALTER FUNCTION public.internal_identity_read_peer_v1(hash text) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_read_registration_v1(id uuid) RETURNS jsonb
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $$
 SELECT to_jsonb(r)-'public_key_spki'-'boot_nonce'-'server_nonce'||jsonb_build_object('public_key_spki',encode(r.public_key_spki,'base64'),'boot_nonce',rtrim(translate(encode(r.boot_nonce,'base64'),'+/','-_'),'='),'server_nonce',rtrim(translate(encode(r.server_nonce,'base64'),'+/','-_'),'='),'challenge_expires_at',to_char(r.challenge_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) FROM public.internal_startup_registrations r WHERE r.challenge_id=id
$$;

ALTER FUNCTION public.internal_identity_read_registration_v1(id uuid) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_read_session_v1(id uuid) RETURNS jsonb
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $$
 SELECT to_jsonb(s)-'public_key_spki'-'boot_nonce'||jsonb_build_object('public_key_spki',encode(s.public_key_spki,'base64'),'session_revision',s.authorization_revision::text) FROM public.internal_startup_sessions s WHERE s.session_id=id
$$;

ALTER FUNCTION public.internal_identity_read_session_v1(id uuid) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_register_v1(p jsonb, id uuid, reg uuid, expected_digest text, ttl integer) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $_$
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
END $_$;

ALTER FUNCTION public.internal_identity_register_v1(p jsonb, id uuid, reg uuid, expected_digest text, ttl integer) OWNER TO core_identity_owner;

CREATE FUNCTION public.internal_identity_registration_guard_v1() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE s public.internal_startup_sessions%ROWTYPE;BEGIN
 IF TG_OP='DELETE' OR OLD.state='REGISTERED' OR (to_jsonb(NEW)-ARRAY['state','session_id','receipt']) IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','session_id','receipt']) THEN RAISE EXCEPTION 'identity_immutable' USING ERRCODE='P0403';END IF;
 IF NEW.state='REGISTERED' THEN
  SELECT * INTO s FROM public.internal_startup_sessions WHERE session_id=NEW.session_id;
  IF NOT FOUND OR s.service<>NEW.service OR s.instance_id<>NEW.instance_id OR s.public_key_spki<>NEW.public_key_spki OR s.boot_nonce<>NEW.boot_nonce OR s.certificate_sha256<>NEW.certificate_sha256 OR NEW.receipt IS DISTINCT FROM jsonb_build_object('session_id',s.session_id,'incarnation_id',s.incarnation_id,'service',s.service,'instance_id',s.instance_id,'certificate_sha256',s.certificate_sha256,'public_key_sha256',s.public_key_sha256,'expires_at',to_char(s.expires_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')) THEN RAISE EXCEPTION 'internal_owner_mismatch' USING ERRCODE='P0403';END IF;
 END IF;
 RETURN NEW;
END $$;

ALTER FUNCTION public.internal_identity_registration_guard_v1() OWNER TO core_identity_owner;

CREATE FUNCTION public.ledger_uuid() RETURNS uuid
    LANGUAGE sql PARALLEL SAFE
    AS $$
 SELECT (pg_catalog.lpad(pg_catalog.to_hex(pg_catalog.floor(extract(epoch FROM pg_catalog.clock_timestamp())*1000)::bigint),12,'0')||'7'||pg_catalog.substr(pg_catalog.replace(pg_catalog.gen_random_uuid()::text,'-',''),14))::uuid
$$;

CREATE FUNCTION public.money_leg(p_account text, p_atoms numeric) RETURNS jsonb
    LANGUAGE sql IMMUTABLE
    AS $$ SELECT jsonb_build_object('account',p_account,'atoms',p_atoms::text) $$;

CREATE FUNCTION public.persist_core_evidence_groups_v1(p_groups jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN RETURN core_funding.persist_v1(p_groups,true);END $$;

CREATE FUNCTION public.persist_core_evidence_slow_v1(g jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE accounts uuid[];BEGIN
 IF jsonb_typeof(g) IS DISTINCT FROM 'object' OR jsonb_typeof(g->'events') IS DISTINCT FROM 'array' OR jsonb_array_length(g->'events') NOT BETWEEN 1 AND 64 THEN RAISE EXCEPTION 'evidence_group_invalid' USING ERRCODE='P0400';END IF;
 SELECT ARRAY(SELECT DISTINCT h.account_id FROM public.upstream_holds h WHERE h.attempt_id IN(SELECT (e->>'attempt_id')::uuid FROM jsonb_array_elements(g->'events') e) ORDER BY h.account_id) INTO accounts;
 PERFORM core_funding.exclusive_v1(accounts);
 PERFORM public.core_catalog_write_v1();
 RETURN core_funding.persist_v1(jsonb_build_array(g),false)->0;
END $$;

CREATE FUNCTION public.plugin_catalog_command_v1(p_kind text, p_actor text, p_command uuid, p_body jsonb, p_owner uuid DEFAULT NULL::uuid, p_trust_expires_at timestamp with time zone DEFAULT NULL::timestamp with time zone, p_credential uuid DEFAULT NULL::uuid, p_administrator uuid DEFAULT NULL::uuid, p_verifier text DEFAULT NULL::text, p_bootstrap boolean DEFAULT false) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE body_digest_value text; cmd public.execution_commands%ROWTYPE; v public.plugin_versions%ROWTYPE; pkg public.plugin_packages%ROWTYPE; run public.plugin_validation_runs%ROWTYPE; channel public.channels%ROWTYPE; publication public.plugin_publication_receipts%ROWTYPE; receipt jsonb; target uuid; old_state text; action text; detail jsonb; next_config_revision numeric; old_binding_digest text; raw bytea; credential public.administrator_credentials%ROWTYPE; administrator public.technical_administrators%ROWTYPE; permission text; authorization_expires_at timestamptz;
BEGIN
 IF p_kind IS NULL OR p_actor IS NULL OR p_command IS NULL OR p_kind NOT IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1','plugin_disable_v1','plugin_quarantine_v1') OR length(p_actor) NOT BETWEEN 1 AND 200 OR p_actor !~ '^(technical-admin:|bootstrap:)' OR p_body IS NULL OR jsonb_typeof(p_body)<>'object' OR octet_length(p_body::text)>65000000 THEN RAISE EXCEPTION 'invalid catalog command' USING ERRCODE='P0422';END IF;
 body_digest_value=encode(sha256(convert_to(p_body::text,'UTF8')),'hex');
 PERFORM pg_advisory_xact_lock(hashtextextended(p_kind||':'||p_actor||':'||p_command::text,0));
 SELECT * INTO cmd FROM execution_commands WHERE kind=p_kind AND caller_identity=p_actor AND command_id=p_command FOR UPDATE;
 IF FOUND THEN
  IF cmd.body_digest<>body_digest_value THEN RAISE EXCEPTION 'catalog command body conflict' USING ERRCODE='P0409';END IF;
  SELECT r.receipt INTO STRICT receipt FROM plugin_catalog_receipts r WHERE r.command_id=cmd.id;
  RETURN receipt;
 END IF;
 IF p_kind NOT IN ('plugin_finish_v1','plugin_resources_v1') THEN
  permission=CASE p_kind WHEN 'plugin_register_v1' THEN CASE WHEN p_body->'release_descriptor'->>'source' LIKE 'upload:%' THEN 'plugins:upload' ELSE 'plugins:install-bundled' END WHEN 'plugin_disable_v1' THEN 'plugins:publish' WHEN 'plugin_quarantine_v1' THEN 'plugins:publish' WHEN 'plugin_validate_v1' THEN 'plugins:validate' WHEN 'plugin_publish_v1' THEN 'plugins:publish' WHEN 'plugin_bind_v1' THEN 'plugins:bind' WHEN 'plugin_recover_v1' THEN 'plugins:recover' END;
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
 IF p_kind IN ('plugin_validate_v1','plugin_publish_v1','plugin_bind_v1','plugin_disable_v1','plugin_quarantine_v1') AND (jsonb_typeof(p_body->'expected_revision') IS DISTINCT FROM 'string' OR p_body->>'expected_revision' !~ '^(0|[1-9][0-9]{0,37})$') THEN RAISE EXCEPTION 'catalog revision required' USING ERRCODE='P0422';END IF;
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
  ELSIF p_kind='plugin_disable_v1' THEN
   IF v.revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric OR v.state<>'PUBLISHED' THEN RAISE EXCEPTION 'plugin disable state conflict' USING ERRCODE='P0409';END IF;
   UPDATE plugin_versions SET state='DISABLED',revision=revision+1 WHERE id=v.id RETURNING * INTO v;
   receipt=jsonb_build_object('version_id',v.id,'package_digest',v.package_digest,'state',v.state,'quarantined',v.quarantined,'revision',v.revision::text,'actor',p_actor,'command_id',p_command);action='plugin_disabled';
  ELSIF p_kind='plugin_quarantine_v1' THEN
   IF v.revision IS DISTINCT FROM (p_body->>'expected_revision')::numeric OR v.quarantined THEN RAISE EXCEPTION 'plugin quarantine state conflict' USING ERRCODE='P0409';END IF;
   UPDATE plugin_versions SET quarantined=true,revision=revision+1 WHERE id=v.id RETURNING * INTO v;
   receipt=jsonb_build_object('version_id',v.id,'package_digest',v.package_digest,'state',v.state,'quarantined',v.quarantined,'revision',v.revision::text,'actor',p_actor,'command_id',p_command);action='plugin_quarantined';
  END IF;
 END IF;
 IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
 IF action IS NOT NULL THEN
  INSERT INTO audit_events(id,action,resource_id,actor,details) VALUES(gen_random_uuid(),action,coalesce(target,v.id)::text,p_actor,jsonb_build_object('command_id',p_command,'old_state',old_state,'new_state',v.state,'reason',p_body->>'reason','package_digest',v.package_digest,'receipt',receipt));
 END IF;
 INSERT INTO plugin_catalog_receipts(command_id,receipt) VALUES(cmd.id,receipt);
 IF authorization_expires_at IS NOT NULL AND authorization_expires_at<=clock_timestamp() THEN RAISE EXCEPTION 'platform credential expired while awaiting lock' USING ERRCODE='P0403';END IF;
 RETURN receipt;
END $_$;

CREATE FUNCTION public.plugin_catalog_receipt_v1(p_kind text, p_actor text, p_command uuid, p_body jsonb DEFAULT NULL::jsonb) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE command public.execution_commands%ROWTYPE; receipt jsonb;
BEGIN
 IF p_kind NOT IN ('plugin_register_v1','plugin_validate_v1','plugin_finish_v1','plugin_publish_v1','plugin_bind_v1','plugin_recover_v1','plugin_resources_v1','plugin_disable_v1','plugin_quarantine_v1') OR p_actor !~ '^(technical-admin:|bootstrap:)' OR length(p_actor) NOT BETWEEN 1 AND 200 THEN RAISE EXCEPTION 'invalid catalog receipt scope' USING ERRCODE='P0403';END IF;
 SELECT * INTO command FROM execution_commands WHERE kind=p_kind AND caller_identity=p_actor AND command_id=p_command AND state='COMMITTED' AND request_id IS NULL AND attempt_id IS NULL;
 IF NOT FOUND THEN RETURN NULL;END IF;
 IF p_body IS NOT NULL AND command.body_digest<>encode(sha256(convert_to(p_body::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'catalog command body conflict' USING ERRCODE='P0409';END IF;
 SELECT r.receipt INTO STRICT receipt FROM plugin_catalog_receipts r WHERE r.command_id=command.id;
 RETURN receipt;
END $$;

CREATE FUNCTION public.post_money_journals(p_items jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$ BEGIN
 IF jsonb_typeof(p_items)<>'array' OR jsonb_array_length(p_items)>65792 THEN RAISE EXCEPTION 'invalid posting batch';END IF;
 IF jsonb_array_length(p_items)=0 THEN RETURN;END IF;
 WITH journals AS MATERIALIZED (
  SELECT * FROM jsonb_to_recordset(p_items) j(id uuid,wallet_id uuid,reference text,kind text,currency text,lines jsonb)
 ), entries AS (
  INSERT INTO money_entries(id,wallet_id,reference,kind,currency)
  SELECT j.id,j.wallet_id,j.reference,j.kind,j.currency FROM journals j
  RETURNING id,created_at
 )
 INSERT INTO journal_payloads(id,created_at,lines)
 SELECT j.id,e.created_at,j.lines FROM journals j JOIN entries e ON e.id=j.id;
END $$;

CREATE FUNCTION public.prepare_evidence_month(p_month date) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE parent text;name text;first_day timestamptz;next_day timestamptz;
BEGIN
 IF p_month IS NULL OR p_month<>date_trunc('month',p_month)::date THEN RAISE EXCEPTION 'month boundary required';END IF;
 first_day=p_month::timestamp AT TIME ZONE 'UTC';next_day=(p_month+interval '1 month')::timestamp AT TIME ZONE 'UTC';
 FOREACH parent IN ARRAY ARRAY['usage_payloads','journal_payloads'] LOOP
  name=parent||'_'||to_char(p_month,'YYYYMM');
  IF to_regclass('public.'||name) IS NULL THEN
   EXECUTE format('CREATE TABLE public.%I PARTITION OF public.%I FOR VALUES FROM (%L) TO (%L)',name,parent,first_day,next_day);
  END IF;
 END LOOP;
END $$;

CREATE FUNCTION public.refund_billing_charge(p_charge uuid, p_atoms numeric, p_ref text, p_reason text) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE wallet wallets%ROWTYPE; charge billing_charges%ROWTYPE; existing billing_refunds%ROWTYPE; allocation record; remaining numeric; take numeric; waived numeric; refunded numeric; expired numeric=0; available numeric=0; legs jsonb='[]';operation uuid;fingerprint text;asof timestamptz;
BEGIN
 IF p_atoms<=0 OR p_atoms<>trunc(p_atoms) OR p_atoms>99999999999999999999999999999999999999 OR length(p_ref) NOT BETWEEN 1 AND 200 OR p_reason NOT IN ('CUSTOMER_REFUND','ADMIN_CORRECTION','PROVIDER_RECONCILIATION') THEN RAISE EXCEPTION 'invalid refund' USING ERRCODE='P0422';END IF;
 SELECT w.* INTO wallet FROM wallets w JOIN billing_holds h ON h.wallet_id=w.id JOIN billing_charges c ON c.hold_id=h.id WHERE c.id=p_charge FOR NO KEY UPDATE OF w;
 IF NOT FOUND THEN RAISE EXCEPTION 'charge not found' USING ERRCODE='P0404';END IF;
 SELECT * INTO charge FROM billing_charges WHERE id=p_charge FOR UPDATE;
 fingerprint=encode(sha256(convert_to(jsonb_build_array(p_charge,p_atoms::text,p_reason)::text,'UTF8')),'hex');
 SELECT * INTO existing FROM billing_refunds WHERE charge_id=p_charge AND external_ref=p_ref;
 IF FOUND THEN IF existing.fingerprint<>fingerprint THEN RAISE EXCEPTION 'refund identity conflict' USING ERRCODE='P0409';END IF;RETURN jsonb_build_object('id',existing.id,'replayed',true);END IF;
 IF p_atoms>charge.amount_atoms-charge.refunded_atoms THEN RAISE EXCEPTION 'refund exceeds charge' USING ERRCODE='P0422';END IF;
 asof=clock_timestamp();
 waived=least(p_atoms,charge.debt_atoms);remaining=p_atoms-waived;refunded=remaining;
 legs=jsonb_build_array(money_leg('system:revenue',-p_atoms));IF waived>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',waived));END IF;
 FOR allocation IN SELECT a.*,l.expires_at FROM charge_allocations a JOIN wallet_lots l ON l.id=a.lot_id WHERE a.charge_id=p_charge AND a.refunded_atoms<a.amount_atoms ORDER BY a.ordinal DESC LOOP
  EXIT WHEN remaining=0;take=least(remaining,allocation.amount_atoms-allocation.refunded_atoms);remaining=remaining-take;
  UPDATE charge_allocations SET refunded_atoms=refunded_atoms+take WHERE charge_id=p_charge AND lot_id=allocation.lot_id;
  IF allocation.expires_at<=asof THEN
   UPDATE wallet_lots SET spent_atoms=spent_atoms-take,expired_atoms=expired_atoms+take WHERE id=allocation.lot_id;expired=expired+take;legs=legs||jsonb_build_array(money_leg('lot:'||allocation.lot_id||':expired',take));
  ELSE UPDATE wallet_lots SET spent_atoms=spent_atoms-take,available_atoms=available_atoms+take WHERE id=allocation.lot_id;available=available+take;legs=legs||jsonb_build_array(money_leg('lot:'||allocation.lot_id||':available',take));END IF;
 END LOOP;
 IF remaining<>0 THEN RAISE EXCEPTION 'refund allocation invariant';END IF;
 UPDATE billing_charges SET debt_atoms=debt_atoms-waived,debt_refunded_atoms=debt_refunded_atoms+waived,refunded_atoms=refunded_atoms+p_atoms WHERE id=p_charge;
 UPDATE wallets SET available_atoms=available_atoms+available,expired_atoms=expired_atoms+expired,spent_atoms=spent_atoms-refunded,debt_atoms=debt_atoms-waived WHERE id=wallet.id;
 UPDATE wallets SET freeze_reason=wallet_freeze_reason(evidence_disputed,manual_frozen,debt_atoms,credit_reserved_atoms,credit_limit_atoms) WHERE id=wallet.id;
 operation=gen_random_uuid();INSERT INTO billing_refunds(id,charge_id,external_ref,amount_atoms,fingerprint,reason) VALUES(operation,p_charge,p_ref,p_atoms,fingerprint,p_reason);
 INSERT INTO money_journal(id,wallet_id,reference,kind,currency,lines) VALUES(operation,wallet.id,'refund:'||operation,'REFUND',wallet.currency,legs);
 IF available>0 AND wallet.debt_atoms>waived THEN PERFORM wake_wallet_debt(wallet.id);END IF;
 RETURN jsonb_build_object('id',operation,'refunded_atoms',p_atoms::text,'replayed',false);
END $$;

CREATE FUNCTION public.register_wallet_dispute() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 INSERT INTO wallet_disputes(wallet_id,request_id) SELECT h.wallet_id,h.request_id FROM billing_holds h JOIN attempts a ON a.request_id=h.request_id WHERE a.id=NEW.attempt_id ON CONFLICT DO NOTHING;RETURN NEW;
END $$;

CREATE FUNCTION public.resource_billing_state() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE req uuid;outcome text;
BEGIN
 IF NEW.kind NOT IN ('video','response','operation') OR NEW.attempt_id IS NULL THEN RETURN NEW;END IF;
 outcome=CASE WHEN NEW.upstream_state IN ('completed','succeeded') THEN 'SUCCESS' WHEN NEW.upstream_state IN ('cancelled','canceled') THEN 'CANCELLED' WHEN NEW.upstream_state IN ('failed','expired') THEN 'UNKNOWN' END;
 IF outcome IS NULL THEN RETURN NEW;END IF;
 SELECT request_id INTO req FROM attempts WHERE id=NEW.attempt_id;
 UPDATE gateway_requests SET billing_outcome=outcome WHERE id=req AND billing_outcome IN ('PENDING','UNKNOWN','CANCELLED') AND billing_outcome<>outcome AND selected_attempt_id=NEW.attempt_id;
 IF FOUND THEN UPDATE billing_holds SET next_reconcile_at=now() WHERE request_id=req AND state IN ('HELD','RECONCILING');END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.settle_covered_requests(p_requests uuid[]) RETURNS jsonb
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE receipts jsonb;journals jsonb;
BEGIN
 WITH targets AS MATERIALIZED (
  SELECT h.* FROM billing_holds h WHERE h.request_id=ANY(p_requests)
 ), candidates AS MATERIALIZED (
  SELECT h.id AS hold_id,h.request_id,h.wallet_id,h.amount_atoms AS held,w.currency,
   l.id AS lot_id,p.id AS pricing_id,e.id AS evidence_id,exact_billing_price(e.usage,p.rates) AS actual
  FROM targets h JOIN wallets w ON w.id=h.wallet_id
  JOIN LATERAL(SELECT * FROM gateway_requests WHERE id=h.request_id OFFSET 0) q ON true
  JOIN LATERAL(SELECT * FROM attempts WHERE id=q.selected_attempt_id OFFSET 0) a ON true
  JOIN LATERAL(SELECT * FROM usage_events WHERE id=a.evidence_id OFFSET 0) e ON true
  JOIN billing_prices p ON p.id=a.billing_pricing_id
  JOIN LATERAL(SELECT * FROM hold_allocations WHERE hold_id=h.id AND ordinal=1 AND amount_atoms=h.amount_atoms OFFSET 0) s ON true
  JOIN wallet_lots l ON l.id=s.lot_id AND l.wallet_id=w.id AND l.expires_at IS NULL
  WHERE h.state IN ('HELD','RECONCILING') AND h.credit_atoms=0 AND w.debt_atoms=0 AND w.freeze_reason IS NULL AND NOT w.evidence_disputed
   AND q.operation IN ('chat','messages','gemini','embeddings','completions') AND q.billing_outcome='SUCCESS'
   AND a.state='SUCCEEDED' AND e.source='execution' AND e.finality='FINAL' AND p.basis='PROVIDER_USAGE' AND p.currency=w.currency
   AND NOT EXISTS(SELECT 1 FROM jsonb_object_keys(p.rates) dimension WHERE NOT e.usage?dimension)
   AND NOT EXISTS(SELECT 1 FROM hold_allocations extra WHERE extra.hold_id=h.id AND extra.ordinal<>1)
   AND NOT EXISTS(SELECT 1 FROM attempts x WHERE x.request_id=q.id AND x.metering_conflict)
   AND NOT EXISTS(SELECT 1 FROM billing_resolutions x WHERE x.request_id=q.id)
   AND NOT EXISTS(SELECT 1 FROM billing_charges x WHERE x.request_id=q.id)
 ), wallet_counts AS (
  SELECT wallet_id,count(*) AS requests FROM targets GROUP BY wallet_id
 ), covered AS (
  SELECT c.wallet_id FROM candidates c JOIN wallet_counts t ON t.wallet_id=c.wallet_id
  WHERE c.actual>0 AND c.actual<=c.held GROUP BY c.wallet_id,t.requests HAVING count(*)=t.requests
 ), entries AS MATERIALIZED (
  SELECT c.*,ledger_uuid() AS charge_id,ledger_uuid() AS journal_id FROM candidates c JOIN covered w ON w.wallet_id=c.wallet_id
 ), charges AS (
  INSERT INTO billing_charges(id,request_id,hold_id,pricing_id,evidence_id,currency,amount_atoms,funded_atoms,debt_atoms)
  SELECT charge_id,request_id,hold_id,pricing_id,evidence_id,currency,actual,actual,0 FROM entries RETURNING id
 ), allocations AS (
  INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms)
  SELECT c.id,e.lot_id,1,e.actual FROM entries e JOIN charges c ON c.id=e.charge_id
 ), lot_totals AS (
  SELECT lot_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY lot_id
 ), lots AS (
  UPDATE wallet_lots l SET reserved_atoms=l.reserved_atoms-t.held,available_atoms=l.available_atoms+t.held-t.actual,spent_atoms=l.spent_atoms+t.actual
  FROM lot_totals t WHERE l.id=t.lot_id
 ), wallet_totals AS (
  SELECT wallet_id,sum(held) AS held,sum(actual) AS actual FROM entries GROUP BY wallet_id
 ), balances AS (
  UPDATE wallets w SET reserved_atoms=w.reserved_atoms-t.held,available_atoms=w.available_atoms+t.held-t.actual,spent_atoms=w.spent_atoms+t.actual
  FROM wallet_totals t WHERE w.id=t.wallet_id
 ), holds AS (
  UPDATE billing_holds h SET state='SETTLED',closed_at=now(),next_reconcile_at=now()+interval '1 hour',reconcile_until=NULL,reconcile_reason=NULL
  FROM entries e WHERE h.id=e.hold_id
 )
 SELECT coalesce(jsonb_object_agg(e.request_id,jsonb_build_object('state','SETTLED','charge_id',e.charge_id,'charged_atoms',e.actual::text,'funded_atoms',e.actual::text,'debt_atoms','0','replayed',false)),'{}'::jsonb),
  coalesce(jsonb_agg(jsonb_build_object('id',e.journal_id,'wallet_id',e.wallet_id,'reference','settle:'||e.request_id,'kind','SETTLE','currency',e.currency,'lines',
   jsonb_build_array(money_leg('lot:'||e.lot_id||':reserved',-e.held))||
   CASE WHEN e.held>e.actual THEN jsonb_build_array(money_leg('lot:'||e.lot_id||':available',e.held-e.actual)) ELSE '[]'::jsonb END||
   jsonb_build_array(money_leg('system:revenue',e.actual)))),'[]'::jsonb)
 INTO receipts,journals FROM entries e;
 PERFORM post_money_journals(journals);
 RETURN receipts;
EXCEPTION WHEN numeric_value_out_of_range OR check_violation OR raise_exception OR invalid_parameter_value THEN RETURN '{}'::jsonb;
END $$;

CREATE FUNCTION public.settle_requests(p_requests uuid[], p_token uuid DEFAULT NULL::uuid) RETURNS jsonb
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE snapshot record;source record;req gateway_requests%ROWTYPE;hold billing_holds%ROWTYPE;
 attempt attempts%ROWTYPE;evidence usage_events%ROWTYPE;price billing_prices%ROWTYPE;resolution billing_resolutions%ROWTYPE;
 existing billing_charges%ROWTYPE;wallet wallets%ROWTYPE;lot wallet_lots%ROWTYPE;
 wallet_rows wallets[];lot_rows wallet_lots[];local_lots wallet_lots[];
 wallet_index jsonb;lot_index jsonb;lot_ranges jsonb;wallet_dirty boolean[];lot_dirty boolean[];
 staged_charges jsonb[]='{}';staged_allocations jsonb[]='{}';staged_journals jsonb[]='{}';staged_holds jsonb[]='{}';wake_wallets uuid[]='{}';
 local_allocations jsonb;local_journals jsonb;legs jsonb;result jsonb;outcomes jsonb[]='{}';outcome_ids uuid[]='{}';
 wi integer;first_lot integer;last_lot integer;li integer;i integer;sequence integer;failure text;
 actual numeric;remaining numeric;take numeric;returned numeric;expired numeric;released numeric;funded numeric;extra numeric;expired_available numeric;
 max_atoms constant numeric=99999999999999999999999999999999999999;precollect_errors jsonb='{}';covered jsonb='{}';
 charge uuid;pricing_id uuid;evidence_id uuid;release_hold boolean;asof timestamptz;new_charge boolean;dirty_wallet boolean;
BEGIN
 IF p_requests IS NULL OR cardinality(p_requests) NOT BETWEEN 1 AND 256 OR array_position(p_requests,NULL) IS NOT NULL THEN RAISE EXCEPTION 'invalid settlement batch' USING ERRCODE='P0422';END IF;
 p_requests=ARRAY(SELECT DISTINCT id FROM unnest(p_requests) input(id) ORDER BY id);
 IF (SELECT count(*) FROM billing_holds WHERE request_id=ANY(p_requests))<>cardinality(p_requests) THEN RAISE EXCEPTION 'preauthorization missing' USING ERRCODE='P0404';END IF;
 PERFORM w.id FROM wallets w WHERE id IN(SELECT wallet_id FROM billing_holds WHERE request_id=ANY(p_requests)) ORDER BY id FOR NO KEY UPDATE;
 PERFORM id FROM gateway_requests WHERE id=ANY(p_requests) ORDER BY id FOR UPDATE;
 PERFORM id FROM billing_holds WHERE request_id=ANY(p_requests) ORDER BY id FOR UPDATE;
 IF p_token IS NULL THEN
  covered=settle_covered_requests(p_requests);
  IF covered<>'{}'::jsonb THEN
   p_requests=ARRAY(SELECT id FROM unnest(p_requests) input(id) WHERE NOT covered?id::text);
   IF cardinality(p_requests)=0 THEN RETURN covered;END IF;
  END IF;
 END IF;
 FOR source IN SELECT DISTINCT h.wallet_id FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id
  WHERE c.request_id=ANY(p_requests) AND c.debt_atoms>0 AND (p_token IS NULL OR h.reconcile_token=p_token AND h.reconcile_until>clock_timestamp())
  AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id))
  ORDER BY h.wallet_id LOOP
  BEGIN PERFORM collect_wallet_debt(source.wallet_id,32);
  EXCEPTION WHEN numeric_value_out_of_range THEN precollect_errors=jsonb_set(precollect_errors,ARRAY[source.wallet_id::text],to_jsonb('22003'::text));END;
 END LOOP;
 asof=clock_timestamp();
 SELECT array_agg(w ORDER BY w.id) INTO wallet_rows FROM wallets w WHERE id IN(SELECT wallet_id FROM billing_holds WHERE request_id=ANY(p_requests));
 SELECT jsonb_object_agg((wallet_rows[n]).id,n) INTO wallet_index FROM generate_subscripts(wallet_rows,1) input(n);
 SELECT array_agg(l ORDER BY l.wallet_id,
  CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
  CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id)
 INTO lot_rows FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.id IN(SELECT (v).id FROM unnest(wallet_rows) v) AND (l.available_atoms>0 OR l.reserved_atoms>0);
 lot_rows=coalesce(lot_rows,'{}'::wallet_lots[]);
 SELECT coalesce(jsonb_object_agg((lot_rows[n]).id,n),'{}') INTO lot_index FROM generate_subscripts(lot_rows,1) input(n);
 SELECT coalesce(jsonb_object_agg(wallet_id,jsonb_build_array(first_index,last_index)),'{}') INTO lot_ranges FROM(
  SELECT (lot_rows[n]).wallet_id AS wallet_id,min(n) AS first_index,max(n) AS last_index FROM generate_subscripts(lot_rows,1) input(n) GROUP BY (lot_rows[n]).wallet_id
 ) ranges;
 wallet_dirty=array_fill(false,ARRAY[cardinality(wallet_rows)]);lot_dirty=array_fill(false,ARRAY[cardinality(lot_rows)]);
 FOR snapshot IN SELECT q AS req,h AS hold,a AS attempt,e AS evidence,p AS price,r AS resolution,c AS charge,
  EXISTS(SELECT 1 FROM attempts conflict WHERE conflict.request_id=q.id AND conflict.metering_conflict) AS conflicted
  FROM gateway_requests q JOIN billing_holds h ON h.request_id=q.id LEFT JOIN attempts a ON a.id=q.selected_attempt_id
  LEFT JOIN usage_events e ON e.id=a.evidence_id
  LEFT JOIN LATERAL(SELECT * FROM billing_resolutions WHERE request_id=q.id ORDER BY seq DESC LIMIT 1) r ON true
  LEFT JOIN billing_prices p ON p.id=CASE WHEN r.id IS NOT NULL THEN CASE WHEN r.action='SETTLE' THEN r.pricing_id ELSE NULL END ELSE a.billing_pricing_id END
  LEFT JOIN billing_charges c ON c.request_id=q.id WHERE q.id=ANY(p_requests) ORDER BY h.wallet_id,q.id LOOP
  req=snapshot.req;hold=snapshot.hold;attempt=snapshot.attempt;evidence=snapshot.evidence;price=snapshot.price;resolution=snapshot.resolution;existing=snapshot.charge;
  IF p_token IS NOT NULL AND (hold.reconcile_token IS DISTINCT FROM p_token OR hold.reconcile_until IS NULL OR hold.reconcile_until<=clock_timestamp()) THEN
   outcome_ids=array_append(outcome_ids,req.id);outcomes=array_append(outcomes,'{"state":"STALE_LEASE"}'::jsonb);CONTINUE;
  END IF;
  wi=(wallet_index->>hold.wallet_id::text)::integer;wallet=wallet_rows[wi];
  first_lot=(lot_ranges->hold.wallet_id::text->>0)::integer;last_lot=(lot_ranges->hold.wallet_id::text->>1)::integer;
  IF first_lot IS NULL THEN local_lots='{}'::wallet_lots[];ELSE local_lots=lot_rows[first_lot:last_lot];END IF;
  actual=NULL;remaining=NULL;expired=0;released=0;funded=0;extra=0;expired_available=0;sequence=0;legs='[]';
  local_allocations='[]';local_journals='[]';charge=NULL;pricing_id=NULL;evidence_id=NULL;release_hold=false;new_charge=false;dirty_wallet=false;
  BEGIN
   <<decision>> BEGIN
    IF snapshot.conflicted AND resolution.id IS NULL THEN wallet.evidence_disputed=true;wallet.freeze_reason='EVIDENCE_CONFLICT';dirty_wallet=true;result='{"state":"CONFLICT"}';EXIT decision;END IF;
    IF existing.id IS NOT NULL THEN
     IF existing.debt_atoms>0 AND precollect_errors?wallet.id::text THEN result=jsonb_build_object('state','ERROR','error',precollect_errors->>wallet.id::text);EXIT decision;END IF;
     result=jsonb_build_object('state',CASE WHEN hold.state='RELEASED' THEN 'RELEASED' WHEN existing.debt_atoms>0 THEN 'DEBT' ELSE 'SETTLED' END,
      'charge_id',existing.id,'charged_atoms',existing.amount_atoms::text,'funded_atoms',existing.funded_atoms::text,'debt_atoms',existing.debt_atoms::text,'replayed',true);EXIT decision;
    END IF;
    IF hold.state NOT IN ('HELD','RECONCILING') THEN RAISE EXCEPTION 'closed hold lacks its financial receipt';END IF;
    IF resolution.id IS NOT NULL THEN
     release_hold=resolution.action='RELEASE';
     IF release_hold THEN actual=0;
     ELSE
      IF price.id IS NULL OR EXISTS(SELECT 1 FROM jsonb_object_keys(price.rates) dimension WHERE NOT resolution.usage?dimension) THEN RAISE EXCEPTION 'resolution pricing or quantities unavailable' USING ERRCODE='P0422';END IF;
      pricing_id=price.id;actual=exact_billing_price(resolution.usage,price.rates);
     END IF;
    ELSIF req.billing_outcome='FAILED' THEN
     IF EXISTS(SELECT 1 FROM attempts WHERE request_id=req.id AND state IN ('DISPATCHING','UNKNOWN','PARTIAL')) THEN result='{"state":"WAITING"}';EXIT decision;END IF;
     release_hold=true;actual=0;
    ELSE
     IF req.billing_outcome NOT IN ('SUCCESS','CANCELLED') OR req.selected_attempt_id IS NULL OR price.id IS NULL OR evidence.id IS NULL THEN result='{"state":"WAITING"}';EXIT decision;END IF;
     pricing_id=price.id;evidence_id=evidence.id;
     IF price.basis='PROVIDER_USAGE' THEN
      IF evidence.finality<>'FINAL' OR EXISTS(SELECT 1 FROM jsonb_object_keys(price.rates) dimension WHERE NOT evidence.usage?dimension) THEN result='{"state":"WAITING"}';EXIT decision;END IF;
      actual=exact_billing_price(evidence.usage,price.rates);
     ELSE
      IF req.billing_outcome='CANCELLED' THEN
       IF NOT EXISTS(SELECT 1 FROM usage_event_index e WHERE e.attempt_id=attempt.id AND e.source='batch_cancelled' AND e.finality='FINAL') THEN result='{"state":"WAITING"}';EXIT decision;END IF;
       release_hold=true;actual=0;
      ELSE
       actual=exact_billing_price(CASE WHEN price.basis='INPUT_CHARACTERS' THEN jsonb_build_object('characters',req.billing_quote->>'input_characters','requests','1') ELSE '{"requests":"1"}'::jsonb END,price.rates);
      END IF;
     END IF;
    END IF;
    IF pricing_id IS NOT NULL AND price.currency<>wallet.currency THEN RAISE EXCEPTION 'settlement currency mismatch' USING ERRCODE='P0422';END IF;
    IF actual IS NULL OR actual<0 OR actual<>trunc(actual) OR actual>max_atoms THEN RAISE EXCEPTION 'invalid settlement amount' USING ERRCODE='P0422';END IF;
    charge=ledger_uuid();remaining=actual;
    FOR source IN SELECT * FROM hold_allocations WHERE hold_id=hold.id ORDER BY ordinal LOOP
     li=(lot_index->>source.lot_id::text)::integer-first_lot+1;lot=local_lots[li];
     IF lot.id IS NULL OR lot.wallet_id<>wallet.id THEN RAISE EXCEPTION 'reservation source unavailable';END IF;
     take=least(remaining,source.amount_atoms);returned=source.amount_atoms-take;remaining=remaining-take;funded=funded+take;sequence=sequence+1;
     lot.reserved_atoms=lot.reserved_atoms-source.amount_atoms;lot.spent_atoms=lot.spent_atoms+take;
     legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':reserved',-source.amount_atoms));
     IF take>0 THEN local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('charge_id',charge,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));END IF;
     IF returned>0 THEN
      IF lot.expires_at<=asof THEN lot.expired_atoms=lot.expired_atoms+returned;expired=expired+returned;legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':expired',returned));
      ELSE lot.available_atoms=lot.available_atoms+returned;released=released+returned;legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',returned));END IF;
     END IF;
     local_lots[li]=lot;
    END LOOP;
    IF remaining>0 THEN
     FOR i IN 1..cardinality(local_lots) LOOP
      lot=local_lots[i];
      IF lot.expires_at<=asof AND lot.available_atoms>0 THEN
       take=lot.available_atoms;lot.available_atoms=0;lot.expired_atoms=lot.expired_atoms+take;expired_available=expired_available+take;
       local_journals=local_journals||jsonb_build_array(jsonb_build_object('id',gen_random_uuid(),'wallet_id',wallet.id,'reference','expire:'||lot.id||':'||gen_random_uuid(),'kind','EXPIRE','currency',wallet.currency,'lines',jsonb_build_array(money_leg('lot:'||lot.id||':available',-take),money_leg('lot:'||lot.id||':expired',take))));
       local_lots[i]=lot;
      END IF;
     END LOOP;
     FOR i IN 1..cardinality(local_lots) LOOP
      EXIT WHEN remaining=0;lot=local_lots[i];
      IF lot.available_atoms<=0 OR lot.expires_at<=asof OR cardinality(lot.models)>0 AND NOT req.model=ANY(lot.models) OR cardinality(lot.operations)>0 AND NOT req.operation=ANY(lot.operations) THEN CONTINUE;END IF;
      take=least(remaining,lot.available_atoms);remaining=remaining-take;funded=funded+take;extra=extra+take;sequence=sequence+1;
      lot.available_atoms=lot.available_atoms-take;lot.spent_atoms=lot.spent_atoms+take;local_lots[i]=lot;
      local_allocations=local_allocations||jsonb_build_array(jsonb_build_object('charge_id',charge,'lot_id',lot.id,'ordinal',sequence,'amount_atoms',take::text));
      legs=legs||jsonb_build_array(money_leg('lot:'||lot.id||':available',-take));
     END LOOP;
    END IF;
    IF actual>0 THEN legs=legs||jsonb_build_array(money_leg('system:revenue',actual));END IF;
    IF remaining>0 THEN legs=legs||jsonb_build_array(money_leg('system:receivable',-remaining));END IF;
    wallet.reserved_atoms=wallet.reserved_atoms-(hold.amount_atoms-hold.credit_atoms);wallet.credit_reserved_atoms=wallet.credit_reserved_atoms-hold.credit_atoms;wallet.available_atoms=wallet.available_atoms+released-extra-expired_available;
    wallet.spent_atoms=wallet.spent_atoms+funded;wallet.expired_atoms=wallet.expired_atoms+expired+expired_available;wallet.debt_atoms=wallet.debt_atoms+remaining;
    wallet.freeze_reason=wallet_freeze_reason(wallet.evidence_disputed,wallet.manual_frozen,wallet.debt_atoms,wallet.credit_reserved_atoms,wallet.credit_limit_atoms);
    IF wallet.available_atoms NOT BETWEEN 0 AND max_atoms OR wallet.reserved_atoms NOT BETWEEN 0 AND max_atoms OR wallet.credit_reserved_atoms NOT BETWEEN 0 AND max_atoms OR wallet.spent_atoms NOT BETWEEN 0 AND max_atoms OR wallet.expired_atoms NOT BETWEEN 0 AND max_atoms OR wallet.debt_atoms NOT BETWEEN 0 AND max_atoms THEN RAISE EXCEPTION 'staged wallet amount exceeds bounds' USING ERRCODE='22003';END IF;
    FOR i IN 1..cardinality(local_lots) LOOP
     lot=local_lots[i];
     IF lot.available_atoms NOT BETWEEN 0 AND max_atoms OR lot.reserved_atoms NOT BETWEEN 0 AND max_atoms OR lot.spent_atoms NOT BETWEEN 0 AND max_atoms OR lot.expired_atoms NOT BETWEEN 0 AND max_atoms OR lot.issued_atoms<>lot.available_atoms+lot.reserved_atoms+lot.spent_atoms+lot.expired_atoms THEN RAISE EXCEPTION 'staged source amount exceeds bounds' USING ERRCODE='22003';END IF;
    END LOOP;
    IF jsonb_array_length(legs)>0 THEN local_journals=local_journals||jsonb_build_array(jsonb_build_object('id',ledger_uuid(),'wallet_id',wallet.id,'reference','settle:'||req.id,'kind',CASE WHEN release_hold THEN 'RELEASE' ELSE 'SETTLE' END,'currency',wallet.currency,'lines',legs));END IF;
    IF jsonb_array_length(legs)>2048 OR jsonb_array_length(legs)=1 THEN RAISE EXCEPTION 'settlement journal exceeds posting bounds' USING ERRCODE='P0422';END IF;
    staged_charges=array_append(staged_charges,jsonb_build_object('id',charge,'request_id',req.id,'hold_id',hold.id,'pricing_id',pricing_id,'evidence_id',evidence_id,'currency',wallet.currency,'amount_atoms',actual::text,'funded_atoms',funded::text,'debt_atoms',remaining::text));
    staged_allocations=array_append(staged_allocations,local_allocations);staged_journals=array_append(staged_journals,local_journals);
    FOR i IN 1..cardinality(local_lots) LOOP
     li=first_lot+i-1;
     IF local_lots[i] IS DISTINCT FROM lot_rows[li] THEN lot_rows[li]=local_lots[i];lot_dirty[li]=true;END IF;
    END LOOP;
    new_charge=true;dirty_wallet=true;
    IF released>0 AND wallet.debt_atoms>0 THEN wake_wallets=array_append(wake_wallets,wallet.id);END IF;
    result=jsonb_build_object('state',CASE WHEN release_hold THEN 'RELEASED' WHEN remaining>0 THEN 'DEBT' ELSE 'SETTLED' END,'charge_id',charge,'charged_atoms',actual::text,'funded_atoms',funded::text,'debt_atoms',remaining::text,'replayed',false);
   END decision;
   IF dirty_wallet AND wallet IS DISTINCT FROM wallet_rows[wi] THEN wallet_rows[wi]=wallet;wallet_dirty[wi]=true;END IF;
  EXCEPTION WHEN OTHERS THEN
   GET STACKED DIAGNOSTICS failure=RETURNED_SQLSTATE;
   IF failure NOT IN ('22003','22023','P0422','P0001') THEN RAISE;END IF;
   result=jsonb_build_object('state','ERROR','error',failure);new_charge=false;
  END;
  staged_holds=array_append(staged_holds,jsonb_build_object('id',hold.id,
   'state',CASE WHEN result->>'state' IN ('WAITING','CONFLICT','ERROR') AND hold.state IN ('HELD','RECONCILING') THEN 'RECONCILING' WHEN result->>'state'='RELEASED' THEN 'RELEASED' WHEN result->>'state' IN ('SETTLED','DEBT') THEN 'SETTLED' ELSE hold.state END,
   'closed_at',CASE WHEN new_charge THEN now() ELSE hold.closed_at END,
   'next_reconcile_at',now()+CASE WHEN result->>'state' IN ('WAITING','CONFLICT','ERROR') THEN least(interval '1 hour',greatest(interval '30 seconds',now()-hold.created_at)) ELSE interval '1 hour' END,
   'reconcile_reason',CASE WHEN result->>'state' IN ('WAITING','CONFLICT','DEBT','ERROR') THEN result->>'state' ELSE NULL END));
  outcome_ids=array_append(outcome_ids,req.id);outcomes=array_append(outcomes,result);
 END LOOP;
 INSERT INTO billing_charges(id,request_id,hold_id,pricing_id,evidence_id,currency,amount_atoms,funded_atoms,debt_atoms)
 SELECT c.id,c.request_id,c.hold_id,c.pricing_id,c.evidence_id,c.currency,c.amount_atoms,c.funded_atoms,c.debt_atoms FROM jsonb_to_recordset(to_jsonb(staged_charges)) c(id uuid,request_id uuid,hold_id uuid,pricing_id uuid,evidence_id uuid,currency text,amount_atoms numeric,funded_atoms numeric,debt_atoms numeric);
 INSERT INTO charge_allocations(charge_id,lot_id,ordinal,amount_atoms)
 SELECT a.charge_id,a.lot_id,min(a.ordinal),sum(a.amount_atoms)
 FROM unnest(staged_allocations) batch(items) CROSS JOIN LATERAL jsonb_to_recordset(batch.items) a(charge_id uuid,lot_id uuid,ordinal integer,amount_atoms numeric) GROUP BY a.charge_id,a.lot_id;
 UPDATE wallet_lots target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,spent_atoms=state.spent_atoms,expired_atoms=state.expired_atoms
 FROM(SELECT (lot_rows[n]).* FROM generate_subscripts(lot_rows,1) input(n) WHERE lot_dirty[n]) state WHERE target.id=state.id;
 UPDATE wallets target SET available_atoms=state.available_atoms,reserved_atoms=state.reserved_atoms,spent_atoms=state.spent_atoms,expired_atoms=state.expired_atoms,debt_atoms=state.debt_atoms,credit_reserved_atoms=state.credit_reserved_atoms,evidence_disputed=state.evidence_disputed,freeze_reason=state.freeze_reason
 FROM(SELECT (wallet_rows[n]).* FROM generate_subscripts(wallet_rows,1) input(n) WHERE wallet_dirty[n]) state WHERE target.id=state.id;
 PERFORM post_money_journals(coalesce((SELECT jsonb_agg(j.item) FROM unnest(staged_journals) batch(items) CROSS JOIN LATERAL jsonb_array_elements(batch.items) j(item)),'[]'::jsonb));
 UPDATE billing_holds target SET state=state.state,closed_at=state.closed_at,next_reconcile_at=state.next_reconcile_at,reconcile_until=NULL,reconcile_reason=state.reconcile_reason
 FROM jsonb_to_recordset(to_jsonb(staged_holds)) state(id uuid,state text,closed_at timestamptz,next_reconcile_at timestamptz,reconcile_reason text) WHERE target.id=state.id;
 FOR source IN SELECT DISTINCT id FROM unnest(wake_wallets) w(id) LOOP PERFORM wake_wallet_debt(source.id);END LOOP;
 RETURN covered||(SELECT jsonb_object_agg(id,value) FROM unnest(outcome_ids,outcomes) result_rows(id,value));
END $$;

CREATE FUNCTION public.validate_billing_price() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE part record;
BEGIN
 IF jsonb_typeof(NEW.rates)<>'object' OR NEW.rates='{}' THEN RAISE EXCEPTION 'invalid tariff rates';END IF;
 FOR part IN SELECT key,value FROM jsonb_each(NEW.rates) LOOP
  IF part.key NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters') OR jsonb_typeof(part.value)<>'object' OR NOT(part.value?&ARRAY['numerator','denominator']) OR jsonb_typeof(part.value->'numerator')<>'string' OR jsonb_typeof(part.value->'denominator')<>'string' OR part.value->>'numerator' !~ '^(0|[1-9][0-9]{0,37})$' OR part.value->>'denominator' !~ '^[1-9][0-9]{0,37}$' OR NOT NEW.ceilings?part.key OR jsonb_typeof(NEW.ceilings->part.key)<>'string' OR NEW.ceilings->>part.key !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$' THEN RAISE EXCEPTION 'invalid tariff dimension';END IF;
  IF part.key NOT IN ('audio_seconds','video_seconds') AND NEW.ceilings->>part.key LIKE '%.%' THEN RAISE EXCEPTION 'counter ceiling is fractional';END IF;
  IF NEW.basis='REQUEST_COMPLETION' AND part.key<>'requests' OR NEW.basis='INPUT_CHARACTERS' AND part.key NOT IN ('characters','requests') THEN RAISE EXCEPTION 'billing basis dimension mismatch';END IF;
 END LOOP;
 IF NEW.rates?'cache_write_tokens' AND (NEW.rates?'cache_write_5m_tokens' OR NEW.rates?'cache_write_1h_tokens') THEN RAISE EXCEPTION 'duplicate cache write rate';END IF;
 IF NEW.rates?'audio_ms' AND NEW.rates?'audio_seconds' OR NEW.rates?'video_ms' AND NEW.rates?'video_seconds' THEN RAISE EXCEPTION 'duplicate duration unit';END IF;
 IF NEW.rates?'requests' AND (NEW.ceilings->>'requests')::numeric<1 THEN RAISE EXCEPTION 'request ceiling is zero';END IF;
 PERFORM exact_billing_price(NEW.ceilings,NEW.rates);RETURN NEW;
END $_$;

CREATE FUNCTION public.validate_catalog_audit() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF NEW.action LIKE 'plugin_%' AND (NEW.actor IS NULL OR length(NEW.actor) NOT BETWEEN 1 AND 200 OR NEW.details IS NULL OR jsonb_typeof(NEW.details)<>'object' OR octet_length(NEW.details::text)>8192 OR NOT(NEW.details ?& ARRAY['command_id','old_state','new_state','reason']) OR coalesce(length(NEW.details->>'reason'),0) NOT BETWEEN 1 AND 500) THEN RAISE EXCEPTION 'catalog audit context required' USING ERRCODE='P0422';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.validate_channel_binding() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF NEW.binding_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM channel_plugin_bindings WHERE id=NEW.binding_id AND channel_id=NEW.id AND config_revision=NEW.config_revision) THEN RAISE EXCEPTION 'channel binding mismatch' USING ERRCODE='P0422';END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.validate_evidence_lineage() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE core boolean;parent public.usage_event_index%ROWTYPE;fact public.usage_event_index%ROWTYPE;BEGIN
 core=EXISTS(SELECT 1 FROM public.execution_permits WHERE attempt_id=NEW.attempt_id);
 IF NEW.record_kind='GATEWAY' THEN
  IF core THEN RAISE EXCEPTION 'core_attempt_requires_signed_evidence' USING ERRCODE='P0409';END IF;
  RETURN NEW;
 END IF;
 IF NOT core THEN RAISE EXCEPTION 'signed_evidence_requires_core_attempt' USING ERRCODE='P0409';END IF;
 IF NEW.source NOT IN ('execution','execution_outcome','recovery_interruption','async_partial','async_final','batch_completed','batch_cancelled','batch_failed','provider_invoice')
  OR cardinality(NEW.final_dimensions)>15 OR EXISTS(SELECT 1 FROM unnest(NEW.final_dimensions) d GROUP BY d HAVING count(*)>1)
  OR EXISTS(SELECT 1 FROM unnest(NEW.final_dimensions) d WHERE d IS NULL OR d NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','audio_seconds','video_seconds','characters'))
 THEN RAISE EXCEPTION 'evidence_snapshot_invalid' USING ERRCODE='P0400';END IF;
 IF NEW.record_kind='RAW' THEN RETURN NEW;END IF;
 SELECT * INTO fact FROM public.usage_event_index WHERE id=NEW.input_fact_id AND attempt_id=NEW.attempt_id;
 IF fact.id IS NULL OR fact.record_kind<>'RAW' OR fact.snapshot_revision>=NEW.snapshot_revision THEN RAISE EXCEPTION 'evidence_lineage_invalid' USING ERRCODE='P0409';END IF;
 IF NEW.parent_evidence_id IS NOT NULL THEN
  SELECT * INTO parent FROM public.usage_event_index WHERE id=NEW.parent_evidence_id AND attempt_id=NEW.attempt_id;
  IF parent.id IS NULL OR parent.record_kind<>'CANONICAL' OR parent.snapshot_revision>=NEW.snapshot_revision THEN RAISE EXCEPTION 'evidence_lineage_invalid' USING ERRCODE='P0409';END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.validate_journal_payload() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE entry money_entries%ROWTYPE;BEGIN
 SELECT * INTO entry FROM money_entries WHERE id=NEW.id AND created_at=NEW.created_at;
 IF NOT FOUND THEN RAISE EXCEPTION 'journal identity missing';END IF;
 PERFORM validate_money_lines(entry.wallet_id,entry.currency,NEW.lines);RETURN NEW;
END $$;

CREATE FUNCTION public.validate_key_principal() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE principal principals%ROWTYPE;org uuid;cidr text;
BEGIN
 FOREACH cidr IN ARRAY NEW.allowed_cidrs LOOP PERFORM cidr::inet;END LOOP;
 IF NEW.principal_id IS NOT NULL THEN
  SELECT * INTO principal FROM principals WHERE id=NEW.principal_id;SELECT organization_id INTO org FROM projects WHERE id=NEW.project_id;
  IF principal.id IS NULL OR principal.organization_id IS DISTINCT FROM org THEN RAISE EXCEPTION 'principal belongs to another organization' USING ERRCODE='P0403';END IF;
  IF NEW.role='ADMIN' AND principal.role NOT IN ('OWNER','ADMIN') OR NEW.role='BILLING' AND principal.role NOT IN ('OWNER','ADMIN','BILLING') OR NEW.role='INVOKER' AND principal.role NOT IN ('OWNER','ADMIN','MEMBER') THEN RAISE EXCEPTION 'key role exceeds principal authority' USING ERRCODE='P0403';END IF;
 END IF;RETURN NEW;
END $$;

CREATE FUNCTION public.validate_money_journal() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$ BEGIN
 PERFORM validate_money_lines(NEW.wallet_id,NEW.currency,NEW.lines);RETURN NEW;
END $$;

CREATE FUNCTION public.validate_money_lines(p_wallet uuid, p_currency text, p_lines jsonb) RETURNS void
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $_$
DECLARE item jsonb; total numeric=0; count integer; i integer; account text; atoms text; lot_id uuid; prior_lot uuid; lot_ids uuid[]='{}';
BEGIN
 count=jsonb_array_length(p_lines);
 IF count NOT BETWEEN 2 AND 2048 THEN RAISE EXCEPTION 'invalid money entry'; END IF;
 FOR i IN 0..count-1 LOOP
  item=p_lines->i;account=item->>'account';atoms=item->>'atoms';
  IF jsonb_typeof(item)<>'object' OR item-ARRAY['account','atoms']<>'{}'::jsonb OR NOT(item?&ARRAY['account','atoms']) OR jsonb_typeof(item->'account')<>'string' OR account !~ '^(lot:[a-f0-9-]{36}:(available|reserved|spent|expired)|system:(funding|revenue|receivable))$' OR jsonb_typeof(item->'atoms')<>'string' OR atoms !~ '^-?(0|[1-9][0-9]{0,37})$' THEN RAISE EXCEPTION 'invalid money leg'; END IF;
  total=total+atoms::numeric;
  IF left(account,4)='lot:' THEN
   lot_id=substr(account,5,36)::uuid;
   IF lot_id IS DISTINCT FROM prior_lot THEN lot_ids=array_append(lot_ids,lot_id);prior_lot=lot_id;END IF;
  END IF;
 END LOOP;
 IF total<>0 THEN RAISE EXCEPTION 'unbalanced money entry'; END IF;
 IF EXISTS(SELECT 1 FROM unnest(lot_ids) AS sources(id) WHERE NOT EXISTS(SELECT 1 FROM wallet_lots l WHERE l.id=sources.id AND l.wallet_id=p_wallet)) THEN RAISE EXCEPTION 'foreign money source';END IF;
 IF NOT EXISTS(SELECT 1 FROM wallets WHERE id=p_wallet AND currency=p_currency) THEN RAISE EXCEPTION 'wallet currency mismatch'; END IF;
 RETURN;
END $_$;

CREATE FUNCTION public.validate_project_organization() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM api_keys k JOIN principals p ON p.id=k.principal_id WHERE k.project_id=NEW.id AND p.organization_id IS DISTINCT FROM NEW.organization_id) THEN RAISE EXCEPTION 'revoke principal keys before moving project' USING ERRCODE='P0409';END IF;RETURN NEW;
END $$;

CREATE FUNCTION public.validate_upstream_journal() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
DECLARE target uuid; total numeric; lines integer;
BEGIN
 IF TG_TABLE_NAME='upstream_entries' THEN target=NEW.journal_id; ELSE target=NEW.id; END IF;
 SELECT count(*),sum(atoms) INTO lines,total FROM upstream_entries WHERE journal_id=target;
 IF lines<2 OR total<>0 THEN RAISE EXCEPTION 'unbalanced upstream journal' USING ERRCODE='P0422';END IF;
 RETURN NULL;
END $$;

CREATE FUNCTION public.validate_usage() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$ DECLARE k text; v text; BEGIN
 FOR k,v IN SELECT key,value #>> '{}' FROM jsonb_each(NEW.usage) LOOP
  IF jsonb_typeof(NEW.usage->k)<>'string' OR
   (k IN ('audio_seconds','video_seconds') AND v !~ '^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$') OR
   (k NOT IN ('audio_seconds','video_seconds') AND (k NOT IN ('input_tokens','output_tokens','cached_input_tokens','cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens','reasoning_tokens','requests','images','audio_ms','music_ms','video_ms','characters') OR v !~ '^(0|[1-9][0-9]{0,37})$')) THEN
   RAISE EXCEPTION 'invalid usage dimension';
  END IF;
 END LOOP;
 IF NEW.usage?&ARRAY['cache_write_tokens','cache_write_5m_tokens','cache_write_1h_tokens'] AND (NEW.usage->>'cache_write_tokens')::numeric<>(NEW.usage->>'cache_write_5m_tokens')::numeric+(NEW.usage->>'cache_write_1h_tokens')::numeric THEN RAISE EXCEPTION 'cache write partitions disagree';END IF;
 RETURN NEW;
END $_$;

CREATE FUNCTION public.verify_platform_credential_v1(p_verifier text) RETURNS TABLE(actor text, permissions text[], credential_id uuid, administrator_id uuid)
    LANGUAGE sql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 SELECT 'technical-admin:'||a.id::text,a.permissions,c.id,a.id FROM public.administrator_credentials c JOIN public.technical_administrators a ON a.id=c.administrator_id WHERE c.verifier=p_verifier AND c.enabled AND a.enabled AND (c.expires_at IS NULL OR c.expires_at>clock_timestamp())
$$;

CREATE FUNCTION public.wake_wallet_debt(p_wallet uuid) RETURNS void
    LANGUAGE sql
    SET search_path TO 'pg_catalog', 'public', 'pg_temp'
    AS $$
 UPDATE billing_holds h SET next_reconcile_at=now(),reconcile_until=NULL
 FROM billing_charges c WHERE c.hold_id=h.id AND h.wallet_id=p_wallet AND c.debt_atoms>0
 AND EXISTS(SELECT 1 FROM wallet_lots l WHERE l.wallet_id=p_wallet AND l.kind='CASH' AND l.available_atoms>0)
 AND NOT EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=c.request_id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=c.request_id))
$$;

CREATE TABLE public.wallet_lots (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    kind text NOT NULL,
    bucket_digest text NOT NULL,
    priority integer DEFAULT 100 NOT NULL,
    expires_at timestamp with time zone,
    models text[] DEFAULT '{}'::text[] NOT NULL,
    operations text[] DEFAULT '{}'::text[] NOT NULL,
    issued_atoms numeric(38,0) NOT NULL,
    available_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    expired_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallet_lots_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_check CHECK ((issued_atoms = (((available_atoms + reserved_atoms) + spent_atoms) + expired_atoms))),
    CONSTRAINT wallet_lots_check1 CHECK (((kind = 'GIFT'::text) OR (expires_at IS NULL))),
    CONSTRAINT wallet_lots_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_issued_atoms_check CHECK ((issued_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_kind_check CHECK ((kind = ANY (ARRAY['CASH'::text, 'GIFT'::text]))),
    CONSTRAINT wallet_lots_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallet_lots_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
)
WITH (fillfactor='70');

CREATE FUNCTION public.wallet_available_lots(p_wallet uuid, p_model text, p_operation text, p_asof timestamp with time zone) RETURNS SETOF public.wallet_lots
    LANGUAGE sql STABLE
    AS $$
 SELECT l.* FROM public.wallet_lots l JOIN public.wallets w ON w.id=l.wallet_id WHERE l.wallet_id=p_wallet AND l.available_atoms>0
 AND (l.expires_at IS NULL OR l.expires_at>p_asof) AND (cardinality(l.models)=0 OR p_model=ANY(l.models)) AND (cardinality(l.operations)=0 OR p_operation=ANY(l.operations))
 ORDER BY CASE WHEN w.policy='GIFT_FIRST' THEN CASE WHEN l.kind='GIFT' THEN 0 ELSE 1 END WHEN w.policy='CASH_FIRST' THEN CASE WHEN l.kind='CASH' THEN 0 ELSE 1 END ELSE 0 END,
 CASE WHEN w.policy='EARLIEST_EXPIRY' THEN l.expires_at ELSE NULL END NULLS LAST,l.priority,l.expires_at NULLS LAST,l.created_at,l.id
$$;

CREATE FUNCTION public.wallet_freeze_reason(p_disputed boolean, p_manual boolean, p_debt numeric, p_credit_reserved numeric, p_limit numeric) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
 SELECT CASE WHEN p_disputed THEN 'EVIDENCE_CONFLICT' WHEN p_manual THEN 'MANUAL' WHEN p_debt+p_credit_reserved>p_limit THEN 'DEBT' ELSE NULL END
$$;

CREATE TABLE core_catalog_cache.candidates (
    catalog_revision numeric(38,0) NOT NULL,
    scope_digest bytea NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    currency text NOT NULL,
    candidates jsonb NOT NULL,
    payload_bytes integer NOT NULL,
    CONSTRAINT catalog_cache_identity CHECK (((catalog_revision > (0)::numeric) AND (octet_length(scope_digest) = 32))),
    CONSTRAINT catalog_cache_payload CHECK (((jsonb_typeof(candidates) = 'array'::text) AND (jsonb_array_length(candidates) <= 128) AND ((payload_bytes >= 1) AND (payload_bytes <= 67108864))))
);

CREATE TABLE core_catalog_cache.epoch (
    singleton boolean DEFAULT true NOT NULL,
    revision numeric(38,0) DEFAULT '1'::numeric NOT NULL,
    CONSTRAINT catalog_epoch_revision CHECK ((revision > (0)::numeric)),
    CONSTRAINT catalog_epoch_singleton CHECK ((singleton IS TRUE))
);

CREATE TABLE core_catalog_cache.usage (
    singleton boolean DEFAULT true NOT NULL,
    byte_count integer DEFAULT 0 NOT NULL,
    entry_count integer DEFAULT 0 NOT NULL,
    CONSTRAINT catalog_cache_usage_bounds CHECK ((((byte_count >= 0) AND (byte_count <= 67108864)) AND ((entry_count >= 0) AND (entry_count <= 4096)))),
    CONSTRAINT catalog_cache_usage_singleton CHECK ((singleton IS TRUE))
);

CREATE TABLE core_execution_runtime.approvals (
    package_digest text NOT NULL,
    kind text NOT NULL,
    artifact_digest text NOT NULL,
    active boolean NOT NULL,
    facts jsonb NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT execution_runtime_actor CHECK ((((length(actor) >= 1) AND (length(actor) <= 100)) AND ((length(reason) >= 1) AND (length(reason) <= 1000)))),
    CONSTRAINT execution_runtime_digest CHECK ((artifact_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT execution_runtime_facts CHECK (((jsonb_typeof(facts) = 'object'::text) AND (octet_length((facts)::text) <= 16384))),
    CONSTRAINT execution_runtime_kind CHECK ((kind = ANY (ARRAY['wasm'::text, 'builtin-worker'::text])))
);

CREATE TABLE core_funding.balance_cells (
    account_id uuid NOT NULL,
    currency text NOT NULL,
    cell smallint NOT NULL,
    balance_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_limit numeric(38,0) NOT NULL,
    revision_limit numeric(38,0) NOT NULL,
    CONSTRAINT funding_cell_counters CHECK (((reserved_atoms >= (0)::numeric) AND (spent_atoms >= (0)::numeric) AND (revision >= (0)::numeric) AND (spent_limit >= spent_atoms) AND (revision_limit >= revision))),
    CONSTRAINT funding_cell_range CHECK (((cell >= 0) AND (cell <= 15)))
);

CREATE TABLE core_funding.billing_prices (
    price_id uuid NOT NULL,
    compiler_version integer NOT NULL,
    source_digest text NOT NULL,
    dimensions text[] NOT NULL,
    weights numeric[] NOT NULL,
    denominator numeric NOT NULL,
    CONSTRAINT price_program_digest CHECK ((source_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT price_program_shape CHECK (((cardinality(dimensions) = cardinality(weights)) AND (array_position(dimensions, NULL::text) IS NULL) AND (array_position(weights, NULL::numeric) IS NULL) AND (denominator > (0)::numeric) AND (denominator = trunc(denominator)) AND ((0)::numeric <= ALL (weights)))),
    CONSTRAINT price_program_version CHECK ((compiler_version = 1))
);

CREATE TABLE core_funding.budget_transfers (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    currency text NOT NULL,
    cell smallint NOT NULL,
    kind text NOT NULL,
    reference text NOT NULL,
    balance_delta numeric(38,0) NOT NULL,
    spent_allocation_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_limit_delta numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    revision_limit_delta numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT funding_transfer_history CHECK (((spent_allocation_atoms >= (0)::numeric) AND ((kind = 'MIGRATION'::text) OR (spent_allocation_atoms = (0)::numeric)))),
    CONSTRAINT funding_transfer_kind CHECK ((kind = ANY (ARRAY['GRANT'::text, 'RECLAIM'::text, 'HEADROOM'::text, 'MIGRATION'::text, 'REBASE'::text])))
);

CREATE TABLE core_funding.cost_prices (
    price_id uuid NOT NULL,
    compiler_version integer NOT NULL,
    source_digest text NOT NULL,
    dimensions text[] NOT NULL,
    weights numeric[] NOT NULL,
    denominator numeric NOT NULL,
    CONSTRAINT price_program_digest CHECK ((source_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT price_program_shape CHECK (((cardinality(dimensions) = cardinality(weights)) AND (array_position(dimensions, NULL::text) IS NULL) AND (array_position(weights, NULL::numeric) IS NULL) AND (denominator > (0)::numeric) AND (denominator = trunc(denominator)) AND ((0)::numeric <= ALL (weights)))),
    CONSTRAINT price_program_version CHECK ((compiler_version = 1))
);

CREATE TABLE core_funding.writer_groups (
    cell smallint NOT NULL,
    epoch bigint NOT NULL,
    CONSTRAINT writer_groups_cell_check CHECK (((cell >= 0) AND (cell <= 15))),
    CONSTRAINT writer_groups_epoch_check CHECK ((epoch > 0))
);

CREATE TABLE public.administrator_credentials (
    id uuid NOT NULL,
    administrator_id uuid NOT NULL,
    verifier text NOT NULL,
    prefix text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.attempts (
    binding_id uuid,
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    ordinal integer NOT NULL,
    provider_id uuid NOT NULL,
    upstream_model text NOT NULL,
    pricing_id uuid NOT NULL,
    state text NOT NULL,
    provider_request_id text,
    error_code text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    evidence_id uuid,
    metering_conflict boolean DEFAULT false NOT NULL,
    billing_pricing_id uuid,
    CONSTRAINT attempts_error_code_check CHECK ((error_code ~ '^[a-z0-9_]{1,80}$'::text)),
    CONSTRAINT attempts_ordinal_check CHECK ((ordinal > 0)),
    CONSTRAINT attempts_state_check CHECK ((state = ANY (ARRAY['DISPATCHING'::text, 'SUCCEEDED'::text, 'NOT_EXECUTED'::text, 'UNKNOWN'::text, 'PARTIAL'::text, 'FAILED'::text])))
);

CREATE TABLE public.audit_events (
    id uuid NOT NULL,
    action text NOT NULL,
    resource_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    actor text,
    details jsonb
);

CREATE TABLE public.balance_observations (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    channel_id uuid,
    binding_id uuid,
    currency text NOT NULL,
    observed_atoms numeric(38,0),
    observed_at timestamp with time zone NOT NULL,
    source text NOT NULL,
    result text NOT NULL,
    code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT observation_currency CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT observation_result CHECK ((result = ANY (ARRAY['OBSERVED'::text, 'UNSUPPORTED'::text, 'FAILED'::text])))
);

CREATE TABLE public.billing_adjustments (
    id uuid NOT NULL,
    charge_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    target_atoms numeric(38,0) NOT NULL,
    delta_atoms numeric(38,0) NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_adjustments_reason_check CHECK ((reason = ANY (ARRAY['ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text]))),
    CONSTRAINT billing_adjustments_target_atoms_check CHECK ((target_atoms >= (0)::numeric))
);

CREATE TABLE public.billing_charges (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    hold_id uuid NOT NULL,
    pricing_id uuid,
    evidence_id uuid,
    currency text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    funded_atoms numeric(38,0) NOT NULL,
    debt_atoms numeric(38,0) NOT NULL,
    refunded_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    debt_refunded_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    CONSTRAINT billing_charges_amount_atoms_check CHECK ((amount_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_check CHECK (((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms))),
    CONSTRAINT billing_charges_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_debt_refunded_atoms_check CHECK ((debt_refunded_atoms >= (0)::numeric)),
    CONSTRAINT billing_charges_funded_atoms_check CHECK ((funded_atoms >= (0)::numeric)),
    CONSTRAINT charge_value_conserved CHECK (((amount_atoms = ((funded_atoms + debt_atoms) + debt_refunded_atoms)) AND (refunded_atoms >= debt_refunded_atoms) AND ((refunded_atoms - debt_refunded_atoms) <= funded_atoms)))
);

CREATE TABLE public.billing_holds (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    state text DEFAULT 'HELD'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone,
    next_reconcile_at timestamp with time zone DEFAULT now() NOT NULL,
    reconcile_until timestamp with time zone,
    reconcile_token uuid,
    reconcile_reason text,
    credit_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    CONSTRAINT billing_holds_amount_atoms_check CHECK ((amount_atoms >= (0)::numeric)),
    CONSTRAINT billing_holds_check CHECK (((credit_atoms >= (0)::numeric) AND (credit_atoms <= amount_atoms))),
    CONSTRAINT billing_holds_state_check CHECK ((state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text, 'SETTLED'::text, 'RELEASED'::text])))
);

CREATE TABLE public.billing_prices (
    id uuid NOT NULL,
    currency text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb NOT NULL,
    basis text DEFAULT 'PROVIDER_USAGE'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT billing_prices_basis_check CHECK ((basis = ANY (ARRAY['PROVIDER_USAGE'::text, 'INPUT_CHARACTERS'::text, 'REQUEST_COMPLETION'::text]))),
    CONSTRAINT billing_prices_ceilings_check CHECK ((jsonb_typeof(ceilings) = 'object'::text)),
    CONSTRAINT billing_prices_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT billing_prices_rates_check CHECK ((jsonb_typeof(rates) = 'object'::text))
);

CREATE TABLE public.billing_refunds (
    id uuid NOT NULL,
    charge_id uuid NOT NULL,
    external_ref text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'CUSTOMER_REFUND'::text NOT NULL,
    CONSTRAINT billing_refunds_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT billing_refunds_reason_check CHECK ((reason = ANY (ARRAY['CUSTOMER_REFUND'::text, 'ADMIN_CORRECTION'::text, 'PROVIDER_RECONCILIATION'::text])))
);

CREATE TABLE public.billing_resolutions (
    id uuid NOT NULL,
    request_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    action text NOT NULL,
    usage jsonb NOT NULL,
    reason text NOT NULL,
    pricing_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    seq bigint NOT NULL,
    CONSTRAINT billing_resolutions_action_check CHECK ((action = ANY (ARRAY['RELEASE'::text, 'SETTLE'::text]))),
    CONSTRAINT billing_resolutions_reason_check CHECK ((reason = ANY (ARRAY['PROVIDER_INVOICE'::text, 'ADMIN_WRITE_OFF'::text, 'EVIDENCE_RECONCILIATION'::text])))
);

ALTER TABLE public.billing_resolutions ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.billing_resolutions_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE public.billing_tariffs (
    id uuid NOT NULL,
    model text NOT NULL,
    operation text NOT NULL,
    provider_id uuid,
    currency text NOT NULL,
    pricing_id uuid NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    CONSTRAINT billing_tariffs_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text))
);

CREATE TABLE public.channel_plugin_bindings (
    id uuid NOT NULL,
    channel_id uuid NOT NULL,
    plugin_version_id uuid NOT NULL,
    config_revision numeric(38,0) NOT NULL,
    config jsonb NOT NULL,
    config_digest text NOT NULL,
    actor text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    account_id uuid,
    brand_id uuid,
    secret_reference text,
    CONSTRAINT binding_config CHECK (((jsonb_typeof(config) = 'object'::text) AND (config_revision >= (0)::numeric) AND (config_digest ~ '^[a-f0-9]{64}$'::text)))
);

CREATE TABLE public.channels (
    brand_id uuid NOT NULL,
    account_id uuid NOT NULL,
    binding_id uuid,
    config_revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    id uuid NOT NULL,
    name text NOT NULL,
    kind text NOT NULL,
    base_url text NOT NULL,
    secret text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    concurrency integer NOT NULL,
    rpm integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channels_concurrency_check CHECK ((concurrency > 0)),
    CONSTRAINT channels_kind_check CHECK ((kind = ANY (ARRAY['openai'::text, 'anthropic'::text, 'gemini'::text, 'deepseek'::text, 'ollama'::text, 'azure'::text, 'bedrock'::text, 'vertex'::text]))),
    CONSTRAINT channels_rpm_check CHECK ((rpm > 0))
);

CREATE TABLE public.charge_allocations (
    charge_id uuid NOT NULL,
    lot_id uuid NOT NULL,
    ordinal integer NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    refunded_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    CONSTRAINT charge_allocations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT charge_allocations_check CHECK (((refunded_atoms >= (0)::numeric) AND (refunded_atoms <= amount_atoms)))
);

CREATE TABLE public.core_evidence_rejections (
    id uuid NOT NULL,
    batch_id uuid NOT NULL,
    ordinal integer NOT NULL,
    reason text NOT NULL,
    origin_identity text NOT NULL,
    origin_session_id uuid NOT NULL,
    event_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_rejection_shape CHECK (((reason ~ '^[a-z][a-z0-9_]{0,79}$'::text) AND (origin_identity ~ '^gateway:[A-Za-z0-9_-]{1,100}$'::text) AND ((ordinal >= 0) AND (ordinal <= 8191))))
);

CREATE TABLE public.core_resource_actions (
    command_id uuid NOT NULL,
    caller_identity text NOT NULL,
    resource_id text NOT NULL,
    action text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT core_resource_actions_action_check CHECK ((action = ANY (ARRAY['delete'::text, 'cancel'::text])))
);

CREATE TABLE public.upstream_account_balances (
    account_id uuid NOT NULL,
    currency text NOT NULL,
    balance_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    adjustment_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    CONSTRAINT upstream_balance_currency CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT upstream_balance_nonnegative CHECK (((reserved_atoms >= (0)::numeric) AND (spent_atoms >= (0)::numeric) AND (revision >= (0)::numeric)))
);

CREATE VIEW public.core_upstream_balances_v1 AS
 SELECT b.account_id,
    b.currency,
    ((b.balance_atoms + COALESCE(c.balance, (0)::numeric)))::numeric(38,0) AS balance_atoms,
    (COALESCE(c.reserved, (0)::numeric))::numeric(38,0) AS reserved_atoms,
    ((b.spent_atoms + COALESCE(c.spent, (0)::numeric)))::numeric(38,0) AS spent_atoms,
    b.adjustment_atoms,
    ((b.revision + COALESCE(c.revision, (0)::numeric)))::numeric(38,0) AS revision
   FROM (public.upstream_account_balances b
     LEFT JOIN LATERAL ( SELECT sum(c_1.balance_atoms) AS balance,
            sum(c_1.reserved_atoms) AS reserved,
            sum(c_1.spent_atoms) AS spent,
            sum(c_1.revision) AS revision
           FROM core_funding.balance_cells c_1
          WHERE ((c_1.account_id = b.account_id) AND (c_1.currency = b.currency))) c ON (true));

CREATE TABLE public.cost_versions (
    id uuid NOT NULL,
    currency text NOT NULL,
    rates jsonb NOT NULL,
    ceilings jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT cost_versions_ceilings_check CHECK ((jsonb_typeof(ceilings) = 'object'::text)),
    CONSTRAINT cost_versions_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT cost_versions_rates_check CHECK ((jsonb_typeof(rates) = 'object'::text))
);

CREATE TABLE public.event_consumers (
    id uuid NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    replay_cursor bigint DEFAULT 0 NOT NULL,
    replay_until bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.event_deliveries (
    consumer_id uuid NOT NULL,
    event_id uuid NOT NULL,
    state text DEFAULT 'PENDING'::text NOT NULL,
    lease_token uuid,
    lease_until timestamp with time zone,
    attempts integer DEFAULT 0 NOT NULL,
    CONSTRAINT event_deliveries_state_check CHECK ((state = ANY (ARRAY['PENDING'::text, 'LEASED'::text, 'ACKED'::text])))
);

CREATE TABLE public.execution_permits (
    attempt_id uuid NOT NULL,
    command_id uuid NOT NULL,
    gateway_instance_id text NOT NULL,
    incarnation_id uuid NOT NULL,
    permit_epoch numeric(38,0) NOT NULL,
    binding_id uuid NOT NULL,
    dispatched_digest text NOT NULL,
    state text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    granted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    owner_session_id uuid,
    owner_key_sha256 text,
    owner_service text,
    certificate_sha256 text,
    route_id uuid,
    context_id uuid,
    snapshot jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest jsonb DEFAULT '{}'::jsonb NOT NULL,
    manifest_digest text,
    candidate_digest text,
    authorization_plan_digest text,
    prep_session_id uuid,
    prep_binding_digest text,
    CONSTRAINT permit_epoch CHECK (((permit_epoch >= (0)::numeric) AND (dispatched_digest ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT permit_state CHECK ((state = ANY (ARRAY['RESERVED'::text, 'SEND_GRANTED'::text, 'ABORTED'::text, 'UNKNOWN'::text, 'TERMINAL'::text])))
);

CREATE TABLE public.execution_quota_leases (
    lease_id text NOT NULL,
    attempt_id uuid NOT NULL,
    command_id uuid NOT NULL,
    request_id uuid NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    state text NOT NULL,
    revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    descriptor jsonb NOT NULL,
    live_deadline_ms bigint,
    release_pending boolean DEFAULT false NOT NULL,
    CONSTRAINT execution_quota_state CHECK ((state = ANY (ARRAY['ADOPTION_PENDING'::text, 'ACTIVE'::text, 'RELEASE_PENDING'::text, 'RELEASED'::text, 'EXPIRED'::text])))
)
WITH (fillfactor='70');

CREATE TABLE public.financial_vouchers (
    id uuid NOT NULL,
    digest text NOT NULL,
    content_type text NOT NULL,
    content bytea NOT NULL,
    actor text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT voucher_content_digest CHECK ((digest = encode(sha256(content), 'hex'::text))),
    CONSTRAINT voucher_digest CHECK ((digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT voucher_size CHECK (((octet_length(content) > 0) AND (octet_length(content) <= 5242880))),
    CONSTRAINT voucher_type CHECK ((content_type = ANY (ARRAY['image/png'::text, 'image/jpeg'::text, 'application/pdf'::text])))
);

CREATE TABLE public.funding_operations (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    kind text NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    lot_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT funding_operations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric)),
    CONSTRAINT funding_operations_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'REFUND'::text])))
);

CREATE TABLE public.gateway_requests (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    key_id uuid NOT NULL,
    operation text NOT NULL,
    model text NOT NULL,
    intent_digest text,
    fingerprint text NOT NULL,
    state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    billing_currency text,
    billing_quote jsonb,
    selected_attempt_id uuid,
    billing_outcome text DEFAULT 'PENDING'::text NOT NULL,
    execution_budget jsonb,
    execution_closed_at timestamp with time zone,
    CONSTRAINT gateway_requests_billing_outcome_check CHECK ((billing_outcome = ANY (ARRAY['PENDING'::text, 'SUCCESS'::text, 'CANCELLED'::text, 'FAILED'::text, 'UNKNOWN'::text]))),
    CONSTRAINT gateway_requests_state_check CHECK ((state = ANY (ARRAY['ADMITTED'::text, 'RUNNING'::text, 'SUCCEEDED'::text, 'FAILED'::text, 'UNKNOWN'::text, 'CANCELLED'::text])))
)
WITH (fillfactor='70');

CREATE TABLE public.hold_allocations (
    hold_id uuid NOT NULL,
    lot_id uuid NOT NULL,
    ordinal integer NOT NULL,
    amount_atoms numeric(38,0) NOT NULL,
    CONSTRAINT hold_allocations_amount_atoms_check CHECK ((amount_atoms > (0)::numeric))
);

CREATE TABLE public.internal_service_certificates (
    certificate_sha256 text NOT NULL,
    service text NOT NULL,
    instance_id text NOT NULL,
    uri_san text NOT NULL,
    subject_altname_exact text NOT NULL,
    not_before timestamp with time zone NOT NULL,
    not_after timestamp with time zone NOT NULL,
    retired_at timestamp with time zone,
    security_revoked_at timestamp with time zone,
    authorization_revision numeric(38,0) DEFAULT '1'::numeric NOT NULL,
    CONSTRAINT identity_cert_shape CHECK (((certificate_sha256 ~ '^[a-f0-9]{64}$'::text) AND ((length(uri_san) >= 1) AND (length(uri_san) <= 250)) AND ((length(subject_altname_exact) >= 1) AND (length(subject_altname_exact) <= 2000)) AND (not_after > not_before) AND (authorization_revision > (0)::numeric)))
);

CREATE TABLE public.internal_service_instances (
    service text NOT NULL,
    instance_id text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    authorization_revision numeric(38,0) DEFAULT '1'::numeric NOT NULL,
    security_revoked_at timestamp with time zone,
    security_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT identity_instance_security CHECK ((((security_revoked_at IS NULL) AND (security_reason IS NULL)) OR ((security_revoked_at IS NOT NULL) AND (security_reason = 'SECURITY_INCIDENT'::text)))),
    CONSTRAINT identity_instance_shape CHECK (((service = ANY (ARRAY['gateway'::text, 'core-api'::text, 'core-worker'::text])) AND (instance_id ~ '^[A-Za-z0-9_-]{1,100}$'::text) AND (authorization_revision > (0)::numeric)))
);

CREATE TABLE public.internal_startup_registrations (
    challenge_id uuid NOT NULL,
    service text NOT NULL,
    instance_id text NOT NULL,
    registration_id uuid NOT NULL,
    certificate_sha256 text NOT NULL,
    audience text NOT NULL,
    public_key_spki bytea NOT NULL,
    public_key_sha256 text NOT NULL,
    boot_nonce bytea NOT NULL,
    server_nonce bytea NOT NULL,
    registration_digest text NOT NULL,
    challenge_expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    state text DEFAULT 'PENDING'::text NOT NULL,
    session_id uuid,
    receipt jsonb,
    CONSTRAINT identity_registration_shape CHECK (((octet_length(public_key_spki) = 44) AND (encode(sha256(public_key_spki), 'hex'::text) = public_key_sha256) AND (octet_length(boot_nonce) = 32) AND (octet_length(server_nonce) = 32) AND (registration_digest ~ '^[a-f0-9]{64}$'::text) AND (audience ~ '^[A-Za-z0-9_.-]{1,100}/core-api$'::text))),
    CONSTRAINT identity_registration_state CHECK ((((state = 'REGISTERED'::text) AND (session_id IS NOT NULL) AND (receipt IS NOT NULL)) OR ((state = ANY (ARRAY['PENDING'::text, 'EXPIRED'::text])) AND (session_id IS NULL) AND (receipt IS NULL))))
);

CREATE TABLE public.internal_startup_sessions (
    session_id uuid NOT NULL,
    incarnation_id uuid NOT NULL,
    service text NOT NULL,
    instance_id text NOT NULL,
    certificate_sha256 text NOT NULL,
    public_key_spki bytea NOT NULL,
    public_key_sha256 text NOT NULL,
    boot_nonce bytea NOT NULL,
    registered_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    disabled_at timestamp with time zone,
    security_revoked_at timestamp with time zone,
    authorization_revision numeric(38,0) DEFAULT '1'::numeric NOT NULL,
    CONSTRAINT identity_session_shape CHECK (((octet_length(public_key_spki) = 44) AND (encode(sha256(public_key_spki), 'hex'::text) = public_key_sha256) AND (octet_length(boot_nonce) = 32) AND (expires_at > registered_at) AND (authorization_revision > (0)::numeric)))
);

CREATE TABLE public.journal_payloads (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
)
PARTITION BY RANGE (created_at);

CREATE TABLE public.journal_payloads_202611 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);

CREATE TABLE public.journal_payloads_202612 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);

CREATE TABLE public.journal_payloads_202701 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);

CREATE TABLE public.journal_payloads_202702 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);

CREATE TABLE public.journal_payloads_default (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    lines jsonb NOT NULL,
    CONSTRAINT journal_payloads_lines_check CHECK ((jsonb_typeof(lines) = 'array'::text))
);

CREATE TABLE public.models (
    name text NOT NULL,
    operations text[] NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    max_output_tokens integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    routing_strategy text DEFAULT 'WEIGHTED'::text NOT NULL,
    CONSTRAINT models_max_output_tokens_check CHECK ((max_output_tokens > 0)),
    CONSTRAINT models_routing_strategy_check CHECK ((routing_strategy = ANY (ARRAY['WEIGHTED'::text, 'ROUND_ROBIN'::text, 'LEAST_LATENCY'::text])))
);

CREATE TABLE public.money_entries (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    reference text NOT NULL,
    kind text NOT NULL,
    currency text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT money_journal_kind_check CHECK ((kind = ANY (ARRAY['CREDIT'::text, 'HOLD'::text, 'SETTLE'::text, 'RELEASE'::text, 'REFUND'::text, 'EXPIRE'::text, 'DEBT_COLLECTION'::text, 'ADJUSTMENT'::text])))
);

CREATE VIEW public.money_journal AS
 SELECT i.id,
    i.wallet_id,
    i.reference,
    i.kind,
    i.currency,
    p.lines,
    i.created_at
   FROM (public.money_entries i
     JOIN public.journal_payloads p ON (((p.id = i.id) AND (p.created_at = i.created_at))));

CREATE TABLE public.plugin_catalog_receipts (
    command_id uuid NOT NULL,
    receipt jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.plugin_packages (
    digest text NOT NULL,
    package_bytes bytea NOT NULL,
    manifest jsonb NOT NULL,
    signature text NOT NULL,
    signing_key_id text NOT NULL,
    uploaded_by text NOT NULL,
    uploaded_at timestamp with time zone DEFAULT now() NOT NULL,
    release_descriptor jsonb,
    release_signature text,
    release_key_id text,
    CONSTRAINT plugin_digest CHECK ((digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT plugin_manifest CHECK ((jsonb_typeof(manifest) = 'object'::text)),
    CONSTRAINT plugin_package_size CHECK (((octet_length(package_bytes) > 0) AND (octet_length(package_bytes) <= 46137344)))
);

CREATE TABLE public.plugin_publication_receipts (
    id uuid NOT NULL,
    version_id uuid NOT NULL,
    command_id uuid NOT NULL,
    validation_run_id uuid NOT NULL,
    package_digest text NOT NULL,
    result_digest text NOT NULL,
    policy_digest text NOT NULL,
    actor text NOT NULL,
    receipt jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.plugin_validation_runs (
    id uuid NOT NULL,
    version_id uuid NOT NULL,
    command_id uuid NOT NULL,
    package_digest text NOT NULL,
    fixture_digest text NOT NULL,
    policy_digest text NOT NULL,
    runtime_digest text NOT NULL,
    actor text NOT NULL,
    state text NOT NULL,
    lease_owner uuid NOT NULL,
    lease_epoch numeric(38,0) NOT NULL,
    lease_expires_at timestamp with time zone NOT NULL,
    resource_pid integer,
    resource_connection text,
    result jsonb,
    result_digest text,
    error_code text,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    CONSTRAINT plugin_run_digests CHECK (((fixture_digest ~ '^[a-f0-9]{64}$'::text) AND (policy_digest ~ '^[a-f0-9]{64}$'::text) AND (runtime_digest ~ '^[a-f0-9]{64}$'::text) AND ((result_digest IS NULL) OR (result_digest ~ '^[a-f0-9]{64}$'::text)))),
    CONSTRAINT plugin_run_epoch CHECK ((lease_epoch > (0)::numeric)),
    CONSTRAINT plugin_run_result CHECK ((((result IS NULL) OR ((jsonb_typeof(result) = 'object'::text) AND (octet_length((result)::text) <= 4096))) AND ((error_code IS NULL) OR (error_code ~ '^[A-Z][A-Z0-9_]{0,79}$'::text)))),
    CONSTRAINT plugin_run_state CHECK ((state = ANY (ARRAY['PENDING'::text, 'RUNNING'::text, 'CLEANUP_PENDING'::text, 'SUCCEEDED'::text, 'FAILED'::text])))
);

CREATE TABLE public.plugin_versions (
    id uuid NOT NULL,
    name text NOT NULL,
    version text NOT NULL,
    package_digest text NOT NULL,
    abi_version integer NOT NULL,
    config_schema_digest text NOT NULL,
    state text DEFAULT 'UPLOADED'::text NOT NULL,
    quarantined boolean DEFAULT false NOT NULL,
    revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    validation_result jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT plugin_config_digest CHECK ((config_schema_digest ~ '^[a-f0-9]{64}$'::text)),
    CONSTRAINT plugin_version_revision CHECK (((abi_version > 0) AND (revision >= (0)::numeric))),
    CONSTRAINT plugin_version_state CHECK ((state = ANY (ARRAY['UPLOADED'::text, 'VALIDATING'::text, 'VALIDATED'::text, 'PUBLISHED'::text, 'REJECTED'::text, 'DISABLED'::text])))
);

CREATE TABLE public.reconciliations (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    currency text NOT NULL,
    revision numeric(38,0) NOT NULL,
    book_atoms numeric(38,0) NOT NULL,
    observed_atoms numeric(38,0),
    invoice_atoms numeric(38,0),
    difference_atoms numeric(38,0) NOT NULL,
    voucher_id uuid,
    adjustment_id uuid,
    actor text NOT NULL,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reconciliation_revision CHECK (((revision >= (0)::numeric) AND (currency ~ '^[A-Z]{3,8}$'::text)))
);

CREATE TABLE public.routes (
    id uuid NOT NULL,
    request_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    model_name text NOT NULL,
    provider_id uuid NOT NULL,
    upstream_model text NOT NULL,
    operations text[] NOT NULL,
    priority integer NOT NULL,
    weight double precision NOT NULL,
    pricing_id uuid NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    CONSTRAINT routes_request_policy_check CHECK ((jsonb_typeof(request_policy) = 'object'::text)),
    CONSTRAINT routes_weight_check CHECK ((weight > (0)::double precision))
);

CREATE TABLE public.schema_migrations (
    version integer NOT NULL,
    checksum text NOT NULL,
    manifest jsonb NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.supplier_brands (
    id uuid NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.technical_administrators (
    id uuid NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    permissions text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.upstream_accounts (
    id uuid NOT NULL,
    brand_id uuid NOT NULL,
    name text NOT NULL,
    mode text DEFAULT 'PREPAID'::text NOT NULL,
    state text DEFAULT 'ACTIVE'::text NOT NULL,
    revision numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT upstream_account_mode CHECK ((mode = 'PREPAID'::text)),
    CONSTRAINT upstream_account_revision CHECK ((revision >= (0)::numeric)),
    CONSTRAINT upstream_account_state CHECK ((state = ANY (ARRAY['ACTIVE'::text, 'DISABLED'::text, 'RECONCILE_REQUIRED'::text, 'QUARANTINED'::text])))
);

CREATE TABLE public.upstream_audit_events (
    id uuid NOT NULL,
    account_id uuid,
    channel_id uuid,
    actor text NOT NULL,
    action text NOT NULL,
    operation_id uuid,
    revision numeric(38,0),
    old_digest text,
    new_digest text,
    reason text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.upstream_cost_heads (
    attempt_id uuid NOT NULL,
    revision numeric(38,0) NOT NULL
);

CREATE TABLE public.upstream_costs (
    attempt_id uuid NOT NULL,
    revision numeric(38,0) NOT NULL,
    currency text NOT NULL,
    cost_atoms numeric(38,0) NOT NULL,
    complete boolean NOT NULL,
    disputed boolean DEFAULT false NOT NULL,
    evidence_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    evidence_id uuid NOT NULL,
    cost_version_id uuid NOT NULL,
    CONSTRAINT upstream_cost_exact CHECK (((revision >= (1)::numeric) AND (cost_atoms >= (0)::numeric) AND (evidence_digest ~ '^[a-f0-9]{64}$'::text)))
);

CREATE TABLE public.upstream_entries (
    journal_id uuid NOT NULL,
    ordinal integer NOT NULL,
    currency text NOT NULL,
    account text NOT NULL,
    atoms numeric(38,0) NOT NULL,
    CONSTRAINT upstream_entry_ordinal CHECK ((ordinal >= 0))
);

CREATE TABLE public.upstream_funding_operations (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    kind text NOT NULL,
    external_ref text NOT NULL,
    payload_digest text NOT NULL,
    payment_currency text,
    payment_atoms numeric(38,0),
    credited_currency text NOT NULL,
    credited_atoms numeric(38,0) NOT NULL,
    voucher_id uuid,
    voucher_digest text,
    completed_at timestamp with time zone NOT NULL,
    actor text NOT NULL,
    reason text NOT NULL,
    receipt jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT upstream_funding_currency CHECK (((credited_currency ~ '^[A-Z]{3,8}$'::text) AND ((payment_currency IS NULL) OR (payment_currency ~ '^[A-Z]{3,8}$'::text)))),
    CONSTRAINT upstream_funding_kind CHECK ((kind = ANY (ARRAY['RECHARGE'::text, 'ADJUSTMENT'::text, 'TEST_OPENING'::text]))),
    CONSTRAINT upstream_funding_ref CHECK ((((length(external_ref) >= 1) AND (length(external_ref) <= 200)) AND (payload_digest ~ '^[a-f0-9]{64}$'::text))),
    CONSTRAINT upstream_recharge_fact CHECK (((kind <> 'RECHARGE'::text) OR ((payment_atoms IS NOT NULL) AND (payment_atoms > (0)::numeric) AND (credited_atoms > (0)::numeric) AND (voucher_id IS NOT NULL) AND (voucher_digest IS NOT NULL) AND (payment_currency IS NOT NULL))))
);

CREATE TABLE public.upstream_journals (
    id uuid NOT NULL,
    account_id uuid NOT NULL,
    channel_id uuid,
    attempt_id uuid,
    operation_id uuid,
    currency text NOT NULL,
    kind text NOT NULL,
    reference text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT upstream_journal_currency CHECK ((currency ~ '^[A-Z]{3,8}$'::text))
);

CREATE TABLE public.usage_conflicts (
    id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    existing_event_id uuid NOT NULL,
    source text NOT NULL,
    finality text NOT NULL,
    usage jsonb NOT NULL,
    fingerprint text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    incoming_evidence_id uuid,
    disputed_evidence_id uuid,
    incoming_receipt_id uuid,
    CONSTRAINT usage_conflict_core_links CHECK ((((incoming_evidence_id IS NULL) AND (disputed_evidence_id IS NULL) AND (incoming_receipt_id IS NULL)) OR ((incoming_evidence_id IS NOT NULL) AND (disputed_evidence_id IS NOT NULL) AND (incoming_receipt_id IS NOT NULL))))
);

CREATE TABLE public.usage_payloads (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
)
PARTITION BY RANGE (created_at);

CREATE VIEW public.usage_events AS
 SELECT i.id,
    i.seq,
    i.project_id,
    i.request_id,
    i.attempt_id,
    i.source,
    i.finality,
    p.usage,
    i.cost_atoms,
    i.currency,
    i.created_at,
    i.cost_complete,
    i.record_kind,
    i.snapshot_revision,
    i.final_dimensions
   FROM (public.usage_event_index i
     JOIN public.usage_payloads p ON (((p.id = i.id) AND (p.created_at = i.created_at))))
  WHERE (i.record_kind <> 'RAW'::text);

ALTER TABLE public.usage_event_index ALTER COLUMN seq ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.usage_events_seq_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

CREATE TABLE public.usage_payloads_202611 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);

CREATE TABLE public.usage_payloads_202612 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);

CREATE TABLE public.usage_payloads_202701 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);

CREATE TABLE public.usage_payloads_202702 (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);

CREATE TABLE public.usage_payloads_default (
    id uuid NOT NULL,
    created_at timestamp with time zone NOT NULL,
    usage jsonb NOT NULL,
    CONSTRAINT usage_payloads_usage_check CHECK ((jsonb_typeof(usage) = 'object'::text))
);

CREATE TABLE public.wallet_credit_policies (
    id uuid NOT NULL,
    wallet_id uuid NOT NULL,
    external_ref text NOT NULL,
    fingerprint text NOT NULL,
    credit_limit_atoms numeric(38,0) NOT NULL,
    version numeric(38,0) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT wallet_credit_policies_credit_limit_atoms_check CHECK ((credit_limit_atoms >= (0)::numeric)),
    CONSTRAINT wallet_credit_policies_external_ref_check CHECK (((length(external_ref) >= 1) AND (length(external_ref) <= 200))),
    CONSTRAINT wallet_credit_policies_version_check CHECK ((version > (0)::numeric))
);

CREATE TABLE public.wallet_disputes (
    wallet_id uuid NOT NULL,
    request_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.wallets (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    currency text NOT NULL,
    policy text DEFAULT 'GIFT_FIRST'::text NOT NULL,
    available_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    reserved_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    spent_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    expired_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    debt_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    freeze_reason text,
    has_expiring_lots boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    manual_frozen boolean DEFAULT false NOT NULL,
    evidence_disputed boolean DEFAULT false NOT NULL,
    credit_limit_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    credit_reserved_atoms numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    credit_policy_version numeric(38,0) DEFAULT '0'::numeric NOT NULL,
    CONSTRAINT wallets_available_atoms_check CHECK ((available_atoms >= (0)::numeric)),
    CONSTRAINT wallets_credit_limit_atoms_check CHECK ((credit_limit_atoms >= (0)::numeric)),
    CONSTRAINT wallets_credit_policy_version_check CHECK ((credit_policy_version >= (0)::numeric)),
    CONSTRAINT wallets_credit_reserved_atoms_check CHECK ((credit_reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallets_currency_check CHECK ((currency ~ '^[A-Z]{3,8}$'::text)),
    CONSTRAINT wallets_debt_atoms_check CHECK ((debt_atoms >= (0)::numeric)),
    CONSTRAINT wallets_expired_atoms_check CHECK ((expired_atoms >= (0)::numeric)),
    CONSTRAINT wallets_freeze_reason_check CHECK ((freeze_reason = ANY (ARRAY['MANUAL'::text, 'DEBT'::text, 'EVIDENCE_CONFLICT'::text]))),
    CONSTRAINT wallets_policy_check CHECK ((policy = ANY (ARRAY['GIFT_FIRST'::text, 'CASH_FIRST'::text, 'EARLIEST_EXPIRY'::text]))),
    CONSTRAINT wallets_reserved_atoms_check CHECK ((reserved_atoms >= (0)::numeric)),
    CONSTRAINT wallets_spent_atoms_check CHECK ((spent_atoms >= (0)::numeric))
)
WITH (fillfactor='70');

ALTER TABLE ONLY public.journal_payloads ATTACH PARTITION public.journal_payloads_202611 FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

ALTER TABLE ONLY public.journal_payloads ATTACH PARTITION public.journal_payloads_202612 FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

ALTER TABLE ONLY public.journal_payloads ATTACH PARTITION public.journal_payloads_202701 FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');

ALTER TABLE ONLY public.journal_payloads ATTACH PARTITION public.journal_payloads_202702 FOR VALUES FROM ('2027-02-01 00:00:00+00') TO ('2027-03-01 00:00:00+00');

ALTER TABLE ONLY public.journal_payloads ATTACH PARTITION public.journal_payloads_default DEFAULT;

ALTER TABLE ONLY public.usage_payloads ATTACH PARTITION public.usage_payloads_202611 FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

ALTER TABLE ONLY public.usage_payloads ATTACH PARTITION public.usage_payloads_202612 FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

ALTER TABLE ONLY public.usage_payloads ATTACH PARTITION public.usage_payloads_202701 FOR VALUES FROM ('2027-01-01 00:00:00+00') TO ('2027-02-01 00:00:00+00');

ALTER TABLE ONLY public.usage_payloads ATTACH PARTITION public.usage_payloads_202702 FOR VALUES FROM ('2027-02-01 00:00:00+00') TO ('2027-03-01 00:00:00+00');

ALTER TABLE ONLY public.usage_payloads ATTACH PARTITION public.usage_payloads_default DEFAULT;

ALTER TABLE ONLY public.money_journal ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE ONLY public.usage_events ALTER COLUMN created_at SET DEFAULT now();

ALTER TABLE ONLY public.usage_events ALTER COLUMN cost_complete SET DEFAULT false;

ALTER TABLE ONLY core_catalog_cache.candidates
    ADD CONSTRAINT candidates_catalog_revision_scope_digest_pk PRIMARY KEY (catalog_revision, scope_digest);

ALTER TABLE ONLY core_catalog_cache.epoch
    ADD CONSTRAINT epoch_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY core_catalog_cache.usage
    ADD CONSTRAINT usage_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY core_execution_runtime.approvals
    ADD CONSTRAINT approvals_package_digest_kind_artifact_digest_pk PRIMARY KEY (package_digest, kind, artifact_digest);

ALTER TABLE ONLY core_funding.balance_cells
    ADD CONSTRAINT balance_cells_account_id_currency_cell_pk PRIMARY KEY (account_id, currency, cell);

ALTER TABLE ONLY core_funding.billing_prices
    ADD CONSTRAINT billing_prices_pkey PRIMARY KEY (price_id);

ALTER TABLE ONLY core_funding.budget_transfers
    ADD CONSTRAINT budget_transfers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY core_funding.cost_prices
    ADD CONSTRAINT cost_prices_pkey PRIMARY KEY (price_id);

ALTER TABLE ONLY core_funding.budget_transfers
    ADD CONSTRAINT funding_transfer_reference UNIQUE (account_id, currency, reference);

ALTER TABLE ONLY core_funding.writer_groups
    ADD CONSTRAINT writer_groups_pkey PRIMARY KEY (cell);

ALTER TABLE ONLY public.administrator_credentials
    ADD CONSTRAINT administrator_credentials_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.administrator_credentials
    ADD CONSTRAINT administrator_credentials_verifier_unique UNIQUE (verifier);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_verifier_key UNIQUE (verifier);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_request_id_ordinal_key UNIQUE (request_id, ordinal);

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.balance_observations
    ADD CONSTRAINT balance_observations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_charge_id_external_ref_key UNIQUE (charge_id, external_ref);

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_hold_id_key UNIQUE (hold_id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY public.billing_prices
    ADD CONSTRAINT billing_price_currency_unique UNIQUE (id, currency);

ALTER TABLE ONLY public.billing_prices
    ADD CONSTRAINT billing_prices_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_charge_id_external_ref_key UNIQUE (charge_id, external_ref);

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_external_ref_key UNIQUE (request_id, external_ref);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_seq_key UNIQUE (seq);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_model_operation_provider_id_currency_key UNIQUE NULLS NOT DISTINCT (model, operation, provider_id, currency);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.channel_plugin_bindings
    ADD CONSTRAINT channel_plugin_bindings_channel_id_config_revision_unique UNIQUE (channel_id, config_revision);

ALTER TABLE ONLY public.channel_plugin_bindings
    ADD CONSTRAINT channel_plugin_bindings_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_name_key UNIQUE (name);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_pkey PRIMARY KEY (charge_id, lot_id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_evidence_receipts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.core_evidence_rejections
    ADD CONSTRAINT core_evidence_rejections_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_receipt_delivery UNIQUE (event_id, origin_identity, body_digest);

ALTER TABLE ONLY public.core_evidence_rejections
    ADD CONSTRAINT core_rejection_position UNIQUE (batch_id, ordinal);

ALTER TABLE ONLY public.core_resource_actions
    ADD CONSTRAINT core_resource_actions_pkey PRIMARY KEY (command_id);

ALTER TABLE ONLY public.cost_versions
    ADD CONSTRAINT cost_versions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.event_consumers
    ADD CONSTRAINT event_consumers_name_key UNIQUE (name);

ALTER TABLE ONLY public.event_consumers
    ADD CONSTRAINT event_consumers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_pkey PRIMARY KEY (consumer_id, event_id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_kind_caller_identity_command_id_unique UNIQUE (kind, caller_identity, command_id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.execution_contexts
    ADD CONSTRAINT execution_contexts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY public.execution_quota_leases
    ADD CONSTRAINT execution_quota_leases_attempt_id_scope_kind_scope_id_unique UNIQUE (attempt_id, scope_kind, scope_id);

ALTER TABLE ONLY public.execution_quota_leases
    ADD CONSTRAINT execution_quota_leases_pkey PRIMARY KEY (lease_id);

ALTER TABLE ONLY public.financial_vouchers
    ADD CONSTRAINT financial_vouchers_digest_unique UNIQUE (digest);

ALTER TABLE ONLY public.financial_vouchers
    ADD CONSTRAINT financial_vouchers_id_digest_key UNIQUE (id, digest);

ALTER TABLE ONLY public.financial_vouchers
    ADD CONSTRAINT financial_vouchers_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_wallet_id_external_ref_key UNIQUE (wallet_id, external_ref);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_project_id_key_id_operation_intent_digest_key UNIQUE (project_id, key_id, operation, intent_digest);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_project_intent_unique UNIQUE (project_id, intent_digest);

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_pkey PRIMARY KEY (hold_id, lot_id);

ALTER TABLE ONLY public.internal_service_certificates
    ADD CONSTRAINT internal_service_certificates_certificate_sha256_service_instan UNIQUE (certificate_sha256, service, instance_id);

ALTER TABLE ONLY public.internal_service_certificates
    ADD CONSTRAINT internal_service_certificates_pkey PRIMARY KEY (certificate_sha256);

ALTER TABLE ONLY public.internal_service_instances
    ADD CONSTRAINT internal_service_instances_service_instance_id_pk PRIMARY KEY (service, instance_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_pkey PRIMARY KEY (challenge_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_service_instance_id_registration UNIQUE (service, instance_id, registration_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_session_id_unique UNIQUE (session_id);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_incarnation_id_unique UNIQUE (incarnation_id);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_pkey PRIMARY KEY (session_id);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_public_key_sha256_unique UNIQUE (public_key_sha256);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_service_instance_id_boot_nonce_unique UNIQUE (service, instance_id, boot_nonce);

ALTER TABLE ONLY public.money_entries
    ADD CONSTRAINT journal_index_time UNIQUE (id, created_at);

ALTER TABLE ONLY public.journal_payloads
    ADD CONSTRAINT journal_payloads_id_created_at_pk PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.journal_payloads_202611
    ADD CONSTRAINT journal_payloads_202611_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.journal_payloads_202612
    ADD CONSTRAINT journal_payloads_202612_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.journal_payloads_202701
    ADD CONSTRAINT journal_payloads_202701_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.journal_payloads_202702
    ADD CONSTRAINT journal_payloads_202702_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.journal_payloads_default
    ADD CONSTRAINT journal_payloads_default_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_pkey PRIMARY KEY (name);

ALTER TABLE ONLY public.money_entries
    ADD CONSTRAINT money_entries_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.money_entries
    ADD CONSTRAINT money_journal_wallet_id_reference_key UNIQUE (wallet_id, reference);

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_external_ref_key UNIQUE (external_ref);

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.plugin_catalog_receipts
    ADD CONSTRAINT plugin_catalog_receipts_pkey PRIMARY KEY (command_id);

ALTER TABLE ONLY public.plugin_packages
    ADD CONSTRAINT plugin_packages_pkey PRIMARY KEY (digest);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_command_id_unique UNIQUE (command_id);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_version_id_unique UNIQUE (version_id);

ALTER TABLE ONLY public.plugin_validation_runs
    ADD CONSTRAINT plugin_validation_runs_command_id_unique UNIQUE (command_id);

ALTER TABLE ONLY public.plugin_validation_runs
    ADD CONSTRAINT plugin_validation_runs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.plugin_versions
    ADD CONSTRAINT plugin_versions_name_version_unique UNIQUE (name, version);

ALTER TABLE ONLY public.plugin_versions
    ADD CONSTRAINT plugin_versions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_organization_id_external_ref_key UNIQUE (organization_id, external_ref);

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_external_ref_key UNIQUE (external_ref);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.reconciliations
    ADD CONSTRAINT reconciliations_account_id_currency_revision_unique UNIQUE (account_id, currency, revision);

ALTER TABLE ONLY public.reconciliations
    ADD CONSTRAINT reconciliations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_project_id_provider_id_kind_remote_id_key UNIQUE (project_id, provider_id, remote_id, kind);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_binding_unique UNIQUE (model_name, provider_id, upstream_model, operations);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);

ALTER TABLE ONLY public.supplier_brands
    ADD CONSTRAINT supplier_brands_name_unique UNIQUE (name);

ALTER TABLE ONLY public.supplier_brands
    ADD CONSTRAINT supplier_brands_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.technical_administrators
    ADD CONSTRAINT technical_administrators_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.upstream_account_balances
    ADD CONSTRAINT upstream_account_balances_account_id_currency_pk PRIMARY KEY (account_id, currency);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_brand_id_name_unique UNIQUE (brand_id, name);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_id_brand_id_unique UNIQUE (id, brand_id);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.upstream_audit_events
    ADD CONSTRAINT upstream_audit_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.upstream_costs
    ADD CONSTRAINT upstream_cost_evidence UNIQUE (evidence_id);

ALTER TABLE ONLY public.upstream_cost_heads
    ADD CONSTRAINT upstream_cost_heads_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY public.upstream_costs
    ADD CONSTRAINT upstream_costs_attempt_id_revision_pk PRIMARY KEY (attempt_id, revision);

ALTER TABLE ONLY public.upstream_entries
    ADD CONSTRAINT upstream_entries_journal_id_ordinal_pk PRIMARY KEY (journal_id, ordinal);

ALTER TABLE ONLY public.upstream_funding_operations
    ADD CONSTRAINT upstream_funding_operations_account_id_kind_external_ref_unique UNIQUE (account_id, kind, external_ref);

ALTER TABLE ONLY public.upstream_funding_operations
    ADD CONSTRAINT upstream_funding_operations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_pkey PRIMARY KEY (attempt_id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_id_currency_unique UNIQUE (id, currency);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_reference_unique UNIQUE (reference);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_attempt_id_source_fingerprint_key UNIQUE (attempt_id, source, fingerprint);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_event_index_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_events_seq_key UNIQUE (seq);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_index_attempt_identity UNIQUE (id, attempt_id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_index_time UNIQUE (id, created_at);

ALTER TABLE ONLY public.usage_payloads
    ADD CONSTRAINT usage_payloads_id_created_at_pk PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.usage_payloads_202611
    ADD CONSTRAINT usage_payloads_202611_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.usage_payloads_202612
    ADD CONSTRAINT usage_payloads_202612_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.usage_payloads_202701
    ADD CONSTRAINT usage_payloads_202701_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.usage_payloads_202702
    ADD CONSTRAINT usage_payloads_202702_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.usage_payloads_default
    ADD CONSTRAINT usage_payloads_default_pkey PRIMARY KEY (id, created_at);

ALTER TABLE ONLY public.wallet_credit_policies
    ADD CONSTRAINT wallet_credit_policies_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallet_credit_policies
    ADD CONSTRAINT wallet_credit_policies_wallet_id_external_ref_key UNIQUE (wallet_id, external_ref);

ALTER TABLE ONLY public.wallet_credit_policies
    ADD CONSTRAINT wallet_credit_policies_wallet_id_version_key UNIQUE (wallet_id, version);

ALTER TABLE ONLY public.wallet_disputes
    ADD CONSTRAINT wallet_disputes_pkey PRIMARY KEY (wallet_id, request_id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_bucket_digest_key UNIQUE (wallet_id, bucket_digest);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_project_id_currency_key UNIQUE (project_id, currency);

CREATE INDEX api_keys_project ON public.api_keys USING btree (project_id, created_at);

CREATE INDEX attempts_metering_conflict_request_idx ON public.attempts USING btree (request_id) WHERE metering_conflict;

CREATE INDEX attempts_stale ON public.attempts USING btree (started_at) WHERE (state = 'DISPATCHING'::text);

CREATE INDEX billing_holds_wallet ON public.billing_holds USING btree (wallet_id, created_at, id) WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));

CREATE INDEX charges_unpaid ON public.billing_charges USING btree (hold_id, created_at, id) WHERE (debt_atoms > (0)::numeric);

CREATE INDEX core_receipt_attempt_source ON public.core_evidence_receipts USING btree (attempt_id, source, source_revision);

CREATE UNIQUE INDEX core_receipt_event_claim ON public.core_evidence_receipts USING btree (origin_identity, event_id) WHERE event_claim;

CREATE UNIQUE INDEX core_receipt_revision_claim ON public.core_evidence_receipts USING btree (attempt_id, origin_identity, source, source_revision) WHERE revision_claim;

CREATE INDEX core_rejection_origin ON public.core_evidence_rejections USING btree (origin_identity, created_at);

CREATE INDEX delivery_due ON public.event_deliveries USING btree (consumer_id, event_id) WHERE (state <> 'ACKED'::text);

CREATE INDEX execution_quota_live_deadline_idx ON public.execution_quota_leases USING btree (live_deadline_ms, request_id, lease_id) WHERE (live_deadline_ms IS NOT NULL);

CREATE INDEX execution_quota_pending_release_idx ON public.execution_quota_leases USING btree (lease_id) WHERE release_pending;

CREATE INDEX execution_quota_request_idx ON public.execution_quota_leases USING btree (request_id);

CREATE UNIQUE INDEX execution_request_common_quota_unique ON public.execution_quota_leases USING btree (request_id, scope_kind, scope_id) WHERE (scope_kind <> 'channel'::text);

CREATE INDEX holds_all_wallet ON public.billing_holds USING btree (wallet_id, request_id);

CREATE INDEX holds_reconcile_due ON public.billing_holds USING btree (next_reconcile_at, request_id) WHERE (state = ANY (ARRAY['HELD'::text, 'RECONCILING'::text]));

CREATE INDEX identity_cert_instance_idx ON public.internal_service_certificates USING btree (service, instance_id, certificate_sha256);

CREATE INDEX identity_pending_expiry_idx ON public.internal_startup_registrations USING btree (state, challenge_expires_at, challenge_id);

CREATE INDEX identity_session_instance_idx ON public.internal_startup_sessions USING btree (service, instance_id, expires_at, session_id);

CREATE INDEX journal_archive_range ON public.money_entries USING btree (created_at, id);

CREATE INDEX journal_wallet ON public.money_entries USING btree (wallet_id, created_at DESC, id);

CREATE UNIQUE INDEX plugin_one_active_validation ON public.plugin_validation_runs USING btree (version_id) WHERE (state = ANY (ARRAY['PENDING'::text, 'RUNNING'::text, 'CLEANUP_PENDING'::text]));

CREATE INDEX requests_project ON public.gateway_requests USING btree (project_id, created_at DESC, id);

CREATE INDEX resources_poll ON public.resources USING btree (next_poll_at, id) WHERE ((state = ANY (ARRAY['ACTIVE'::text, 'PENDING_USAGE'::text])) AND (kind = ANY (ARRAY['batch'::text, 'video'::text, 'response'::text, 'operation'::text])));

CREATE INDEX resources_project ON public.resources USING btree (project_id, kind, created_at DESC, id);

CREATE INDEX resources_v2_poll ON public.resources USING btree (next_poll_at, id) WHERE ((kind = 'response'::text) AND (binding_id IS NOT NULL) AND (attempt_id IS NOT NULL));

CREATE INDEX routes_lookup ON public.routes USING btree (model_name, priority DESC) WHERE enabled;

CREATE INDEX upstream_funding_voucher_idx ON public.upstream_funding_operations USING btree (voucher_id);

CREATE INDEX upstream_hold_account_state_idx ON public.upstream_holds USING btree (account_id, currency, state, attempt_id);

CREATE INDEX upstream_hold_funding_idx ON public.upstream_holds USING btree (account_id, currency, funding_cell);

CREATE INDEX usage_archive_range ON public.usage_event_index USING btree (created_at, id);

CREATE INDEX usage_attempt_source ON public.usage_event_index USING btree (attempt_id, source, seq);

CREATE INDEX usage_conflicts_attempt ON public.usage_conflicts USING btree (attempt_id);

CREATE UNIQUE INDEX usage_gateway_source_unique ON public.usage_event_index USING btree (attempt_id, source) WHERE (record_kind = 'GATEWAY'::text);

CREATE INDEX usage_project ON public.usage_event_index USING btree (project_id, created_at DESC, id);

CREATE UNIQUE INDEX usage_snapshot_revision_unique ON public.usage_event_index USING btree (attempt_id, snapshot_revision) WHERE (snapshot_revision IS NOT NULL);

CREATE INDEX wallet_lots_expiring ON public.wallet_lots USING btree (expires_at, id) WHERE (kind = 'GIFT'::text);

CREATE INDEX wallet_lots_wallet ON public.wallet_lots USING btree (wallet_id, kind, priority, expires_at, id);

ALTER INDEX public.journal_payloads_id_created_at_pk ATTACH PARTITION public.journal_payloads_202611_pkey;

ALTER INDEX public.journal_payloads_id_created_at_pk ATTACH PARTITION public.journal_payloads_202612_pkey;

ALTER INDEX public.journal_payloads_id_created_at_pk ATTACH PARTITION public.journal_payloads_202701_pkey;

ALTER INDEX public.journal_payloads_id_created_at_pk ATTACH PARTITION public.journal_payloads_202702_pkey;

ALTER INDEX public.journal_payloads_id_created_at_pk ATTACH PARTITION public.journal_payloads_default_pkey;

ALTER INDEX public.usage_payloads_id_created_at_pk ATTACH PARTITION public.usage_payloads_202611_pkey;

ALTER INDEX public.usage_payloads_id_created_at_pk ATTACH PARTITION public.usage_payloads_202612_pkey;

ALTER INDEX public.usage_payloads_id_created_at_pk ATTACH PARTITION public.usage_payloads_202701_pkey;

ALTER INDEX public.usage_payloads_id_created_at_pk ATTACH PARTITION public.usage_payloads_202702_pkey;

ALTER INDEX public.usage_payloads_id_created_at_pk ATTACH PARTITION public.usage_payloads_default_pkey;

CREATE TRIGGER execution_runtime_audit_truncate_v1 AFTER TRUNCATE ON core_execution_runtime.approvals FOR EACH STATEMENT EXECUTE FUNCTION core_execution_runtime.audit_v1();

CREATE TRIGGER execution_runtime_audit_v1 AFTER INSERT OR DELETE OR UPDATE ON core_execution_runtime.approvals FOR EACH ROW EXECUTE FUNCTION core_execution_runtime.audit_v1();

CREATE TRIGGER execution_runtime_epoch_truncate_v1 AFTER TRUNCATE ON core_execution_runtime.approvals FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER execution_runtime_epoch_v1 AFTER INSERT OR DELETE OR UPDATE ON core_execution_runtime.approvals FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER capture_billing_price BEFORE INSERT ON core_funding.billing_prices FOR EACH ROW EXECUTE FUNCTION core_funding.capture_price_v1();

CREATE TRIGGER capture_cost_price BEFORE INSERT ON core_funding.cost_prices FOR EACH ROW EXECUTE FUNCTION core_funding.capture_price_v1();

CREATE CONSTRAINT TRIGGER funding_cell_coverage AFTER INSERT OR UPDATE ON core_funding.balance_cells DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_coverage_v1();

CREATE TRIGGER funding_cell_guard BEFORE INSERT OR DELETE OR UPDATE ON core_funding.balance_cells FOR EACH ROW EXECUTE FUNCTION core_funding.guard_cell_v1();

CREATE TRIGGER immutable_billing_price BEFORE DELETE OR UPDATE ON core_funding.billing_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_budget_transfers BEFORE DELETE OR UPDATE ON core_funding.budget_transfers FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_cost_price BEFORE DELETE OR UPDATE ON core_funding.cost_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.api_keys FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.billing_tariffs FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.channels FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.event_consumers FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.models FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.organizations FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.principals FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.projects FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE ON public.routes FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER audit_config AFTER INSERT OR UPDATE OF policy, manual_frozen ON public.wallets FOR EACH ROW EXECUTE FUNCTION public.audit_configuration();

CREATE TRIGGER authorize_dispatch BEFORE INSERT ON public.gateway_requests FOR EACH ROW EXECUTE FUNCTION public.authorize_gateway_dispatch();

CREATE TRIGGER balanced_journal BEFORE INSERT ON public.journal_payloads FOR EACH ROW EXECUTE FUNCTION public.validate_journal_payload();

CREATE CONSTRAINT TRIGGER balanced_upstream_entries AFTER INSERT ON public.upstream_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.validate_upstream_journal();

CREATE CONSTRAINT TRIGGER balanced_upstream_journal AFTER INSERT ON public.upstream_journals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.validate_upstream_journal();

CREATE TRIGGER billing_resource AFTER UPDATE OF upstream_state ON public.resources FOR EACH ROW WHEN ((old.upstream_state IS DISTINCT FROM new.upstream_state)) EXECUTE FUNCTION public.resource_billing_state();

CREATE TRIGGER catalog_audit_context BEFORE INSERT ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.validate_catalog_audit();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.billing_tariffs FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.channel_plugin_bindings FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.channels FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.models FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.plugin_packages FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.plugin_publication_receipts FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.plugin_versions FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.routes FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.supplier_brands FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR DELETE OR UPDATE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.billing_prices FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.billing_tariffs FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.channel_plugin_bindings FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.channels FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.cost_versions FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.models FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_packages FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_publication_receipts FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_versions FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.routes FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.supplier_brands FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.upstream_accounts FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();

CREATE TRIGGER channel_binding BEFORE INSERT OR UPDATE ON public.channels FOR EACH ROW EXECUTE FUNCTION public.validate_channel_binding();

CREATE TRIGGER compile_billing_price AFTER INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION core_funding.compile_inserted_price_v1();

CREATE TRIGGER compile_cost_price AFTER INSERT ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION core_funding.compile_inserted_price_v1();

CREATE TRIGGER core_context_immutable BEFORE DELETE OR UPDATE ON public.execution_contexts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER core_hold_guard BEFORE INSERT OR DELETE OR UPDATE ON public.upstream_holds FOR EACH ROW EXECUTE FUNCTION public.core_hold_guard_v1();

CREATE TRIGGER core_permit_guard BEFORE DELETE OR UPDATE ON public.execution_permits FOR EACH ROW EXECUTE FUNCTION public.core_permit_guard_v1();

CREATE TRIGGER core_quota_guard BEFORE INSERT OR DELETE OR UPDATE ON public.execution_quota_leases FOR EACH ROW EXECUTE FUNCTION public.core_quota_guard_v1();

CREATE TRIGGER core_request_guard BEFORE UPDATE ON public.gateway_requests FOR EACH ROW EXECUTE FUNCTION public.core_request_guard_v1();

CREATE TRIGGER evidence_lineage BEFORE INSERT ON public.usage_event_index FOR EACH ROW EXECUTE FUNCTION public.validate_evidence_lineage();

CREATE TRIGGER freeze_disputed_wallet AFTER INSERT ON public.wallet_disputes FOR EACH ROW EXECUTE FUNCTION core_funding.freeze_disputed_wallet_v1();

CREATE CONSTRAINT TRIGGER funding_account_coverage AFTER UPDATE ON public.upstream_accounts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_account_trigger_v1();

CREATE TRIGGER funding_account_guard BEFORE DELETE OR UPDATE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION core_funding.guard_account_v1();

CREATE CONSTRAINT TRIGGER funding_root_consistent AFTER INSERT OR UPDATE ON public.upstream_account_balances DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION core_funding.check_root_v1();

CREATE TRIGGER funding_root_guard BEFORE INSERT OR DELETE OR UPDATE ON public.upstream_account_balances FOR EACH ROW EXECUTE FUNCTION core_funding.guard_root_v1();

CREATE TRIGGER identity_certificate_guard BEFORE DELETE OR UPDATE ON public.internal_service_certificates FOR EACH ROW EXECUTE FUNCTION public.internal_identity_guard_v1();

CREATE TRIGGER identity_certificate_notify_v2 AFTER INSERT OR UPDATE ON public.internal_service_certificates FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();

CREATE TRIGGER identity_command_immutable BEFORE DELETE OR UPDATE ON public.execution_commands FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER identity_command_owner BEFORE INSERT ON public.execution_commands FOR EACH ROW EXECUTE FUNCTION public.internal_identity_command_owner_v1();

CREATE TRIGGER identity_instance_guard BEFORE DELETE OR UPDATE ON public.internal_service_instances FOR EACH ROW EXECUTE FUNCTION public.internal_identity_guard_v1();

CREATE TRIGGER identity_instance_notify_v2 AFTER INSERT OR UPDATE ON public.internal_service_instances FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();

CREATE TRIGGER identity_permit_owner BEFORE INSERT OR UPDATE OF owner_session_id, owner_key_sha256, owner_service, certificate_sha256, gateway_instance_id, incarnation_id ON public.execution_permits FOR EACH ROW EXECUTE FUNCTION public.internal_identity_command_owner_v1();

CREATE TRIGGER identity_registration_guard BEFORE DELETE OR UPDATE ON public.internal_startup_registrations FOR EACH ROW EXECUTE FUNCTION public.internal_identity_registration_guard_v1();

CREATE TRIGGER identity_session_guard BEFORE DELETE OR UPDATE ON public.internal_startup_sessions FOR EACH ROW EXECUTE FUNCTION public.internal_identity_guard_v1();

CREATE TRIGGER identity_session_notify_v2 AFTER INSERT OR UPDATE ON public.internal_startup_sessions FOR EACH STATEMENT EXECUTE FUNCTION public.internal_identity_notify_v2();

CREATE TRIGGER immutable_account_identity BEFORE UPDATE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION public.guard_upstream_account_identity();

CREATE TRIGGER immutable_adjustments BEFORE DELETE OR UPDATE ON public.billing_adjustments FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_audit BEFORE DELETE OR UPDATE ON public.audit_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_billing_prices BEFORE DELETE OR UPDATE ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_catalog_receipts BEFORE DELETE OR UPDATE ON public.plugin_catalog_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_conflicts BEFORE DELETE OR UPDATE ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_core_receipts BEFORE DELETE OR UPDATE ON public.core_evidence_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_core_rejections BEFORE DELETE OR UPDATE ON public.core_evidence_rejections FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_credit_policies BEFORE DELETE OR UPDATE ON public.wallet_credit_policies FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_disputes BEFORE DELETE OR UPDATE ON public.wallet_disputes FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_funding BEFORE DELETE OR UPDATE ON public.funding_operations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_journal BEFORE DELETE OR UPDATE ON public.money_entries FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_journal_payload BEFORE DELETE OR UPDATE ON public.journal_payloads FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_journal_view INSTEAD OF DELETE OR UPDATE ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_observations BEFORE DELETE OR UPDATE ON public.balance_observations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_plugin_bindings BEFORE DELETE OR UPDATE ON public.channel_plugin_bindings FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_plugin_identity BEFORE DELETE OR UPDATE ON public.plugin_versions FOR EACH ROW EXECUTE FUNCTION public.guard_plugin_version();

CREATE TRIGGER immutable_plugin_packages BEFORE DELETE OR UPDATE ON public.plugin_packages FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_plugin_publications BEFORE DELETE OR UPDATE ON public.plugin_publication_receipts FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_prices BEFORE DELETE OR UPDATE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_reconciliations BEFORE DELETE OR UPDATE ON public.reconciliations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_refunds BEFORE DELETE OR UPDATE ON public.billing_refunds FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_resolutions BEFORE DELETE OR UPDATE ON public.billing_resolutions FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_upstream_audit BEFORE DELETE OR UPDATE ON public.upstream_audit_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_upstream_costs BEFORE DELETE OR UPDATE ON public.upstream_costs FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_upstream_entries BEFORE DELETE OR UPDATE ON public.upstream_entries FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_upstream_funding BEFORE DELETE OR UPDATE ON public.upstream_funding_operations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_upstream_journals BEFORE DELETE OR UPDATE ON public.upstream_journals FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_usage BEFORE DELETE OR UPDATE ON public.usage_event_index FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_usage_payload BEFORE DELETE OR UPDATE ON public.usage_payloads FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_usage_view INSTEAD OF DELETE OR UPDATE ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER immutable_vouchers BEFORE UPDATE ON public.financial_vouchers FOR EACH ROW EXECUTE FUNCTION public.immutable_row();

CREATE TRIGGER insert_evidence INSTEAD OF INSERT ON public.usage_events FOR EACH ROW EXECUTE FUNCTION public.insert_usage_view();

CREATE TRIGGER insert_journal INSTEAD OF INSERT ON public.money_journal FOR EACH ROW EXECUTE FUNCTION public.insert_journal_view();

CREATE TRIGGER key_principal BEFORE INSERT OR UPDATE ON public.api_keys FOR EACH ROW EXECUTE FUNCTION public.validate_key_principal();

CREATE TRIGGER project_organization BEFORE UPDATE OF organization_id ON public.projects FOR EACH ROW EXECUTE FUNCTION public.validate_project_organization();

CREATE TRIGGER resolution_usage BEFORE INSERT ON public.billing_resolutions FOR EACH ROW EXECUTE FUNCTION public.validate_usage();

CREATE TRIGGER usage_whitelist BEFORE INSERT ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.validate_usage();

CREATE TRIGGER usage_whitelist BEFORE INSERT ON public.usage_payloads FOR EACH ROW EXECUTE FUNCTION public.validate_usage();

CREATE TRIGGER valid_tariff BEFORE INSERT ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION public.validate_billing_price();

CREATE TRIGGER wallet_dispute_fence AFTER INSERT ON public.usage_conflicts FOR EACH ROW EXECUTE FUNCTION public.register_wallet_dispute();

CREATE TRIGGER wallet_send_change AFTER UPDATE ON public.wallets FOR EACH ROW WHEN (((((((old.freeze_reason IS DISTINCT FROM new.freeze_reason) OR (old.evidence_disputed IS DISTINCT FROM new.evidence_disputed)) OR (old.manual_frozen IS DISTINCT FROM new.manual_frozen)) OR (old.debt_atoms IS DISTINCT FROM new.debt_atoms)) OR (old.credit_reserved_atoms IS DISTINCT FROM new.credit_reserved_atoms)) OR (old.credit_limit_atoms IS DISTINCT FROM new.credit_limit_atoms))) EXECUTE FUNCTION core_funding.fence_wallet_send_change_v1();

ALTER TABLE ONLY core_execution_runtime.approvals
    ADD CONSTRAINT approvals_package_digest_plugin_packages_digest_fk FOREIGN KEY (package_digest) REFERENCES public.plugin_packages(digest);

ALTER TABLE ONLY core_funding.balance_cells
    ADD CONSTRAINT balance_cells_account_id_currency_upstream_account_balances_acc FOREIGN KEY (account_id, currency) REFERENCES public.upstream_account_balances(account_id, currency);

ALTER TABLE ONLY core_funding.billing_prices
    ADD CONSTRAINT billing_prices_price_id_billing_prices_id_fk FOREIGN KEY (price_id) REFERENCES public.billing_prices(id);

ALTER TABLE ONLY core_funding.budget_transfers
    ADD CONSTRAINT budget_transfers_account_id_currency_cell_balance_cells_account FOREIGN KEY (account_id, currency, cell) REFERENCES core_funding.balance_cells(account_id, currency, cell);

ALTER TABLE ONLY core_funding.cost_prices
    ADD CONSTRAINT cost_prices_price_id_cost_versions_id_fk FOREIGN KEY (price_id) REFERENCES public.cost_versions(id);

ALTER TABLE ONLY public.administrator_credentials
    ADD CONSTRAINT administrator_credentials_administrator_id_technical_administra FOREIGN KEY (administrator_id) REFERENCES public.technical_administrators(id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_principal_id_fkey FOREIGN KEY (principal_id) REFERENCES public.principals(id);

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_billing_pricing_id_fkey FOREIGN KEY (billing_pricing_id) REFERENCES public.billing_prices(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_binding_id_channel_plugin_bindings_id_fk FOREIGN KEY (binding_id) REFERENCES public.channel_plugin_bindings(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.usage_event_index(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.cost_versions(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.attempts
    ADD CONSTRAINT attempts_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.balance_observations
    ADD CONSTRAINT balance_observations_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.balance_observations
    ADD CONSTRAINT balance_observations_binding_id_channel_plugin_bindings_id_fk FOREIGN KEY (binding_id) REFERENCES public.channel_plugin_bindings(id);

ALTER TABLE ONLY public.balance_observations
    ADD CONSTRAINT balance_observations_channel_id_channels_id_fk FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.billing_adjustments
    ADD CONSTRAINT billing_adjustments_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.usage_event_index(id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_hold_id_fkey FOREIGN KEY (hold_id) REFERENCES public.billing_holds(id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);

ALTER TABLE ONLY public.billing_charges
    ADD CONSTRAINT billing_charges_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.billing_holds
    ADD CONSTRAINT billing_holds_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.billing_refunds
    ADD CONSTRAINT billing_refunds_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);

ALTER TABLE ONLY public.billing_resolutions
    ADD CONSTRAINT billing_resolutions_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_model_fkey FOREIGN KEY (model) REFERENCES public.models(name);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.billing_prices(id);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT billing_tariffs_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channel_account_brand FOREIGN KEY (account_id, brand_id) REFERENCES public.upstream_accounts(id, brand_id);

ALTER TABLE ONLY public.channel_plugin_bindings
    ADD CONSTRAINT channel_plugin_bindings_channel_id_channels_id_fk FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.channel_plugin_bindings
    ADD CONSTRAINT channel_plugin_bindings_plugin_version_id_plugin_versions_id_fk FOREIGN KEY (plugin_version_id) REFERENCES public.plugin_versions(id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_binding_id_channel_plugin_bindings_id_fk FOREIGN KEY (binding_id) REFERENCES public.channel_plugin_bindings(id);

ALTER TABLE ONLY public.channels
    ADD CONSTRAINT channels_brand_id_supplier_brands_id_fk FOREIGN KEY (brand_id) REFERENCES public.supplier_brands(id);

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_charge_id_fkey FOREIGN KEY (charge_id) REFERENCES public.billing_charges(id);

ALTER TABLE ONLY public.charge_allocations
    ADD CONSTRAINT charge_allocations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_evidence_receipts_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_evidence_receipts_origin_session_id_internal_startup_sessi FOREIGN KEY (origin_session_id) REFERENCES public.internal_startup_sessions(session_id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_receipt_canonical FOREIGN KEY (canonical_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_receipt_conflict FOREIGN KEY (conflict_id) REFERENCES public.usage_conflicts(id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_receipt_disputed FOREIGN KEY (disputed_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.core_evidence_receipts
    ADD CONSTRAINT core_receipt_raw FOREIGN KEY (raw_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.core_resource_actions
    ADD CONSTRAINT core_resource_actions_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resources(id);

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_consumer_id_fkey FOREIGN KEY (consumer_id) REFERENCES public.event_consumers(id);

ALTER TABLE ONLY public.event_deliveries
    ADD CONSTRAINT event_deliveries_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.usage_event_index(id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_context_id_execution_contexts_id_fk FOREIGN KEY (context_id) REFERENCES public.execution_contexts(id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_owner_session_id_internal_startup_sessions_s FOREIGN KEY (owner_session_id) REFERENCES public.internal_startup_sessions(session_id);

ALTER TABLE ONLY public.execution_commands
    ADD CONSTRAINT execution_commands_request_id_gateway_requests_id_fk FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.execution_contexts
    ADD CONSTRAINT execution_contexts_key_id_api_keys_id_fk FOREIGN KEY (key_id) REFERENCES public.api_keys(id);

ALTER TABLE ONLY public.execution_contexts
    ADD CONSTRAINT execution_contexts_owner_session_id_internal_startup_sessions_s FOREIGN KEY (owner_session_id) REFERENCES public.internal_startup_sessions(session_id);

ALTER TABLE ONLY public.execution_contexts
    ADD CONSTRAINT execution_contexts_project_id_projects_id_fk FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_binding_id_channel_plugin_bindings_id_fk FOREIGN KEY (binding_id) REFERENCES public.channel_plugin_bindings(id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_command_id_execution_commands_id_fk FOREIGN KEY (command_id) REFERENCES public.execution_commands(id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_context_id_execution_contexts_id_fk FOREIGN KEY (context_id) REFERENCES public.execution_contexts(id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_owner_session_id_internal_startup_sessions_se FOREIGN KEY (owner_session_id) REFERENCES public.internal_startup_sessions(session_id);

ALTER TABLE ONLY public.execution_permits
    ADD CONSTRAINT execution_permits_route_id_routes_id_fk FOREIGN KEY (route_id) REFERENCES public.routes(id);

ALTER TABLE ONLY public.execution_quota_leases
    ADD CONSTRAINT execution_quota_leases_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.execution_quota_leases
    ADD CONSTRAINT execution_quota_leases_command_id_execution_commands_id_fk FOREIGN KEY (command_id) REFERENCES public.execution_commands(id);

ALTER TABLE ONLY public.execution_quota_leases
    ADD CONSTRAINT execution_quota_leases_request_id_gateway_requests_id_fk FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);

ALTER TABLE ONLY public.funding_operations
    ADD CONSTRAINT funding_operations_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.upstream_funding_operations
    ADD CONSTRAINT funding_voucher_digest_fk FOREIGN KEY (voucher_id, voucher_digest) REFERENCES public.financial_vouchers(id, digest);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_key_id_fkey FOREIGN KEY (key_id) REFERENCES public.api_keys(id);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.gateway_requests
    ADD CONSTRAINT gateway_requests_selected_attempt_id_fkey FOREIGN KEY (selected_attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_hold_id_fkey FOREIGN KEY (hold_id) REFERENCES public.billing_holds(id);

ALTER TABLE ONLY public.hold_allocations
    ADD CONSTRAINT hold_allocations_lot_id_fkey FOREIGN KEY (lot_id) REFERENCES public.wallet_lots(id);

ALTER TABLE ONLY public.internal_service_certificates
    ADD CONSTRAINT internal_service_certificates_service_instance_id_internal_serv FOREIGN KEY (service, instance_id) REFERENCES public.internal_service_instances(service, instance_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_certificate_sha256_service_insta FOREIGN KEY (certificate_sha256, service, instance_id) REFERENCES public.internal_service_certificates(certificate_sha256, service, instance_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_service_instance_id_internal_ser FOREIGN KEY (service, instance_id) REFERENCES public.internal_service_instances(service, instance_id);

ALTER TABLE ONLY public.internal_startup_registrations
    ADD CONSTRAINT internal_startup_registrations_session_id_internal_startup_sess FOREIGN KEY (session_id) REFERENCES public.internal_startup_sessions(session_id);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_certificate_sha256_service_instance_i FOREIGN KEY (certificate_sha256, service, instance_id) REFERENCES public.internal_service_certificates(certificate_sha256, service, instance_id);

ALTER TABLE ONLY public.internal_startup_sessions
    ADD CONSTRAINT internal_startup_sessions_service_instance_id_internal_service_ FOREIGN KEY (service, instance_id) REFERENCES public.internal_service_instances(service, instance_id);

ALTER TABLE ONLY public.money_entries
    ADD CONSTRAINT journal_index_payload FOREIGN KEY (id, created_at) REFERENCES public.journal_payloads(id, created_at) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE public.journal_payloads
    ADD CONSTRAINT journal_payloads_id_created_at_fkey FOREIGN KEY (id, created_at) REFERENCES public.money_entries(id, created_at);

ALTER TABLE ONLY public.money_entries
    ADD CONSTRAINT money_journal_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.plugin_catalog_receipts
    ADD CONSTRAINT plugin_catalog_receipts_command_id_execution_commands_id_fk FOREIGN KEY (command_id) REFERENCES public.execution_commands(id);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_command_id_execution_commands_id_fk FOREIGN KEY (command_id) REFERENCES public.execution_commands(id);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_package_digest_plugin_packages_dige FOREIGN KEY (package_digest) REFERENCES public.plugin_packages(digest);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_validation_run_id_plugin_validation FOREIGN KEY (validation_run_id) REFERENCES public.plugin_validation_runs(id);

ALTER TABLE ONLY public.plugin_publication_receipts
    ADD CONSTRAINT plugin_publication_receipts_version_id_plugin_versions_id_fk FOREIGN KEY (version_id) REFERENCES public.plugin_versions(id);

ALTER TABLE ONLY public.plugin_validation_runs
    ADD CONSTRAINT plugin_validation_runs_command_id_execution_commands_id_fk FOREIGN KEY (command_id) REFERENCES public.execution_commands(id);

ALTER TABLE ONLY public.plugin_validation_runs
    ADD CONSTRAINT plugin_validation_runs_package_digest_plugin_packages_digest_fk FOREIGN KEY (package_digest) REFERENCES public.plugin_packages(digest);

ALTER TABLE ONLY public.plugin_validation_runs
    ADD CONSTRAINT plugin_validation_runs_version_id_plugin_versions_id_fk FOREIGN KEY (version_id) REFERENCES public.plugin_versions(id);

ALTER TABLE ONLY public.plugin_versions
    ADD CONSTRAINT plugin_versions_package_digest_plugin_packages_digest_fk FOREIGN KEY (package_digest) REFERENCES public.plugin_packages(digest);

ALTER TABLE ONLY public.principals
    ADD CONSTRAINT principals_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);

ALTER TABLE ONLY public.reconciliations
    ADD CONSTRAINT reconciliations_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.reconciliations
    ADD CONSTRAINT reconciliations_adjustment_id_upstream_funding_operations_id_fk FOREIGN KEY (adjustment_id) REFERENCES public.upstream_funding_operations(id);

ALTER TABLE ONLY public.reconciliations
    ADD CONSTRAINT reconciliations_voucher_id_financial_vouchers_id_fk FOREIGN KEY (voucher_id) REFERENCES public.financial_vouchers(id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_binding_id_channel_plugin_bindings_id_fk FOREIGN KEY (binding_id) REFERENCES public.channel_plugin_bindings(id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_model_name_fkey FOREIGN KEY (model_name) REFERENCES public.models(name);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_pricing_id_fkey FOREIGN KEY (pricing_id) REFERENCES public.cost_versions(id);

ALTER TABLE ONLY public.routes
    ADD CONSTRAINT routes_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.billing_tariffs
    ADD CONSTRAINT tariff_currency_matches FOREIGN KEY (pricing_id, currency) REFERENCES public.billing_prices(id, currency);

ALTER TABLE ONLY public.upstream_account_balances
    ADD CONSTRAINT upstream_account_balances_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.upstream_accounts
    ADD CONSTRAINT upstream_accounts_brand_id_supplier_brands_id_fk FOREIGN KEY (brand_id) REFERENCES public.supplier_brands(id);

ALTER TABLE ONLY public.upstream_audit_events
    ADD CONSTRAINT upstream_audit_events_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.upstream_audit_events
    ADD CONSTRAINT upstream_audit_events_channel_id_channels_id_fk FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.upstream_audit_events
    ADD CONSTRAINT upstream_audit_events_operation_id_upstream_funding_operations_ FOREIGN KEY (operation_id) REFERENCES public.upstream_funding_operations(id);

ALTER TABLE ONLY public.upstream_costs
    ADD CONSTRAINT upstream_cost_evidence_attempt FOREIGN KEY (evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.upstream_cost_heads
    ADD CONSTRAINT upstream_cost_heads_attempt_id_revision_upstream_costs_attempt_ FOREIGN KEY (attempt_id, revision) REFERENCES public.upstream_costs(attempt_id, revision);

ALTER TABLE ONLY public.upstream_costs
    ADD CONSTRAINT upstream_costs_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.upstream_costs
    ADD CONSTRAINT upstream_costs_cost_version_id_cost_versions_id_fk FOREIGN KEY (cost_version_id) REFERENCES public.cost_versions(id);

ALTER TABLE ONLY public.upstream_entries
    ADD CONSTRAINT upstream_entries_journal_id_currency_upstream_journals_id_curre FOREIGN KEY (journal_id, currency) REFERENCES public.upstream_journals(id, currency);

ALTER TABLE ONLY public.upstream_funding_operations
    ADD CONSTRAINT upstream_funding_operations_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.upstream_funding_operations
    ADD CONSTRAINT upstream_funding_operations_voucher_id_financial_vouchers_id_fk FOREIGN KEY (voucher_id) REFERENCES public.financial_vouchers(id);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_account_id_currency_funding_cell_balance_cells_a FOREIGN KEY (account_id, currency, funding_cell) REFERENCES core_funding.balance_cells(account_id, currency, cell);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_account_id_currency_upstream_account_balances_ac FOREIGN KEY (account_id, currency) REFERENCES public.upstream_account_balances(account_id, currency);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_channel_id_channels_id_fk FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.upstream_holds
    ADD CONSTRAINT upstream_holds_cost_version_id_cost_versions_id_fk FOREIGN KEY (cost_version_id) REFERENCES public.cost_versions(id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_account_id_upstream_accounts_id_fk FOREIGN KEY (account_id) REFERENCES public.upstream_accounts(id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_attempt_id_attempts_id_fk FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_channel_id_channels_id_fk FOREIGN KEY (channel_id) REFERENCES public.channels(id);

ALTER TABLE ONLY public.upstream_journals
    ADD CONSTRAINT upstream_journals_operation_id_upstream_funding_operations_id_f FOREIGN KEY (operation_id) REFERENCES public.upstream_funding_operations(id);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflict_disputed FOREIGN KEY (disputed_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflict_incoming FOREIGN KEY (incoming_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflict_incoming_receipt FOREIGN KEY (incoming_receipt_id) REFERENCES public.core_evidence_receipts(id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.usage_conflicts
    ADD CONSTRAINT usage_conflicts_existing_event_id_fkey FOREIGN KEY (existing_event_id) REFERENCES public.usage_event_index(id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_events_attempt_id_fkey FOREIGN KEY (attempt_id) REFERENCES public.attempts(id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_events_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_events_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.gateway_requests(id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_index_payload FOREIGN KEY (id, created_at) REFERENCES public.usage_payloads(id, created_at) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_input_fact FOREIGN KEY (input_fact_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE ONLY public.usage_event_index
    ADD CONSTRAINT usage_parent_evidence FOREIGN KEY (parent_evidence_id, attempt_id) REFERENCES public.usage_event_index(id, attempt_id);

ALTER TABLE public.usage_payloads
    ADD CONSTRAINT usage_payloads_id_created_at_fkey FOREIGN KEY (id, created_at) REFERENCES public.usage_event_index(id, created_at);

ALTER TABLE ONLY public.wallet_credit_policies
    ADD CONSTRAINT wallet_credit_policies_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.wallet_lots
    ADD CONSTRAINT wallet_lots_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id);

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

GRANT USAGE ON SCHEMA public TO core_identity_owner;

REVOKE ALL ON TYPE core_funding.evidence_attempt_stage_v1 FROM PUBLIC;

REVOKE ALL ON TYPE core_funding.evidence_request_stage_v1 FROM PUBLIC;

REVOKE ALL ON TYPE core_funding.evidence_snapshot_stage_v1 FROM PUBLIC;

REVOKE ALL ON TYPE core_funding.first_final_cost_v1 FROM PUBLIC;

REVOKE ALL ON FUNCTION core_catalog_cache.compute_v2(model text, op text, cur text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_catalog_cache.invalidate_v2() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_execution_runtime.allowed_v1(candidates jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_execution_runtime.approve_v1(package text, runtime jsonb, proof jsonb, enabled boolean, who text, why text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_execution_runtime.audit_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.account_key_v1(account uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.apply_v1(attempt uuid, cost_delta numeric, new_reserve numeric, advance boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.audit_v1(account uuid, cur text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.auth_decision_v1(k public.api_keys, pr public.projects, o public.organizations, u public.principals, ip text, op text, model text, asof timestamp with time zone) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.bill_first_final_v1(p_requests uuid[], snapshots core_funding.evidence_snapshot_stage_v1[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.bind_chat_context_v1(p jsonb, d jsonb, route jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.capture_price_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.catalog_held_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.catalog_v1(cache jsonb, model text, op text, cur text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.check_account_trigger_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.check_account_v1(account uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.check_coverage_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.check_root_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.coalesce_groups_v1(groups jsonb, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.collect_v1(account uuid, cur text, needed numeric, exclude_cell integer) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.compile_inserted_price_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.compile_price_v1(rates jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.cost_v1(price uuid, usage jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.customer_authorities_v1(items jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.evidence_attempt_stages_v1(ids uuid[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.evidence_group_v1(g jsonb, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.exclusive_v1(accounts uuid[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.execution_command_receipt_v1(cmd public.execution_commands) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.execution_item_v1(p jsonb, e jsonb, d jsonb, computed jsonb, authority_times jsonb, cache jsonb, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.exposure_v1(price uuid, estimated jsonb, usage jsonb, finals text[], risk jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.fence_wallet_send_change_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_admit_v1(pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_begin_v1(pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_costs_v1(pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_evidence_stages_v1(snapshots core_funding.evidence_snapshot_stage_v1[], receipts public.core_evidence_receipts[], attempts core_funding.evidence_attempt_stage_v1[], requests core_funding.evidence_request_stage_v1[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_first_final_costs_v1(items core_funding.first_final_cost_v1[], pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.flush_v1(pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.freeze_disputed_wallet_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.guard_account_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.guard_cell_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.guard_root_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.headroom_v1(account uuid, cur text, target integer, needed_spent numeric, needed_revision numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.held_v1(account uuid, exclusive boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.make_evidence_snapshot_v1(attempt uuid, project uuid, request_id uuid, currency text, revision numeric, kind text, source text, finality text, usage jsonb, finals text[], parent uuid, input uuid, cost numeric, complete boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.normalize_v1(account uuid, cur text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.offset_debt_v1(account uuid, cur text, needed numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.persist_v1(p_groups jsonb, bulk boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.price_v1(usage jsonb, dimensions text[], weights numeric[], denominator numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.project_v1(h public.upstream_holds, cost_delta numeric, new_reserve numeric, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.quantity_v1(quantity text) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.reserve_batch_v1(account uuid, cur text, attempt uuid, amount numeric, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.reserve_v1(account uuid, cur text, attempt uuid, amount numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.run_execution_v1(items jsonb, time_bounds jsonb[], group_indices integer[], item_errors jsonb[], group_errors jsonb[], bulk boolean, proof_bounds numeric[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.share_v1(accounts uuid[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.supplier_settle_v1(attempt uuid, snapshot uuid, reference text, pending jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION core_funding.transfer_v1(account uuid, cur text, target_cell integer, balance_delta numeric, spent_delta numeric, revision_delta numeric, ref text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.adjust_billing_charge(p_charge uuid, p_target numeric, p_ref text, p_reason text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.allocate_customer_batch_v1(p_inputs jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.allocate_customer_rows_v1(p_inputs jsonb, bind_execution boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.audit_configuration() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.authorize_gateway_dispatch() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.collect_wallet_debt(p_wallet uuid, p_limit integer) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.configure_wallet_credit(p_project uuid, p_currency text, p_ref text, p_version numeric, p_limit numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_abort_recovery_slow_v1(p jsonb, e jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_abort_recovery_v1(p jsonb, e jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_account_auth_v1(proof text, ip text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_auth_v1(kid uuid, ip text, op text, model text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_batch_files_v1(r public.resources, remotes jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_caller_projection_v2(p jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_caller_v1(p jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_candidates_v1(model text, op text, cur text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_canonical_v1(v jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_catalog_write_v1() FROM PUBLIC;
GRANT ALL ON FUNCTION public.core_catalog_write_v1() TO pg_write_all_data;

REVOKE ALL ON FUNCTION public.core_context_v1(p jsonb, id uuid, input jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_digest_v1(v jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_evidence_snapshot_v1(attempt uuid, kind text, source text, finality text, usage jsonb, finals text[], parent uuid, input uuid, cost numeric, complete boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_evidence_validate_v1(ev jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_execution_batch_v1(items jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_execution_slow_v1(item jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_funding_control_v1(account uuid, catalog boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_hold_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_lookup_v1(p jsonb, e jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_permit_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_platform_authorize_v1(proof jsonb, permission text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_plugin_package_v1(p jsonb, package_digest text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_preflight_v1(p jsonb, context_id uuid, attempt_id uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_quota_close_v1(p jsonb, e jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_quota_expire_v1(batch integer, after_key jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_quota_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_quota_recovery_v1(p jsonb, items jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_quota_v1(p jsonb, attempt uuid, action text, items jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_reconcile_holds_v1(batch integer) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_request_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_claim_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_commit_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_key_v1(verifier text, ip text, op text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_lease_v1(p jsonb, d jsonb, live boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_list_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_operation_v1(kind text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_permit_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_poll_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_release_v1(p jsonb, d jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_resource_resolution_v1(r public.resources, deadline timestamp with time zone) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_route_authority_v1(p jsonb, credential text, client_ip text, chat_model text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_short_chat_funding_v1(group_id smallint, expected_epoch bigint) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_supplier_risk_v1(attempt uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_upstream_fund_v1(account uuid, cur text, delta numeric, adjustment boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_upstream_journal_v1(account uuid, cur text, channel uuid, attempt uuid, kind text, amount numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_upstream_normalize_v1(account uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.core_utf16_sort_v1(value text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.credit_wallet(p_project uuid, p_currency text, p_ref text, p_atoms numeric, p_kind text, p_priority integer, p_expiry timestamp with time zone, p_models text[], p_operations text[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.exact_billing_price(p_usage jsonb, p_rates jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.expire_wallet(p_wallet uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.guard_plugin_version() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.guard_upstream_account_identity() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.immutable_row() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.insert_journal_view() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.insert_usage_view() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_challenge_v1(p jsonb, reg uuid, spki bytea, boot bytea, aud text, digest text, nonce bytea, ttl integer) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_change_v1(svc text, inst text, hash text, sid uuid, action text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_command_owner_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_current_v1(p jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_locks_v1(keys text[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_notify_v2() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_peer_v1(p jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_projection_v1(p jsonb, endpoint text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_provision_v1(svc text, inst text, hash text, san text, exact_san text, valid_from timestamp with time zone, valid_until timestamp with time zone) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_read_peer_v1(hash text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_read_registration_v1(id uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_read_session_v1(id uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_register_v1(p jsonb, id uuid, reg uuid, expected_digest text, ttl integer) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.internal_identity_registration_guard_v1() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.ledger_uuid() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.money_leg(p_account text, p_atoms numeric) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.persist_core_evidence_groups_v1(p_groups jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.persist_core_evidence_slow_v1(g jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.plugin_catalog_command_v1(p_kind text, p_actor text, p_command uuid, p_body jsonb, p_owner uuid, p_trust_expires_at timestamp with time zone, p_credential uuid, p_administrator uuid, p_verifier text, p_bootstrap boolean) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.plugin_catalog_receipt_v1(p_kind text, p_actor text, p_command uuid, p_body jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.post_money_journals(p_items jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.prepare_evidence_month(p_month date) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.refund_billing_charge(p_charge uuid, p_atoms numeric, p_ref text, p_reason text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.register_wallet_dispute() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.resource_billing_state() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.settle_covered_requests(p_requests uuid[]) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.settle_requests(p_requests uuid[], p_token uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_billing_price() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_catalog_audit() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_channel_binding() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_evidence_lineage() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_journal_payload() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_key_principal() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_money_journal() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_money_lines(p_wallet uuid, p_currency text, p_lines jsonb) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_project_organization() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_upstream_journal() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.validate_usage() FROM PUBLIC;

REVOKE ALL ON FUNCTION public.verify_platform_credential_v1(p_verifier text) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.wake_wallet_debt(p_wallet uuid) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.wallet_available_lots(p_wallet uuid, p_model text, p_operation text, p_asof timestamp with time zone) FROM PUBLIC;

REVOKE ALL ON FUNCTION public.wallet_freeze_reason(p_disputed boolean, p_manual boolean, p_debt numeric, p_credit_reserved numeric, p_limit numeric) FROM PUBLIC;

GRANT INSERT ON TABLE public.audit_events TO core_identity_owner;

GRANT SELECT ON TABLE public.core_upstream_balances_v1 TO pg_read_all_data;

GRANT SELECT,INSERT,UPDATE ON TABLE public.internal_service_certificates TO core_identity_owner;

GRANT SELECT,INSERT,UPDATE ON TABLE public.internal_service_instances TO core_identity_owner;

GRANT SELECT,INSERT,UPDATE ON TABLE public.internal_startup_registrations TO core_identity_owner;

GRANT SELECT,INSERT,UPDATE ON TABLE public.internal_startup_sessions TO core_identity_owner;

INSERT INTO core_catalog_cache.epoch VALUES (true, 1);

INSERT INTO core_catalog_cache.usage VALUES (true, 0, 0);

INSERT INTO core_funding.writer_groups VALUES (0, 1);
INSERT INTO core_funding.writer_groups VALUES (1, 1);
INSERT INTO core_funding.writer_groups VALUES (2, 1);
INSERT INTO core_funding.writer_groups VALUES (3, 1);
INSERT INTO core_funding.writer_groups VALUES (4, 1);
INSERT INTO core_funding.writer_groups VALUES (5, 1);
INSERT INTO core_funding.writer_groups VALUES (6, 1);
INSERT INTO core_funding.writer_groups VALUES (7, 1);
INSERT INTO core_funding.writer_groups VALUES (8, 1);
INSERT INTO core_funding.writer_groups VALUES (9, 1);
INSERT INTO core_funding.writer_groups VALUES (10, 1);
INSERT INTO core_funding.writer_groups VALUES (11, 1);
INSERT INTO core_funding.writer_groups VALUES (12, 1);
INSERT INTO core_funding.writer_groups VALUES (13, 1);
INSERT INTO core_funding.writer_groups VALUES (14, 1);
INSERT INTO core_funding.writer_groups VALUES (15, 1);

SELECT pg_catalog.setval('public.billing_resolutions_seq_seq', 1, false);

SELECT pg_catalog.setval('public.usage_events_seq_seq', 1, false);

SET check_function_bodies = true;
