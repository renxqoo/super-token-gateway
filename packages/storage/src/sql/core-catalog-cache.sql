REVOKE ALL ON SCHEMA core_catalog_cache FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core_catalog_cache FROM PUBLIC;
DO $$ DECLARE r record;BEGIN
 FOR r IN SELECT DISTINCT grantee FROM pg_namespace n CROSS JOIN LATERAL aclexplode(n.nspacl) a WHERE n.nspname='core_catalog_cache' AND grantee<>0 AND grantee<>n.nspowner LOOP
  EXECUTE format('REVOKE ALL ON SCHEMA core_catalog_cache FROM %I',pg_get_userbyid(r.grantee));
 END LOOP;
END $$;
INSERT INTO core_catalog_cache.epoch(singleton,revision) VALUES(true,1);
INSERT INTO core_catalog_cache.usage(singleton,byte_count,entry_count) VALUES(true,0,0);
CREATE FUNCTION core_catalog_cache.compute_v2(model text,op text,cur text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
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
END $$;

REVOKE ALL ON FUNCTION core_catalog_cache.compute_v2(text,text,text) FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.core_candidates_v1(model text,op text,cur text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
DECLARE current_revision numeric;scope bytea;result jsonb;payload_size integer;used_bytes integer;used_entries integer;BEGIN
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
REVOKE ALL ON FUNCTION public.core_candidates_v1(text,text,text) FROM PUBLIC;
CREATE FUNCTION core_catalog_cache.invalidate_v2() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
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
REVOKE ALL ON FUNCTION core_catalog_cache.invalidate_v2() FROM PUBLIC;
DO $$ DECLARE candidate_owner name;BEGIN
 SELECT pg_get_userbyid(proowner) INTO STRICT candidate_owner FROM pg_proc WHERE oid='public.core_candidates_v1(text,text,text)'::regprocedure;
 EXECUTE format('GRANT USAGE,CREATE ON SCHEMA core_catalog_cache TO %I',candidate_owner);
 EXECUTE format('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA core_catalog_cache TO %I',candidate_owner);
 EXECUTE format('ALTER FUNCTION core_catalog_cache.compute_v2(text,text,text) OWNER TO %I',candidate_owner);
 EXECUTE format('ALTER FUNCTION core_catalog_cache.invalidate_v2() OWNER TO %I',candidate_owner);
END $$;
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.models FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.models FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.routes FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.routes FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.channels FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.channels FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.channel_plugin_bindings FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.channel_plugin_bindings FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.plugin_versions FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_versions FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.billing_tariffs FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.billing_tariffs FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.supplier_brands FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.supplier_brands FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.upstream_accounts FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.cost_versions FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.cost_versions FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.billing_prices FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.billing_prices FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.plugin_packages FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_packages FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_change_v2 AFTER INSERT OR UPDATE OR DELETE ON public.plugin_publication_receipts FOR EACH ROW EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
CREATE TRIGGER catalog_cache_truncate_v2 AFTER TRUNCATE ON public.plugin_publication_receipts FOR EACH STATEMENT EXECUTE FUNCTION core_catalog_cache.invalidate_v2();
