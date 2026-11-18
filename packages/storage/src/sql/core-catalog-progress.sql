CREATE FUNCTION public.core_catalog_write_v1() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended('stg.catalog.control.v1',0));
END $$;
REVOKE ALL ON FUNCTION public.core_catalog_write_v1() FROM PUBLIC;
DO $$ DECLARE writer name;BEGIN
 FOR writer IN SELECT rolname FROM pg_roles WHERE has_any_column_privilege(oid,'public.routes','INSERT') OR has_any_column_privilege(oid,'public.routes','UPDATE') LOOP
  EXECUTE format('GRANT EXECUTE ON FUNCTION public.core_catalog_write_v1() TO %I',writer);
 END LOOP;
END $$;
CREATE OR REPLACE FUNCTION public.core_candidates_v1(model text,op text,cur text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $$
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
REVOKE ALL ON FUNCTION public.core_candidates_v1(text,text,text) FROM PUBLIC;
