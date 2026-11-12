CREATE FUNCTION public.validate_upstream_journal() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
DECLARE target uuid; total numeric; lines integer;
BEGIN
 IF TG_TABLE_NAME='upstream_entries' THEN target=NEW.journal_id; ELSE target=NEW.id; END IF;
 SELECT count(*),sum(atoms) INTO lines,total FROM upstream_entries WHERE journal_id=target;
 IF lines<2 OR total<>0 THEN RAISE EXCEPTION 'unbalanced upstream journal' USING ERRCODE='P0422';END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER balanced_upstream_journal AFTER INSERT ON public.upstream_journals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.validate_upstream_journal();
CREATE CONSTRAINT TRIGGER balanced_upstream_entries AFTER INSERT ON public.upstream_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.validate_upstream_journal();
CREATE FUNCTION public.guard_upstream_account_identity() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
BEGIN IF NEW.brand_id<>OLD.brand_id OR NEW.id<>OLD.id THEN RAISE EXCEPTION 'upstream account identity is immutable' USING ERRCODE='P0409';END IF;RETURN NEW;END $$;
CREATE TRIGGER immutable_account_identity BEFORE UPDATE ON public.upstream_accounts FOR EACH ROW EXECUTE FUNCTION public.guard_upstream_account_identity();
CREATE FUNCTION public.validate_channel_binding() RETURNS trigger LANGUAGE plpgsql SET search_path TO 'pg_catalog','public','pg_temp' AS $$
BEGIN
 IF NEW.binding_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM channel_plugin_bindings WHERE id=NEW.binding_id AND channel_id=NEW.id AND config_revision=NEW.config_revision) THEN RAISE EXCEPTION 'channel binding mismatch' USING ERRCODE='P0422';END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER channel_binding BEFORE INSERT OR UPDATE ON public.channels FOR EACH ROW EXECUTE FUNCTION public.validate_channel_binding();
CREATE TRIGGER immutable_plugin_packages BEFORE UPDATE OR DELETE ON public.plugin_packages FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_plugin_bindings BEFORE UPDATE OR DELETE ON public.channel_plugin_bindings FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_upstream_funding BEFORE UPDATE OR DELETE ON public.upstream_funding_operations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_upstream_costs BEFORE UPDATE OR DELETE ON public.upstream_costs FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_upstream_journals BEFORE UPDATE OR DELETE ON public.upstream_journals FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_upstream_entries BEFORE UPDATE OR DELETE ON public.upstream_entries FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_observations BEFORE UPDATE OR DELETE ON public.balance_observations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_reconciliations BEFORE UPDATE OR DELETE ON public.reconciliations FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_upstream_audit BEFORE UPDATE OR DELETE ON public.upstream_audit_events FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
CREATE TRIGGER immutable_vouchers BEFORE UPDATE ON public.financial_vouchers FOR EACH ROW EXECUTE FUNCTION public.immutable_row();
REVOKE ALL ON FUNCTION public.validate_upstream_journal(),public.guard_upstream_account_identity(),public.validate_channel_binding() FROM PUBLIC;
