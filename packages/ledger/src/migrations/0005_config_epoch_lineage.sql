ALTER TABLE public.config_epoch
  ADD COLUMN IF NOT EXISTS lineage_token UUID NOT NULL DEFAULT gen_random_uuid();

CREATE OR REPLACE FUNCTION public.refresh_config_epoch_lineage() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.version IS DISTINCT FROM OLD.version AND NEW.lineage_token IS NOT DISTINCT FROM OLD.lineage_token THEN
    NEW.lineage_token := gen_random_uuid();
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS config_epoch_refresh_lineage ON public.config_epoch;
CREATE TRIGGER config_epoch_refresh_lineage
BEFORE UPDATE ON public.config_epoch
FOR EACH ROW EXECUTE FUNCTION public.refresh_config_epoch_lineage();
