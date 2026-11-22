ALTER TABLE public.plugins ALTER COLUMN enabled SET DEFAULT false;
ALTER TABLE public.plugin_releases ADD COLUMN expected_nodes uuid[] NOT NULL DEFAULT '{}';
ALTER TABLE public.plugin_releases ADD CONSTRAINT plugin_releases_config_epoch_key UNIQUE (config_epoch);

CREATE TABLE public.plugin_revocations (
  plugin_id uuid PRIMARY KEY REFERENCES public.plugins(id),
  actor text NOT NULL,
  reason text NOT NULL,
  config_epoch bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE public.gateway_config_nodes (
  id uuid PRIMARY KEY,
  applied_epoch bigint NOT NULL,
  last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  stopped_at timestamptz
);

CREATE TABLE public.plugin_release_acks (
  config_epoch bigint NOT NULL REFERENCES public.plugin_releases(config_epoch),
  node_id uuid NOT NULL REFERENCES public.gateway_config_nodes(id),
  applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (config_epoch, node_id)
);

CREATE FUNCTION public.reject_revoked_plugin_activation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF NEW.enabled AND EXISTS (SELECT 1 FROM public.plugin_revocations WHERE plugin_id = NEW.id) THEN
    RAISE EXCEPTION 'Revoked plugin revisions cannot be enabled' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER plugins_reject_revoked_activation
BEFORE UPDATE OF enabled ON public.plugins
FOR EACH ROW EXECUTE FUNCTION public.reject_revoked_plugin_activation();
