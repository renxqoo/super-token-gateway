ALTER TABLE public.plugin_releases RENAME TO config_releases;
ALTER TABLE public.plugin_release_acks RENAME TO config_release_acks;

ALTER TABLE public.config_releases
    ADD COLUMN kind text NOT NULL DEFAULT 'PLUGIN',
    ADD COLUMN model_name text,
    ADD CONSTRAINT config_releases_scope_check CHECK (
        (kind = 'PLUGIN' AND model_name IS NULL)
        OR (kind = 'ROUTE' AND model_name IS NOT NULL)
    );

ALTER TABLE public.routes
    ADD COLUMN managed_by_release boolean NOT NULL DEFAULT false;
