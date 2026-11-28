ALTER TABLE public.config_releases
    DROP CONSTRAINT config_releases_scope_check,
    ADD CONSTRAINT config_releases_scope_check CHECK (
        (kind = 'PLUGIN' AND model_name IS NULL)
        OR (kind IN ('ROUTE', 'MODEL') AND model_name IS NOT NULL)
    );

ALTER TABLE public.models
    ADD COLUMN managed_by_release boolean NOT NULL DEFAULT false;
