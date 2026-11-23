ALTER TABLE public.projects
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.api_keys
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.upstream_accounts
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.channels
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.models
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.routes
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);

ALTER TABLE public.project_pricing_overrides
  ADD COLUMN IF NOT EXISTS source_revision INTEGER NOT NULL DEFAULT 0 CHECK (source_revision >= 0);
