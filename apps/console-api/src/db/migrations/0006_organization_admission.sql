ALTER TABLE public.organizations
  ADD COLUMN inference_target_status TEXT NOT NULL DEFAULT 'ACTIVE'
    CHECK (inference_target_status IN ('ACTIVE', 'SUSPENDED')),
  ADD COLUMN inference_revision INTEGER NOT NULL DEFAULT 0 CHECK (inference_revision >= 0),
  ADD COLUMN inference_synced_revision INTEGER NOT NULL DEFAULT 0
    CHECK (inference_synced_revision >= 0 AND inference_synced_revision <= inference_revision);

UPDATE public.organizations SET inference_target_status = 'SUSPENDED', inference_revision = 1
  WHERE status = 'SUSPENDED';

CREATE INDEX organizations_inference_pending_idx ON public.organizations (updated_at, id)
  WHERE inference_synced_revision < inference_revision;
