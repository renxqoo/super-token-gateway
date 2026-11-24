ALTER TABLE public.channels
  ADD COLUMN secret_sync_pending BOOLEAN NOT NULL DEFAULT false;

UPDATE public.channels
SET secret_sync_pending = true
WHERE sync_status <> 'SYNCED' OR desired_revision > synced_revision;
