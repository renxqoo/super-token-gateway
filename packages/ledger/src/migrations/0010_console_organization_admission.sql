CREATE TABLE public.console_organization_admission (
  organization_id UUID PRIMARY KEY,
  suspended BOOLEAN NOT NULL,
  source_revision INTEGER NOT NULL CHECK (source_revision > 0)
);
