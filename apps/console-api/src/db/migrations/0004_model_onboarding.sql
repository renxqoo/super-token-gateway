ALTER TABLE public.model_catalog
    ADD COLUMN pending_release_id uuid REFERENCES public.route_release_requests (id);

ALTER TABLE public.route_release_requests
    ADD COLUMN rejection jsonb;
