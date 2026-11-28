CREATE TABLE public.route_release_requests (
    id uuid PRIMARY KEY,
    model_name text NOT NULL,
    actor_user_id uuid NOT NULL,
    actor_email text NOT NULL,
    digest text NOT NULL,
    intent jsonb NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING', 'COMMITTED', 'REJECTED')),
    next_attempt_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX route_release_requests_pending_idx
    ON public.route_release_requests (next_attempt_at, id) WHERE state = 'PENDING';

ALTER TABLE public.routes
    ADD COLUMN pending_release_id uuid REFERENCES public.route_release_requests (id);
