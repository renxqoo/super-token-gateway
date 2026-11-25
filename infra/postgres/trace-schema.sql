CREATE SCHEMA IF NOT EXISTS telemetry;

CREATE TABLE IF NOT EXISTS telemetry.spans (
  trace_id TEXT NOT NULL CHECK (trace_id ~ '^[0-9a-f]{32}$' AND trace_id <> repeat('0', 32)),
  span_id TEXT NOT NULL CHECK (span_id ~ '^[0-9a-f]{16}$' AND span_id <> repeat('0', 16)),
  request_id UUID NOT NULL,
  billing_account_id UUID,
  source TEXT NOT NULL CHECK (source IN ('gateway', 'client')),
  started_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  span JSONB NOT NULL,
  PRIMARY KEY (trace_id, span_id)
);

SELECT billing_account_id FROM telemetry.spans LIMIT 0;

CREATE INDEX IF NOT EXISTS telemetry_spans_request_idx ON telemetry.spans (request_id, started_at);
CREATE INDEX IF NOT EXISTS telemetry_spans_expires_idx ON telemetry.spans (expires_at);
