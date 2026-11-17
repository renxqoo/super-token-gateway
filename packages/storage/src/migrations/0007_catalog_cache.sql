CREATE SCHEMA "core_catalog_cache";
CREATE TABLE "core_catalog_cache"."usage" (
	"singleton" boolean PRIMARY KEY DEFAULT true NOT NULL,
	"byte_count" integer DEFAULT 0 NOT NULL,
	"entry_count" integer DEFAULT 0 NOT NULL,
	CONSTRAINT "catalog_cache_usage_singleton" CHECK (singleton IS TRUE),
	CONSTRAINT "catalog_cache_usage_bounds" CHECK (byte_count BETWEEN 0 AND 67108864 AND entry_count BETWEEN 0 AND 4096)
);
CREATE TABLE "core_catalog_cache"."candidates" (
	"catalog_revision" numeric(38, 0) NOT NULL,
	"scope_digest" "bytea" NOT NULL,
	"model" text NOT NULL,
	"operation" text NOT NULL,
	"currency" text NOT NULL,
	"candidates" jsonb NOT NULL,
	"payload_bytes" integer NOT NULL,
	CONSTRAINT "candidates_catalog_revision_scope_digest_pk" PRIMARY KEY("catalog_revision","scope_digest"),
	CONSTRAINT "catalog_cache_identity" CHECK (catalog_revision>0 AND octet_length(scope_digest)=32),
	CONSTRAINT "catalog_cache_payload" CHECK (jsonb_typeof(candidates)='array' AND jsonb_array_length(candidates)<=128 AND payload_bytes BETWEEN 1 AND 67108864)
);
CREATE TABLE "core_catalog_cache"."epoch" (
	"singleton" boolean PRIMARY KEY DEFAULT true NOT NULL,
	"revision" numeric(38, 0) DEFAULT '1' NOT NULL,
	CONSTRAINT "catalog_epoch_singleton" CHECK (singleton IS TRUE),
	CONSTRAINT "catalog_epoch_revision" CHECK (revision>0)
);
