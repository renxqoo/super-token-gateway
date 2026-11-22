ALTER TABLE public.plugins ADD COLUMN builtin BOOLEAN NOT NULL DEFAULT false;

INSERT INTO public.plugins (id, name, version, digest, provider_config, capability, kind, builtin, created_at)
VALUES
  ('4f08eb09-7967-5719-8323-10e3d951253b', 'openai', 'builtin:1.0.0', '4f08eb0979677719f32310e3d951253b7958379ae928be35398491f0d7634e77', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"},"responses":{"path":"/v1/responses"}},"include_stream_usage_option":true}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('3600d2ef-af02-5c5a-82ba-510820ee5906', 'openai_compatible', 'builtin:1.0.0', '3600d2efaf028c5ad2ba510820ee59068e78ccad23a4e73398a1720b8ce6b430', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}},"include_stream_usage_option":true}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('6ca156f2-6757-5490-8efd-90aa65c438fe', 'anthropic', 'builtin:1.0.0', '6ca156f267572490defd90aa65c438fe1b77cd9ea5a9e6c039762c401723a9e5', '{"base_dialect":"anthropic_messages","usage_dialect":"anthropic","stream_framing":"sse","auth":{"scheme":"header","header_name":"x-api-key"},"extra_headers":{"anthropic-version":"2023-06-01"},"endpoints":{"messages":{"path":"/v1/messages"},"chat":{"path":"/v1/messages"},"responses":{"path":"/v1/messages"},"gemini":{"path":"/v1/messages"},"default":{"path":"/v1/messages"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('db2b9ec8-ec60-5fca-8a06-5890e0070e8d', 'gemini', 'builtin:1.0.0', 'db2b9ec8ec60efca5a065890e0070e8deaf4d954aa4654bd156e1f3409e095be', '{"base_dialect":"gemini","usage_dialect":"gemini","stream_framing":"sse","auth":{"scheme":"header","header_name":"x-goog-api-key"},"model_field":null,"stream_field":null,"endpoints":{"default":{"path":"/v1beta/models/${encoded_model}:generateContent","stream_path":"/v1beta/models/${encoded_model}:streamGenerateContent","stream_query":{"alt":"sse"}}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('6e4afa95-2559-5721-8ef7-069bb063c5f8', 'azure', 'builtin:1.0.0', '6e4afa952559b7217ef7069bb063c5f85cb6881590fd59db013d908054689e55', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"azure_key","default_api_version":"2024-10-21"},"include_stream_usage_option":true,"endpoints":{"embeddings":{"path":"/openai/deployments/${encoded_model}/embeddings"},"completions":{"path":"/openai/deployments/${encoded_model}/completions"},"default":{"path":"/openai/deployments/${encoded_model}/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('a3786dc7-28da-570d-8fa3-6ee1857473a2', 'bedrock', 'builtin:1.0.0', 'a3786dc728da370d8fa36ee1857473a22b37c543925495e0ef68440137571598', '{"base_dialect":"anthropic_messages","usage_dialect":"anthropic","stream_framing":"bedrock_eventstream","auth":{"scheme":"aws_sigv4","service":"bedrock"},"model_field":null,"stream_field":null,"request_transforms":[{"op":"drop","path":"model"},{"op":"drop","path":"stream"},{"op":"force_set","path":"anthropic_version","value":"bedrock-2023-05-31"}],"endpoints":{"default":{"path":"/model/${encoded_model}/invoke","stream_path":"/model/${encoded_model}/invoke-with-response-stream"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('159bb4c4-6295-5883-8919-ca9409afc47b', 'vertex', 'builtin:1.0.0', '159bb4c46295d8837919ca9409afc47befb2277f0772f26141a25d857d232645', '{"base_dialect":"gemini","usage_dialect":"gemini","stream_framing":"sse","auth":{"scheme":"gcp_vertex_oauth"},"model_field":null,"stream_field":null,"endpoints":{"default":{"path":"/models/${encoded_model}:generateContent","stream_path":"/models/${encoded_model}:streamGenerateContent","stream_query":{"alt":"sse"}}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('3a663e13-dc03-5610-84cf-f3183e22f0c3', 'deepseek', 'builtin:1.0.0', '3a663e13dc032610b4cff3183e22f0c3226dd61f06088167d70e1f9218d5606c', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"usage_aliases":[{"target":"cached_input_tokens","source_path":"usage.prompt_cache_hit_tokens"}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('97b8f143-a5a1-5de1-88a5-c2e4c05b3401', 'ollama', 'builtin:1.0.0', '97b8f143a5a1ade108a5c2e4c05b3401125bf7654cab554d59e2a86f94a250e2', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"},"responses":{"path":"/v1/responses"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('2b9f2f42-23dc-50c1-885c-6bfb109e5106', 'qwen', 'builtin:1.0.0', '2b9f2f4223dcc0c1d85c6bfb109e510661963579254a8e54e3ed7147e17a1e6d', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"error_rules":[{"when_http_status":[200],"path":"code","in":["Throttling","Throttling.RateQuota","Throttling.AllocationQuota"],"retry_kind":"rate_limit","message_path":"message"},{"when_http_status":[200],"path":"code","in":["InvalidApiKey","Arrearage"],"retry_kind":"auth_error","message_path":"message"}],"endpoints":{"chat":{"path":"/compatible-mode/v1/chat/completions"},"embeddings":{"path":"/compatible-mode/v1/embeddings"},"completions":{"path":"/compatible-mode/v1/completions"},"default":{"path":"/compatible-mode/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('896fcbc6-7f9d-5701-8064-495eb9bed6d0', 'volcengine', 'builtin:1.0.0', '896fcbc67f9db7015064495eb9bed6d0d822d17d1ee36d2924413120ce7ff167', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning_content","request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"}],"endpoints":{"chat":{"path":"/api/v3/chat/completions"},"embeddings":{"path":"/api/v3/embeddings"},"default":{"path":"/api/v3/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('d083aa1d-f6fa-5ee3-821d-6223d582a4ac', 'zhipu', 'builtin:1.0.0', 'd083aa1df6fa8ee3021d6223d582a4acc1d8ab101d60d10c4402a19402997030', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":false,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0.01,"max":0.99}],"error_rules":[{"when_http_status":[200],"path":"error.code","in":["1302","1303","1305"],"retry_kind":"rate_limit","message_path":"error.message"},{"when_http_status":[200],"path":"error.code","in":["1001","1002","1003","1004","1113"],"retry_kind":"auth_error","message_path":"error.message"}],"endpoints":{"chat":{"path":"/api/paas/v4/chat/completions"},"embeddings":{"path":"/api/paas/v4/embeddings"},"default":{"path":"/api/paas/v4/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('138c925a-81a8-561e-8270-ac4d40704b96', 'moonshot', 'builtin:1.0.0', '138c925a81a8661e1270ac4d40704b96e4bb36fd607eeae06b191362daf2708f', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0,"max":1}],"endpoints":{"chat":{"path":"/v1/chat/completions"},"embeddings":{"path":"/v1/embeddings"},"moderations":{"path":"/v1/moderations"},"completions":{"path":"/v1/completions"},"images":{"path":"/v1/images/generations"},"image_edits":{"path":"/v1/images/edits"},"speech":{"path":"/v1/audio/speech"},"transcription":{"path":"/v1/audio/transcriptions"},"translation":{"path":"/v1/audio/translations"},"default":{"path":"/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('1f6add9d-08fb-5ed2-8da3-fa160682728c', 'minimax', 'builtin:1.0.0', '1f6add9d08fbfed2ada3fa160682728ca4003f5495a2a46d0ba2a9a35af970dd', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":false,"request_transforms":[{"op":"rename","from":"max_completion_tokens","to":"max_tokens","overwrite":false},{"op":"drop","path":"max_completion_tokens"},{"op":"clamp","path":"temperature","min":0.01,"max":1}],"error_rules":[{"when_http_status":[200],"path":"base_resp.status_code","equals":1002,"retry_kind":"rate_limit","message_path":"base_resp.status_msg"},{"when_http_status":[200],"path":"base_resp.status_code","in":[1004,1008],"retry_kind":"auth_error","message_path":"base_resp.status_msg"},{"when_http_status":[200],"path":"base_resp.status_code","in":[1000,1001,2013],"retry_kind":"retryable","message_path":"base_resp.status_msg"}],"endpoints":{"chat":{"path":"/v1/text/chatcompletion_v2"},"embeddings":{"path":"/v1/embeddings"},"default":{"path":"/v1/text/chatcompletion_v2"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch'),
  ('45a233d2-b479-5d01-8ae9-a5c155b49c50', 'openrouter', 'builtin:1.0.0', '45a233d2b4792d01fae9a5c155b49c505a7cdd9ff7b048124973aa1d454bf8a4', '{"base_dialect":"openai_chat","usage_dialect":"openai","stream_framing":"sse","auth":{"scheme":"bearer"},"include_stream_usage_option":true,"reasoning_field":"reasoning","endpoints":{"chat":{"path":"/api/v1/chat/completions"},"embeddings":{"path":"/api/v1/embeddings"},"completions":{"path":"/api/v1/completions"},"default":{"path":"/api/v1/chat/completions"}}}', 'PROVIDER', 'DECLARATIVE', true, 'epoch');

WITH previous_by_name AS (
  SELECT DISTINCT ON (name) name, id, provider_config
  FROM public.plugins
  WHERE capability = 'PROVIDER' AND enabled AND NOT builtin
  ORDER BY name, created_at DESC, id ASC
), current_providers AS (
  SELECT name, id FROM previous_by_name WHERE provider_config IS NOT NULL
  UNION ALL
  SELECT builtin.name, builtin.id
  FROM public.plugins builtin
  LEFT JOIN previous_by_name previous ON previous.name = builtin.name
  WHERE builtin.builtin AND previous.provider_config IS NULL
)
UPDATE public.channels channel
SET provider_plugin_id = provider.id
FROM current_providers provider
WHERE channel.provider_plugin_id IS NULL AND channel.kind = provider.name;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.channels channel
    LEFT JOIN public.plugins provider ON provider.id = channel.provider_plugin_id
    WHERE channel.enabled AND (provider.id IS NULL OR NOT provider.enabled
      OR provider.capability <> 'PROVIDER' OR provider.provider_config IS NULL)
  ) THEN
    RAISE EXCEPTION 'Enabled channels with an invalid provider must be repaired before plugin revision migration';
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.routes route
    CROSS JOIN LATERAL jsonb_array_elements(route.plugin_pipeline) step
    LEFT JOIN public.plugins content ON content.id::text = step->>'plugin_id'
    WHERE route.enabled AND step->'enabled' IS DISTINCT FROM 'false'::jsonb
      AND (content.id IS NULL OR NOT content.enabled OR content.capability <> 'CONTENT')
  ) THEN
    RAISE EXCEPTION 'Enabled routes with an invalid content plugin must be repaired before plugin revision migration';
  END IF;
END;
$$;

ALTER TABLE public.channels ADD CONSTRAINT channels_enabled_provider_binding
  CHECK (NOT enabled OR provider_plugin_id IS NOT NULL);

CREATE FUNCTION public.keep_plugin_revision() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
  IF (to_jsonb(NEW) - 'enabled') IS DISTINCT FROM (to_jsonb(OLD) - 'enabled') THEN
    RAISE EXCEPTION 'Plugin revision fields are immutable' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER plugins_immutable_revision
BEFORE UPDATE ON public.plugins
FOR EACH ROW EXECUTE FUNCTION public.keep_plugin_revision();

CREATE TABLE public.plugin_releases (
  id UUID PRIMARY KEY,
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[a-f0-9]{64}$'),
  actor TEXT NOT NULL,
  config_epoch BIGINT NOT NULL,
  changes JSONB NOT NULL CHECK (jsonb_typeof(changes) = 'array'),
  rollback_of UUID REFERENCES public.plugin_releases(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

UPDATE public.config_epoch SET version = version + 1, updated_at = now() WHERE id = 1;
