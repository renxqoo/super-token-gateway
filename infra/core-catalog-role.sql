DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='core_catalog') THEN CREATE ROLE core_catalog NOLOGIN;END IF;
END $$;
GRANT USAGE ON SCHEMA public TO core_catalog;
GRANT SELECT ON plugin_packages,plugin_versions,plugin_validation_runs,plugin_publication_receipts,plugin_catalog_receipts,channel_plugin_bindings,channels,upstream_accounts,supplier_brands TO core_catalog;
GRANT EXECUTE ON FUNCTION public.verify_platform_credential_v1(text),public.plugin_catalog_command_v1(text,text,uuid,jsonb,uuid,timestamptz,uuid,uuid,text,boolean),public.plugin_catalog_receipt_v1(text,text,uuid,jsonb) TO core_catalog;
REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON plugin_packages,plugin_versions,plugin_validation_runs,plugin_publication_receipts,plugin_catalog_receipts,channel_plugin_bindings,technical_administrators,administrator_credentials FROM core_catalog;
REVOKE ALL ON administrator_credentials FROM core_catalog;
