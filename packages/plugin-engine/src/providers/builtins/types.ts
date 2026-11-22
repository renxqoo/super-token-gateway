import type { DeclarativeProviderConfig } from "@gateway/ledger";

export type BuiltinProviderDefinition = {
  name: string;
  version: string;
  description: string;
  provider_config: DeclarativeProviderConfig;
};
