import { sha256 } from "./abi.ts";

declare const pluginModuleIdentity: unique symbol;
export type PluginModule = Readonly<{ [pluginModuleIdentity]: true }>;
type ModuleRecord = { bytes: Uint8Array; info: Readonly<{ hash: string; byteLength: number }> };
const modules = new WeakMap<PluginModule, ModuleRecord>();

export function adoptPluginModule(bytes: Uint8Array, hash: string): PluginModule {
  const module = Object.freeze({}) as PluginModule;
  modules.set(module, { bytes, info: Object.freeze({ hash, byteLength: bytes.byteLength }) });
  return module;
}

export function ownPluginModule(bytes: Uint8Array): PluginModule {
  const owned = new Uint8Array(bytes);
  return adoptPluginModule(owned, sha256(owned));
}

function record(module: PluginModule): ModuleRecord {
  const owned = modules.get(module);
  if (!owned) throw new Error("MODULE_NOT_OWNED");
  return owned;
}

export function pluginModuleInfo(module: PluginModule) {
  return record(module).info;
}

export function pluginModuleBase64(module: PluginModule): string {
  return Buffer.from(record(module).bytes).toString("base64");
}
