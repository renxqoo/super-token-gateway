import type { PackageFile } from "@gateway/plugin-sdk";
import { fileURLToPath } from "node:url";
export async function openAIChatPackageFiles(): Promise<PackageFile[]> {
  const root = new URL("../openai-chat/package/", import.meta.url);
  const files: PackageFile[] = [];
  for await (const path of new Bun.Glob("**/*").scan({ cwd: fileURLToPath(root), onlyFiles: true }))
    files.push({ path, bytes: new Uint8Array(await Bun.file(new URL(path, root)).arrayBuffer()) });
  return files;
}
