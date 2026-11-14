import { openAIChatReleaseId, type BundledReleaseId, type PackageFile } from "@gateway/plugin-sdk";
import { fileURLToPath } from "node:url";
async function packageFiles(name: string): Promise<PackageFile[]> {
  const root = new URL(`../${name}/package/`, import.meta.url);
  const files: PackageFile[] = [];
  for await (const path of new Bun.Glob("**/*").scan({ cwd: fileURLToPath(root), onlyFiles: true }))
    files.push({ path, bytes: new Uint8Array(await Bun.file(new URL(path, root)).arrayBuffer()) });
  return files;
}
export function openAIChatPackageFiles(): Promise<PackageFile[]> {
  return packageFiles("openai-chat");
}
export function anthropicMessagesPackageFiles(): Promise<PackageFile[]> {
  return packageFiles("anthropic-messages");
}
export function bundledPackageFiles(releaseId: BundledReleaseId): Promise<PackageFile[]> {
  return releaseId === openAIChatReleaseId
    ? openAIChatPackageFiles()
    : anthropicMessagesPackageFiles();
}
