import {
  fixtureSuiteSchema,
  verifiedPackageSnapshot,
  type VerifiedPackage,
} from "@gateway/plugin-sdk";
import { createPluginAttempts } from "./attempts.ts";
import { PluginHostError, type PluginSupervisor } from "./supervisor.ts";
export async function runPluginFixtures(supervisor: PluginSupervisor, pkg: VerifiedPackage) {
  const trusted = verifiedPackageSnapshot(pkg),
    source = trusted.files.find((file) => file.path === "fixtures/cases.json")!;
  const suite = fixtureSuiteSchema.parse(
    JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(source.bytes)),
  );
  const host = createPluginAttempts(supervisor);
  function matches(actual: unknown, expected: unknown, depth = 0): boolean {
    if (depth > 64) throw new PluginHostError("FIXTURE_DEPTH");
    if (Array.isArray(expected))
      return (
        Array.isArray(actual) &&
        actual.length === expected.length &&
        expected.every((item, i) => matches(actual[i], item, depth + 1))
      );
    if (expected !== null && typeof expected === "object")
      return (
        actual !== null &&
        typeof actual === "object" &&
        Object.entries(expected).every(
          ([key, value]) =>
            Object.hasOwn(actual, key) &&
            matches((actual as Record<string, unknown>)[key], value, depth + 1),
        )
      );
    return Object.is(actual, expected);
  }
  let steps = 0;
  try {
    for (const fixture of suite.cases) {
      const attempt = host.create(pkg, {
        tenant: "synthetic-package-fixture",
        attempt: crypto.randomUUID(),
        incarnation: crypto.randomUUID(),
        deadlineAt: Date.now() + 30000,
        config: suite.config,
      });
      try {
        for (const step of fixture.steps) {
          steps++;
          if (step.action === "cancel") {
            const controller = new AbortController();
            controller.abort();
            let error: unknown;
            try {
              await attempt.invoke(
                step.hook,
                new Uint8Array(Buffer.from(step.input, "base64")),
                controller.signal,
              );
            } catch (caught) {
              error = caught;
            }
            if (!(error instanceof PluginHostError) || error.code !== step.expected_error)
              throw new PluginHostError("FIXTURE_EXPECTATION");
          } else if (step.action === "json") {
            const output = await attempt.transformJSON(
              step.hook,
              step.metadata,
              new Uint8Array(Buffer.from(step.body, "base64")),
            );
            if (
              !matches(output.result, step.expected) ||
              (step.expected_body !== undefined &&
                (!output.body ||
                  !Buffer.from(output.body).equals(Buffer.from(step.expected_body, "base64"))))
            )
              throw new PluginHostError("FIXTURE_EXPECTATION");
          } else if (
            !matches(
              await attempt.invoke(step.hook, new Uint8Array(Buffer.from(step.input, "base64"))),
              step.expected,
            )
          )
            throw new PluginHostError("FIXTURE_EXPECTATION");
        }
      } finally {
        attempt.dispose();
      }
    }
  } finally {
    host.dispose();
  }
  return { cases: suite.cases.length, steps, state: host.snapshot() };
}
