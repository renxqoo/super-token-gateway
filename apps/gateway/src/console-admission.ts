import { GatewayError } from "@gateway/contracts";
import type { ApiKeyRecord, ProjectRecord, createCatalogRepository } from "@gateway/ledger";

const consoleReference =
  /^console:org:([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}):project:/i;

export function createConsoleAdmissionCheck(
  getAdmission: ReturnType<typeof createCatalogRepository>["getConsoleAdmission"],
) {
  return function checkConsoleAdmission(project: ProjectRecord, key: ApiKeyRecord) {
    const externalRef = project.external_ref;
    if (!externalRef?.startsWith("console:org:")) return;
    const organizationId = consoleReference.exec(externalRef)?.[1];
    if (!organizationId) {
      throw new GatewayError(
        503,
        "config_unavailable",
        "Console organization reference is invalid",
      );
    }
    return getAdmission(organizationId, externalRef, project.id, key.id)
      .catch(() => {
        throw new GatewayError(503, "config_unavailable", "Console admission state is unavailable");
      })
      .then((admission) => {
        if (!admission?.key_enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
        if (!admission.project_enabled) {
          throw new GatewayError(403, "project_disabled", "Project is disabled");
        }
        if (admission.suspended)
          throw new GatewayError(403, "org_suspended", "Organization is suspended");
      });
  };
}

export type ConsoleAdmissionCheck = ReturnType<typeof createConsoleAdmissionCheck>;
