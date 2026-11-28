import { GatewayError } from "@gateway/contracts";
import type { ApiKeyRecord, BillingAccountRecord, createCatalogRepository } from "@gateway/ledger";

const consoleReference =
  /^console:org:([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}):billing-account:/i;

export function requiresConsoleAdmission(billingAccount: BillingAccountRecord): boolean {
  return billingAccount.external_ref?.startsWith("console:org:") === true;
}

export function createConsoleAdmissionCheck(
  getAdmission: ReturnType<typeof createCatalogRepository>["getConsoleAdmission"],
) {
  return function checkConsoleAdmission(
    billingAccount: BillingAccountRecord,
    key: ApiKeyRecord,
    policy: "snapshot" | "status" = "snapshot",
  ) {
    const externalRef = billingAccount.external_ref;
    if (!externalRef || !requiresConsoleAdmission(billingAccount)) return;
    const organizationId = consoleReference.exec(externalRef)?.[1];
    if (!organizationId) {
      throw new GatewayError(
        503,
        "config_unavailable",
        "Console organization reference is invalid",
      );
    }
    return getAdmission(organizationId, externalRef, billingAccount.id, key)
      .catch(() => {
        throw new GatewayError(503, "config_unavailable", "Console admission state is unavailable");
      })
      .then((admission) => {
        if (!admission?.key_enabled) throw new GatewayError(401, "unauthorized", "Invalid API key");
        if (!admission.billing_account_enabled) {
          throw new GatewayError(403, "billing_account_disabled", "Billing account is disabled");
        }
        if (admission.suspended)
          throw new GatewayError(403, "org_suspended", "Organization is suspended");
        if (policy === "snapshot" && !admission.key_policy_matches) {
          throw new GatewayError(
            503,
            "config_unavailable",
            "API key policy changed before admission",
          );
        }
      });
  };
}

export type ConsoleAdmissionCheck = ReturnType<typeof createConsoleAdmissionCheck>;
