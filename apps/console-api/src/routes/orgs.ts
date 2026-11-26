import { Hono } from "hono";
import { and, asc, desc, eq, getTableColumns, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import {
  defaultBillingAccountId,
  generateOpaqueToken,
  recordAuditLog,
  requireOrgMember,
  requireBillingAccountAccess,
  sha256Hex,
} from "../auth.ts";
import {
  organizations,
  orgInvitations,
  orgMembers,
  billingAccounts,
  users,
  type OrgRole,
} from "../db/schema.ts";
import {
  currencySchema,
  nonNegativeAtomsSchema,
  parseJsonBody,
  parseLimitOffset,
  parseUuid,
  requireClientAuth,
  sanitizeBillingAccountForClient,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const createOrgSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    billing_currency: currencySchema.default("USD"),
    slug: z
      .string()
      .trim()
      .regex(/^[a-z0-9][a-z0-9-]{2,63}$/),
  })
  .strict();

const updateOrgSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
  })
  .strict();

const inviteMemberSchema = z
  .object({
    email: z.email().max(200),
    role: z.enum(["ADMIN", "FINANCE", "DEVELOPER", "VIEWER"]),
  })
  .strict();

const updateMemberRoleSchema = z
  .object({
    role: z.enum(["OWNER", "ADMIN", "FINANCE", "DEVELOPER", "VIEWER"]),
  })
  .strict();

const updateBillingAccountSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
    developer_key_spend_limit_atoms: nonNegativeAtomsSchema.nullable().optional(),
    developer_max_active_keys: z.number().int().min(1).max(1000).nullable().optional(),
  })
  .strict();

export function createOrgAndBillingAccountRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireClientAuth(deps);
  app.use("/orgs", auth);
  app.use("/orgs/*", auth);
  app.use("/invitations/*", auth);
  app.use("/billing-accounts/*", auth);

  app.get("/orgs", async (c) => {
    const user = c.get("user");
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const rows = await deps.orm
      .select({
        id: organizations.id,
        slug: organizations.slug,
        name: organizations.name,
        kind: organizations.kind,
        status: organizations.status,
        role: orgMembers.role,
        default_billing_account_id: defaultBillingAccountId,
        created_at: organizations.createdAt,
      })
      .from(orgMembers)
      .innerJoin(organizations, eq(organizations.id, orgMembers.orgId))
      .where(eq(orgMembers.userId, user.id))
      .orderBy(desc(organizations.createdAt))
      .limit(limit)
      .offset(offset);
    return c.json({ items: rows, limit, offset });
  });

  app.post("/orgs", async (c) => {
    const user = c.get("user");
    const body = await parseJsonBody(c, createOrgSchema);
    const orgId = crypto.randomUUID();
    const billingAccountId = crypto.randomUUID();

    await deps.orm.transaction(async (tx) => {
      await tx.insert(organizations).values({
        id: orgId,
        slug: body.slug,
        name: body.name,
        kind: "ENTERPRISE",
        ownerUserId: user.id,
        status: "ACTIVE",
      });
      await tx.insert(orgMembers).values({
        orgId,
        userId: user.id,
        role: "OWNER",
      });
      await tx.insert(billingAccounts).values({
        id: billingAccountId,
        orgId,
        gatewayBillingAccountId: billingAccountId,
        name: body.name,
        currency: body.billing_currency,
        gatewaySynced: false,
        syncStatus: "PENDING",
      });
    });

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.create",
      targetType: "organization",
      targetId: orgId,
      metadata: { slug: body.slug, kind: "ENTERPRISE" },
    });

    return c.json(
      {
        id: orgId,
        slug: body.slug,
        name: body.name,
        kind: "ENTERPRISE",
        role: "OWNER",
        default_billing_account_id: billingAccountId,
      },
      201,
    );
  });

  app.get("/orgs/:orgId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const { role } = await requireOrgMember(deps.orm, user.id, orgId);
    const [org] = await deps.orm
      .select({
        ...getTableColumns(organizations),
        default_billing_account_id: defaultBillingAccountId,
      })
      .from(organizations)
      .where(eq(organizations.id, orgId))
      .limit(1);
    if (!org) throw new GatewayError(404, "not_found", "Organization not found");
    return c.json({ ...org, role });
  });

  app.patch("/orgs/:orgId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const body = await parseJsonBody(c, updateOrgSchema);
    const [updated] = await deps.orm
      .update(organizations)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        updatedAt: sql`now()`,
      })
      .where(eq(organizations.id, orgId))
      .returning();
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.update",
      targetType: "organization",
      targetId: orgId,
    });
    return c.json(updated);
  });

  app.get("/orgs/:orgId/members", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId);
    const rows = await deps.orm
      .select({
        user_id: users.id,
        email: users.email,
        display_name: users.displayName,
        role: orgMembers.role,
        joined_at: orgMembers.createdAt,
      })
      .from(orgMembers)
      .innerJoin(users, eq(users.id, orgMembers.userId))
      .where(eq(orgMembers.orgId, orgId));
    return c.json({ items: rows });
  });

  app.get("/orgs/:orgId/invitations", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const items = await deps.orm
      .select({
        id: orgInvitations.id,
        org_id: orgInvitations.orgId,
        email: orgInvitations.email,
        role: orgInvitations.role,
        status: orgInvitations.status,
        expires_at: orgInvitations.expiresAt,
        created_at: orgInvitations.createdAt,
      })
      .from(orgInvitations)
      .where(eq(orgInvitations.orgId, orgId))
      .orderBy(desc(orgInvitations.createdAt));
    return c.json({ items });
  });

  app.post("/orgs/:orgId/invitations", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const { role: callerRole, orgKind } = await requireOrgMember(deps.orm, user.id, orgId, [
      "OWNER",
      "ADMIN",
    ]);
    if (orgKind !== "ENTERPRISE") {
      throw new GatewayError(
        400,
        "invalid_org_kind",
        "Only enterprise organizations support members",
      );
    }
    const body = await parseJsonBody(c, inviteMemberSchema);
    const email = body.email.trim().toLowerCase();
    if (callerRole === "ADMIN" && body.role === "ADMIN") {
      throw new GatewayError(403, "forbidden", "Only organization OWNER can invite ADMIN members");
    }
    const existingMember = await deps.orm
      .select({ userId: orgMembers.userId })
      .from(orgMembers)
      .innerJoin(users, eq(users.id, orgMembers.userId))
      .where(and(eq(orgMembers.orgId, orgId), eq(users.email, email)))
      .limit(1);
    if (existingMember.length > 0) {
      throw new GatewayError(
        409,
        "already_member",
        "User is already a member of this organization",
      );
    }
    const rawToken = generateOpaqueToken("stg_inv_");
    const inviteId = crypto.randomUUID();
    const expiresAt = new Date(Date.now() + 7 * 24 * 3_600_000);
    await deps.orm.transaction(async (tx) => {
      await tx
        .update(orgInvitations)
        .set({ status: "EXPIRED" })
        .where(
          and(
            eq(orgInvitations.orgId, orgId),
            sql`lower(${orgInvitations.email}) = ${email}`,
            eq(orgInvitations.status, "PENDING"),
            sql`${orgInvitations.expiresAt} <= now()`,
          ),
        );
      await tx.insert(orgInvitations).values({
        id: inviteId,
        orgId,
        email,
        role: body.role,
        tokenHash: sha256Hex(rawToken),
        invitedBy: user.id,
        status: "PENDING",
        expiresAt,
      });
    });
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.invite",
      targetType: "invitation",
      targetId: inviteId,
      metadata: { email, role: body.role },
    });
    return c.json(
      {
        id: inviteId,
        org_id: orgId,
        email,
        role: body.role,
        token: rawToken,
        expires_at: expiresAt.toISOString(),
      },
      201,
    );
  });

  app.delete("/orgs/:orgId/invitations/:invitationId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const invitationId = parseUuid(c.req.param("invitationId"), "invitationId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const updated = await deps.orm
      .update(orgInvitations)
      .set({ status: "REVOKED" })
      .where(
        and(
          eq(orgInvitations.id, invitationId),
          eq(orgInvitations.orgId, orgId),
          eq(orgInvitations.status, "PENDING"),
        ),
      )
      .returning({ id: orgInvitations.id });
    if (updated.length === 0) {
      throw new GatewayError(404, "not_found", "Pending invitation not found");
    }
    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.invitation_revoke",
      targetType: "invitation",
      targetId: invitationId,
    });
    return c.json({ ok: true, id: invitationId });
  });

  app.post("/invitations/:token/accept", async (c) => {
    const user = c.get("user");
    const rawToken = c.req.param("token");
    const hash = sha256Hex(rawToken);

    const outcome = await deps.orm.transaction(async (tx) => {
      const [inv] = await tx
        .select()
        .from(orgInvitations)
        .where(eq(orgInvitations.tokenHash, hash))
        .for("update");
      if (!inv || inv.status !== "PENDING") {
        throw new GatewayError(404, "not_found", "Invitation not found or already used");
      }
      if (inv.expiresAt.getTime() <= Date.now()) {
        await tx
          .update(orgInvitations)
          .set({ status: "EXPIRED" })
          .where(eq(orgInvitations.id, inv.id));
        return { expired: true as const };
      }
      if (inv.email.toLowerCase() !== user.email.toLowerCase()) {
        throw new GatewayError(
          403,
          "forbidden",
          "Invitation email does not match authenticated user",
        );
      }
      const [org] = await tx
        .select({
          status: organizations.status,
          inferenceReady: sql<boolean>`${organizations.inferenceRevision} = ${organizations.inferenceSyncedRevision}`,
        })
        .from(organizations)
        .where(eq(organizations.id, inv.orgId))
        .limit(1);
      if (!org || org.status !== "ACTIVE" || !org.inferenceReady) {
        throw new GatewayError(403, "org_suspended", "Organization is suspended");
      }
      const existing = await tx
        .select({ role: orgMembers.role })
        .from(orgMembers)
        .where(and(eq(orgMembers.orgId, inv.orgId), eq(orgMembers.userId, user.id)))
        .limit(1);
      if (existing.length > 0) {
        throw new GatewayError(409, "already_member", "Already a member of this organization");
      }
      await tx.insert(orgMembers).values({
        orgId: inv.orgId,
        userId: user.id,
        role: inv.role,
      });
      await tx
        .update(orgInvitations)
        .set({ status: "ACCEPTED" })
        .where(eq(orgInvitations.id, inv.id));
      return { expired: false as const, orgId: inv.orgId, role: inv.role };
    });

    if (outcome.expired) {
      throw new GatewayError(410, "invitation_expired", "Invitation has expired");
    }

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: outcome.orgId,
      action: "org.invitation_accept",
      targetType: "organization",
      targetId: outcome.orgId,
      metadata: { role: outcome.role },
    });
    return c.json({ ok: true, org_id: outcome.orgId, role: outcome.role });
  });

  app.patch("/orgs/:orgId/members/:userId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const targetUserId = parseUuid(c.req.param("userId"), "userId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const body = await parseJsonBody(c, updateMemberRoleSchema);

    const prevRole = await deps.orm.transaction(async (tx) => {
      const members = await tx
        .select()
        .from(orgMembers)
        .where(eq(orgMembers.orgId, orgId))
        .for("update");
      const caller = members.find((m) => m.userId === user.id);
      if (!caller) throw new GatewayError(404, "not_found", "Organization not found");
      if (caller.role !== "OWNER" && caller.role !== "ADMIN") {
        throw new GatewayError(403, "forbidden", "Insufficient organization role");
      }
      const target = members.find((m) => m.userId === targetUserId);
      if (!target) throw new GatewayError(404, "not_found", "Member not found");

      if (
        caller.role !== "OWNER" &&
        (target.role === "OWNER" ||
          target.role === "ADMIN" ||
          body.role === "OWNER" ||
          body.role === "ADMIN")
      ) {
        throw new GatewayError(403, "forbidden", "Only OWNER can manage OWNER or ADMIN roles");
      }
      if (target.role === "OWNER" && body.role !== "OWNER") {
        const otherOwners = members.filter((m) => m.role === "OWNER" && m.userId !== targetUserId);
        if (otherOwners.length === 0) {
          throw new GatewayError(400, "last_owner", "Organization must retain at least one OWNER");
        }
        await tx
          .update(organizations)
          .set({ ownerUserId: otherOwners[0]!.userId, updatedAt: sql`now()` })
          .where(and(eq(organizations.id, orgId), eq(organizations.ownerUserId, targetUserId)));
      }
      await tx
        .update(orgMembers)
        .set({ role: body.role as OrgRole })
        .where(and(eq(orgMembers.orgId, orgId), eq(orgMembers.userId, targetUserId)));
      return target.role;
    });

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.member_role_update",
      targetType: "org_member",
      targetId: targetUserId,
      metadata: { from: prevRole, to: body.role },
    });
    return c.json({ org_id: orgId, user_id: targetUserId, role: body.role });
  });

  app.delete("/orgs/:orgId/members/:userId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const targetUserId = parseUuid(c.req.param("userId"), "userId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);

    await deps.orm.transaction(async (tx) => {
      const members = await tx
        .select()
        .from(orgMembers)
        .where(eq(orgMembers.orgId, orgId))
        .for("update");
      const caller = members.find((m) => m.userId === user.id);
      if (!caller) throw new GatewayError(404, "not_found", "Organization not found");
      if (caller.role !== "OWNER" && caller.role !== "ADMIN") {
        throw new GatewayError(403, "forbidden", "Insufficient organization role");
      }
      const target = members.find((m) => m.userId === targetUserId);
      if (!target) throw new GatewayError(404, "not_found", "Member not found");
      if (caller.role !== "OWNER" && (target.role === "OWNER" || target.role === "ADMIN")) {
        throw new GatewayError(403, "forbidden", "Only OWNER can remove OWNER or ADMIN members");
      }
      if (target.role === "OWNER") {
        const otherOwners = members.filter((m) => m.role === "OWNER" && m.userId !== targetUserId);
        if (otherOwners.length === 0) {
          throw new GatewayError(400, "last_owner", "Cannot remove the last OWNER");
        }
        await tx
          .update(organizations)
          .set({ ownerUserId: otherOwners[0]!.userId, updatedAt: sql`now()` })
          .where(and(eq(organizations.id, orgId), eq(organizations.ownerUserId, targetUserId)));
      }
      await tx
        .delete(orgMembers)
        .where(and(eq(orgMembers.orgId, orgId), eq(orgMembers.userId, targetUserId)));
    });

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "org.member_remove",
      targetType: "org_member",
      targetId: targetUserId,
    });
    return c.json({ ok: true });
  });

  app.get("/orgs/:orgId/billing-accounts", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId);
    const { limit, offset } = parseLimitOffset(c, 50, 200);
    const items = await deps.orm
      .select()
      .from(billingAccounts)
      .where(eq(billingAccounts.orgId, orgId))
      .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id))
      .limit(limit)
      .offset(offset);
    return c.json({ items: items.map(sanitizeBillingAccountForClient), limit, offset });
  });

  app.get("/billing-accounts/:billingAccountId", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const { billingAccount, role } = await requireBillingAccountAccess(
      deps.orm,
      user.id,
      billingAccountId,
    );
    return c.json({ ...sanitizeBillingAccountForClient(billingAccount), role });
  });

  app.patch("/billing-accounts/:billingAccountId", async (c) => {
    const user = c.get("user");
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const { billingAccount, role } = await requireBillingAccountAccess(
      deps.orm,
      user.id,
      billingAccountId,
      ["OWNER", "ADMIN", "FINANCE"],
    );
    const body = await parseJsonBody(c, updateBillingAccountSchema);
    if (role === "FINANCE" && body.name !== undefined) {
      throw new GatewayError(403, "forbidden", "FINANCE role can only update developer key limits");
    }
    const touchesGateway = body.name !== undefined;
    const [updated] = await deps.orm
      .update(billingAccounts)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        ...(body.developer_key_spend_limit_atoms !== undefined
          ? { developerKeySpendLimitAtoms: body.developer_key_spend_limit_atoms }
          : {}),
        ...(body.developer_max_active_keys !== undefined
          ? { developerMaxActiveKeys: body.developer_max_active_keys }
          : {}),
        ...(touchesGateway
          ? { desiredRevision: sql`${billingAccounts.desiredRevision} + 1`, syncStatus: "PENDING" }
          : {}),
        updatedAt: sql`now()`,
      })
      .where(eq(billingAccounts.id, billingAccountId))
      .returning();

    const finalBillingAccount =
      touchesGateway && updated!.gatewaySynced
        ? await deps.sync.admission.ensureBillingAccountSynced(billingAccountId)
        : updated!;

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: billingAccount.orgId,
      action: "billing_account.update",
      targetType: "billing_account",
      targetId: billingAccountId,
    });
    return c.json(sanitizeBillingAccountForClient(finalBillingAccount));
  });

  return app;
}
