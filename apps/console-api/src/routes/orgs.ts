import { Hono } from "hono";
import { and, desc, eq, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import {
  generateOpaqueToken,
  recordAuditLog,
  requireOrgMember,
  requireProjectAccess,
  sha256Hex,
} from "../auth.ts";
import {
  organizations,
  orgInvitations,
  orgMembers,
  projects,
  users,
  type OrgRole,
} from "../db/schema.ts";
import {
  currencySchema,
  parseJsonBody,
  parseUuid,
  requireAuth,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const createOrgSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
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

const createProjectSchema = z
  .object({
    name: z.string().trim().min(1).max(120),
    currency: currencySchema.default("USD"),
  })
  .strict();

const updateProjectSchema = z
  .object({
    name: z.string().trim().min(1).max(120).optional(),
  })
  .strict();

export function createOrgAndProjectRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireAuth(deps);
  app.use("/orgs", auth);
  app.use("/orgs/*", auth);
  app.use("/invitations/*", auth);
  app.use("/projects/*", auth);

  app.get("/orgs", async (c) => {
    const user = c.get("user");
    const rows = await deps.orm
      .select({
        id: organizations.id,
        slug: organizations.slug,
        name: organizations.name,
        kind: organizations.kind,
        status: organizations.status,
        role: orgMembers.role,
        created_at: organizations.createdAt,
      })
      .from(orgMembers)
      .innerJoin(organizations, eq(organizations.id, orgMembers.orgId))
      .where(eq(orgMembers.userId, user.id))
      .orderBy(desc(organizations.createdAt));
    return c.json({ items: rows });
  });

  app.post("/orgs", async (c) => {
    const user = c.get("user");
    const body = await parseJsonBody(c, createOrgSchema);
    const orgId = crypto.randomUUID();
    const projectId = crypto.randomUUID();

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
      await tx.insert(projects).values({
        id: projectId,
        orgId,
        gatewayProjectId: projectId,
        name: "Production",
        currency: "USD",
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
        default_project_id: projectId,
      },
      201,
    );
  });

  app.get("/orgs/:orgId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const role = await requireOrgMember(deps.orm, user.id, orgId);
    const [org] = await deps.orm
      .select()
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

  app.post("/orgs/:orgId/invitations", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const callerRole = await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const [org] = await deps.orm
      .select({ kind: organizations.kind })
      .from(organizations)
      .where(eq(organizations.id, orgId))
      .limit(1);
    if (!org || org.kind !== "ENTERPRISE") {
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
    const expiresAt = new Date(Date.now() + 7 * 24 * 3_600_000).toISOString();
    await deps.orm.insert(orgInvitations).values({
      id: inviteId,
      orgId,
      email,
      role: body.role,
      tokenHash: sha256Hex(rawToken),
      invitedBy: user.id,
      status: "PENDING",
      expiresAt,
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
        expires_at: expiresAt,
      },
      201,
    );
  });

  app.post("/invitations/:token/accept", async (c) => {
    const user = c.get("user");
    const rawToken = c.req.param("token");
    const hash = sha256Hex(rawToken);

    const accepted = await deps.orm.transaction(async (tx) => {
      const [inv] = await tx
        .select()
        .from(orgInvitations)
        .where(eq(orgInvitations.tokenHash, hash))
        .for("update");
      if (!inv || inv.status !== "PENDING") {
        throw new GatewayError(404, "not_found", "Invitation not found or already used");
      }
      if (Date.parse(inv.expiresAt) <= Date.now()) {
        await tx
          .update(orgInvitations)
          .set({ status: "EXPIRED" })
          .where(eq(orgInvitations.id, inv.id));
        throw new GatewayError(410, "invitation_expired", "Invitation has expired");
      }
      if (inv.email.toLowerCase() !== user.email.toLowerCase()) {
        throw new GatewayError(
          403,
          "forbidden",
          "Invitation email does not match authenticated user",
        );
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
      return { orgId: inv.orgId, role: inv.role };
    });

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: accepted.orgId,
      action: "org.invitation_accept",
      targetType: "organization",
      targetId: accepted.orgId,
      metadata: { role: accepted.role },
    });
    return c.json({ ok: true, org_id: accepted.orgId, role: accepted.role });
  });

  app.patch("/orgs/:orgId/members/:userId", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    const targetUserId = parseUuid(c.req.param("userId"), "userId");
    const callerRole = await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const body = await parseJsonBody(c, updateMemberRoleSchema);

    const prevRole = await deps.orm.transaction(async (tx) => {
      const members = await tx
        .select()
        .from(orgMembers)
        .where(eq(orgMembers.orgId, orgId))
        .for("update");
      const target = members.find((m) => m.userId === targetUserId);
      if (!target) throw new GatewayError(404, "not_found", "Member not found");

      if (
        callerRole !== "OWNER" &&
        (target.role === "OWNER" ||
          target.role === "ADMIN" ||
          body.role === "OWNER" ||
          body.role === "ADMIN")
      ) {
        throw new GatewayError(403, "forbidden", "Only OWNER can manage OWNER or ADMIN roles");
      }
      if (target.role === "OWNER" && body.role !== "OWNER") {
        const ownerCount = members.filter((m) => m.role === "OWNER").length;
        if (ownerCount <= 1) {
          throw new GatewayError(400, "last_owner", "Organization must retain at least one OWNER");
        }
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
    const callerRole = await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);

    await deps.orm.transaction(async (tx) => {
      const members = await tx
        .select()
        .from(orgMembers)
        .where(eq(orgMembers.orgId, orgId))
        .for("update");
      const target = members.find((m) => m.userId === targetUserId);
      if (!target) throw new GatewayError(404, "not_found", "Member not found");
      if (callerRole !== "OWNER" && (target.role === "OWNER" || target.role === "ADMIN")) {
        throw new GatewayError(403, "forbidden", "Only OWNER can remove OWNER or ADMIN members");
      }
      if (target.role === "OWNER") {
        const ownerCount = members.filter((m) => m.role === "OWNER").length;
        if (ownerCount <= 1) {
          throw new GatewayError(400, "last_owner", "Cannot remove the last OWNER");
        }
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

  app.get("/orgs/:orgId/projects", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId);
    const items = await deps.orm
      .select()
      .from(projects)
      .where(eq(projects.orgId, orgId))
      .orderBy(desc(projects.createdAt));
    return c.json({ items });
  });

  app.post("/orgs/:orgId/projects", async (c) => {
    const user = c.get("user");
    const orgId = parseUuid(c.req.param("orgId"), "orgId");
    await requireOrgMember(deps.orm, user.id, orgId, ["OWNER", "ADMIN"]);
    const body = await parseJsonBody(c, createProjectSchema);
    const projectId = crypto.randomUUID();
    const [created] = await deps.orm
      .insert(projects)
      .values({
        id: projectId,
        orgId,
        gatewayProjectId: projectId,
        name: body.name,
        currency: body.currency,
        enabled: true,
        gatewaySynced: false,
        syncStatus: "PENDING",
      })
      .returning();

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId,
      action: "project.create",
      targetType: "project",
      targetId: projectId,
      metadata: { name: body.name, currency: body.currency },
    });
    return c.json(created, 201);
  });

  app.get("/projects/:projectId", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project, role } = await requireProjectAccess(deps.orm, user.id, projectId);
    return c.json({ ...project, role });
  });

  app.patch("/projects/:projectId", async (c) => {
    const user = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const { project } = await requireProjectAccess(deps.orm, user.id, projectId, [
      "OWNER",
      "ADMIN",
    ]);
    const body = await parseJsonBody(c, updateProjectSchema);
    const [updated] = await deps.orm
      .update(projects)
      .set({
        ...(body.name !== undefined ? { name: body.name } : {}),
        desiredRevision: sql`${projects.desiredRevision} + 1`,
        syncStatus: "PENDING",
        updatedAt: sql`now()`,
      })
      .where(eq(projects.id, projectId))
      .returning();

    const finalProject = project.gatewaySynced
      ? await deps.sync.ensureProjectSynced(projectId)
      : updated!;

    await recordAuditLog(deps.orm, {
      actor: user,
      orgId: project.orgId,
      action: "project.update",
      targetType: "project",
      targetId: projectId,
    });
    return c.json(finalProject);
  });

  return app;
}
