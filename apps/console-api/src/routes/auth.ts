import { Hono } from "hono";
import { and, eq, isNull, ne, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import {
  constantTimeEquals,
  generateOpaqueToken,
  hashPassword,
  recordAuditLog,
  sha256Hex,
  verifyPassword,
} from "../auth.ts";
import { organizations, orgMembers, projects, sessions, users } from "../db/schema.ts";
import { parseJsonBody, requireAuth, type ConsoleDeps, type ConsoleEnv } from "./shared.ts";

const registerSchema = z
  .object({
    email: z.email().max(200),
    password: z.string().min(8).max(128),
    display_name: z.string().trim().min(1).max(120),
    bootstrap_token: z.string().min(1).max(256).optional(),
  })
  .strict();

const loginSchema = z
  .object({
    email: z.email().max(200),
    password: z.string().min(1).max(128),
  })
  .strict();

const refreshSchema = z
  .object({
    refresh_token: z.string().min(16).max(256),
  })
  .strict();

const changePasswordSchema = z
  .object({
    current_password: z.string().min(1).max(128),
    new_password: z.string().min(8).max(128),
  })
  .strict();

export function createAuthRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireAuth(deps);

  function clientIpOf(c: { req: { header(name: string): string | undefined } }): string {
    if (deps.config.trustProxyHeaders) {
      const forwarded = c.req.header("x-forwarded-for")?.split(",")[0]?.trim();
      if (forwarded) return forwarded;
    }
    return "direct";
  }

  async function issueSessionInTx(
    tx: Pick<typeof deps.orm, "insert">,
    userId: string,
    userAgent: string | null,
    ipAddress: string | null,
  ) {
    const now = Date.now();
    const accessToken = generateOpaqueToken("stg_sess_");
    const refreshToken = generateOpaqueToken("stg_ref_");
    const expiresAt = new Date(now + deps.config.sessionTtlMs).toISOString();
    const refreshExpiresAt = new Date(now + deps.config.refreshTtlMs).toISOString();
    const id = crypto.randomUUID();
    await tx.insert(sessions).values({
      id,
      userId,
      tokenHash: sha256Hex(accessToken),
      refreshTokenHash: sha256Hex(refreshToken),
      expiresAt,
      refreshExpiresAt,
      userAgent,
      ipAddress,
    });
    return {
      session_id: id,
      access_token: accessToken,
      refresh_token: refreshToken,
      expires_at: expiresAt,
      refresh_expires_at: refreshExpiresAt,
    };
  }

  app.post("/register", async (c) => {
    const body = await parseJsonBody(c, registerSchema);
    const email = body.email.trim().toLowerCase();
    const existing = await deps.orm
      .select({ id: users.id })
      .from(users)
      .where(eq(users.email, email))
      .limit(1);
    if (existing.length > 0) {
      throw new GatewayError(409, "email_exists", "An account with this email already exists");
    }

    const userId = crypto.randomUUID();
    const orgId = crypto.randomUUID();
    const projectId = crypto.randomUUID();
    const passwordHash = await hashPassword(body.password);
    const slug = `personal-${userId.slice(0, 8)}`;

    const matchesBootstrapEmail =
      deps.config.bootstrapAdminEmail !== undefined && email === deps.config.bootstrapAdminEmail;
    const bootstrapTokenOk =
      deps.config.bootstrapAdminToken === undefined ||
      (body.bootstrap_token !== undefined &&
        constantTimeEquals(body.bootstrap_token, deps.config.bootstrapAdminToken));

    let isAdmin = false;
    const session = await deps.orm.transaction(async (tx) => {
      if (matchesBootstrapEmail && bootstrapTokenOk) {
        const existingAdmins = await tx
          .select({ id: users.id })
          .from(users)
          .where(eq(users.isPlatformAdmin, true))
          .limit(1);
        isAdmin = existingAdmins.length === 0;
      }
      await tx.insert(users).values({
        id: userId,
        email,
        passwordHash,
        displayName: body.display_name,
        isPlatformAdmin: isAdmin,
        status: "ACTIVE",
      });
      await tx.insert(organizations).values({
        id: orgId,
        slug,
        name: `${body.display_name}'s Workspace`,
        kind: "PERSONAL",
        ownerUserId: userId,
        status: "ACTIVE",
      });
      await tx.insert(orgMembers).values({
        orgId,
        userId,
        role: "OWNER",
      });
      await tx.insert(projects).values({
        id: projectId,
        orgId,
        gatewayProjectId: projectId,
        name: "Default Project",
        currency: "USD",
        gatewaySynced: false,
        syncStatus: "PENDING",
      });
      return issueSessionInTx(tx, userId, c.req.header("user-agent") ?? null, clientIpOf(c));
    });

    await recordAuditLog(deps.orm, {
      actor: {
        id: userId,
        email,
        displayName: body.display_name,
        isPlatformAdmin: isAdmin,
        status: "ACTIVE",
        sessionId: session.session_id,
      },
      orgId,
      action: "auth.register",
      targetType: "user",
      targetId: userId,
    });

    return c.json(
      {
        user: {
          id: userId,
          email,
          display_name: body.display_name,
          is_platform_admin: isAdmin,
        },
        default_org_id: orgId,
        default_project_id: projectId,
        ...session,
      },
      201,
    );
  });

  app.post("/login", async (c) => {
    const body = await parseJsonBody(c, loginSchema);
    const email = body.email.trim().toLowerCase();
    const ip = clientIpOf(c);
    const emailKey = `email:${email}`;
    const ipKey = `ip:${ip}`;
    deps.loginLimiter.check(emailKey);
    if (ip !== "direct") deps.loginLimiter.check(ipKey);

    const [user] = await deps.orm.select().from(users).where(eq(users.email, email)).limit(1);
    if (!user || !(await verifyPassword(body.password, user.passwordHash))) {
      deps.loginLimiter.recordFailure(emailKey);
      if (ip !== "direct") deps.loginLimiter.recordFailure(ipKey);
      throw new GatewayError(401, "invalid_credentials", "Invalid email or password");
    }
    if (user.status !== "ACTIVE") {
      throw new GatewayError(403, "account_suspended", "User account is suspended");
    }
    deps.loginLimiter.reset(emailKey);

    const session = await issueSessionInTx(
      deps.orm,
      user.id,
      c.req.header("user-agent") ?? null,
      ip,
    );
    return c.json({
      user: {
        id: user.id,
        email: user.email,
        display_name: user.displayName,
        is_platform_admin: user.isPlatformAdmin,
      },
      ...session,
    });
  });

  app.post("/refresh", async (c) => {
    const body = await parseJsonBody(c, refreshSchema);
    const refreshHash = sha256Hex(body.refresh_token);
    const userAgent = c.req.header("user-agent") ?? null;
    const ip = clientIpOf(c);

    const rotated = await deps.orm.transaction(async (tx) => {
      const claimed = await tx
        .update(sessions)
        .set({ revokedAt: sql`now()` })
        .where(
          and(
            eq(sessions.refreshTokenHash, refreshHash),
            isNull(sessions.revokedAt),
            sql`${sessions.refreshExpiresAt} > now()`,
          ),
        )
        .returning({ id: sessions.id, userId: sessions.userId });
      const row = claimed[0];
      if (!row) {
        throw new GatewayError(401, "invalid_refresh_token", "Invalid or expired refresh token");
      }
      const [user] = await tx.select().from(users).where(eq(users.id, row.userId)).limit(1);
      if (!user || user.status !== "ACTIVE") {
        throw new GatewayError(403, "account_suspended", "User account is suspended");
      }
      const next = await issueSessionInTx(tx, user.id, userAgent, ip);
      return { user, next };
    });

    return c.json({
      user: {
        id: rotated.user.id,
        email: rotated.user.email,
        display_name: rotated.user.displayName,
        is_platform_admin: rotated.user.isPlatformAdmin,
      },
      ...rotated.next,
    });
  });

  app.post("/logout", auth, async (c) => {
    const user = c.get("user");
    await deps.orm
      .update(sessions)
      .set({ revokedAt: sql`now()` })
      .where(eq(sessions.id, user.sessionId));
    return c.json({ ok: true });
  });

  app.get("/me", auth, async (c) => {
    const user = c.get("user");
    const memberships = await deps.orm
      .select({
        org_id: organizations.id,
        slug: organizations.slug,
        name: organizations.name,
        kind: organizations.kind,
        role: orgMembers.role,
      })
      .from(orgMembers)
      .innerJoin(organizations, eq(organizations.id, orgMembers.orgId))
      .where(eq(orgMembers.userId, user.id));
    return c.json({
      user: {
        id: user.id,
        email: user.email,
        display_name: user.displayName,
        is_platform_admin: user.isPlatformAdmin,
      },
      organizations: memberships,
    });
  });

  app.post("/change-password", auth, async (c) => {
    const user = c.get("user");
    const body = await parseJsonBody(c, changePasswordSchema);
    const [row] = await deps.orm.select().from(users).where(eq(users.id, user.id)).limit(1);
    if (!row || !(await verifyPassword(body.current_password, row.passwordHash))) {
      throw new GatewayError(400, "invalid_password", "Current password is incorrect");
    }
    const nextHash = await hashPassword(body.new_password);
    await deps.orm.transaction(async (tx) => {
      await tx
        .update(users)
        .set({ passwordHash: nextHash, updatedAt: sql`now()` })
        .where(eq(users.id, user.id));
      await tx
        .update(sessions)
        .set({ revokedAt: sql`now()` })
        .where(
          and(
            eq(sessions.userId, user.id),
            ne(sessions.id, user.sessionId),
            isNull(sessions.revokedAt),
          ),
        );
    });
    await recordAuditLog(deps.orm, {
      actor: user,
      action: "auth.change_password",
      targetType: "user",
      targetId: user.id,
    });
    return c.json({ ok: true });
  });

  return app;
}
