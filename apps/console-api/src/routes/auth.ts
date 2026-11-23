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
import {
  parseJsonBody,
  requireAuth,
  requirePlatformAdmin,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const registerSchema = z
  .object({
    email: z.email().max(200),
    password: z.string().min(8).max(128),
    display_name: z.string().trim().min(1).max(120),
  })
  .strict();

const bootstrapAdminSchema = z
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

function clientIpOf(
  deps: ConsoleDeps,
  c: { req: { header(name: string): string | undefined } },
): string {
  if (deps.config.trustProxyHeaders) {
    const forwarded = c.req.header("x-forwarded-for")?.split(",")[0]?.trim();
    if (forwarded) return forwarded;
  }
  return "direct";
}

async function issueSessionInTx(
  deps: ConsoleDeps,
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

async function rotateSession(
  deps: ConsoleDeps,
  refreshToken: string,
  userAgent: string | null,
  ip: string | null,
  requireAdmin: boolean,
) {
  const refreshHash = sha256Hex(refreshToken);
  return deps.orm.transaction(async (tx) => {
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
    if (requireAdmin && !user.isPlatformAdmin) {
      throw new GatewayError(403, "forbidden", "Platform administrator privileges required");
    }
    const next = await issueSessionInTx(deps, tx, user.id, userAgent, ip);
    return { user, next };
  });
}

async function changeUserPassword(
  deps: ConsoleDeps,
  user: ConsoleEnv["Variables"]["user"],
  currentPassword: string,
  newPassword: string,
  action: string,
): Promise<void> {
  const [row] = await deps.orm.select().from(users).where(eq(users.id, user.id)).limit(1);
  if (!row || !(await verifyPassword(currentPassword, row.passwordHash))) {
    throw new GatewayError(400, "invalid_password", "Current password is incorrect");
  }
  const nextHash = await hashPassword(newPassword);
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
    action,
    targetType: "user",
    targetId: user.id,
  });
}

export function createClientAuthRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireAuth(deps);

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

    const session = await deps.orm.transaction(async (tx) => {
      await tx.insert(users).values({
        id: userId,
        email,
        passwordHash,
        displayName: body.display_name,
        isPlatformAdmin: false,
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
      return issueSessionInTx(
        deps,
        tx,
        userId,
        c.req.header("user-agent") ?? null,
        clientIpOf(deps, c),
      );
    });

    await recordAuditLog(deps.orm, {
      actor: {
        id: userId,
        email,
        displayName: body.display_name,
        isPlatformAdmin: false,
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
          is_platform_admin: false,
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
    const ip = clientIpOf(deps, c);
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
      deps,
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
    const rotated = await rotateSession(
      deps,
      body.refresh_token,
      c.req.header("user-agent") ?? null,
      clientIpOf(deps, c),
      false,
    );
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
    await changeUserPassword(
      deps,
      user,
      body.current_password,
      body.new_password,
      "auth.change_password",
    );
    return c.json({ ok: true });
  });

  return app;
}

export function createAdminAuthRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const adminOnly = requirePlatformAdmin(deps);

  app.post("/bootstrap", async (c) => {
    const body = await parseJsonBody(c, bootstrapAdminSchema);
    const email = body.email.trim().toLowerCase();
    if (!deps.config.bootstrapAdminEmail || email !== deps.config.bootstrapAdminEmail) {
      throw new GatewayError(
        403,
        "forbidden",
        "Bootstrap admin email does not match configuration",
      );
    }
    if (
      deps.config.bootstrapAdminToken &&
      (!body.bootstrap_token ||
        !constantTimeEquals(body.bootstrap_token, deps.config.bootstrapAdminToken))
    ) {
      throw new GatewayError(403, "forbidden", "Invalid bootstrap admin token");
    }

    const userId = crypto.randomUUID();
    const passwordHash = await hashPassword(body.password);

    const session = await deps.orm.transaction(async (tx) => {
      await tx.execute(sql`SELECT pg_advisory_xact_lock(88421001)`);
      const existingAdmins = await tx
        .select({ id: users.id })
        .from(users)
        .where(eq(users.isPlatformAdmin, true))
        .for("update");
      if (existingAdmins.length > 0) {
        throw new GatewayError(
          409,
          "admin_already_bootstrapped",
          "Platform administrator already exists",
        );
      }
      const existingEmail = await tx
        .select({ id: users.id })
        .from(users)
        .where(eq(users.email, email))
        .limit(1);
      if (existingEmail.length > 0) {
        throw new GatewayError(409, "email_exists", "An account with this email already exists");
      }
      await tx.insert(users).values({
        id: userId,
        email,
        passwordHash,
        displayName: body.display_name,
        isPlatformAdmin: true,
        status: "ACTIVE",
      });
      return issueSessionInTx(
        deps,
        tx,
        userId,
        c.req.header("user-agent") ?? null,
        clientIpOf(deps, c),
      );
    });

    await recordAuditLog(deps.orm, {
      actor: {
        id: userId,
        email,
        displayName: body.display_name,
        isPlatformAdmin: true,
        status: "ACTIVE",
        sessionId: session.session_id,
      },
      action: "admin.auth.bootstrap",
      targetType: "user",
      targetId: userId,
    });

    return c.json(
      {
        user: {
          id: userId,
          email,
          display_name: body.display_name,
          is_platform_admin: true,
        },
        ...session,
      },
      201,
    );
  });

  app.post("/login", async (c) => {
    const body = await parseJsonBody(c, loginSchema);
    const email = body.email.trim().toLowerCase();
    const ip = clientIpOf(deps, c);
    const emailKey = `admin-email:${email}`;
    const ipKey = `admin-ip:${ip}`;
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
    if (!user.isPlatformAdmin) {
      throw new GatewayError(403, "forbidden", "Platform administrator privileges required");
    }
    deps.loginLimiter.reset(emailKey);

    const session = await issueSessionInTx(
      deps,
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
        is_platform_admin: true,
      },
      ...session,
    });
  });

  app.post("/refresh", async (c) => {
    const body = await parseJsonBody(c, refreshSchema);
    const rotated = await rotateSession(
      deps,
      body.refresh_token,
      c.req.header("user-agent") ?? null,
      clientIpOf(deps, c),
      true,
    );
    return c.json({
      user: {
        id: rotated.user.id,
        email: rotated.user.email,
        display_name: rotated.user.displayName,
        is_platform_admin: true,
      },
      ...rotated.next,
    });
  });

  app.post("/logout", adminOnly, async (c) => {
    const user = c.get("user");
    await deps.orm
      .update(sessions)
      .set({ revokedAt: sql`now()` })
      .where(eq(sessions.id, user.sessionId));
    return c.json({ ok: true });
  });

  app.get("/me", adminOnly, (c) => {
    const user = c.get("user");
    return c.json({
      user: {
        id: user.id,
        email: user.email,
        display_name: user.displayName,
        is_platform_admin: true,
      },
    });
  });

  app.post("/change-password", adminOnly, async (c) => {
    const user = c.get("user");
    const body = await parseJsonBody(c, changePasswordSchema);
    await changeUserPassword(
      deps,
      user,
      body.current_password,
      body.new_password,
      "admin.auth.change_password",
    );
    return c.json({ ok: true });
  });

  return app;
}
