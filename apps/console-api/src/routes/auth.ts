import { Hono } from "hono";
import { and, eq, isNull, ne, sql } from "drizzle-orm";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import {
  assertAccountCanUseScope,
  burnDummyPasswordVerify,
  constantTimeEquals,
  generateOpaqueToken,
  hashPassword,
  recordAuditLog,
  sha256Hex,
  verifyPassword,
} from "../auth.ts";
import {
  organizations,
  orgMembers,
  projects,
  sessions,
  users,
  type SessionScope,
} from "../db/schema.ts";
import {
  getClientIp,
  parseJsonBody,
  requireClientAuth,
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

async function issueSessionInTx(
  deps: ConsoleDeps,
  tx: Pick<typeof deps.orm, "insert">,
  userId: string,
  scope: SessionScope,
  userAgent: string | null,
  ipAddress: string | null,
) {
  const now = Date.now();
  const accessToken = generateOpaqueToken(scope === "ADMIN" ? "stg_adm_" : "stg_sess_");
  const refreshToken = generateOpaqueToken(scope === "ADMIN" ? "stg_aref_" : "stg_ref_");
  const expiresAt = new Date(now + deps.config.sessionTtlMs);
  const refreshExpiresAt = new Date(now + deps.config.refreshTtlMs);
  const id = crypto.randomUUID();
  await tx.insert(sessions).values({
    id,
    userId,
    scope,
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
    expires_at: expiresAt.toISOString(),
    refresh_expires_at: refreshExpiresAt.toISOString(),
  };
}

async function rotateSession(
  deps: ConsoleDeps,
  refreshToken: string,
  expectedScope: SessionScope,
  userAgent: string | null,
  ip: string | null,
) {
  const refreshHash = sha256Hex(refreshToken);
  return deps.orm.transaction(async (tx) => {
    const [candidate] = await tx
      .select({ userId: sessions.userId })
      .from(sessions)
      .where(
        and(
          eq(sessions.refreshTokenHash, refreshHash),
          eq(sessions.scope, expectedScope),
          isNull(sessions.revokedAt),
          sql`${sessions.refreshExpiresAt} > now()`,
        ),
      );
    if (!candidate)
      throw new GatewayError(401, "invalid_refresh_token", "Invalid or expired refresh token");
    const [user] = await tx
      .select()
      .from(users)
      .where(eq(users.id, candidate.userId))
      .for("update");
    if (!user)
      throw new GatewayError(401, "invalid_refresh_token", "Invalid or expired refresh token");
    const claimed = await tx
      .update(sessions)
      .set({ revokedAt: sql`now()` })
      .where(
        and(
          eq(sessions.refreshTokenHash, refreshHash),
          eq(sessions.scope, expectedScope),
          isNull(sessions.revokedAt),
          sql`${sessions.refreshExpiresAt} > now()`,
        ),
      )
      .returning({ id: sessions.id, userId: sessions.userId });
    const row = claimed[0];
    if (!row) {
      throw new GatewayError(401, "invalid_refresh_token", "Invalid or expired refresh token");
    }
    assertAccountCanUseScope(user, expectedScope);
    const next = await issueSessionInTx(deps, tx, user.id, expectedScope, userAgent, ip);
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
  if (
    !row ||
    !(await deps.loginLimiter.runPasswordWork(() =>
      verifyPassword(currentPassword, row.passwordHash),
    ))
  ) {
    throw new GatewayError(400, "invalid_password", "Current password is incorrect");
  }
  const nextHash = await deps.loginLimiter.runPasswordWork(() => hashPassword(newPassword));
  await deps.orm.transaction(async (tx) => {
    const [changed] = await tx
      .update(users)
      .set({ passwordHash: nextHash, updatedAt: sql`now()` })
      .where(
        and(
          eq(users.id, user.id),
          eq(users.passwordHash, row.passwordHash),
          eq(users.status, "ACTIVE"),
          eq(users.isPlatformAdmin, user.sessionScope === "ADMIN"),
        ),
      )
      .returning({ id: users.id });
    if (!changed) throw new GatewayError(400, "invalid_password", "Current password is incorrect");
    const [activeSession] = await tx
      .select({ id: sessions.id })
      .from(sessions)
      .where(
        and(
          eq(sessions.id, user.sessionId),
          isNull(sessions.revokedAt),
          sql`${sessions.expiresAt} > now()`,
        ),
      )
      .for("update");
    if (!activeSession) throw new GatewayError(401, "unauthorized", "Invalid or expired session");
    await tx
      .update(sessions)
      .set({ revokedAt: sql`now()` })
      .where(
        and(
          eq(sessions.userId, user.id),
          eq(sessions.scope, user.sessionScope),
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

async function loginWithPassword(
  deps: ConsoleDeps,
  scope: SessionScope,
  email: string,
  password: string,
  userAgent: string | null,
  ip: string | null,
) {
  const prefix = scope === "ADMIN" ? "admin-" : "";
  const emailKey = `${prefix}email:${email}`;
  const ipKey = ip ? `${prefix}ip:${ip}` : null;
  const keys = ipKey ? [emailKey, ipKey] : [emailKey];
  const user = await deps.loginLimiter.verifyAttempt(keys, async () => {
    const [account] = await deps.orm.select().from(users).where(eq(users.email, email)).limit(1);
    if (!account) {
      await burnDummyPasswordVerify(password);
      return undefined;
    }
    return (await verifyPassword(password, account.passwordHash)) ? account : undefined;
  });
  if (!user) throw new GatewayError(401, "invalid_credentials", "Invalid email or password");
  const result = await deps.orm.transaction(async (tx) => {
    const [current] = await tx
      .select()
      .from(users)
      .where(and(eq(users.id, user.id), eq(users.passwordHash, user.passwordHash)))
      .for("update");
    if (!current) throw new GatewayError(401, "invalid_credentials", "Invalid email or password");
    assertAccountCanUseScope(current, scope);
    const session = await issueSessionInTx(deps, tx, user.id, scope, userAgent, ip);
    return {
      user: {
        id: current.id,
        email: current.email,
        display_name: current.displayName,
        is_platform_admin: current.isPlatformAdmin,
      },
      ...session,
    };
  });
  deps.loginLimiter.reset(emailKey);
  return result;
}

export function createClientAuthRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();
  const auth = requireClientAuth(deps);

  app.post("/register", async (c) => {
    const body = await parseJsonBody(c, registerSchema);
    const email = body.email.trim().toLowerCase();
    if (deps.config.bootstrapAdminEmail && email === deps.config.bootstrapAdminEmail) {
      throw new GatewayError(403, "reserved_email", "This email address is reserved");
    }
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
    const passwordHash = await deps.loginLimiter.runPasswordWork(() => hashPassword(body.password));
    const slug = `personal-${userId.replaceAll("-", "").slice(0, 16)}`;

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
        "CLIENT",
        c.req.header("user-agent") ?? null,
        getClientIp(deps, c),
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
        sessionScope: "CLIENT",
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
    return c.json(
      await loginWithPassword(
        deps,
        "CLIENT",
        body.email.trim().toLowerCase(),
        body.password,
        c.req.header("user-agent") ?? null,
        getClientIp(deps, c),
      ),
    );
  });

  app.post("/refresh", async (c) => {
    const body = await parseJsonBody(c, refreshSchema);
    const rotated = await rotateSession(
      deps,
      body.refresh_token,
      "CLIENT",
      c.req.header("user-agent") ?? null,
      getClientIp(deps, c),
    );
    return c.json({
      user: {
        id: rotated.user.id,
        email: rotated.user.email,
        display_name: rotated.user.displayName,
        is_platform_admin: false,
      },
      ...rotated.next,
    });
  });

  app.post("/logout", auth, async (c) => {
    const user = c.get("user");
    await deps.orm
      .update(sessions)
      .set({ revokedAt: sql`now()` })
      .where(and(eq(sessions.id, user.sessionId), eq(sessions.scope, "CLIENT")));
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
        is_platform_admin: false,
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
    const ip = getClientIp(deps, c);
    const bootstrapKey = `admin-bootstrap:${ip ?? "local"}`;
    deps.loginLimiter.check(bootstrapKey);

    if (!deps.config.bootstrapAdminEmail || !deps.config.bootstrapAdminToken) {
      throw new GatewayError(403, "forbidden", "Bootstrap administrator is not configured");
    }
    if (
      email !== deps.config.bootstrapAdminEmail ||
      !body.bootstrap_token ||
      !constantTimeEquals(body.bootstrap_token, deps.config.bootstrapAdminToken)
    ) {
      deps.loginLimiter.recordFailure(bootstrapKey);
      throw new GatewayError(403, "forbidden", "Invalid bootstrap administrator credentials");
    }
    deps.loginLimiter.reset(bootstrapKey);

    const userId = crypto.randomUUID();
    const passwordHash = await deps.loginLimiter.runPasswordWork(() => hashPassword(body.password));

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
      return issueSessionInTx(deps, tx, userId, "ADMIN", c.req.header("user-agent") ?? null, ip);
    });

    await recordAuditLog(deps.orm, {
      actor: {
        id: userId,
        email,
        displayName: body.display_name,
        isPlatformAdmin: true,
        status: "ACTIVE",
        sessionId: session.session_id,
        sessionScope: "ADMIN",
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
    return c.json(
      await loginWithPassword(
        deps,
        "ADMIN",
        body.email.trim().toLowerCase(),
        body.password,
        c.req.header("user-agent") ?? null,
        getClientIp(deps, c),
      ),
    );
  });

  app.post("/refresh", async (c) => {
    const body = await parseJsonBody(c, refreshSchema);
    const rotated = await rotateSession(
      deps,
      body.refresh_token,
      "ADMIN",
      c.req.header("user-agent") ?? null,
      getClientIp(deps, c),
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
      .where(and(eq(sessions.id, user.sessionId), eq(sessions.scope, "ADMIN")));
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
