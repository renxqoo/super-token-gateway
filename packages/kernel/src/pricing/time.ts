import type { TimeWindow } from "./types.ts";

export type CompiledTimeWindow = {
  timezone: string;
  isUtc: boolean;
  daysMask: number;
  startMinute: number;
  endMinute: number;
  formatter: Intl.DateTimeFormat | null;
  minuteCache: Map<number, { dayOfWeek: number; minuteOfDay: number }>;
};

const isoTimestampRegex =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/;

const weekdayMap: Record<string, number> = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

const MAX_FORMATTER_CACHE = 512;
const formatterCache = new Map<string, Intl.DateTimeFormat>();

export function parseIsoTimestampMs(raw: string): number {
  if (typeof raw !== "string") {
    throw new TypeError(`Invalid ISO-8601 timestamp: ${String(raw)}`);
  }
  const m = isoTimestampRegex.exec(raw);
  if (!m) {
    throw new TypeError(`Invalid ISO-8601 timestamp: ${raw}`);
  }
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const hour = Number(m[4]);
  const minute = Number(m[5]);
  const second = Number(m[6]);
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 23 || minute > 59 || second > 59) {
    throw new TypeError(`Invalid ISO-8601 calendar timestamp: ${raw}`);
  }
  const checkDate = new Date(Date.UTC(year, month - 1, day));
  if (
    checkDate.getUTCFullYear() !== year ||
    checkDate.getUTCMonth() !== month - 1 ||
    checkDate.getUTCDate() !== day
  ) {
    throw new TypeError(`Invalid ISO-8601 calendar date: ${raw}`);
  }
  if (m[8] !== "Z") {
    const tzHour = Number(m[10]);
    const tzMin = Number(m[11]);
    if (tzHour > 23 || tzMin > 59) {
      throw new TypeError(`Invalid ISO-8601 timezone offset: ${raw}`);
    }
  }
  const ms = Date.parse(raw);
  if (!Number.isFinite(ms)) {
    throw new TypeError(`Invalid ISO-8601 timestamp: ${raw}`);
  }
  return ms;
}

export function isValidIanaTimeZone(timezone: string): boolean {
  if (typeof timezone !== "string" || timezone.length === 0 || timezone.length > 100) {
    return false;
  }
  if (timezone === "UTC" || timezone === "Etc/UTC") return true;
  if (/^[+-]\d{2}:\d{2}$/.test(timezone)) return false;
  if (formatterCache.has(timezone)) return true;
  try {
    const fmt = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    });
    const resolved = fmt.resolvedOptions().timeZone;
    if (resolved !== timezone) return false;
    if (formatterCache.size >= MAX_FORMATTER_CACHE) formatterCache.clear();
    formatterCache.set(timezone, fmt);
    return true;
  } catch {
    return false;
  }
}

function getFormatter(timezone: string): Intl.DateTimeFormat {
  let fmt = formatterCache.get(timezone);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat("en-US", {
      timeZone: timezone,
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    });
    if (formatterCache.size >= MAX_FORMATTER_CACHE) formatterCache.clear();
    formatterCache.set(timezone, fmt);
  }
  return fmt;
}

export function compileTimeWindow(window: TimeWindow): CompiledTimeWindow {
  const isUtc = window.timezone === "UTC" || window.timezone === "Etc/UTC";
  let daysMask = 0;
  if (!window.days_of_week || window.days_of_week.length === 0) {
    daysMask = 0b1111111;
  } else {
    for (const day of window.days_of_week) {
      daysMask |= 1 << day;
    }
  }
  return {
    timezone: window.timezone,
    isUtc,
    daysMask,
    startMinute: window.start_minute,
    endMinute: window.end_minute,
    formatter: isUtc ? null : getFormatter(window.timezone),
    minuteCache: new Map(),
  };
}

export function matchesEffectiveRange(
  ratedAtMs: number,
  fromMs: number | null,
  toMs: number | null,
): boolean {
  if (fromMs !== null && ratedAtMs < fromMs) return false;
  if (toMs !== null && ratedAtMs >= toMs) return false;
  return true;
}

function resolveLocalDayMinute(
  ratedAtMs: number,
  compiled: CompiledTimeWindow,
): { dayOfWeek: number; minuteOfDay: number } {
  const minuteEpoch = Math.floor(ratedAtMs / 60_000);
  if (compiled.isUtc) {
    const totalDays = Math.floor(minuteEpoch / 1440);
    const minuteOfDay = minuteEpoch - totalDays * 1440;
    const dayOfWeek = (((totalDays + 4) % 7) + 7) % 7;
    return { dayOfWeek, minuteOfDay };
  }
  const cached = compiled.minuteCache.get(minuteEpoch);
  if (cached) return cached;
  const parts = compiled.formatter!.formatToParts(new Date(minuteEpoch * 60_000));
  let weekday = 0;
  let hour = 0;
  let minute = 0;
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]!;
    if (part.type === "weekday") weekday = weekdayMap[part.value] ?? 0;
    else if (part.type === "hour") hour = Number(part.value) % 24;
    else if (part.type === "minute") minute = Number(part.value);
  }
  const resolved = { dayOfWeek: weekday, minuteOfDay: hour * 60 + minute };
  if (compiled.minuteCache.size >= 4096) compiled.minuteCache.clear();
  compiled.minuteCache.set(minuteEpoch, resolved);
  return resolved;
}

export function matchesCompiledTimeWindow(
  ratedAtMs: number,
  compiled: CompiledTimeWindow,
): boolean {
  const { dayOfWeek, minuteOfDay } = resolveLocalDayMinute(ratedAtMs, compiled);
  if (compiled.startMinute < compiled.endMinute) {
    if ((compiled.daysMask & (1 << dayOfWeek)) === 0) return false;
    return minuteOfDay >= compiled.startMinute && minuteOfDay < compiled.endMinute;
  }
  if (minuteOfDay >= compiled.startMinute) {
    return (compiled.daysMask & (1 << dayOfWeek)) !== 0;
  }
  if (minuteOfDay < compiled.endMinute) {
    const prevDay = (dayOfWeek + 6) % 7;
    return (compiled.daysMask & (1 << prevDay)) !== 0;
  }
  return false;
}
