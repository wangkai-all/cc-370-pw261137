// Calendar helpers. All business dates are plain UTC calendar dates ("YYYY-MM-DD"),
// so week boundaries never depend on the server's local timezone.
import { ValidationError } from './errors.js';

const DAY_MS = 86_400_000;
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

export function isDateString(value) {
  if (typeof value !== 'string') return false;
  const m = DATE_RE.exec(value);
  if (!m) return false;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (mo < 1 || mo > 12 || d < 1 || d > 31) return false;
  const dt = new Date(Date.UTC(y, mo - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === mo - 1 && dt.getUTCDate() === d;
}

export function assertDate(value, field = 'date') {
  if (!isDateString(value)) {
    throw new ValidationError(`${field} must be a real calendar date in YYYY-MM-DD form`, { field, value });
  }
  return value;
}

export function toUtcMs(date) {
  const m = DATE_RE.exec(date);
  return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

export function fromUtcMs(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

export function addDays(date, n) {
  return fromUtcMs(toUtcMs(date) + n * DAY_MS);
}

export function diffDays(a, b) {
  return Math.round((toUtcMs(b) - toUtcMs(a)) / DAY_MS);
}

/** ISO weekday, Monday = 1 ... Sunday = 7. */
export function isoWeekday(date) {
  const d = new Date(toUtcMs(date)).getUTCDay();
  return d === 0 ? 7 : d;
}

/** Monday of the ISO week containing `date`. */
export function weekStart(date) {
  return addDays(date, 1 - isoWeekday(date));
}

/** ISO-8601 week label ("2026-W38"), using the Thursday rule. */
export function isoWeekLabel(date) {
  const thursday = addDays(date, 4 - isoWeekday(date));
  const year = Number(thursday.slice(0, 4));
  const jan1 = `${year}-01-01`;
  const week = Math.floor(diffDays(jan1, thursday) / 7) + 1;
  return `${year}-W${String(week).padStart(2, '0')}`;
}

export function weekIdOf(date) {
  const start = weekStart(date);
  return { weekId: isoWeekLabel(date), weekStart: start, weekEnd: addDays(start, 6) };
}

export function isMonthStart(date) {
  return date.slice(8, 10) === '01';
}

export function monthOf(date) {
  return date.slice(0, 7);
}

/** Every date from `from` to `to` inclusive. */
export function dateRange(from, to) {
  const out = [];
  for (let d = from; diffDays(d, to) >= 0; d = addDays(d, 1)) out.push(d);
  return out;
}

