// Phase 3: Natural language date parser for temporal navigation

const MONTH_NAMES: Record<string, number> = {
  jan: 0, january: 0,
  feb: 1, february: 1,
  mar: 2, march: 2,
  apr: 3, april: 3,
  may: 4,
  jun: 5, june: 5,
  jul: 6, july: 6,
  aug: 7, august: 7,
  sep: 8, september: 8,
  oct: 9, october: 9,
  nov: 10, november: 10,
  dec: 11, december: 11,
};

const RELATIVE_PATTERN = /^(\d+)\s+(minute|hour|day|week|month)s?\s+ago$/i;
const MONTH_YEAR_PATTERN = /^([a-z]+)\s+(\d{4})$/i;
const MONTH_ONLY_PATTERN = /^([a-z]+)$/i;
const YYYY_MM_PATTERN = /^(\d{4})-(\d{2})$/;

/**
 * Parse natural language or ISO date strings into ISO 8601.
 * Returns null if unparseable.
 */
export function parseTemporalRef(input: string): string | null {
  const trimmed = input.trim();
  if (!trimmed) return null;

  // ISO 8601 date or datetime — pass through
  if (/^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
    const d = new Date(trimmed);
    if (!isNaN(d.getTime())) return d.toISOString();
  }

  // YYYY-MM → 1st of that month
  const ym = trimmed.match(YYYY_MM_PATTERN);
  if (ym) {
    const d = new Date(parseInt(ym[1]), parseInt(ym[2]) - 1, 1);
    return d.toISOString();
  }

  const lower = trimmed.toLowerCase();

  // Named constants
  if (lower === 'now') return new Date().toISOString();

  if (lower === 'today') {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  if (lower === 'yesterday') {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  if (lower === 'last week') {
    const d = new Date();
    d.setDate(d.getDate() - 7);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  if (lower === 'last month') {
    const d = new Date();
    d.setMonth(d.getMonth() - 1);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  if (lower === 'this week') {
    const d = new Date();
    const day = d.getDay();
    const diff = day === 0 ? 6 : day - 1; // Monday = 0
    d.setDate(d.getDate() - diff);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  if (lower === 'this month') {
    const d = new Date();
    d.setDate(1);
    d.setHours(0, 0, 0, 0);
    return d.toISOString();
  }

  // "N units ago"
  const relative = lower.match(RELATIVE_PATTERN);
  if (relative) {
    const n = parseInt(relative[1]);
    const unit = relative[2].toLowerCase();
    const d = new Date();
    switch (unit) {
      case 'minute': d.setMinutes(d.getMinutes() - n); break;
      case 'hour': d.setHours(d.getHours() - n); break;
      case 'day': d.setDate(d.getDate() - n); break;
      case 'week': d.setDate(d.getDate() - n * 7); break;
      case 'month': d.setMonth(d.getMonth() - n); break;
    }
    return d.toISOString();
  }

  // "March 2026" or "Mar 2026"
  const monthYear = lower.match(MONTH_YEAR_PATTERN);
  if (monthYear) {
    const monthNum = MONTH_NAMES[monthYear[1].toLowerCase()];
    if (monthNum !== undefined) {
      const d = new Date(parseInt(monthYear[2]), monthNum, 1);
      return d.toISOString();
    }
  }

  // "March" or "Mar" (current or most recent occurrence)
  const monthOnly = lower.match(MONTH_ONLY_PATTERN);
  if (monthOnly) {
    const monthNum = MONTH_NAMES[monthOnly[1].toLowerCase()];
    if (monthNum !== undefined) {
      const now = new Date();
      let year = now.getFullYear();
      // If the month is in the future, use last year
      if (monthNum > now.getMonth()) year--;
      const d = new Date(year, monthNum, 1);
      return d.toISOString();
    }
  }

  return null;
}
