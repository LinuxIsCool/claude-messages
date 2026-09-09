/**
 * The address book: names Shawn typed himself.
 *
 * Under task-885 D7 these are the name authority. A Signal profile name, a
 * Telegram username and an email display name are all guesses a platform made
 * about a person; an address book entry is what Shawn calls them. Under D3 that
 * makes it a `curated` claim, and a machine may overwrite `derived` names but
 * never curated ones.
 *
 * This module is pure: parsing and normalisation only, no database. Storage and
 * the naming pass live in db.ts, which imports from here.
 */

/** The default calling region for a bare national number, as digits without `+`. */
export const DEFAULT_CALLING_CODE = '1';

/**
 * A phone number in E.164-ish form: `+` followed by digits.
 *
 * Not a full E.164 validator, deliberately. The corpus holds numbers from at
 * least Canada, the US, India and Turkey, and Telegram shortcodes like `42777`
 * that are not phone numbers at all. The job here is to make two spellings of
 * the same number compare equal, not to reject the strange ones.
 *
 * The only inference made is the national-number case: ten digits with no `+`
 * is a NANP number and gains `+1`. Every other length is passed through with a
 * `+`, because guessing a country for an 8- or 9-digit string would invent a
 * person's number rather than normalise it.
 */
export function normalizePhone(raw: string): string | null {
  const trimmed = (raw ?? '').trim();
  if (!trimmed) return null;
  const explicitIntl = trimmed.startsWith('+') || trimmed.startsWith('00');
  const digits = trimmed.replace(/\D/g, '');
  if (!digits) return null;
  if (explicitIntl) {
    // `00` is the ITU international prefix; strip it so +1604… and 001604… agree.
    const body = trimmed.startsWith('00') ? digits.replace(/^00/, '') : digits;
    return body ? '+' + body : null;
  }
  if (digits.length === 10) return '+' + DEFAULT_CALLING_CODE + digits;
  return '+' + digits;
}

export function normalizeEmail(raw: string): string | null {
  const v = (raw ?? '').trim().toLowerCase();
  return v.includes('@') ? v : null;
}

/**
 * True when a display name names nobody.
 *
 * These are the names the address book may overwrite: a bare phone number, a
 * UUID, a raw platform id, a Telegram `User 2028622406` placeholder, a handle
 * standing in for a name. Everything else is a name some source asserted, and
 * replacing one asserted name with another is a decision for merge review, not
 * for an importer (task-885 P7).
 *
 * Deliberately conservative. A false positive rewrites a real name; a false
 * negative only leaves a row for review, which is where it was going anyway.
 */
export function namesNobody(name: string): boolean {
  const n = (name ?? '').trim();
  if (!n) return true;
  if (/^\+?[0-9][0-9\s\-().]{5,}$/.test(n)) return true;                    // a phone number
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-/i.test(n)) return true;                     // a UUID
  if (/^(user|phone|group|channel):/i.test(n)) return true;                  // a raw platform id
  if (/^(user|unknown|contact|unnamed)\s*[#_-]?\s*[0-9]+$/i.test(n)) return true; // "User 2028622406"
  if (/^@?[a-z0-9_.]+$/i.test(n) && !/\s/.test(n) && /[0-9_]/.test(n)) return true; // handle-shaped
  return false;
}

export interface AddressBookHandle {
  kind: 'phone' | 'email';
  value: string;      // normalised
  raw_value: string;  // exactly as the export wrote it
  label: string | null;
}

export interface AddressBookEntry {
  full_name: string;
  given_name: string | null;
  family_name: string | null;
  nickname: string | null;
  organization: string | null;
  labels: string | null;
  handles: AddressBookHandle[];
}

/** Google Contacts packs repeated values into one cell separated by ` ::: `. */
function splitMulti(cell: string): string[] {
  return (cell ?? '').split(':::').map(s => s.trim()).filter(Boolean);
}

function firstNonEmpty(...vals: Array<string | undefined>): string | null {
  for (const v of vals) {
    const t = (v ?? '').trim();
    if (t) return t;
  }
  return null;
}

/**
 * Rows from a Google Contacts CSV export ("Google CSV" format) to entries.
 *
 * The export has no stable contact id, which is why the importer replaces a
 * source wholesale rather than upserting: with no key, a rename and a new
 * person are indistinguishable, and merging would accumulate both.
 *
 * A row with no usable name is skipped rather than stored under a placeholder.
 * The whole point of this table is that its names are trustworthy, so an entry
 * whose name would have to be invented does not belong in it.
 */
export function entriesFromGoogleRows(rows: Array<Record<string, string>>): AddressBookEntry[] {
  const out: AddressBookEntry[] = [];
  for (const row of rows) {
    const given = firstNonEmpty(row['First Name']);
    const middle = firstNonEmpty(row['Middle Name']);
    const family = firstNonEmpty(row['Last Name']);
    const nickname = firstNonEmpty(row['Nickname']);
    const organization = firstNonEmpty(row['Organization Name']);
    const composed = [given, middle, family].filter(Boolean).join(' ').trim();
    const full_name = composed || firstNonEmpty(row['File As'], nickname ?? undefined, organization ?? undefined);
    if (!full_name) continue;

    const handles: AddressBookHandle[] = [];
    const seen = new Set<string>();
    for (const [key, cell] of Object.entries(row)) {
      const phoneMatch = /^Phone (\d+) - Value$/.exec(key);
      const emailMatch = /^E-mail (\d+) - Value$/.exec(key);
      if (!phoneMatch && !emailMatch) continue;
      const kind: 'phone' | 'email' = phoneMatch ? 'phone' : 'email';
      const label = firstNonEmpty(row[key.replace(/ - Value$/, ' - Label')]);
      for (const rawValue of splitMulti(cell)) {
        const value = kind === 'phone' ? normalizePhone(rawValue) : normalizeEmail(rawValue);
        if (!value) continue;
        const dedupeKey = `${kind}:${value}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        handles.push({ kind, value, raw_value: rawValue, label });
      }
    }

    out.push({
      full_name,
      given_name: given,
      family_name: family,
      nickname,
      organization,
      labels: firstNonEmpty(row['Labels']),
      handles,
    });
  }
  return out;
}

/**
 * A minimal RFC 4180 CSV reader.
 *
 * Written here rather than pulled in as a dependency because the shape needed
 * is small and fully specified: quoted fields, doubled quotes inside them, and
 * newlines inside quotes (Google puts them in Notes). Everything else in this
 * bundle is `external` in build.mjs, and adding a runtime dependency for eleven
 * lines of state machine would be the more expensive choice.
 */
export function parseCsv(text: string): Array<Record<string, string>> {
  const stripped = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const rows: string[][] = [];
  let field = '';
  let record: string[] = [];
  let inQuotes = false;
  for (let i = 0; i < stripped.length; i++) {
    const ch = stripped[i];
    if (inQuotes) {
      if (ch === '"') {
        if (stripped[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += ch;
      continue;
    }
    if (ch === '"') { inQuotes = true; continue; }
    if (ch === ',') { record.push(field); field = ''; continue; }
    if (ch === '\r') continue;
    if (ch === '\n') { record.push(field); rows.push(record); record = []; field = ''; continue; }
    field += ch;
  }
  if (field !== '' || record.length) { record.push(field); rows.push(record); }
  if (!rows.length) return [];

  const header = rows[0];
  return rows.slice(1)
    .filter(r => r.some(c => c.trim() !== ''))
    .map(r => Object.fromEntries(header.map((h, i) => [h, r[i] ?? ''])));
}
