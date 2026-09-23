import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import type { CalendarDateValue, CalendarEventPayload } from './adapters/email-calendar.js';

interface ProjectionReceipt {
  status: 'ok' | 'degraded';
  projection_at: string;
  source_contact_at: string | null;
  source_evidence: string | null;
  source_age_seconds: number | null;
  source_messages_considered: number;
  observations_added: number;
  events_inserted: number;
  events_updated: number;
  events_unchanged: number;
  current_events: number;
  cancelled_events: number;
  parse_issues: number;
  latest_source_message_at: string | null;
}

interface MessageRow {
  id: string;
  metadata: string;
  platform_ts: string;
  synced_at: string;
}

interface ExistingEvent {
  sequence: number;
  source_platform_ts: string;
  payload_sha256: string;
}

const WINDOWS_TIMEZONES: Record<string, string> = {
  'Pacific Standard Time': 'America/Vancouver',
  'Mountain Standard Time': 'America/Edmonton',
  'Central Standard Time': 'America/Winnipeg',
  'Eastern Standard Time': 'America/Toronto',
  UTC: 'UTC',
};

function atomicWriteJson(filePath: string, value: unknown): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true, mode: 0o700 });
  const tempPath = `${filePath}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    fs.writeFileSync(tempPath, `${JSON.stringify(value, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
    fs.renameSync(tempPath, filePath);
  } catch (error) {
    if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
    throw error;
  }
}

function eventKey(event: CalendarEventPayload): string {
  return `${event.uid}::${event.recurrence_id?.value ?? ''}`;
}

function payloadDigest(event: CalendarEventPayload): string {
  return crypto.createHash('sha256').update(JSON.stringify(event)).digest('hex');
}

function parseCompactDate(value: string): {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  allDay: boolean;
  utc: boolean;
} | null {
  const match = value.match(/^(\d{4})(\d{2})(\d{2})(?:T(\d{2})(\d{2})(\d{2})?(Z)?)?$/);
  if (!match) return null;
  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
    hour: Number(match[4] ?? 0),
    minute: Number(match[5] ?? 0),
    second: Number(match[6] ?? 0),
    allDay: !match[4],
    utc: Boolean(match[7]),
  };
}

function partsInTimeZone(date: Date, timeZone: string): Record<string, number> {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  return Object.fromEntries(
    formatter.formatToParts(date)
      .filter(part => part.type !== 'literal')
      .map(part => [part.type, Number(part.value)]),
  );
}

function zonedToUtc(parts: ReturnType<typeof parseCompactDate> & object, timeZone: string): Date {
  const intended = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  let candidate = intended;
  for (let attempt = 0; attempt < 2; attempt++) {
    const actual = partsInTimeZone(new Date(candidate), timeZone);
    const represented = Date.UTC(actual.year, actual.month - 1, actual.day, actual.hour, actual.minute, actual.second);
    candidate += intended - represented;
  }
  return new Date(candidate);
}

export function normalizeCalendarDate(value: CalendarDateValue | null): {
  iso: string | null;
  all_day: boolean;
  issue: string | null;
} {
  if (!value) return { iso: null, all_day: false, issue: null };
  const parsed = parseCompactDate(value.value);
  if (!parsed) return { iso: null, all_day: false, issue: `invalid-date:${value.value}` };
  if (parsed.allDay) {
    return {
      iso: `${String(parsed.year).padStart(4, '0')}-${String(parsed.month).padStart(2, '0')}-${String(parsed.day).padStart(2, '0')}`,
      all_day: true,
      issue: null,
    };
  }
  if (parsed.utc) {
    return {
      iso: new Date(Date.UTC(parsed.year, parsed.month - 1, parsed.day, parsed.hour, parsed.minute, parsed.second)).toISOString(),
      all_day: false,
      issue: null,
    };
  }
  const timeZone = value.timezone ? (WINDOWS_TIMEZONES[value.timezone] ?? value.timezone) : null;
  if (!timeZone) return { iso: null, all_day: false, issue: `floating-time:${value.value}` };
  try {
    return { iso: zonedToUtc(parsed, timeZone).toISOString(), all_day: false, issue: null };
  } catch {
    return { iso: null, all_day: false, issue: `unknown-timezone:${value.timezone}` };
  }
}

function sourceHealth(healthPath: string): { observedAt: string | null; evidence: string | null } {
  try {
    const health = JSON.parse(fs.readFileSync(healthPath, 'utf8'));
    const email = health?.adapters?.email;
    return {
      observedAt: email?.source_observed_at ?? null,
      evidence: email?.source_evidence ?? null,
    };
  } catch {
    return { observedAt: null, evidence: null };
  }
}

export function projectGmailSchedule(options: {
  messagesDbPath: string;
  scheduleDbPath: string;
  receiptPath: string;
  healthPath: string;
}): ProjectionReceipt {
  const source = new Database(options.messagesDbPath, { readonly: true, fileMustExist: true });
  const target = new Database(options.scheduleDbPath);
  target.pragma('journal_mode = WAL');
  target.pragma('synchronous = FULL');
  target.exec(`
    CREATE TABLE IF NOT EXISTS schedule_observations (
      observation_id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL,
      payload_sha256 TEXT NOT NULL,
      source_platform_ts TEXT NOT NULL,
      source_synced_at TEXT NOT NULL,
      raw_ref TEXT,
      raw_sha256 TEXT,
      payload_json TEXT NOT NULL,
      projected_at TEXT NOT NULL,
      UNIQUE(message_id, payload_sha256)
    );
    CREATE INDEX IF NOT EXISTS idx_schedule_observations_message
      ON schedule_observations(message_id);
    CREATE TABLE IF NOT EXISTS schedule_events (
      event_key TEXT PRIMARY KEY,
      calendar_uid TEXT NOT NULL,
      recurrence_id_raw TEXT,
      recurrence_timezone TEXT,
      method TEXT,
      sequence INTEGER NOT NULL,
      status TEXT,
      summary TEXT,
      description TEXT,
      start_raw TEXT,
      start_timezone TEXT,
      start_iso TEXT,
      end_raw TEXT,
      end_timezone TEXT,
      end_iso TEXT,
      all_day INTEGER NOT NULL DEFAULT 0,
      location TEXT,
      organizer_email TEXT,
      organizer_name TEXT,
      attendees_json TEXT NOT NULL,
      conference_url TEXT,
      source_message_id TEXT NOT NULL,
      source_platform_ts TEXT NOT NULL,
      raw_ref TEXT,
      payload_sha256 TEXT NOT NULL,
      projected_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_schedule_events_start ON schedule_events(start_iso);
    CREATE INDEX IF NOT EXISTS idx_schedule_events_status ON schedule_events(status);
  `);

  const rows = source.prepare(`
    SELECT id, metadata, platform_ts, synced_at
    FROM messages
    WHERE platform = 'email'
      AND json_type(metadata, '$.calendar_events') = 'array'
    ORDER BY platform_ts, id
  `).all() as MessageRow[];

  const insertObservation = target.prepare(`
    INSERT OR IGNORE INTO schedule_observations
      (observation_id, message_id, payload_sha256, source_platform_ts,
       source_synced_at, raw_ref, raw_sha256, payload_json, projected_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const existingEvent = target.prepare(`
    SELECT sequence, source_platform_ts, payload_sha256
    FROM schedule_events WHERE event_key = ?
  `);
  const upsertEvent = target.prepare(`
    INSERT INTO schedule_events
      (event_key, calendar_uid, recurrence_id_raw, recurrence_timezone, method,
       sequence, status, summary, description, start_raw, start_timezone,
       start_iso, end_raw, end_timezone, end_iso, all_day, location,
       organizer_email, organizer_name, attendees_json, conference_url,
       source_message_id, source_platform_ts, raw_ref, payload_sha256, projected_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(event_key) DO UPDATE SET
      method = excluded.method,
      sequence = excluded.sequence,
      status = excluded.status,
      summary = excluded.summary,
      description = excluded.description,
      start_raw = excluded.start_raw,
      start_timezone = excluded.start_timezone,
      start_iso = excluded.start_iso,
      end_raw = excluded.end_raw,
      end_timezone = excluded.end_timezone,
      end_iso = excluded.end_iso,
      all_day = excluded.all_day,
      location = excluded.location,
      organizer_email = excluded.organizer_email,
      organizer_name = excluded.organizer_name,
      attendees_json = excluded.attendees_json,
      conference_url = excluded.conference_url,
      source_message_id = excluded.source_message_id,
      source_platform_ts = excluded.source_platform_ts,
      raw_ref = excluded.raw_ref,
      payload_sha256 = excluded.payload_sha256,
      projected_at = excluded.projected_at
    WHERE excluded.sequence > schedule_events.sequence
       OR (excluded.sequence = schedule_events.sequence
           AND schedule_events.method != 'CANCEL'
           AND excluded.method = 'CANCEL')
       OR (excluded.sequence = schedule_events.sequence
           AND excluded.method = schedule_events.method
           AND excluded.source_platform_ts > schedule_events.source_platform_ts)
  `);

  const projectionAt = new Date().toISOString();
  let observationsAdded = 0;
  let eventsInserted = 0;
  let eventsUpdated = 0;
  let eventsUnchanged = 0;
  let parseIssues = 0;
  let latestSourceMessageAt: string | null = null;

  const project = target.transaction(() => {
    for (const row of rows) {
      const metadata = JSON.parse(row.metadata);
      const events = metadata.calendar_events as CalendarEventPayload[];
      const capture = metadata.calendar_capture ?? {};
      parseIssues += Array.isArray(capture.parse_issues) ? capture.parse_issues.length : 0;
      if (!latestSourceMessageAt || row.platform_ts > latestSourceMessageAt) latestSourceMessageAt = row.platform_ts;

      for (const event of events) {
        const digest = payloadDigest(event);
        const observationId = crypto.createHash('sha256').update(`${row.id}\n${digest}`).digest('hex');
        const observation = insertObservation.run(
          observationId,
          row.id,
          digest,
          row.platform_ts,
          row.synced_at,
          capture.raw_ref ?? null,
          capture.raw_sha256 ?? null,
          JSON.stringify(event),
          projectionAt,
        );
        observationsAdded += observation.changes;

        const key = eventKey(event);
        const before = existingEvent.get(key) as ExistingEvent | undefined;
        const start = normalizeCalendarDate(event.start);
        const end = normalizeCalendarDate(event.end);
        if (start.issue) parseIssues++;
        if (end.issue) parseIssues++;
        const normalizedStatus = event.method === 'CANCEL'
          ? 'cancelled'
          : (event.status?.toLowerCase() ?? 'confirmed');
        const result = upsertEvent.run(
          key,
          event.uid,
          event.recurrence_id?.value ?? null,
          event.recurrence_id?.timezone ?? null,
          event.method,
          event.sequence,
          normalizedStatus,
          event.summary,
          event.description,
          event.start?.value ?? null,
          event.start?.timezone ?? null,
          start.iso,
          event.end?.value ?? null,
          event.end?.timezone ?? null,
          end.iso,
          start.all_day ? 1 : 0,
          event.location,
          event.organizer?.address ?? null,
          event.organizer?.name ?? null,
          JSON.stringify(event.attendees),
          event.conference_url,
          row.id,
          row.platform_ts,
          capture.raw_ref ?? null,
          digest,
          projectionAt,
        );
        if (!before && result.changes) eventsInserted++;
        else if (before && result.changes) eventsUpdated++;
        else eventsUnchanged++;
      }
    }
  });

  try {
    project();
    const counts = target.prepare(`
      SELECT COUNT(*) AS total,
             SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
      FROM schedule_events
    `).get() as { total: number; cancelled: number | null };
    const health = sourceHealth(options.healthPath);
    const sourceAgeSeconds = health.observedAt
      ? Math.max(0, Math.floor((Date.parse(projectionAt) - Date.parse(health.observedAt)) / 1000))
      : null;
    const sourceIsCurrent = sourceAgeSeconds !== null && Number.isFinite(sourceAgeSeconds) && sourceAgeSeconds <= 300;
    const receipt: ProjectionReceipt = {
      status: parseIssues || !sourceIsCurrent || !health.evidence ? 'degraded' : 'ok',
      projection_at: projectionAt,
      source_contact_at: health.observedAt,
      source_evidence: health.evidence,
      source_age_seconds: sourceAgeSeconds,
      source_messages_considered: rows.length,
      observations_added: observationsAdded,
      events_inserted: eventsInserted,
      events_updated: eventsUpdated,
      events_unchanged: eventsUnchanged,
      current_events: counts.total,
      cancelled_events: counts.cancelled ?? 0,
      parse_issues: parseIssues,
      latest_source_message_at: latestSourceMessageAt,
    };
    atomicWriteJson(options.receiptPath, receipt);
    return receipt;
  } finally {
    source.close();
    target.close();
  }
}
