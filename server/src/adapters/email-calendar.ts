import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { simpleParser } from 'mailparser';
import type { MessageStructureObject } from 'imapflow';

export interface CalendarDateValue {
  value: string;
  timezone: string | null;
  value_type: string | null;
}

export interface CalendarPersonValue {
  address: string | null;
  name: string | null;
  participation_status?: string | null;
  role?: string | null;
}

export interface CalendarEventPayload {
  uid: string;
  recurrence_id: CalendarDateValue | null;
  method: string | null;
  sequence: number;
  status: string | null;
  summary: string | null;
  description: string | null;
  start: CalendarDateValue | null;
  end: CalendarDateValue | null;
  location: string | null;
  organizer: CalendarPersonValue | null;
  attendees: CalendarPersonValue[];
  conference_url: string | null;
  attachment_sha256: string;
  event_index: number;
}

export interface CalendarCapture {
  raw_ref: string;
  raw_sha256: string;
  raw_bytes: number;
  calendar_attachments: number;
  calendar_events: CalendarEventPayload[];
  parse_issues: string[];
}

interface ContentLine {
  name: string;
  params: Record<string, string>;
  value: string;
}

function safeSegment(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'unknown';
}

function unfoldIcs(text: string): string[] {
  return text
    .replace(/\r\n[ \t]/g, '')
    .replace(/\n[ \t]/g, '')
    .split(/\r?\n/)
    .filter(Boolean);
}

function splitOutsideQuotes(value: string, separator: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let quoted = false;
  for (let i = 0; i < value.length; i++) {
    if (value[i] === '"') quoted = !quoted;
    if (value[i] === separator && !quoted) {
      parts.push(value.slice(start, i));
      start = i + 1;
    }
  }
  parts.push(value.slice(start));
  return parts;
}

function parseContentLine(line: string): ContentLine | null {
  let quoted = false;
  let colon = -1;
  for (let i = 0; i < line.length; i++) {
    if (line[i] === '"') quoted = !quoted;
    if (line[i] === ':' && !quoted) {
      colon = i;
      break;
    }
  }
  if (colon < 1) return null;

  const headerParts = splitOutsideQuotes(line.slice(0, colon), ';');
  const name = headerParts.shift()!.toUpperCase();
  const params: Record<string, string> = {};
  for (const token of headerParts) {
    const equals = token.indexOf('=');
    if (equals < 1) continue;
    const key = token.slice(0, equals).toUpperCase();
    let paramValue = token.slice(equals + 1);
    if (paramValue.startsWith('"') && paramValue.endsWith('"')) {
      paramValue = paramValue.slice(1, -1);
    }
    params[key] = paramValue;
  }
  return { name, params, value: line.slice(colon + 1) };
}

function unescapeText(value: string): string {
  return value
    .replace(/\\[nN]/g, '\n')
    .replace(/\\,/g, ',')
    .replace(/\\;/g, ';')
    .replace(/\\\\/g, '\\');
}

function first(lines: ContentLine[], name: string): ContentLine | null {
  return lines.find(line => line.name === name) ?? null;
}

function calendarDate(line: ContentLine | null): CalendarDateValue | null {
  if (!line) return null;
  return {
    value: line.value,
    timezone: line.params.TZID ?? null,
    value_type: line.params.VALUE ?? null,
  };
}

function calendarPerson(line: ContentLine, attendee = false): CalendarPersonValue {
  const rawAddress = line.value.replace(/^mailto:/i, '').trim();
  return {
    address: rawAddress || null,
    name: line.params.CN ? unescapeText(line.params.CN) : null,
    ...(attendee ? {
      participation_status: line.params.PARTSTAT ?? null,
      role: line.params.ROLE ?? null,
    } : {}),
  };
}

function conferenceUrl(lines: ContentLine[]): string | null {
  for (const name of ['X-MICROSOFT-SKYPETEAMSMEETINGURL', 'URL']) {
    const value = first(lines, name)?.value;
    if (value && /^https?:\/\//i.test(value)) return value;
  }
  const description = first(lines, 'DESCRIPTION')?.value ?? '';
  const match = description.match(/https:\/\/teams\.microsoft\.com\/[^\s\\]+/i);
  return match?.[0] ?? null;
}

export function calendarPartNumbers(structure: MessageStructureObject | undefined): string[] {
  if (!structure) return [];
  const parts: string[] = [];
  const visit = (node: MessageStructureObject): void => {
    const filename = node.dispositionParameters?.filename ?? node.parameters?.name ?? '';
    if (node.part && (node.type.toLowerCase() === 'text/calendar' || /\.ics$/i.test(filename))) {
      parts.push(node.part);
    }
    for (const child of node.childNodes ?? []) visit(child);
  };
  visit(structure);
  return parts;
}

export function parseCalendarText(text: string, attachmentSha256: string): {
  events: CalendarEventPayload[];
  issues: string[];
} {
  const rawLines = unfoldIcs(text);
  const parsedLines = rawLines.map(parseContentLine).filter((line): line is ContentLine => line !== null);
  const method = first(parsedLines, 'METHOD')?.value.toUpperCase() ?? null;
  const eventBlocks: ContentLine[][] = [];
  let current: ContentLine[] | null = null;

  for (const line of parsedLines) {
    if (line.name === 'BEGIN' && line.value.toUpperCase() === 'VEVENT') {
      current = [];
      continue;
    }
    if (line.name === 'END' && line.value.toUpperCase() === 'VEVENT') {
      if (current) eventBlocks.push(current);
      current = null;
      continue;
    }
    if (current) current.push(line);
  }

  const issues: string[] = [];
  const events: CalendarEventPayload[] = [];
  eventBlocks.forEach((lines, eventIndex) => {
    const uid = first(lines, 'UID')?.value.trim();
    if (!uid) {
      issues.push(`event-${eventIndex}:missing-uid`);
      return;
    }
    const sequenceValue = Number.parseInt(first(lines, 'SEQUENCE')?.value ?? '0', 10);
    const organizerLine = first(lines, 'ORGANIZER');
    const statusValue = first(lines, 'STATUS')?.value.toUpperCase() ??
      (method === 'CANCEL' ? 'CANCELLED' : null);
    const summaryValue = first(lines, 'SUMMARY')?.value;
    const descriptionValue = first(lines, 'DESCRIPTION')?.value;
    const locationValue = first(lines, 'LOCATION')?.value;

    events.push({
      uid,
      recurrence_id: calendarDate(first(lines, 'RECURRENCE-ID')),
      method,
      sequence: Number.isFinite(sequenceValue) ? sequenceValue : 0,
      status: statusValue,
      summary: summaryValue ? unescapeText(summaryValue) : null,
      description: descriptionValue ? unescapeText(descriptionValue) : null,
      start: calendarDate(first(lines, 'DTSTART')),
      end: calendarDate(first(lines, 'DTEND')),
      location: locationValue ? unescapeText(locationValue) : null,
      organizer: organizerLine ? calendarPerson(organizerLine) : null,
      attendees: lines.filter(line => line.name === 'ATTENDEE').map(line => calendarPerson(line, true)),
      conference_url: conferenceUrl(lines),
      attachment_sha256: attachmentSha256,
      event_index: eventIndex,
    });
  });

  if (!eventBlocks.length) issues.push('no-vevent');
  return { events, issues };
}

export async function extractCalendarSource(source: Buffer): Promise<{
  attachmentCount: number;
  events: CalendarEventPayload[];
  issues: string[];
}> {
  const parsed = await simpleParser(source);
  const attachments = parsed.attachments.filter(attachment =>
    attachment.contentType.toLowerCase() === 'text/calendar' || /\.ics$/i.test(attachment.filename ?? ''),
  );
  const events: CalendarEventPayload[] = [];
  const issues: string[] = [];
  for (const [index, attachment] of attachments.entries()) {
    const sha256 = crypto.createHash('sha256').update(attachment.content).digest('hex');
    const parsedCalendar = parseCalendarText(attachment.content.toString('utf8'), sha256);
    events.push(...parsedCalendar.events);
    issues.push(...parsedCalendar.issues.map(issue => `attachment-${index}:${issue}`));
  }
  if (!attachments.length) issues.push('calendar-part-not-extracted');
  return { attachmentCount: attachments.length, events, issues };
}

export async function captureCalendarSource(
  dataDir: string,
  accountId: string,
  folder: string,
  uid: number,
  source: Buffer,
): Promise<CalendarCapture> {
  const rawSha256 = crypto.createHash('sha256').update(source).digest('hex');
  const relativeDir = path.posix.join('raw', 'email-calendar', safeSegment(accountId), safeSegment(folder));
  const fileName = `${uid}-${rawSha256.slice(0, 16)}.eml`;
  const rawRef = path.posix.join(relativeDir, fileName);
  const absoluteDir = path.join(dataDir, ...relativeDir.split('/'));
  const absolutePath = path.join(absoluteDir, fileName);
  fs.mkdirSync(absoluteDir, { recursive: true, mode: 0o700 });

  if (fs.existsSync(absolutePath)) {
    const existingSha = crypto.createHash('sha256').update(fs.readFileSync(absolutePath)).digest('hex');
    if (existingSha !== rawSha256) throw new Error(`Calendar capture checksum mismatch: ${rawRef}`);
  } else {
    const tempPath = path.join(absoluteDir, `.${fileName}.${process.pid}.${crypto.randomUUID()}.tmp`);
    try {
      fs.writeFileSync(tempPath, source, { flag: 'wx', mode: 0o600 });
      fs.renameSync(tempPath, absolutePath);
    } catch (error) {
      if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
      throw error;
    }
  }

  const extraction = await extractCalendarSource(source);
  return {
    raw_ref: rawRef,
    raw_sha256: rawSha256,
    raw_bytes: source.length,
    calendar_attachments: extraction.attachmentCount,
    calendar_events: extraction.events,
    parse_issues: extraction.issues,
  };
}
