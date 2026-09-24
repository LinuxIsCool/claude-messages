import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  calendarPartNumbers,
  captureCalendarSource,
  extractCalendarSource,
  parseCalendarText,
} from './email-calendar.js';

const ics = [
  'BEGIN:VCALENDAR',
  'METHOD:CANCEL',
  'BEGIN:VTIMEZONE',
  'TZID:Pacific Standard Time',
  'BEGIN:STANDARD',
  'DTSTART:16010101T020000',
  'END:STANDARD',
  'END:VTIMEZONE',
  'BEGIN:VEVENT',
  'UID:event-123',
  'SEQUENCE:4',
  'STATUS:CANCELLED',
  'RECURRENCE-ID;TZID=Pacific Standard Time:20260918T110000',
  'DTSTART;TZID=Pacific Standard Time:20260918T110000',
  'DTEND;TZID=Pacific Standard Time:20260918T113000',
  'SUMMARY;LANGUAGE=en-US:Canceled: Creative tech team',
  'LOCATION;LANGUAGE=en-US:Microsoft Teams Meeting',
  'ORGANIZER;CN=Carol Anne Hilton:mailto:organizer@example.test',
  'ATTENDEE;CN=Shawn;PARTSTAT=ACCEPTED;ROLE=REQ-PARTICIPANT:mailto:shawn@example.test',
  'X-MICROSOFT-SKYPETEAMSMEETINGURL:https://teams.microsoft.com/meet/example',
  'END:VEVENT',
  'END:VCALENDAR',
].join('\r\n');

function mimeSource(calendar: string): Buffer {
  const boundary = 'calendar-boundary';
  return Buffer.from([
    'Subject: Calendar fixture',
    `Content-Type: multipart/alternative; boundary="${boundary}"`,
    '',
    `--${boundary}`,
    'Content-Type: text/plain; charset=utf-8',
    '',
    'Meeting changed.',
    `--${boundary}`,
    'Content-Type: text/calendar; method=CANCEL; charset=utf-8',
    'Content-Transfer-Encoding: base64',
    '',
    Buffer.from(calendar).toString('base64'),
    `--${boundary}--`,
    '',
  ].join('\r\n'));
}

describe('calendarPartNumbers', () => {
  it('finds nested calendar and ICS attachment parts', () => {
    expect(calendarPartNumbers({
      type: 'multipart/mixed',
      childNodes: [
        { part: '1', type: 'text/plain' },
        { part: '2', type: 'multipart/alternative', childNodes: [
          { part: '2.1', type: 'text/html' },
          { part: '2.2', type: 'text/calendar' },
        ] },
        { part: '3', type: 'application/octet-stream', dispositionParameters: { filename: 'invite.ics' } },
      ],
    })).toEqual(['2.2', '3']);
  });
});

describe('calendar payload parsing', () => {
  it('reads VEVENT time instead of the VTIMEZONE DTSTART', () => {
    const sha = crypto.createHash('sha256').update(ics).digest('hex');
    const parsed = parseCalendarText(ics, sha);
    expect(parsed.issues).toEqual([]);
    expect(parsed.events).toHaveLength(1);
    expect(parsed.events[0]).toMatchObject({
      uid: 'event-123',
      method: 'CANCEL',
      sequence: 4,
      status: 'CANCELLED',
      summary: 'Canceled: Creative tech team',
      start: { value: '20260918T110000', timezone: 'Pacific Standard Time' },
      recurrence_id: { value: '20260918T110000', timezone: 'Pacific Standard Time' },
      organizer: { address: 'organizer@example.test', name: 'Carol Anne Hilton' },
      conference_url: 'https://teams.microsoft.com/meet/example',
    });
    expect(parsed.events[0].attendees[0]).toMatchObject({
      address: 'shawn@example.test',
      name: 'Shawn',
      participation_status: 'ACCEPTED',
    });
  });

  it('extracts a base64 calendar MIME attachment', async () => {
    const extracted = await extractCalendarSource(mimeSource(ics));
    expect(extracted.attachmentCount).toBe(1);
    expect(extracted.issues).toEqual([]);
    expect(extracted.events[0].uid).toBe('event-123');
  });
});

describe('calendar raw capture', () => {
  const roots: string[] = [];

  afterEach(() => {
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it('writes one checksum-addressed raw email and reuses it idempotently', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'email-calendar-'));
    roots.push(root);
    const source = mimeSource(ics);
    const first = await captureCalendarSource(root, 'LTF', 'INBOX', 33664, source);
    const second = await captureCalendarSource(root, 'LTF', 'INBOX', 33664, source);

    expect(second).toEqual(first);
    expect(first.calendar_events).toHaveLength(1);
    expect(first.raw_ref).toMatch(/^raw\/email-calendar\/ltf\/inbox\/33664-[a-f0-9]{16}\.eml$/);
    expect(fs.readFileSync(path.join(root, ...first.raw_ref.split('/')))).toEqual(source);
  });
});
