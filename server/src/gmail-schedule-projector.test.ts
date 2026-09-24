import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import { afterEach, describe, expect, it } from 'vitest';
import { normalizeCalendarDate, projectGmailSchedule } from './gmail-schedule-projector.js';

describe('normalizeCalendarDate', () => {
  it('converts the Outlook Pacific timezone identifier using DST', () => {
    expect(normalizeCalendarDate({
      value: '20260918T110000',
      timezone: 'Pacific Standard Time',
      value_type: null,
    })).toEqual({ iso: '2026-09-18T18:00:00.000Z', all_day: false, issue: null });
  });

  it('preserves all-day values', () => {
    expect(normalizeCalendarDate({
      value: '20260918',
      timezone: null,
      value_type: 'DATE',
    })).toEqual({ iso: '2026-09-18', all_day: true, issue: null });
  });
});

describe('projectGmailSchedule', () => {
  const roots: string[] = [];

  afterEach(() => {
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });

  it('projects request then higher-sequence cancellation idempotently', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'gmail-schedule-'));
    roots.push(root);
    const messagesPath = path.join(root, 'messages.db');
    const schedulePath = path.join(root, 'schedule.db');
    const receiptPath = path.join(root, 'receipt.json');
    const healthPath = path.join(root, 'health.json');
    const db = new Database(messagesPath);
    db.exec(`
      CREATE TABLE messages (
        id TEXT PRIMARY KEY, platform TEXT NOT NULL, metadata TEXT NOT NULL,
        platform_ts TEXT NOT NULL, synced_at TEXT NOT NULL
      )
    `);
    const baseEvent = {
      uid: 'event-123',
      recurrence_id: { value: '20260918T110000', timezone: 'Pacific Standard Time', value_type: null },
      method: 'REQUEST',
      sequence: 3,
      status: 'CONFIRMED',
      summary: 'Creative tech team',
      description: null,
      start: { value: '20260918T110000', timezone: 'Pacific Standard Time', value_type: null },
      end: { value: '20260918T113000', timezone: 'Pacific Standard Time', value_type: null },
      location: 'Microsoft Teams Meeting',
      organizer: { address: 'organizer@example.test', name: 'Organizer' },
      attendees: [],
      conference_url: 'https://teams.microsoft.com/meet/example',
      attachment_sha256: 'a'.repeat(64),
      event_index: 0,
    };
    const insert = db.prepare('INSERT INTO messages VALUES (?, ?, ?, ?, ?)');
    insert.run('invite', 'email', JSON.stringify({
      calendar_capture: { raw_ref: 'raw/invite.eml', raw_sha256: '1'.repeat(64), parse_issues: [] },
      calendar_events: [baseEvent],
    }), '2026-09-18T16:00:00.000Z', '2026-09-18T16:01:00.000Z');
    insert.run('cancel', 'email', JSON.stringify({
      calendar_capture: { raw_ref: 'raw/cancel.eml', raw_sha256: '2'.repeat(64), parse_issues: [] },
      calendar_events: [{ ...baseEvent, method: 'CANCEL', sequence: 4, status: 'CANCELLED' }],
    }), '2026-09-18T17:00:00.000Z', '2026-09-18T17:01:00.000Z');
    db.close();
    const sourceObservedAt = new Date().toISOString();
    fs.writeFileSync(healthPath, JSON.stringify({ adapters: { email: {
      source_observed_at: sourceObservedAt,
      source_evidence: 'IMAP successful folder scan',
    } } }));

    const first = projectGmailSchedule({ messagesDbPath: messagesPath, scheduleDbPath: schedulePath, receiptPath, healthPath });
    const second = projectGmailSchedule({ messagesDbPath: messagesPath, scheduleDbPath: schedulePath, receiptPath, healthPath });
    expect(first).toMatchObject({
      status: 'ok',
      observations_added: 2,
      events_inserted: 1,
      events_updated: 1,
      current_events: 1,
      cancelled_events: 1,
      source_contact_at: sourceObservedAt,
      source_age_seconds: 0,
    });
    expect(second).toMatchObject({ observations_added: 0, events_unchanged: 2 });

    const lateDb = new Database(messagesPath);
    lateDb.prepare('INSERT INTO messages VALUES (?, ?, ?, ?, ?)').run(
      'late-request',
      'email',
      JSON.stringify({
        calendar_capture: { raw_ref: 'raw/late.eml', raw_sha256: '3'.repeat(64), parse_issues: [] },
        calendar_events: [{ ...baseEvent, sequence: 4 }],
      }),
      '2026-09-18T18:00:00.000Z',
      '2026-09-18T18:01:00.000Z',
    );
    lateDb.close();
    projectGmailSchedule({ messagesDbPath: messagesPath, scheduleDbPath: schedulePath, receiptPath, healthPath });

    const projected = new Database(schedulePath, { readonly: true });
    const event = projected.prepare('SELECT * FROM schedule_events').get() as Record<string, unknown>;
    expect(event.status).toBe('cancelled');
    expect(event.sequence).toBe(4);
    expect(event.start_iso).toBe('2026-09-18T18:00:00.000Z');
    expect(event.source_message_id).toBe('cancel');
    projected.close();
  });
});
