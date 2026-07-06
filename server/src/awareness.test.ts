import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';
import { isQuietHours, dedupeByThread, formatNotification } from './awareness.js';
import type { InboxEntry } from './types.js';

const now = new Date().toISOString();

function seedCritical(db: MessageDB, id: string, thread: string) {
  (db as any).db.prepare(
    `INSERT INTO messages (id, platform, thread_id, sender_id, content, content_type, platform_ts, synced_at, direction)
     VALUES (?, 'signal', ?, 'signal:x', ?, 'text', ?, ?, 'received')`
  ).run(id, thread, 'urgent thing', now, now);
}

describe('awareness DB layer', () => {
  let db: MessageDB;
  beforeEach(() => {
    db = new MessageDB(':memory:');
    db.addPriorityRule('thread', 'signal:gpu-thread', 'critical');
    seedCritical(db, 'c1', 'signal:gpu-thread');
    seedCritical(db, 'c2', 'signal:gpu-thread');
    db.rescoreAllPriority();
  });

  it('returns unnotified critical messages, then excludes them once marked', () => {
    const before = db.getUnnotifiedCritical();
    expect(before.map(e => e.message_id).sort()).toEqual(['c1', 'c2']);
    db.markNotified(['c1', 'c2']);
    expect(db.getUnnotifiedCritical()).toHaveLength(0);
  });

  it('awarenessCounts reports unseen critical/exceptional counts', () => {
    const counts = db.awarenessCounts();
    expect(counts.critical).toBe(2);
    expect(counts.exceptional).toBe(0);
  });

  it('marking notified does not change seen/awareness counts', () => {
    db.markNotified(['c1']);
    expect(db.awarenessCounts().critical).toBe(2); // still unseen
  });

  it('awarenessCounts returns zeros on an empty DB (null-safe branch)', () => {
    const fresh = new MessageDB(':memory:');
    expect(fresh.awarenessCounts()).toEqual({ critical: 0, exceptional: 0 });
  });

  it('markNotified([]) is a no-op and does not throw', () => {
    expect(() => db.markNotified([])).not.toThrow();
    expect(db.getUnnotifiedCritical().map(e => e.message_id).sort()).toEqual(['c1', 'c2']);
  });
});

function entry(id: string, thread: string, content = 'hello'): InboxEntry {
  return {
    message_id: id, importance: 0.95, urgency: 0.5, attention: 0.8, tier: 'critical',
    source: 'rule', model_version: null, rationale: null, needs_llm: 1, seen: 0,
    scored_at: now, content, sender_id: 'signal:x', thread_id: thread,
  };
}

describe('awareness pure helpers', () => {
  it('isQuietHours handles a window crossing midnight', () => {
    const q = { start: '22:00', end: '07:00' };
    expect(isQuietHours(new Date('2026-07-06T23:30:00'), q)).toBe(true);
    expect(isQuietHours(new Date('2026-07-06T03:00:00'), q)).toBe(true);
    expect(isQuietHours(new Date('2026-07-06T12:00:00'), q)).toBe(false);
  });

  it('isQuietHours handles a same-day window and no-config', () => {
    expect(isQuietHours(new Date('2026-07-06T13:00:00'), { start: '09:00', end: '17:00' })).toBe(true);
    expect(isQuietHours(new Date('2026-07-06T20:00:00'), { start: '09:00', end: '17:00' })).toBe(false);
    expect(isQuietHours(new Date('2026-07-06T03:00:00'), undefined)).toBe(false);
  });

  it('dedupeByThread keeps one entry per thread, in order', () => {
    const out = dedupeByThread([entry('a', 't1'), entry('b', 't1'), entry('c', 't2')]);
    expect(out.map(e => e.message_id)).toEqual(['a', 'c']);
  });

  it('formatNotification builds title + truncated body', () => {
    const long = 'x'.repeat(200);
    const n = formatNotification(entry('a', 't1', long), 'Carole Anne');
    expect(n.title).toBe('⚡ Carole Anne');
    expect(n.body.length).toBeLessThanOrEqual(140);
  });
});
