import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';

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
