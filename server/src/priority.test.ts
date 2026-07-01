import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';

function tableNames(db: MessageDB): string[] {
  // reach into the raw handle for a schema assertion
  const rows = (db as any).db
    .prepare("SELECT name FROM sqlite_master WHERE type='table'")
    .all() as Array<{ name: string }>;
  return rows.map(r => r.name);
}

describe('priority schema', () => {
  let db: MessageDB;
  beforeEach(() => { db = new MessageDB(':memory:'); });

  it('creates all five priority tables', () => {
    const names = tableNames(db);
    for (const t of ['priority_rules', 'cohorts', 'cohort_members', 'message_priority', 'priority_feedback']) {
      expect(names).toContain(t);
    }
  });
});
