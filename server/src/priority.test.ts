import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';
import { tierToImportance, importanceToTier, blendAttention, detectUrgencySignals } from './priority.js';

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

describe('priority scoring functions', () => {
  it('maps tiers to descending importance floors', () => {
    expect(tierToImportance('critical')).toBeGreaterThan(tierToImportance('exceptional'));
    expect(tierToImportance('exceptional')).toBeGreaterThan(tierToImportance('somewhat'));
    expect(tierToImportance('somewhat')).toBeGreaterThan(tierToImportance('irrelevant'));
  });

  it('buckets importance back into tiers monotonically', () => {
    expect(importanceToTier(0.95)).toBe('critical');
    expect(importanceToTier(0.7)).toBe('exceptional');
    expect(importanceToTier(0.3)).toBe('somewhat');
    expect(importanceToTier(0.05)).toBe('irrelevant');
  });

  it('blends attention 60/40 importance/urgency', () => {
    expect(blendAttention(1, 0)).toBeCloseTo(0.6);
    expect(blendAttention(0, 1)).toBeCloseTo(0.4);
  });

  it('detects urgency from questions and deadline words', () => {
    expect(detectUrgencySignals('can you send it?')).toBeGreaterThan(0);
    expect(detectUrgencySignals('need this ASAP by Friday')).toBeGreaterThan(0);
    expect(detectUrgencySignals('cool, thanks')).toBe(0);
    expect(detectUrgencySignals(null)).toBe(0);
  });

  it('detects a bare "by <full weekday>" deadline without other signals', () => {
    expect(detectUrgencySignals('let us finalize by Friday')).toBeGreaterThan(0);
    expect(detectUrgencySignals('review by Wednesday please')).toBeGreaterThan(0);
  });

  it('caps combined signals at 1.0', () => {
    expect(detectUrgencySignals('urgent — can you send it by Monday?')).toBe(1);
  });
});

describe('priority rule + cohort storage', () => {
  let db: MessageDB;
  beforeEach(() => { db = new MessageDB(':memory:'); });

  it('adds, lists, and disables rules', () => {
    const id = db.addPriorityRule('thread', 'signal:gpu-thread', 'critical', 'GPU thread');
    let rules = db.listPriorityRules();
    expect(rules).toHaveLength(1);
    expect(rules[0].tier_floor).toBe('critical');
    expect(rules[0].importance_floor).toBeCloseTo(0.95);

    db.disablePriorityRule(id);
    expect(db.listPriorityRules()).toHaveLength(0);
    expect(db.listPriorityRules(true)).toHaveLength(1);
  });

  it('creates cohorts and stores members', () => {
    const cohortId = db.createCohort('Telus', 'Telus contacts');
    const identity = (db as any).createIdentity('Someone At Telus');
    db.addCohortMember(cohortId, identity.id);
    expect(db.getCohortMembers(cohortId)).toEqual([identity.id]);
    expect(db.listCohorts()[0].name).toBe('Telus');
  });
});

describe('scoreMessage', () => {
  let db: MessageDB;
  const now = new Date().toISOString();

  beforeEach(() => {
    db = db = new MessageDB(':memory:');
    // minimal message + thread + identity link so rules can match
    (db as any).db.prepare(
      `INSERT INTO messages (id, platform, thread_id, sender_id, content, content_type, platform_ts, synced_at, direction)
       VALUES (?, ?, ?, ?, ?, 'text', ?, ?, 'received')`
    ).run('m1', 'signal', 'signal:gpu-thread', 'signal:alice', 'when is the GPU arriving?', now, now);
  });

  it('applies a matching thread rule as a critical floor', () => {
    db.addPriorityRule('thread', 'signal:gpu-thread', 'critical');
    const sp = db.scoreMessage('m1');
    expect(sp).not.toBeNull();
    expect(sp!.tier).toBe('critical');
    expect(sp!.importance).toBeGreaterThanOrEqual(0.95);
    expect(sp!.urgency).toBeGreaterThan(0); // has a '?'
  });

  it('scores an unmatched message as low tier', () => {
    const sp = db.scoreMessage('m1');
    expect(sp!.tier === 'somewhat' || sp!.tier === 'irrelevant').toBe(true);
  });

  it('matches a cohort rule via sender identity', () => {
    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'signal', 'alice', 1.0, 'manual');
    const cohortId = db.createCohort('Telus');
    db.addCohortMember(cohortId, identity.id);
    db.addPriorityRule('cohort', String(cohortId), 'exceptional');
    const sp = db.scoreMessage('m1');
    expect(sp!.importance).toBeGreaterThanOrEqual(0.75);
  });

  it('matches an identity rule via resolved sender identity', () => {
    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'signal', 'alice', 1.0, 'manual');
    db.addPriorityRule('identity', identity.id, 'exceptional');
    const sp = db.scoreMessage('m1');
    expect(sp!.importance).toBeGreaterThanOrEqual(0.75);
    expect(sp!.source).toBe('rule');
  });

  it('matches a keyword rule against message content', () => {
    db.addPriorityRule('keyword', 'gpu', 'exceptional');
    const sp = db.scoreMessage('m1'); // content: 'when is the GPU arriving?'
    expect(sp!.importance).toBeGreaterThanOrEqual(0.75);
  });

  it('matches a platform_folder rule by platform:thread prefix', () => {
    const now2 = new Date().toISOString();
    (db as any).db.prepare(
      `INSERT INTO messages (id, platform, thread_id, sender_id, content, content_type, platform_ts, synced_at, direction)
       VALUES ('mMail', 'email', 'INBOX/Telus', 'email:boss@telus.com', 'hi', 'text', ?, ?, 'received')`
    ).run(now2, now2);
    db.addPriorityRule('platform_folder', 'email:INBOX/Telus', 'exceptional');
    const sp = db.scoreMessage('mMail'); // computed 'email:INBOX/Telus' startsWith rule value
    expect(sp!.importance).toBeGreaterThanOrEqual(0.75);
  });

  it('does not crash on a malformed keyword regex; skips the bad rule', () => {
    db.addPriorityRule('keyword', '(unbalanced', 'critical');
    expect(() => db.scoreMessage('m1')).not.toThrow();
    const sp = db.scoreMessage('m1');
    expect(sp!.tier).not.toBe('critical'); // bad rule skipped => no critical floor
  });

  it('rule floor overrides a lower relationship score (hard-floor invariant)', () => {
    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'signal', 'alice', 1.0, 'manual');
    const now2 = new Date().toISOString();
    (db as any).db.prepare(
      'INSERT INTO contact_scores (identity_id, composite, computed_at) VALUES (?, ?, ?)'
    ).run(identity.id, 0.30, now2);
    db.addPriorityRule('thread', 'signal:gpu-thread', 'critical');
    const sp = db.scoreMessage('m1');
    expect(sp!.importance).toBeGreaterThanOrEqual(0.95); // floor wins over 0.30
  });

  it('uses relationship score when no rule matches', () => {
    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'signal', 'alice', 1.0, 'manual');
    const now2 = new Date().toISOString();
    (db as any).db.prepare(
      'INSERT INTO contact_scores (identity_id, composite, computed_at) VALUES (?, ?, ?)'
    ).run(identity.id, 0.30, now2);
    const sp = db.scoreMessage('m1'); // no rules added
    expect(sp!.importance).toBeCloseTo(0.30, 5);
    expect(sp!.source).toBe('heuristic');
  });

  it('question mark contributes urgency independent of direction', () => {
    const now2 = new Date().toISOString();
    (db as any).db.prepare(
      `INSERT INTO messages (id, platform, thread_id, sender_id, content, content_type, platform_ts, synced_at, direction)
       VALUES ('mSent', 'signal', 'signal:random', 'signal:alice', 'ready?', 'text', ?, ?, 'sent')`
    ).run(now2, now2);
    const sp = db.scoreMessage('mSent'); // direction 'sent' => urgReply 0; '?' => 0.5
    expect(sp!.urgency).toBeCloseTo(0.5, 5);
  });
});

describe('inbox + feedback', () => {
  let db: MessageDB;
  const now = new Date().toISOString();
  beforeEach(() => {
    db = new MessageDB(':memory:');
    const ins = (db as any).db.prepare(
      `INSERT INTO messages (id, platform, thread_id, sender_id, content, content_type, platform_ts, synced_at, direction)
       VALUES (?, 'signal', ?, 'signal:x', ?, 'text', ?, ?, 'received')`
    );
    ins.run('mA', 'signal:gpu-thread', 'gpu?', now, now);
    ins.run('mB', 'signal:random', 'hi', now, now);
    db.addPriorityRule('thread', 'signal:gpu-thread', 'critical');
  });

  it('rescoreAll scores all messages and inbox ranks critical first', () => {
    expect(db.rescoreAllPriority()).toBe(2);
    const inbox = db.getPriorityInbox({ limit: 10 });
    expect(inbox[0].message_id).toBe('mA');
    expect(inbox[0].tier).toBe('critical');
  });

  it('feedback overrides tier and persists through rescore', () => {
    db.rescoreAllPriority();
    db.setPriorityFeedback('mB', { tier: 'exceptional', note: 'actually important' });
    const sp = db.explainPriority('mB');
    expect(sp!.tier).toBe('exceptional');
    expect(sp!.source).toBe('feedback');
    // rescore must NOT clobber feedback
    db.scoreMessage('mB');
    expect(db.explainPriority('mB')!.tier).toBe('exceptional');
  });

  it('markPrioritySeen clears unseen count', () => {
    db.rescoreAllPriority();
    expect(db.priorityStats().unseen).toBeGreaterThan(0);
    db.markPrioritySeen({ messageId: 'mA' });
    const seenRow = db.explainPriority('mA');
    expect(seenRow!.seen).toBe(1);
  });
});
