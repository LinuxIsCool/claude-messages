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
