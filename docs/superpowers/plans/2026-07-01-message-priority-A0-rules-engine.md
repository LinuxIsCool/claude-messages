# Message Priority — Phase A0 (Rules Engine + Priority Inbox) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deterministically score every message into a four-tier priority (critical/exceptional/somewhat/irrelevant) using the user's explicit rules plus local heuristics, and expose a ranked priority inbox + rule/cohort management over MCP — no LLM, no embeddings.

**Architecture:** Pure scoring functions (`priority.ts`) + `MessageDB` methods that persist scores to a new `message_priority` table. Rules provide a hard importance *floor*; relationship strength and local urgency heuristics fill the rest. A0 is the "fast path" of the two-speed engine in the design spec; the warm (embeddings) and slow (LLM judge) paths are Phases A2/A3.

**Tech Stack:** TypeScript (ESM, `.js` import specifiers), `better-sqlite3` (synchronous), `@modelcontextprotocol/sdk` `server.tool()`, `zod`, `vitest`.

## Global Constraints

- Node `>=20`; package is `"type": "module"` — **import specifiers end in `.js`** even for `.ts` sources.
- Tests: `vitest run`; construct DB with `new MessageDB(':memory:')` in `beforeEach`.
- DB access is **synchronous** (`better-sqlite3`) — no `await` on DB calls.
- New tables are added inside `MessageDB.migrate()` as `CREATE TABLE IF NOT EXISTS` (idempotent).
- `sender_id` format is `"<platform>:<platform_id>"`; `platform_id` may contain colons → split on the **first** colon only.
- Tiers (verbatim): `critical` ≈0.1%, `exceptional` ≈0.9%, `somewhat` ≈4%, `irrelevant` ≈95%.
- `attention = 0.6*importance + 0.4*urgency`. Awareness (later phases) fires on **tier/importance**; inbox orders by **attention**.
- Rules are a **hard floor**: scoring may raise importance above a rule floor but must never place a rule-matched message below it.
- Build check after code changes: `npm run build` (esbuild) must succeed.

---

### Task 1: Priority schema migration

**Files:**
- Modify: `plugins/claude-messages/server/src/db.ts` (inside `migrate()`, append to the `this.db.exec(\`...\`)` block ending at line ~163)
- Test: `plugins/claude-messages/server/src/priority.test.ts` (new)

**Interfaces:**
- Consumes: existing `identities(id)`, `messages(id)` tables.
- Produces: tables `priority_rules`, `cohorts`, `cohort_members`, `message_priority`, `priority_feedback`.

- [ ] **Step 1: Write the failing test**

```typescript
// plugins/claude-messages/server/src/priority.test.ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — `expected [ ...tables ] to contain "priority_rules"`.

- [ ] **Step 3: Add the tables to `migrate()`**

Insert this SQL just before the closing `` `); `` of the first `this.db.exec(\`...\`)` block in `migrate()` (immediately after the `config` table definition, around line 162):

```sql
      CREATE TABLE IF NOT EXISTS priority_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_type TEXT NOT NULL,
        match_value TEXT NOT NULL,
        importance_floor REAL NOT NULL,
        tier_floor TEXT NOT NULL,
        note TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS cohorts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS cohort_members (
        cohort_id INTEGER NOT NULL REFERENCES cohorts(id) ON DELETE CASCADE,
        identity_id TEXT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
        PRIMARY KEY (cohort_id, identity_id)
      );

      CREATE TABLE IF NOT EXISTS message_priority (
        message_id TEXT PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
        importance REAL NOT NULL,
        urgency REAL NOT NULL,
        attention REAL NOT NULL,
        tier TEXT NOT NULL,
        source TEXT NOT NULL,
        model_version TEXT,
        rationale TEXT,
        needs_llm INTEGER NOT NULL DEFAULT 1,
        seen INTEGER NOT NULL DEFAULT 0,
        scored_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_mp_tier ON message_priority(tier);
      CREATE INDEX IF NOT EXISTS idx_mp_attention ON message_priority(attention DESC);
      CREATE INDEX IF NOT EXISTS idx_mp_unseen ON message_priority(seen, tier);

      CREATE TABLE IF NOT EXISTS priority_feedback (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
        user_tier TEXT,
        user_importance REAL,
        user_urgency REAL,
        note TEXT,
        created_at TEXT NOT NULL
      );
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): add A0 priority/rules/cohort schema"
```

---

### Task 2: Pure scoring functions (`priority.ts`)

**Files:**
- Create: `plugins/claude-messages/server/src/priority.ts`
- Test: `plugins/claude-messages/server/src/priority.test.ts` (append)

**Interfaces:**
- Produces:
  - `type PriorityTier = 'critical' | 'exceptional' | 'somewhat' | 'irrelevant'`
  - `type RuleType = 'thread' | 'identity' | 'cohort' | 'platform_folder' | 'keyword'`
  - `tierToImportance(tier: PriorityTier): number` — floor value for a tier.
  - `importanceToTier(importance: number): PriorityTier` — fixed A0 thresholds.
  - `blendAttention(importance: number, urgency: number): number`
  - `detectUrgencySignals(content: string | null): number` — 0..1 from `?`, imperative/deadline words.

- [ ] **Step 1: Write the failing test** (append to `priority.test.ts`)

```typescript
import { tierToImportance, importanceToTier, blendAttention, detectUrgencySignals } from './priority.js';

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
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — cannot find module `./priority.js`.

- [ ] **Step 3: Implement `priority.ts`**

```typescript
// plugins/claude-messages/server/src/priority.ts
export type PriorityTier = 'critical' | 'exceptional' | 'somewhat' | 'irrelevant';
export type RuleType = 'thread' | 'identity' | 'cohort' | 'platform_folder' | 'keyword';

// Floor value assigned when a rule declares a tier.
const TIER_FLOOR: Record<PriorityTier, number> = {
  critical: 0.95,
  exceptional: 0.75,
  somewhat: 0.40,
  irrelevant: 0.10,
};

export function tierToImportance(tier: PriorityTier): number {
  return TIER_FLOOR[tier];
}

// A0 uses fixed thresholds. Phase A2 replaces this with quantile calibration.
export function importanceToTier(importance: number): PriorityTier {
  if (importance >= 0.90) return 'critical';
  if (importance >= 0.60) return 'exceptional';
  if (importance >= 0.25) return 'somewhat';
  return 'irrelevant';
}

export function blendAttention(importance: number, urgency: number): number {
  const a = 0.6 * importance + 0.4 * urgency;
  return Math.max(0, Math.min(1, a));
}

const DEADLINE_WORDS = /\b(asap|urgent|today|tonight|tomorrow|by (mon|tue|wed|thu|fri|sat|sun)|deadline|eod|end of day)\b/i;

export function detectUrgencySignals(content: string | null): number {
  if (!content) return 0;
  let score = 0;
  if (content.includes('?')) score += 0.5;
  if (DEADLINE_WORDS.test(content)) score += 0.5;
  return Math.min(1, score);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/priority.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): pure tier/attention/urgency scoring functions"
```

---

### Task 3: Rule & cohort storage (`MessageDB` methods)

**Files:**
- Modify: `plugins/claude-messages/server/src/db.ts` (add methods to `MessageDB`)
- Test: `plugins/claude-messages/server/src/priority.test.ts` (append)

**Interfaces:**
- Consumes: `tierToImportance`, `PriorityTier`, `RuleType` from `./priority.js`.
- Produces:
  - `addPriorityRule(ruleType: RuleType, matchValue: string, tierFloor: PriorityTier, note?: string): number`
  - `listPriorityRules(includeDisabled?: boolean): PriorityRule[]`
  - `disablePriorityRule(id: number): void`
  - `createCohort(name: string, description?: string): number`
  - `addCohortMember(cohortId: number, identityId: string): void`
  - `getCohortMembers(cohortId: number): string[]`
  - `listCohorts(): Cohort[]`
  - types `PriorityRule`, `Cohort`.

- [ ] **Step 1: Write the failing test** (append to `priority.test.ts`)

```typescript
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — `db.addPriorityRule is not a function`.

- [ ] **Step 3: Implement the methods**

Add near the top of `db.ts` imports:

```typescript
import { tierToImportance, importanceToTier, blendAttention, detectUrgencySignals } from './priority.js';
import type { PriorityTier, RuleType } from './priority.js';
```

Add these exported interfaces to `types.ts`:

```typescript
export interface PriorityRule {
  id: number;
  rule_type: string;
  match_value: string;
  importance_floor: number;
  tier_floor: string;
  note: string | null;
  enabled: number;
  created_at: string;
}

export interface Cohort {
  id: number;
  name: string;
  description: string | null;
  created_at: string;
}
```

Import them in `db.ts` (extend the existing `import type { ... } from './types.js'` line) with `PriorityRule, Cohort`.

Add these methods inside the `MessageDB` class (e.g. after the scoring section):

```typescript
  addPriorityRule(ruleType: RuleType, matchValue: string, tierFloor: PriorityTier, note?: string): number {
    const now = new Date().toISOString();
    const info = this.db.prepare(
      `INSERT INTO priority_rules (rule_type, match_value, importance_floor, tier_floor, note, enabled, created_at)
       VALUES (?, ?, ?, ?, ?, 1, ?)`
    ).run(ruleType, matchValue, tierToImportance(tierFloor), tierFloor, note ?? null, now);
    return Number(info.lastInsertRowid);
  }

  listPriorityRules(includeDisabled = false): PriorityRule[] {
    const sql = includeDisabled
      ? 'SELECT * FROM priority_rules ORDER BY id'
      : 'SELECT * FROM priority_rules WHERE enabled = 1 ORDER BY id';
    return this.db.prepare(sql).all() as PriorityRule[];
  }

  disablePriorityRule(id: number): void {
    this.db.prepare('UPDATE priority_rules SET enabled = 0 WHERE id = ?').run(id);
  }

  createCohort(name: string, description?: string): number {
    const now = new Date().toISOString();
    const info = this.db.prepare(
      'INSERT INTO cohorts (name, description, created_at) VALUES (?, ?, ?)'
    ).run(name, description ?? null, now);
    return Number(info.lastInsertRowid);
  }

  addCohortMember(cohortId: number, identityId: string): void {
    this.db.prepare(
      'INSERT OR IGNORE INTO cohort_members (cohort_id, identity_id) VALUES (?, ?)'
    ).run(cohortId, identityId);
  }

  getCohortMembers(cohortId: number): string[] {
    const rows = this.db.prepare(
      'SELECT identity_id FROM cohort_members WHERE cohort_id = ?'
    ).all(cohortId) as Array<{ identity_id: string }>;
    return rows.map(r => r.identity_id);
  }

  listCohorts(): Cohort[] {
    return this.db.prepare('SELECT * FROM cohorts ORDER BY name').all() as Cohort[];
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/types.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): rule + cohort storage methods"
```

---

### Task 4: Score a single message (rule floor + relationship + urgency)

**Files:**
- Modify: `plugins/claude-messages/server/src/db.ts`
- Test: `plugins/claude-messages/server/src/priority.test.ts` (append)

**Interfaces:**
- Consumes: `priority_rules`, `cohort_members`, `contact_scores`, `messages`, `identity_links`.
- Produces:
  - `identityForSender(senderId: string): string | null` — split on first `:`, look up `identity_links`.
  - `scoreMessage(messageId: string): MessagePriority | null` — computes and upserts a `message_priority` row.
  - type `MessagePriority`.

- [ ] **Step 1: Write the failing test** (append)

```typescript
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

  it('matches identity and cohort rules via sender identity', () => {
    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'signal', 'alice', 1.0, 'manual');
    const cohortId = db.createCohort('Telus');
    db.addCohortMember(cohortId, identity.id);
    db.addPriorityRule('cohort', String(cohortId), 'exceptional');
    const sp = db.scoreMessage('m1');
    expect(sp!.importance).toBeGreaterThanOrEqual(0.75);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — `db.scoreMessage is not a function`.

- [ ] **Step 3: Implement scoring**

Add `MessagePriority` to `types.ts`:

```typescript
export interface MessagePriority {
  message_id: string;
  importance: number;
  urgency: number;
  attention: number;
  tier: string;
  source: string;
  model_version: string | null;
  rationale: string | null;
  needs_llm: number;
  seen: number;
  scored_at: string;
}
```

Import `MessagePriority` into `db.ts`'s `types.js` import. Add these methods to `MessageDB`:

```typescript
  identityForSender(senderId: string): string | null {
    const idx = senderId.indexOf(':');
    if (idx < 0) return null;
    const platform = senderId.slice(0, idx);
    const platformId = senderId.slice(idx + 1);
    const row = this.db.prepare(
      'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
    ).get(platform, platformId) as { identity_id: string } | undefined;
    return row?.identity_id ?? null;
  }

  scoreMessage(messageId: string): MessagePriority | null {
    const msg = this.db.prepare(
      'SELECT id, thread_id, sender_id, content, platform, direction FROM messages WHERE id = ?'
    ).get(messageId) as
      | { id: string; thread_id: string | null; sender_id: string | null; content: string | null; platform: string; direction: string }
      | undefined;
    if (!msg) return null;

    const identityId = msg.sender_id ? this.identityForSender(msg.sender_id) : null;

    // --- rule floor: max importance_floor over matching enabled rules ---
    const rules = this.listPriorityRules();
    let ruleFloor = 0;
    let matchedNote: string | null = null;
    for (const r of rules) {
      let hit = false;
      switch (r.rule_type) {
        case 'thread': hit = msg.thread_id === r.match_value; break;
        case 'identity': hit = identityId !== null && identityId === r.match_value; break;
        case 'cohort':
          hit = identityId !== null &&
            this.getCohortMembers(Number(r.match_value)).includes(identityId);
          break;
        case 'platform_folder': hit = `${msg.platform}:${msg.thread_id ?? ''}`.startsWith(r.match_value); break;
        case 'keyword': hit = !!msg.content && new RegExp(r.match_value, 'i').test(msg.content); break;
      }
      if (hit && r.importance_floor > ruleFloor) {
        ruleFloor = r.importance_floor;
        matchedNote = r.note ?? `${r.rule_type}:${r.match_value}`;
      }
    }

    // --- relationship importance from ContactRank composite ---
    let relImportance = 0;
    if (identityId) {
      const cs = this.db.prepare(
        'SELECT composite FROM contact_scores WHERE identity_id = ?'
      ).get(identityId) as { composite: number } | undefined;
      if (cs) relImportance = Math.min(1, cs.composite);
    }

    const importance = Math.max(ruleFloor, relImportance);

    // --- urgency: unanswered inbound + textual signals ---
    const urgReply = msg.direction === 'received' ? 0.5 : 0;
    const urgency = Math.min(1, urgReply + detectUrgencySignals(msg.content));

    const attention = blendAttention(importance, urgency);
    const tier = importanceToTier(importance);
    const source = ruleFloor > 0 ? 'rule' : 'heuristic';
    const rationale = matchedNote;
    const now = new Date().toISOString();

    this.db.prepare(
      `INSERT INTO message_priority
        (message_id, importance, urgency, attention, tier, source, model_version, rationale, needs_llm, seen, scored_at)
       VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 1, 0, ?)
       ON CONFLICT(message_id) DO UPDATE SET
         importance=excluded.importance, urgency=excluded.urgency, attention=excluded.attention,
         tier=excluded.tier, source=excluded.source, rationale=excluded.rationale, scored_at=excluded.scored_at`
    ).run(messageId, importance, urgency, attention, tier, source, rationale, now);

    return this.db.prepare('SELECT * FROM message_priority WHERE message_id = ?').get(messageId) as MessagePriority;
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/types.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): score a message from rules + relationship + urgency"
```

---

### Task 5: Batch rescore, inbox query, explain, feedback, mark-seen

**Files:**
- Modify: `plugins/claude-messages/server/src/db.ts`
- Test: `plugins/claude-messages/server/src/priority.test.ts` (append)

**Interfaces:**
- Produces:
  - `rescoreAllPriority(): number` — scores every message lacking an up-to-date row; returns count.
  - `getPriorityInbox(opts?: { tier?: PriorityTier; limit?: number; unseenOnly?: boolean }): InboxEntry[]`
  - `explainPriority(messageId: string): MessagePriority | null`
  - `setPriorityFeedback(messageId: string, fb: { tier?: PriorityTier; importance?: number; urgency?: number; note?: string }): void`
  - `markPrioritySeen(target: { messageId?: string; threadId?: string }): number`
  - `priorityStats(): { byTier: Record<string, number>; scored: number; unseen: number }`
  - type `InboxEntry` = `MessagePriority & { content: string | null; sender_id: string | null; thread_id: string | null }`.

- [ ] **Step 1: Write the failing test** (append)

```typescript
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — `db.rescoreAllPriority is not a function`.

- [ ] **Step 3: Implement the methods**

Add `InboxEntry` to `types.ts`:

```typescript
export interface InboxEntry {
  message_id: string;
  importance: number;
  urgency: number;
  attention: number;
  tier: string;
  source: string;
  model_version: string | null;
  rationale: string | null;
  needs_llm: number;
  seen: number;
  scored_at: string;
  content: string | null;
  sender_id: string | null;
  thread_id: string | null;
}
```

Import `InboxEntry` into `db.ts`. Add methods to `MessageDB`. Note `scoreMessage` must be made feedback-aware — update its final upsert guard: at the **top** of `scoreMessage`, short-circuit if a feedback row governs the message:

```typescript
  // add at the very start of scoreMessage(), after fetching `msg`:
  private hasFeedback(messageId: string): boolean {
    const row = this.db.prepare(
      'SELECT 1 FROM priority_feedback WHERE message_id = ? LIMIT 1'
    ).get(messageId);
    return !!row;
  }
```

Then in `scoreMessage`, immediately after the `if (!msg) return null;` line, add:

```typescript
    if (this.hasFeedback(messageId)) {
      return this.db.prepare('SELECT * FROM message_priority WHERE message_id = ?').get(messageId) as MessagePriority ?? null;
    }
```

Now the new methods:

```typescript
  rescoreAllPriority(): number {
    const ids = this.db.prepare('SELECT id FROM messages').all() as Array<{ id: string }>;
    let n = 0;
    const tx = this.db.transaction((rows: Array<{ id: string }>) => {
      for (const r of rows) { if (this.scoreMessage(r.id)) n++; }
    });
    tx(ids);
    return n;
  }

  getPriorityInbox(opts: { tier?: PriorityTier; limit?: number; unseenOnly?: boolean } = {}): InboxEntry[] {
    const { tier, limit = 30, unseenOnly = false } = opts;
    const clauses: string[] = [];
    const params: unknown[] = [];
    if (tier) { clauses.push('mp.tier = ?'); params.push(tier); }
    if (unseenOnly) { clauses.push('mp.seen = 0'); }
    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
    params.push(limit);
    return this.db.prepare(
      `SELECT mp.*, m.content, m.sender_id, m.thread_id
       FROM message_priority mp JOIN messages m ON m.id = mp.message_id
       ${where}
       ORDER BY mp.attention DESC, mp.scored_at DESC
       LIMIT ?`
    ).all(...params) as InboxEntry[];
  }

  explainPriority(messageId: string): MessagePriority | null {
    return (this.db.prepare('SELECT * FROM message_priority WHERE message_id = ?').get(messageId) as MessagePriority) ?? null;
  }

  setPriorityFeedback(messageId: string, fb: { tier?: PriorityTier; importance?: number; urgency?: number; note?: string }): void {
    const now = new Date().toISOString();
    this.db.prepare(
      `INSERT INTO priority_feedback (message_id, user_tier, user_importance, user_urgency, note, created_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    ).run(messageId, fb.tier ?? null, fb.importance ?? null, fb.urgency ?? null, fb.note ?? null, now);

    const importance = fb.importance ?? (fb.tier ? tierToImportance(fb.tier) : undefined);
    const existing = this.explainPriority(messageId);
    const urgency = fb.urgency ?? existing?.urgency ?? 0;
    const finalImp = importance ?? existing?.importance ?? 0;
    const tier = fb.tier ?? importanceToTier(finalImp);
    const attention = blendAttention(finalImp, urgency);
    this.db.prepare(
      `INSERT INTO message_priority
         (message_id, importance, urgency, attention, tier, source, model_version, rationale, needs_llm, seen, scored_at)
       VALUES (?, ?, ?, ?, ?, 'feedback', NULL, ?, 0, 0, ?)
       ON CONFLICT(message_id) DO UPDATE SET
         importance=excluded.importance, urgency=excluded.urgency, attention=excluded.attention,
         tier=excluded.tier, source='feedback', rationale=excluded.rationale, needs_llm=0, scored_at=excluded.scored_at`
    ).run(messageId, finalImp, urgency, attention, tier, fb.note ?? 'user feedback', now);
  }

  markPrioritySeen(target: { messageId?: string; threadId?: string }): number {
    if (target.messageId) {
      return this.db.prepare('UPDATE message_priority SET seen = 1 WHERE message_id = ?').run(target.messageId).changes;
    }
    if (target.threadId) {
      return this.db.prepare(
        `UPDATE message_priority SET seen = 1 WHERE message_id IN (SELECT id FROM messages WHERE thread_id = ?)`
      ).run(target.threadId).changes;
    }
    return 0;
  }

  priorityStats(): { byTier: Record<string, number>; scored: number; unseen: number } {
    const rows = this.db.prepare('SELECT tier, COUNT(*) as c FROM message_priority GROUP BY tier').all() as Array<{ tier: string; c: number }>;
    const byTier: Record<string, number> = {};
    let scored = 0;
    for (const r of rows) { byTier[r.tier] = r.c; scored += r.c; }
    const unseen = (this.db.prepare("SELECT COUNT(*) as c FROM message_priority WHERE seen = 0 AND tier IN ('critical','exceptional')").get() as { c: number }).c;
    return { byTier, scored, unseen };
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS (all describe blocks).

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/types.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): rescore, inbox, explain, feedback, mark-seen"
```

---

### Task 6: MCP tools for priority + rules + cohorts

**Files:**
- Modify: `plugins/claude-messages/server/src/mcp.ts` (append `server.tool(...)` blocks before the transport connect at the bottom)
- Test: manual verification (MCP tools are thin wrappers over Task 3–5 methods, which are already unit-tested)

**Interfaces:**
- Consumes: `db.getPriorityInbox`, `db.explainPriority`, `db.setPriorityFeedback`, `db.markPrioritySeen`, `db.addPriorityRule`, `db.listPriorityRules`, `db.disablePriorityRule`, `db.createCohort`, `db.addCohortMember`, `db.listCohorts`, `db.priorityStats`.
- Produces: MCP tools `priority_inbox`, `priority_explain`, `priority_feedback`, `priority_mark_seen`, `priority_rule_add`, `priority_rule_list`, `priority_rule_disable`, `cohort_create`, `cohort_add_member`, `cohort_list`, `priority_stats`.

- [ ] **Step 1: Add the tool blocks**

Append to `mcp.ts` (before the final `await server.connect(transport)` / transport wiring). The `tierEnum` mirrors `PriorityTier`:

```typescript
const tierEnum = z.enum(['critical', 'exceptional', 'somewhat', 'irrelevant']);

server.tool(
  'priority_inbox',
  'Messages ranked by attention (importance + urgency), grouped by priority tier',
  {
    tier: tierEnum.optional().describe('Filter to a single tier'),
    limit: z.number().optional().default(30).describe('Max results'),
    unseen_only: z.boolean().optional().default(false).describe('Only messages not yet marked seen'),
  },
  async ({ tier, limit, unseen_only }) => {
    const rows = db.getPriorityInbox({ tier, limit, unseenOnly: unseen_only });
    const names = db.resolveContactNames([...new Set(rows.map(r => r.sender_id).filter(Boolean) as string[])]);
    const lines = rows.map(r =>
      `[${r.tier}] a=${r.attention.toFixed(2)} ${names.get(r.sender_id ?? '') ?? r.sender_id ?? '?'}: ${(r.content ?? '').slice(0, 120)}${r.rationale ? `  (${r.rationale})` : ''}`
    );
    return { content: [{ type: 'text' as const, text: lines.join('\n') || 'No scored messages.' }] };
  }
);

server.tool(
  'priority_explain',
  'Show the priority score breakdown for a message',
  { message_id: z.string().describe('Message id') },
  async ({ message_id }) => {
    const sp = db.explainPriority(message_id);
    return { content: [{ type: 'text' as const, text: sp ? JSON.stringify(sp, null, 2) : 'Not scored.' }] };
  }
);

server.tool(
  'priority_feedback',
  'Correct a message\'s priority (active-learning signal; overrides scoring)',
  {
    message_id: z.string(),
    tier: tierEnum.optional(),
    importance: z.number().min(0).max(1).optional(),
    urgency: z.number().min(0).max(1).optional(),
    note: z.string().optional(),
  },
  async ({ message_id, tier, importance, urgency, note }) => {
    db.setPriorityFeedback(message_id, { tier, importance, urgency, note });
    return { content: [{ type: 'text' as const, text: `Recorded feedback for ${message_id}.` }] };
  }
);

server.tool(
  'priority_mark_seen',
  'Mark a message or an entire thread as seen (clears awareness)',
  { message_id: z.string().optional(), thread_id: z.string().optional() },
  async ({ message_id, thread_id }) => {
    const n = db.markPrioritySeen({ messageId: message_id, threadId: thread_id });
    return { content: [{ type: 'text' as const, text: `Marked ${n} message(s) seen.` }] };
  }
);

server.tool(
  'priority_rule_add',
  'Add a priority rule (hard importance floor for matching messages)',
  {
    rule_type: z.enum(['thread', 'identity', 'cohort', 'platform_folder', 'keyword']),
    match_value: z.string().describe('thread_id | identity_id | cohort_id | "platform:prefix" | regex'),
    tier_floor: tierEnum,
    note: z.string().optional(),
  },
  async ({ rule_type, match_value, tier_floor, note }) => {
    const id = db.addPriorityRule(rule_type, match_value, tier_floor, note);
    return { content: [{ type: 'text' as const, text: `Added rule #${id}.` }] };
  }
);

server.tool(
  'priority_rule_list',
  'List priority rules',
  { include_disabled: z.boolean().optional().default(false) },
  async ({ include_disabled }) => {
    const rules = db.listPriorityRules(include_disabled);
    return { content: [{ type: 'text' as const, text: JSON.stringify(rules, null, 2) }] };
  }
);

server.tool(
  'priority_rule_disable',
  'Disable a priority rule by id',
  { id: z.number() },
  async ({ id }) => {
    db.disablePriorityRule(id);
    return { content: [{ type: 'text' as const, text: `Disabled rule #${id}.` }] };
  }
);

server.tool(
  'cohort_create',
  'Create a named cohort of identities (e.g. "Telus")',
  { name: z.string(), description: z.string().optional() },
  async ({ name, description }) => {
    const id = db.createCohort(name, description);
    return { content: [{ type: 'text' as const, text: `Created cohort #${id} (${name}).` }] };
  }
);

server.tool(
  'cohort_add_member',
  'Add an identity to a cohort',
  { cohort_id: z.number(), identity_id: z.string() },
  async ({ cohort_id, identity_id }) => {
    db.addCohortMember(cohort_id, identity_id);
    return { content: [{ type: 'text' as const, text: `Added ${identity_id} to cohort #${cohort_id}.` }] };
  }
);

server.tool(
  'cohort_list',
  'List cohorts',
  {},
  async () => {
    return { content: [{ type: 'text' as const, text: JSON.stringify(db.listCohorts(), null, 2) }] };
  }
);

server.tool(
  'priority_stats',
  'Priority tier distribution and unseen counts',
  {},
  async () => {
    return { content: [{ type: 'text' as const, text: JSON.stringify(db.priorityStats(), null, 2) }] };
  }
);
```

- [ ] **Step 2: Build to verify the tools compile**

Run: `cd plugins/claude-messages/server && npm run build`
Expected: build succeeds with no TypeScript errors.

- [ ] **Step 3: Smoke-test the MCP server boots**

Run: `cd plugins/claude-messages/server && node build/mcp.mjs <<< '' ; echo "exit=$?"`
Expected: process starts (stdio server); no throw on load. (Ctrl-C / empty stdin exits cleanly.)

- [ ] **Step 4: Commit**

```bash
git add plugins/claude-messages/server/src/mcp.ts
git commit -m "feat(priority): MCP tools for inbox, rules, cohorts, feedback"
```

---

### Task 7: Score messages as they sync (daemon hook)

**Files:**
- Modify: `plugins/claude-messages/server/src/db.ts` (in the message-insert method — locate `INSERT INTO messages`)
- Test: `plugins/claude-messages/server/src/priority.test.ts` (append)

**Interfaces:**
- Consumes: `scoreMessage`.
- Produces: newly-inserted messages get a `message_priority` row automatically.

- [ ] **Step 1: Write the failing test** (append)

```typescript
describe('auto-score on insert', () => {
  let db: MessageDB;
  const now = new Date().toISOString();
  beforeEach(() => { db = new MessageDB(':memory:'); db.addPriorityRule('thread', 'signal:gpu-thread', 'critical'); });

  it('scores a message when inserted via insertMessages', () => {
    db.insertMessages([{
      id: 'mZ', platform: 'signal', thread_id: 'signal:gpu-thread', sender_id: 'signal:x',
      content: 'ping?', content_type: 'text', reply_to: null, metadata: {},
      platform_ts: now, direction: 'received',
    } as any]);
    const sp = db.explainPriority('mZ');
    expect(sp).not.toBeNull();
    expect(sp!.tier).toBe('critical');
  });
});
```

> Locate the real bulk-insert method name first: `grep -n "INSERT INTO messages" src/db.ts`. If it is not `insertMessages`, adjust the test call and Step 3 to the actual method (e.g. `upsertMessage`/`addMessages`). Keep the signature used elsewhere in the codebase.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: FAIL — `sp` is null (message inserted but not scored).

- [ ] **Step 3: Hook scoring into the insert path**

In the message-insert method, after the loop that inserts message rows commits, add a scoring pass over the just-inserted ids (inside the same method, reusing the ids it already has):

```typescript
    // after messages are inserted (collect their ids as `insertedIds: string[]`)
    for (const id of insertedIds) {
      try { this.scoreMessage(id); } catch { /* scoring must never break ingestion */ }
    }
```

If the method does not already track inserted ids, capture them from the input array before/inside the insert loop. Scoring is best-effort and wrapped in try/catch so a scoring bug can never block sync (same isolation principle as the daemon's per-adapter failure handling).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd plugins/claude-messages/server && npx vitest run src/priority.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/priority.test.ts
git commit -m "feat(priority): auto-score messages on ingestion"
```

---

### Task 8: Seed the user's declared signals + backfill

**Files:**
- Create: `plugins/claude-messages/server/src/seed-priority.ts` (a small runnable script)
- Create: `plugins/claude-messages/docs/priority-setup.md` (runbook for resolving IDs)

**Interfaces:**
- Consumes: `createCohort`, `addPriorityRule`, `rescoreAllPriority`.
- Produces: the two empty cohorts (`Telus`, `Indigenomics Capstone`) and a documented process for adding thread/identity rules once IDs are resolved. Backfills scores over existing history.

> The exact GPU/Regen thread ids and cohort membership are open questions in the spec (§14) — they require interactive lookup, so this task creates the **scaffold** + runbook, not hard-coded ids.

- [ ] **Step 1: Write the seed script**

```typescript
// plugins/claude-messages/server/src/seed-priority.ts
import path from 'node:path';
import { MessageDB } from './db.js';

function resolveHome(p: string): string {
  return p.startsWith('~/') ? path.join(process.env.HOME ?? '', p.slice(2)) : p;
}

const dataDir = process.env.MESSAGES_DATA_DIR ?? resolveHome('~/.claude/local/messages');
const db = new MessageDB(path.join(dataDir, 'messages.db'));

// Idempotent: only create cohorts if absent.
const existing = new Set(db.listCohorts().map(c => c.name));
for (const name of ['Telus', 'Indigenomics Capstone']) {
  if (!existing.has(name)) { db.createCohort(name); console.log(`created cohort: ${name}`); }
}

const n = db.rescoreAllPriority();
console.log(`rescored ${n} messages`);
console.log('Next: resolve thread/identity ids and add rules via MCP tools (see docs/priority-setup.md).');
```

- [ ] **Step 2: Write the runbook**

```markdown
<!-- plugins/claude-messages/docs/priority-setup.md -->
# Priority setup (A0)

1. Build: `cd server && npm run build`
2. Seed cohorts + backfill: `node build/seed-priority.mjs`
3. Resolve ids and add rules (via MCP tools or a Claude session):
   - GPU thread (Signal): find thread id via `list_threads` → `priority_rule_add(thread, <id>, critical)`
   - Regen Gaia (Telegram): `priority_rule_add(thread, <id>, exceptional)`
   - Carole Anne Hilton / Eve / Pravin / Darren Zal / Mom: resolve identity ids via `list_identities`/`who_is` → `priority_rule_add(identity, <identity_id>, exceptional)`
   - Telus / Capstone: `cohort_list` → `cohort_add_member(<cohort_id>, <identity_id>)` for each; then `priority_rule_add(cohort, <cohort_id>, exceptional)`
4. Re-backfill after adding rules: `node build/seed-priority.mjs`
5. Verify distribution: `priority_stats` (expect the vast majority `irrelevant`).
```

- [ ] **Step 3: Build and run the seed against a throwaway DB**

Run:
```bash
cd plugins/claude-messages/server && npm run build && \
MESSAGES_DATA_DIR=$(mktemp -d) node build/seed-priority.mjs
```
Expected: prints `created cohort: Telus`, `created cohort: Indigenomics Capstone`, `rescored 0 messages` (empty throwaway DB), no errors.

- [ ] **Step 4: Commit**

```bash
git add plugins/claude-messages/server/src/seed-priority.ts plugins/claude-messages/docs/priority-setup.md
git commit -m "feat(priority): seed cohorts + backfill script and setup runbook"
```

---

## Self-Review

**Spec coverage (A0 scope only):**
- Schema §4 (rules/cohorts/message_priority/feedback) → Task 1. *(message_embeddings deferred to A2 — correct.)*
- Rule floor + relationship + urgency composition §5 → Tasks 2, 4.
- Rules-as-hard-floor §3.2 → Task 4 (`Math.max(ruleFloor, relImportance)`), feedback override §5 → Task 5.
- Priority inbox §8 (on-demand channel) + MCP surface §9 → Tasks 5, 6. *(desktop + statusline channels are A1 — out of scope here.)*
- Encode declared signals §10 → Task 8 (scaffold + runbook; hard ids deferred per §14).
- Two-speed isolation §3 (scoring never blocks ingestion) → Task 7 try/catch.
- Calibration §6.3, LLM judge §6, learned ranker §7 → **A2/A3, intentionally not in this plan.**

**Placeholder scan:** No TBD/TODO; every code step has complete code. Task 7 Step 1/3 note the one lookup the implementer must confirm (`INSERT INTO messages` method name) with an exact grep — this is verification, not a placeholder.

**Type consistency:** `PriorityTier`/`RuleType` (priority.ts) used identically in Tasks 2–6; `tierToImportance`/`importanceToTier`/`blendAttention`/`detectUrgencySignals` signatures match across tasks; `scoreMessage`→`rescoreAllPriority`→`getPriorityInbox`→`setPriorityFeedback` names consistent; MCP `tierEnum` mirrors `PriorityTier`. `MessagePriority`/`InboxEntry`/`PriorityRule`/`Cohort` defined in types.ts before use.
