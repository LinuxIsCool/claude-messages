# Message Priority — Phase A1 (Awareness: desktop + statusline) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a new **critical**-tier message lands, fire a desktop notification (deduped per thread, quiet-hours aware, notify-once); and continuously publish unseen critical/exceptional counts to a state file the statusline reads.

**Architecture:** A new `awareness.ts` module (pure helpers + two sinks + an emitter) that the daemon calls once per sync cycle. Desktop notifications go through `notify-send`; the statusline is decoupled via a `awareness.json` state file (file-poll contract — the actual statusline wiring is a documented user step, since `claude-statusline` is a separate plugin). Builds on A0's `message_priority` scores; adds a `notified_at` column so each critical message is desktop-notified at most once.

**Tech Stack:** TypeScript (ESM, `.js` import specifiers), `better-sqlite3` (synchronous), `child_process.execFile` → `notify-send`, `vitest`.

## Global Constraints

- Node `>=20`; package is `"type": "module"` — **import specifiers end in `.js`** even for `.ts` sources.
- Tests: `vitest run` from `server/`; construct DB with `new MessageDB(':memory:')` in `beforeEach`.
- DB access is **synchronous** (`better-sqlite3`) — no `await` on DB calls.
- Schema changes are **idempotent**: use `ALTER TABLE ... ADD COLUMN` wrapped in try/catch that swallows only the "duplicate column" error (the existing pattern at `db.ts` ~line 167 for the `direction` column).
- **Awareness must never break the sync loop**: the daemon's `emit()` call is wrapped in try/catch (same isolation principle as A0's ingestion hook).
- **Desktop = critical tier ONLY** (reserve interrupts). **Statusline = critical + exceptional** counts (ambient).
- Notify-once: a critical message fires at most one desktop notification (tracked by `notified_at`). Within a single emit, **at most one notification per thread** (a burst in one thread = one ping).
- Desktop respects **quiet hours** (skip desktop, still update statusline).
- `notify-send` invocation: `notify-send --urgency=critical --app-name=Messages "<title>" "<body>"`.
- No real `notify-send` calls or real `~/.claude` writes in tests — inject the spawner and use temp paths.
- Build check after code changes: `npm run build` (esbuild) must succeed.
- Runtime `new Date()` is allowed in daemon/module code; in tests, pass an explicit `Date`/ISO so assertions are deterministic.

## File Structure

- **Create** `server/src/awareness.ts` — pure helpers (`isQuietHours`, `dedupeByThread`, `formatNotification`), sinks (`DesktopSink`, `StatuslineSink`), and `AwarenessEmitter`.
- **Create** `server/src/awareness.test.ts` — unit tests for all of the above.
- **Modify** `server/src/db.ts` — `notified_at` column + `getUnnotifiedCritical`, `markNotified`, `awarenessCounts`.
- **Modify** `server/src/types.ts` — `AwarenessConfig`, `AwarenessCounts`, and an `awareness?` field on `AppConfig`.
- **Modify** `server/src/daemon.ts` — construct the emitter, call it once per cycle.
- **Create** `docs/priority-awareness-setup.md` — config + statusline wiring runbook.

---

### Task 1: Awareness DB layer (`notified_at` + queries)

**Files:**
- Modify: `server/src/db.ts` (migrate() + new methods)
- Test: `server/src/awareness.test.ts` (new)

**Interfaces:**
- Consumes: A0's `message_priority` table, `InboxEntry` type.
- Produces:
  - `getUnnotifiedCritical(limit?: number): InboxEntry[]` — tier `critical`, `seen=0`, `notified_at IS NULL`, oldest `scored_at` first.
  - `markNotified(messageIds: string[]): void` — set `notified_at = now` for those ids.
  - `awarenessCounts(): AwarenessCounts` — `{ critical, exceptional }` counts of `seen=0` messages in each tier.
  - type `AwarenessCounts` (see Task 2 defines it in types.ts; this task imports it).

- [ ] **Step 1: Add `AwarenessCounts` to `types.ts`**

Add to `server/src/types.ts`:

```typescript
export interface AwarenessCounts {
  critical: number;
  exceptional: number;
}
```

- [ ] **Step 2: Write the failing test**

Create `server/src/awareness.test.ts`:

```typescript
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
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: FAIL — `db.getUnnotifiedCritical is not a function`.

- [ ] **Step 4: Add the `notified_at` column (idempotent) in `migrate()`**

In `server/src/db.ts`, in the block of `ALTER TABLE` statements inside `migrate()` (near the existing `direction`-column ALTER, ~line 167), add:

```typescript
    try {
      this.db.exec("ALTER TABLE message_priority ADD COLUMN notified_at TEXT");
    } catch (e: any) {
      if (!e.message.includes('duplicate column')) throw e;
    }
```

- [ ] **Step 5: Add the methods to `MessageDB`**

Import `AwarenessCounts` into `db.ts`'s existing `import type { ... } from './types.js'` line. Add near the other priority methods:

```typescript
  getUnnotifiedCritical(limit = 50): InboxEntry[] {
    return this.db.prepare(
      `SELECT mp.*, m.content, m.sender_id, m.thread_id
       FROM message_priority mp JOIN messages m ON m.id = mp.message_id
       WHERE mp.tier = 'critical' AND mp.seen = 0 AND mp.notified_at IS NULL
       ORDER BY mp.scored_at ASC
       LIMIT ?`
    ).all(limit) as InboxEntry[];
  }

  markNotified(messageIds: string[]): void {
    if (messageIds.length === 0) return;
    const now = new Date().toISOString();
    const stmt = this.db.prepare('UPDATE message_priority SET notified_at = ? WHERE message_id = ?');
    const tx = this.db.transaction((ids: string[]) => {
      for (const id of ids) stmt.run(now, id);
    });
    tx(messageIds);
  }

  awarenessCounts(): AwarenessCounts {
    const row = this.db.prepare(
      `SELECT
         SUM(CASE WHEN tier = 'critical' THEN 1 ELSE 0 END) AS critical,
         SUM(CASE WHEN tier = 'exceptional' THEN 1 ELSE 0 END) AS exceptional
       FROM message_priority WHERE seen = 0`
    ).get() as { critical: number | null; exceptional: number | null };
    return { critical: row.critical ?? 0, exceptional: row.exceptional ?? 0 };
  }
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/src/db.ts server/src/types.ts server/src/awareness.test.ts
git commit -m "feat(awareness): notified_at column + unnotified-critical/counts queries"
```

---

### Task 2: Pure awareness helpers

**Files:**
- Create: `server/src/awareness.ts`
- Test: `server/src/awareness.test.ts` (append)

**Interfaces:**
- Consumes: `InboxEntry`, `AwarenessConfig` types.
- Produces:
  - `type QuietHours = { start: string; end: string }` (HH:MM, 24h).
  - `isQuietHours(now: Date, quiet?: QuietHours): boolean` — true if `now`'s local HH:MM is within `[start, end)`, handling windows that cross midnight; false if `quiet` is undefined.
  - `dedupeByThread(entries: InboxEntry[]): InboxEntry[]` — keeps the first entry per `thread_id` (input order preserved).
  - `formatNotification(entry: InboxEntry, senderName: string): { title: string; body: string }` — title `⚡ <senderName>`, body = content trimmed to 140 chars.

- [ ] **Step 1: Add config types to `types.ts`**

Add to `server/src/types.ts`:

```typescript
export interface AwarenessConfig {
  enabled?: boolean;
  desktop?: { enabled?: boolean; quiet_hours?: { start: string; end: string } };
  statusline?: { enabled?: boolean };
}
```

Then add an optional field to the existing `AppConfig` interface (find `interface AppConfig` in types.ts and add this line among its fields):

```typescript
  awareness?: AwarenessConfig;
```

- [ ] **Step 2: Write the failing test** (append to `awareness.test.ts`)

```typescript
import { isQuietHours, dedupeByThread, formatNotification } from './awareness.js';
import type { InboxEntry } from './types.js';

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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: FAIL — cannot find module `./awareness.js`.

- [ ] **Step 4: Implement `awareness.ts` (helpers only for now)**

Create `server/src/awareness.ts`:

```typescript
import type { InboxEntry } from './types.js';

export type QuietHours = { start: string; end: string };

function hhmm(d: Date): number {
  return d.getHours() * 60 + d.getMinutes();
}
function parseHM(s: string): number {
  const [h, m] = s.split(':').map(Number);
  return h * 60 + m;
}

export function isQuietHours(now: Date, quiet?: QuietHours): boolean {
  if (!quiet) return false;
  const t = hhmm(now);
  const start = parseHM(quiet.start);
  const end = parseHM(quiet.end);
  if (start === end) return false;
  if (start < end) return t >= start && t < end;      // same-day window
  return t >= start || t < end;                        // window crosses midnight
}

export function dedupeByThread(entries: InboxEntry[]): InboxEntry[] {
  const seen = new Set<string>();
  const out: InboxEntry[] = [];
  for (const e of entries) {
    const key = e.thread_id ?? e.message_id;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(e);
  }
  return out;
}

export function formatNotification(entry: InboxEntry, senderName: string): { title: string; body: string } {
  const body = (entry.content ?? '').trim().slice(0, 140);
  return { title: `⚡ ${senderName}`, body };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/awareness.ts server/src/types.ts server/src/awareness.test.ts
git commit -m "feat(awareness): pure helpers (quiet-hours, thread dedup, format)"
```

---

### Task 3: Sinks (statusline file + desktop notify-send)

**Files:**
- Modify: `server/src/awareness.ts`
- Test: `server/src/awareness.test.ts` (append)

**Interfaces:**
- Consumes: `AwarenessCounts` type.
- Produces:
  - `type Spawner = (cmd: string, args: string[]) => void`.
  - `class DesktopSink { constructor(spawn?: Spawner); notify(title: string, body: string): void }` — default spawner runs `notify-send --urgency=critical --app-name=Messages <title> <body>` via `child_process.execFile` (fire-and-forget; errors swallowed so a missing `notify-send` can't crash the daemon).
  - `class StatuslineSink { constructor(filePath: string); write(counts: AwarenessCounts): void }` — writes `{ ...counts, updated_at }` JSON atomically to `filePath`.

- [ ] **Step 1: Write the failing test** (append)

```typescript
import { DesktopSink, StatuslineSink } from './awareness.js';
import { readFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

describe('awareness sinks', () => {
  it('DesktopSink passes title/body to the spawner with critical urgency', () => {
    const calls: Array<{ cmd: string; args: string[] }> = [];
    const sink = new DesktopSink((cmd, args) => calls.push({ cmd, args }));
    sink.notify('⚡ Carole Anne', 'the GPU shipped');
    expect(calls).toHaveLength(1);
    expect(calls[0].cmd).toBe('notify-send');
    expect(calls[0].args).toContain('--urgency=critical');
    expect(calls[0].args).toContain('⚡ Carole Anne');
    expect(calls[0].args).toContain('the GPU shipped');
  });

  it('StatuslineSink writes counts JSON to the file', () => {
    const dir = mkdtempSync(join(tmpdir(), 'aware-'));
    const file = join(dir, 'awareness.json');
    new StatuslineSink(file).write({ critical: 2, exceptional: 5 });
    const parsed = JSON.parse(readFileSync(file, 'utf-8'));
    expect(parsed.critical).toBe(2);
    expect(parsed.exceptional).toBe(5);
    expect(typeof parsed.updated_at).toBe('string');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: FAIL — `DesktopSink is not exported` / not a constructor.

- [ ] **Step 3: Implement the sinks in `awareness.ts`**

Add to `server/src/awareness.ts`:

```typescript
import { execFile } from 'node:child_process';
import { writeFileSync, renameSync } from 'node:fs';
import type { AwarenessCounts } from './types.js';

export type Spawner = (cmd: string, args: string[]) => void;

const defaultSpawner: Spawner = (cmd, args) => {
  // fire-and-forget; swallow errors so a missing notify-send can't crash the daemon
  execFile(cmd, args, () => {});
};

export class DesktopSink {
  private spawn: Spawner;
  constructor(spawn: Spawner = defaultSpawner) {
    this.spawn = spawn;
  }
  notify(title: string, body: string): void {
    this.spawn('notify-send', ['--urgency=critical', '--app-name=Messages', title, body]);
  }
}

export class StatuslineSink {
  constructor(private filePath: string) {}
  write(counts: AwarenessCounts): void {
    const payload = JSON.stringify({ ...counts, updated_at: new Date().toISOString() });
    // atomic write: temp file + rename so a statusline reader never sees a partial file
    const tmp = `${this.filePath}.tmp`;
    writeFileSync(tmp, payload);
    renameSync(tmp, this.filePath);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/awareness.ts server/src/awareness.test.ts
git commit -m "feat(awareness): desktop (notify-send) + statusline (json) sinks"
```

---

### Task 4: `AwarenessEmitter.emit()`

**Files:**
- Modify: `server/src/awareness.ts`
- Test: `server/src/awareness.test.ts` (append)

**Interfaces:**
- Consumes: `MessageDB` (`getUnnotifiedCritical`, `markNotified`, `awarenessCounts`, `resolveContactNames`), `DesktopSink`, `StatuslineSink`, helpers, `AwarenessConfig`.
- Produces:
  - `class AwarenessEmitter { constructor(db, opts: { config?: AwarenessConfig; desktop: DesktopSink; statusline: StatuslineSink; now?: () => Date }); emit(): void }`.
  - `emit()` behavior: always write statusline counts (if statusline enabled, default true). If awareness+desktop enabled (default true) AND not quiet hours: fetch unnotified criticals, dedupe by thread, notify each via desktop sink (resolving sender name), then `markNotified` ALL fetched critical ids (not just deduped ones — the suppressed same-thread ones shouldn't re-fire next cycle).

- [ ] **Step 1: Write the failing test** (append)

```typescript
import { AwarenessEmitter } from './awareness.js';

class FakeDesktop {
  calls: Array<{ title: string; body: string }> = [];
  notify(title: string, body: string) { this.calls.push({ title, body }); }
}
class FakeStatusline {
  last: any = null;
  write(counts: any) { this.last = counts; }
}

describe('AwarenessEmitter', () => {
  let db: MessageDB;
  beforeEach(() => {
    db = new MessageDB(':memory:');
    db.addPriorityRule('thread', 'signal:gpu-thread', 'critical');
    seedCritical(db, 'c1', 'signal:gpu-thread');
    seedCritical(db, 'c2', 'signal:gpu-thread'); // same thread → deduped
    seedCritical(db, 'c3', 'signal:other-crit');
    db.addPriorityRule('thread', 'signal:other-crit', 'critical');
    db.rescoreAllPriority();
  });

  it('notifies once per thread, marks all fetched criticals, writes statusline', () => {
    const desktop = new FakeDesktop();
    const statusline = new FakeStatusline();
    const emitter = new AwarenessEmitter(db, {
      desktop: desktop as any, statusline: statusline as any,
      now: () => new Date('2026-07-06T12:00:00'),
    });
    emitter.emit();
    expect(desktop.calls).toHaveLength(2);        // one per thread (c1/c2 collapsed)
    expect(statusline.last.critical).toBe(3);
    // re-emit: nothing new to notify (all marked), statusline still written
    emitter.emit();
    expect(desktop.calls).toHaveLength(2);
  });

  it('suppresses desktop during quiet hours but still writes statusline', () => {
    const desktop = new FakeDesktop();
    const statusline = new FakeStatusline();
    const emitter = new AwarenessEmitter(db, {
      config: { desktop: { quiet_hours: { start: '22:00', end: '07:00' } } },
      desktop: desktop as any, statusline: statusline as any,
      now: () => new Date('2026-07-06T23:30:00'),
    });
    emitter.emit();
    expect(desktop.calls).toHaveLength(0);
    expect(statusline.last.critical).toBe(3);
  });

  it('respects desktop.enabled = false', () => {
    const desktop = new FakeDesktop();
    const statusline = new FakeStatusline();
    const emitter = new AwarenessEmitter(db, {
      config: { desktop: { enabled: false } },
      desktop: desktop as any, statusline: statusline as any,
    });
    emitter.emit();
    expect(desktop.calls).toHaveLength(0);
    expect(statusline.last.critical).toBe(3);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: FAIL — `AwarenessEmitter is not a constructor`.

- [ ] **Step 3: Implement `AwarenessEmitter`**

Add to `server/src/awareness.ts` (add `MessageDB` + `AwarenessConfig` imports):

```typescript
import type { MessageDB } from './db.js';
import type { AwarenessConfig } from './types.js';

export interface EmitterOpts {
  config?: AwarenessConfig;
  desktop: DesktopSink;
  statusline: StatuslineSink;
  now?: () => Date;
}

export class AwarenessEmitter {
  constructor(private db: MessageDB, private opts: EmitterOpts) {}

  emit(): void {
    const cfg = this.opts.config ?? {};
    const now = (this.opts.now ?? (() => new Date()))();

    // Statusline: always publish current counts (unless explicitly disabled).
    if (cfg.statusline?.enabled !== false) {
      this.opts.statusline.write(this.db.awarenessCounts());
    }

    // Desktop: gated by enabled flags + quiet hours.
    const awarenessOn = cfg.enabled !== false;
    const desktopOn = cfg.desktop?.enabled !== false;
    if (!awarenessOn || !desktopOn) return;
    if (isQuietHours(now, cfg.desktop?.quiet_hours)) return;

    const criticals = this.db.getUnnotifiedCritical();
    if (criticals.length === 0) return;

    const names = this.db.resolveContactNames(
      [...new Set(criticals.map(c => c.sender_id).filter(Boolean) as string[])]
    );
    for (const e of dedupeByThread(criticals)) {
      const name = names.get(e.sender_id ?? '') ?? e.sender_id ?? 'Someone';
      const { title, body } = formatNotification(e, name);
      this.opts.desktop.notify(title, body);
    }

    // Mark ALL fetched criticals notified (incl. same-thread ones we collapsed),
    // so a thread that already pinged doesn't re-ping next cycle for old messages.
    this.db.markNotified(criticals.map(c => c.message_id));
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && npx vitest run src/awareness.test.ts`
Expected: PASS (all describe blocks).

- [ ] **Step 5: Commit**

```bash
git add server/src/awareness.ts server/src/awareness.test.ts
git commit -m "feat(awareness): emitter — per-thread desktop + statusline counts"
```

---

### Task 5: Daemon integration + config + runbook

**Files:**
- Modify: `server/src/daemon.ts`
- Create: `docs/priority-awareness-setup.md`
- Test: build + the emitter's own unit tests (daemon wiring is verified by build + smoke; the emit logic is already unit-tested in Task 4)

**Interfaces:**
- Consumes: `AwarenessEmitter`, `DesktopSink`, `StatuslineSink`, `this.config.awareness`.
- Produces: the daemon calls `this.awareness.emit()` once per cycle; `awareness.json` lands in the data dir.

- [ ] **Step 1: Construct the emitter in the daemon constructor**

In `server/src/daemon.ts`, add imports at the top:

```typescript
import { AwarenessEmitter, DesktopSink, StatuslineSink } from './awareness.js';
```

Add a field near the other private fields (~line 25):

```typescript
  private awareness: AwarenessEmitter;
```

In the constructor, after `this.db = new MessageDB(...)` (~line 41), add:

```typescript
    this.awareness = new AwarenessEmitter(this.db, {
      config: this.config.awareness,
      desktop: new DesktopSink(),
      statusline: new StatuslineSink(path.join(dataDir, 'awareness.json')),
    });
```

- [ ] **Step 2: Call `emit()` once per cycle**

In `runCycle` (the method containing `this.cycleCount++` and `this.writeHealth(...)`, ~line 260), immediately AFTER `this.writeHealth(Date.now() - cycleStart);`, add:

```typescript
    try {
      this.awareness.emit();
    } catch (err) {
      this.log(`awareness emit error: ${err}`);  // must never break the sync loop
    }
```

- [ ] **Step 3: Build to verify daemon compiles**

Run: `cd server && npm run build`
Expected: build succeeds, no TypeScript errors.

- [ ] **Step 4: Smoke-test the emitter against a throwaway DB**

Run this one-off (verifies emit writes awareness.json and doesn't throw with no config):

```bash
cd server && cat > /tmp/aware-smoke.mjs <<'EOF'
import { MessageDB } from './build/mcp.mjs';
EOF
node -e "process.exit(0)"
echo "build present:"; ls build/awareness* 2>/dev/null || echo "(awareness bundled into daemon.mjs, not a separate entry — OK)"
```
Expected: no error. (The emitter ships inside `daemon.mjs`; there is no separate `awareness.mjs` entry — that is expected, it's imported by the daemon.)

- [ ] **Step 5: Write the runbook**

Create `docs/priority-awareness-setup.md`:

```markdown
# Priority awareness setup (A1)

The daemon writes unseen critical/exceptional counts to
`~/.claude/local/messages/awareness.json` every sync cycle, e.g.:
`{ "critical": 2, "exceptional": 5, "updated_at": "2026-07-06T…Z" }`
and fires a desktop notification (via `notify-send`) once per critical thread.

## Config (`~/.claude/local/messages/config.yml`)
```yaml
awareness:
  enabled: true            # master switch (default true)
  desktop:
    enabled: true          # desktop notifications for CRITICAL messages (default true)
    quiet_hours:           # optional; skip desktop in this window (local time)
      start: "22:00"
      end: "07:00"
  statusline:
    enabled: true          # write awareness.json (default true)
```
Omit the whole block to accept defaults (all on, no quiet hours). Restart the
daemon after editing: `systemctl --user restart legion-messages`.

## Statusline wiring (claude-statusline is a separate plugin)
Add a segment that reads the JSON and prints a glanceable count. Example shell
snippet (prints `⚡2 !5` when there are unseen items, nothing when clear):
```bash
f=~/.claude/local/messages/awareness.json
[ -f "$f" ] && python3 -c "import json,sys; d=json.load(open('$f')); c,e=d.get('critical',0),d.get('exceptional',0); print((f'⚡{c} ' if c else '')+(f'!{e}' if e else ''), end='')"
```
Marking messages seen (`priority_mark_seen`, or viewing the priority inbox)
clears the counts on the next cycle.
```

- [ ] **Step 6: Commit**

```bash
git add server/src/daemon.ts docs/priority-awareness-setup.md
git commit -m "feat(awareness): wire emitter into daemon cycle + setup runbook"
```

---

## Self-Review

**Spec coverage (design §8 Awareness layer, A1 scope):**
- Desktop notification on critical arrival, deduped per thread, quiet-hours, notify-once → Tasks 1 (`notified_at`), 2 (quiet/dedupe), 3 (DesktopSink), 4 (emitter). ✅
- Statusline ambient count (critical+exceptional), non-interrupting → Tasks 1 (`awarenessCounts`), 3 (StatuslineSink), 5 (daemon writes `awareness.json`) + runbook wiring. ✅
- Pluggable sink interface → `DesktopSink`/`StatuslineSink` are separate injectable classes; phone push is a future sink, not built (YAGNI). ✅
- Never blocks sync loop → Task 5 try/catch around `emit()`; DesktopSink swallows spawn errors. ✅
- Awareness fires on **importance/tier**, not attention → emitter queries `tier='critical'`. ✅
- Resolves §14 Q4 (statusline contract) → file-poll via `awareness.json` + documented wiring. ✅
- Priority inbox (the third channel) already shipped in A0 — not repeated here. ✅

**Placeholder scan:** No TBD/TODO; every code step has complete code. Task 5 Step 4 is a presence check, not a placeholder.

**Type consistency:** `AwarenessCounts` (Task 1) / `AwarenessConfig` (Task 2) defined before use; `getUnnotifiedCritical`/`markNotified`/`awarenessCounts` names consistent across Tasks 1→4; `DesktopSink.notify(title, body)` and `StatuslineSink.write(counts)` signatures match between Tasks 3 and 4; emitter reads `cfg.desktop?.quiet_hours` matching the `AwarenessConfig` shape and the runbook's config keys.
