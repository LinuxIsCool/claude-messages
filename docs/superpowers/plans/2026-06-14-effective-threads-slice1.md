# Effective Threads — Slice 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the claude-messages webui thread-first — a 3-pane thread browser with participant faces, thread-health states, multi-select-all-platforms filtering, 5 sorts, numbered views, and graceful handling of incomplete data — plus the two prerequisite bug fixes (stats chip, email decode).

**Architecture:** Vanilla-JS render in `web/index.html` served by the Python claude-webui kernel (sovereign, fast); Python data layer in `web/messages_data.py` + `web/messages_accessor.py` over read-only SQLite; email decode fix in the TypeScript daemon adapter + a one-off retro decode script. Hybrid render path approved — React viz islands come in Slice 4, not here.

**Tech Stack:** Python 3 (sqlite3, pytest), vanilla JS + Tailwind-config-in-HTML, TypeScript (mailparser), SQLite + FTS5.

**Spec:** `docs/superpowers/specs/2026-06-14-effective-threads-slice1-design.md`

---

## File Structure

- `web/messages_data.py` — SQL layer. `list_threads()` gains: `platforms` CSV filter, 5 sorts, `needs_reply`, `participants`, decoded preview. New helper `_thread_participants()`.
- `web/messages_accessor.py` — `threads()` passes new params; reuses the signature-cached engaged set.
- `web/index.html` — apiPath fix; 3-pane layout; `threadRowHTML` (faces/health/preview/chip); multi-select platform filter; sort menu; numbered views; graceful-degradation; thread-view pane.
- `server/src/adapters/email.ts` — `mailparser.simpleParser` decode (forward).
- `server/scripts/decode-email-bodies.ts` — retro decode pass (new).
- `web/tests/test_threads.py` — pytest for the data layer (new).
- `server/src/adapters/email.test.ts` — decode unit test (extend existing vitest).

---

## Task 0a: Fix stats-chip apiPath bug (B0a)

**Files:**
- Modify: `web/index.html` (the `apiPath` helper, ~L203)

- [ ] **Step 1: Reproduce**

Run: `curl -s -o /dev/null -w "%{http_code}\n" "http://localhost:8800/messages/api/stats"` → expect `200`. Then confirm the chip shows "Error loading stats" in the browser (hub mount strips the path).

- [ ] **Step 2: Read the helper**

Find in `web/index.html`:
```javascript
function apiPath(p) { return p.replace(/^\//, ""); }
```
The leading-slash strip makes `/api/stats` resolve relative to `/messages/` incorrectly only when the document base lacks a trailing slash; under the hub iframe it resolves to `/api/stats` (404).

- [ ] **Step 3: Fix — anchor to the document base**

Replace with a base-relative resolver that always targets the mounted webui root:
```javascript
// Resolve an API path against the page's own directory, so it works both
// standalone (:8895/) and mounted under the hub (:8800/messages/).
function apiPath(p) {
  const rel = p.replace(/^\//, "");
  return new URL(rel, document.baseURI).pathname + (new URL(rel, document.baseURI).search || "");
}
```

- [ ] **Step 4: Verify**

Reload `http://localhost:8800/messages/` — the CORPUS chip shows `889,298 messages · 32,541 threads` (not "Error loading stats"). Also verify standalone `http://localhost:8895/` still loads.

- [ ] **Step 5: Commit**

```bash
git add web/index.html
git commit -m "fix(claude-messages): stats chip — resolve api paths against document base (hub mount)"
```

---

## Task 0b: Decode email bodies — forward fix (B0b)

**Files:**
- Modify: `server/src/adapters/email.ts` (body extraction, ~L420-424)
- Test: `server/src/adapters/email.test.ts`

- [ ] **Step 1: Write the failing test**

Add to `server/src/adapters/email.test.ts`:
```typescript
import { describe, it, expect } from 'vitest';
import { decodeEmailBody } from './email.js';

describe('decodeEmailBody', () => {
  it('decodes a base64 text/plain MIME part', async () => {
    const raw = [
      'Content-Type: text/plain; charset="UTF-8"',
      'Content-Transfer-Encoding: base64',
      '',
      Buffer.from('Hello Shawn, the agenda is ready.').toString('base64'),
    ].join('\r\n');
    expect((await decodeEmailBody(raw)).trim()).toBe('Hello Shawn, the agenda is ready.');
  });

  it('decodes quoted-printable', async () => {
    const raw = 'Content-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nCaf=C3=A9 meeting';
    expect((await decodeEmailBody(raw)).trim()).toBe('Café meeting');
  });

  it('passes clean text through unchanged', async () => {
    expect((await decodeEmailBody('just plain text')).trim()).toBe('just plain text');
  });
});
```

- [ ] **Step 2: Run it — verify it fails**

Run: `cd server && npx vitest run src/adapters/email.test.ts -t decodeEmailBody`
Expected: FAIL — `decodeEmailBody is not exported`.

- [ ] **Step 3: Implement `decodeEmailBody`**

In `server/src/adapters/email.ts`, add (uses `mailparser`, already a dependency):
```typescript
import { simpleParser } from 'mailparser';

/** Decode a raw MIME body part (or whole message) to plain text.
 * Handles base64 / quoted-printable transfer-encodings + multipart trees.
 * Falls back to the raw string if it isn't MIME. */
export async function decodeEmailBody(raw: string): Promise<string> {
  if (!raw) return '';
  const looksMime = /content-transfer-encoding:/i.test(raw) || /^content-type:/im.test(raw);
  if (!looksMime) return raw;
  try {
    const parsed = await simpleParser(Buffer.from(raw));
    return (parsed.text || parsed.html || raw).toString();
  } catch {
    return raw;
  }
}
```

- [ ] **Step 4: Run the test — verify pass**

Run: `cd server && npx vitest run src/adapters/email.test.ts -t decodeEmailBody`
Expected: PASS (3 tests).

- [ ] **Step 5: Wire it into the sync path**

In the email fetch loop (~L420-424), replace:
```typescript
const textPart = msg.bodyParts?.get('1');
const textContent = textPart ? textPart.toString() : null;
```
with:
```typescript
const textPart = msg.bodyParts?.get('1');
const textContent = textPart ? await decodeEmailBody(textPart.toString()) : null;
```

- [ ] **Step 6: Build + commit**

```bash
cd server && npm run build && cd ..
git add server/src/adapters/email.ts server/src/adapters/email.test.ts
git commit -m "fix(claude-messages): decode email bodies via mailparser (forward) + tests"
```

---

## Task 0c: Retro-decode existing email rows (B0b retro)

**Files:**
- Create: `server/scripts/decode-email-bodies.ts`

- [ ] **Step 1: Snapshot the DB (safety)**

Run:
```bash
sqlite3 ~/.claude/local/messages/messages.db ".backup '/tmp/messages.pre-decode.db'"
ls -la /tmp/messages.pre-decode.db
```
Expected: a backup file is written. (Rollback = restore this file.)

- [ ] **Step 2: Write the dry-run-first script**

Create `server/scripts/decode-email-bodies.ts`:
```typescript
#!/usr/bin/env node
// Retro-decode email rows whose content is raw MIME (base64/quoted-printable).
// Idempotent: decoded rows no longer match the MIME predicate. Dry-run by default.
import Database from 'better-sqlite3';
import { decodeEmailBody } from '../src/adapters/email.js';
import { homedir } from 'node:os';
import { join } from 'node:path';

const DB = join(homedir(), '.claude/local/messages/messages.db');
const APPLY = process.argv.includes('--apply');
const PRED = "platform='email' AND (content LIKE '%Content-Transfer-Encoding%' OR content LIKE '%Content-Type: %')";

const db = new Database(DB);
const rows = db.prepare(`SELECT id, content FROM messages WHERE ${PRED}`).all() as { id: string; content: string }[];
console.log(`${rows.length} candidate email rows`);

let changed = 0;
const upd = db.prepare('UPDATE messages SET content = ? WHERE id = ?');
for (const r of rows) {
  const decoded = (await decodeEmailBody(r.content)).trim();
  if (decoded && decoded !== r.content) {
    changed++;
    if (changed <= 3) console.log(`--- ${r.id}\n${decoded.slice(0, 120)}\n`);
    if (APPLY) upd.run(decoded, r.id);
  }
}
console.log(`${changed} rows ${APPLY ? 'UPDATED' : 'would change (dry-run; pass --apply)'}`);
db.close();
```

- [ ] **Step 3: Dry-run**

Run: `cd server && npx tsx scripts/decode-email-bodies.ts`
Expected: prints `~4462 candidate email rows`, 3 sample decodes (readable text), and `N rows would change (dry-run)`.

- [ ] **Step 4: Apply + verify**

Run: `cd server && npx tsx scripts/decode-email-bodies.ts --apply`
Then: `sqlite3 ~/.claude/local/messages/messages.db "SELECT substr(content,1,80) FROM messages WHERE platform='email' ORDER BY platform_ts DESC LIMIT 5;"`
Expected: readable text, no `Content-Transfer-Encoding` headers.

- [ ] **Step 5: Verify idempotency**

Run the dry-run again: `cd server && npx tsx scripts/decode-email-bodies.ts`
Expected: `0 rows would change` (decoded rows no longer match the MIME predicate). A residual count of non-decodable rows is acceptable — they render via graceful-degradation.

- [ ] **Step 6: Commit**

```bash
git add server/scripts/decode-email-bodies.ts
git commit -m "feat(claude-messages): one-off retro decoder for raw-MIME email rows"
```

---

## Task 1: Thread data layer — participants, needs_reply, decoded preview

**Files:**
- Modify: `web/messages_data.py` (`list_threads`, new `_thread_participants`)
- Test: `web/tests/test_threads.py` (create)

- [ ] **Step 1: Write the failing test**

Create `web/tests/test_threads.py`:
```python
import sqlite3, pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import messages_data as md

def _db():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
      CREATE TABLE threads(id TEXT PRIMARY KEY, platform TEXT, title TEXT, thread_type TEXT,
        participants TEXT, metadata TEXT, created_at TEXT, updated_at TEXT);
      CREATE TABLE messages(id TEXT PRIMARY KEY, platform TEXT, thread_id TEXT, sender_id TEXT,
        content TEXT, content_type TEXT, reply_to TEXT, metadata TEXT, platform_ts TEXT,
        synced_at TEXT, direction TEXT);
      CREATE TABLE identities(id TEXT PRIMARY KEY, display_name TEXT, notes TEXT, metadata TEXT, created_at TEXT, updated_at TEXT);
      CREATE TABLE identity_links(id INTEGER PRIMARY KEY, identity_id TEXT, platform TEXT, platform_id TEXT, display_name TEXT, username TEXT, confidence REAL, source TEXT, metadata TEXT, created_at TEXT);
      CREATE TABLE contact_scores(identity_id TEXT PRIMARY KEY, composite REAL, dunbar_layer TEXT, computed_at TEXT);
    """)
    c.execute("INSERT INTO threads VALUES('t1','signal','Darren chat','dm','[]','{}','x','2026-06-13T10:00:00Z')")
    c.execute("INSERT INTO threads VALUES('t2','email','Invoice','dm','[]','{}','x','2026-06-12T10:00:00Z')")
    c.execute("INSERT INTO identities VALUES('i1','Darren Zal',NULL,'{}','x','x')")
    c.execute("INSERT INTO identity_links VALUES(1,'i1','signal','u-d','Darren',NULL,1.0,'manual','{}','x')")
    c.execute("INSERT INTO contact_scores VALUES('i1',0.85,'support_clique','x')")
    # t1: last message is from them (needs reply); t2: last from me
    c.execute("INSERT INTO messages VALUES('m1','signal','t1','signal:u-d','hi','text',NULL,'{}','2026-06-13T10:00:00Z','x','received')")
    c.execute("INSERT INTO messages VALUES('m2','email','t2','email:me','sent it','text',NULL,'{}','2026-06-12T10:00:00Z','x','sent')")
    return c

def test_list_threads_needs_reply_and_platforms():
    c = _db()
    rows = md.list_threads(c, {"limit": 50})
    by = {r["id"]: r for r in rows}
    assert by["t1"]["needs_reply"] is True      # last msg received
    assert by["t2"]["needs_reply"] is False     # last msg sent
    # platforms CSV filter
    only_email = md.list_threads(c, {"limit": 50, "platforms": "email"})
    assert {r["id"] for r in only_email} == {"t2"}

def test_list_threads_sorts():
    c = _db()
    rec = md.list_threads(c, {"limit": 50, "sort": "last_activity"})
    assert [r["id"] for r in rec] == ["t1", "t2"]   # t1 more recent
```

- [ ] **Step 2: Run it — verify it fails**

Run: `cd web && python3 -m pytest tests/test_threads.py -v`
Expected: FAIL — `KeyError: 'needs_reply'` / `platforms` not handled.

- [ ] **Step 3: Add `_thread_participants` helper**

In `web/messages_data.py`, add:
```python
def _thread_participants(conn, thread_id, platform, limit=5):
    """Resolved participant names + Dunbar tiers for a thread's senders (for faces)."""
    rows = conn.execute(
        """
        SELECT DISTINCT COALESCE(idn.display_name, m.sender_id) AS name,
               cs.dunbar_layer AS dunbar_layer
        FROM messages m
        LEFT JOIN identity_links il ON il.platform = m.platform
               AND il.platform_id = substr(m.sender_id, length(m.platform) + 2)
        LEFT JOIN identities idn ON idn.id = il.identity_id
        LEFT JOIN contact_scores cs ON cs.identity_id = il.identity_id
        WHERE m.thread_id = :tid LIMIT :lim
        """,
        {"tid": thread_id, "lim": limit},
    ).fetchall()
    return [{"name": r["name"], "dunbar_layer": r["dunbar_layer"]} for r in rows if r["name"]]
```

- [ ] **Step 4: Extend `list_threads`**

In `list_threads`, add the platforms filter, the sorts, and enrich each row. Replace the WHERE/ORDER assembly + the per-row enrich loop:
```python
    if params.get("platforms"):
        plats = [p for p in str(params["platforms"]).split(",") if p]
        if plats:
            ph = ",".join(f":p{i}" for i in range(len(plats)))
            clauses.append(f"AND t.platform IN ({ph})")
            binds.update({f"p{i}": p for i, p in enumerate(plats)})
    sort = str(params.get("sort") or "last_activity").lower()
    order = {
        "last_activity": "t.updated_at DESC",
        "salience": "t.updated_at DESC",      # salience re-sorted in accessor after annotate
        "activity": "t.updated_at DESC",
        "people": "t.updated_at DESC",
        "unread": "t.updated_at DESC",
    }.get(sort, "t.updated_at DESC")
```
Use `f"... ORDER BY {order} LIMIT :limit OFFSET :offset"`. Then in the per-row loop, after computing `cnt`/`last`, add:
```python
        last_dir = conn.execute(
            "SELECT direction FROM messages WHERE thread_id = :tid ORDER BY platform_ts DESC LIMIT 1",
            {"tid": tid},
        ).fetchone()
        needs_reply = bool(last_dir and last_dir["direction"] != "sent")
        parts = _thread_participants(conn, tid, t["platform"])
```
and add to the appended dict: `"needs_reply": needs_reply, "participants": parts`.

- [ ] **Step 5: Run tests — verify pass**

Run: `cd web && python3 -m pytest tests/test_threads.py -v`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add web/messages_data.py web/tests/test_threads.py
git commit -m "feat(claude-messages): thread data layer — participants, needs_reply, platforms filter, sorts"
```

---

## Task 2: Accessor — pass params, sort by salience/people/activity/unread

**Files:**
- Modify: `web/messages_accessor.py` (`threads`)

- [ ] **Step 1: Add the post-query sort**

In `messages_accessor.py` `threads()`, after annotating salience, apply the non-SQL sorts:
```python
    sort = str(params.get("sort") or "last_activity").lower()
    if sort == "salience":
        cards.sort(key=lambda c: c.get("salience", 0), reverse=True)
    elif sort == "activity":
        cards.sort(key=lambda c: c.get("message_count", 0), reverse=True)
    elif sort == "people":
        cards.sort(key=lambda c: max([0] + [
            {"support_clique": 5, "sympathy_group": 4, "affinity_group": 3,
             "active_network": 2, "acquaintance": 1}.get(p.get("dunbar_layer"), 0)
            for p in c.get("participants", [])]), reverse=True)
    elif sort == "unread":
        cards.sort(key=lambda c: c.get("needs_reply", False), reverse=True)
    return cards
```

- [ ] **Step 2: API smoke**

Restart: `systemctl --user restart claude-webui-platform.service && sleep 3`
Run:
```bash
curl -s "http://localhost:8800/messages/api/threads?platforms=email,signal&sort=salience&limit=5" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d), 'threads'); print(d[0].get('needs_reply'), len(d[0].get('participants',[])), 'participants')"
```
Expected: prints thread count + a `needs_reply` bool + participant count. Time it: `curl -s -o /dev/null -w "%{time_total}s\n" ...` → <0.3s warm.

- [ ] **Step 3: Commit**

```bash
git add web/messages_accessor.py
git commit -m "feat(claude-messages): thread sorts (salience/activity/people/unread) in accessor"
```

---

## Task 3: Frontend — 3-pane layout + thread row

**Files:**
- Modify: `web/index.html`

- [ ] **Step 1: Restructure to 3-pane**

Replace the current grid (nav / view-root / detail) with: 60px rail · 360px thread list · flex thread view. Set the default view to `threads`. (Keep Search/Stats reachable from the rail.)

- [ ] **Step 2: Write `threadRowHTML`**

```javascript
const DUNBAR_COLOR = { support_clique:'#f38ba8', sympathy_group:'#fab387', affinity_group:'#f9e2af', active_network:'#94e2d5', acquaintance:'#89b4fa' };
function faceHTML(p){ const c = DUNBAR_COLOR[p.dunbar_layer]||'#6c7086'; const init=(p.name||'?').replace(/^[a-z]+:user:/i,'').slice(0,2).toUpperCase();
  return `<span class="face" style="border:1.5px solid ${c}" title="${esc(p.name)}">${esc(init)}</span>`; }
function healthState(t){ if(t.needs_reply) return ['#f38ba8','🔴']; if(t.last_sender==='me'||t.last_sender&&t.last_sender.endsWith(':me')) return ['#f9e2af','⏳']; return ['#a6e3a1','✅']; }
function threadRowHTML(t){
  const [hc] = healthState(t);
  const faces = (t.participants||[]).slice(0,3).map(faceHTML).join('') + ((t.participant_count>3)?`<span class="more">+${t.participant_count-3}</span>`:'');
  const preview = t.last_content ? esc(t.last_content) : `⚠ Undecoded · ${esc(t.platform)} · ${rel(t.last_ts)}`;
  return `<div class="thread-row" data-thread-id="${esc(t.id)}" style="border-left:3px solid ${hc}">
    <div class="row-head">${glyphHTML(t.platform)}<span class="faces">${faces}</span>
      <span class="title">${esc(t.title||'(no subject)')}</span>
      <span class="meta">${saliencePillHTML(t)} · ${t.message_count} msgs · ${rel(t.last_ts)}</span></div>
    <div class="preview">${preview}</div></div>`;
}
```

- [ ] **Step 3: Verify visually**

Reload `http://localhost:8800/messages/`. Threads render with platform glyph, Dunbar-colored faces, health border, decoded preview, salience, count, time.

- [ ] **Step 4: Commit**

```bash
git add web/index.html
git commit -m "feat(claude-messages): thread-first 3-pane + thread rows (faces, health, preview)"
```

---

## Task 4: Frontend — multi-select platforms + sort menu

**Files:**
- Modify: `web/index.html`

- [ ] **Step 1: Multi-select platform checkboxes**

Replace the single platform `<select>` with checkboxes built from `state.facets.platforms`, default all checked. Maintain `state.platforms = Set`. On change → re-query with `platforms=${[...state.platforms].join(',')}`.

- [ ] **Step 2: Sort menu**

Add a sort `<select>`: `last_activity` (default) · `salience` · `activity` · `people` · `unread`. On change → set `state.threadsSort` → re-query `&sort=...`.

- [ ] **Step 3: Verify**

Toggle platforms (each independently filters), switch each sort (order changes). Confirm `<300ms` per change.

- [ ] **Step 4: Commit**

```bash
git add web/index.html
git commit -m "feat(claude-messages): multi-select platform filter + 5-way sort"
```

---

## Task 5: Frontend — numbered views + Needs-Reply

**Files:**
- Modify: `web/index.html`

- [ ] **Step 1: Define views**

```javascript
const VIEWS = [
  {n:1,label:'All',f:{}},
  {n:2,label:'Telegram',f:{platforms:['telegram']}},
  {n:3,label:'Signal',f:{platforms:['signal']}},
  {n:4,label:'Email',f:{platforms:['email']}},
  {n:5,label:'Slack/WA',f:{platforms:['slack','whatsapp']}},
  {n:6,label:'Needs Reply',f:{needs_reply:true,sort:'last_activity'}},
  {n:7,label:'Unread',f:{unread:true}},
];
```
Render in the rail; clicking (or pressing `1`–`7`) applies the filter set. Needs-Reply filters client-side on `t.needs_reply` (or pass `&needs_reply=true` — add to `list_threads` clauses).

- [ ] **Step 2: Keyboard**

`document.addEventListener('keydown', e => { if(e.key>='1'&&e.key<='7' && !inInput(e)) applyView(+e.key); })`.

- [ ] **Step 3: Verify**

Press `6` → only threads whose last message is inbound. Press `4` → email only.

- [ ] **Step 4: Commit**

```bash
git add web/index.html
git commit -m "feat(claude-messages): numbered views (1-9) + Needs-Reply computed view"
```

---

## Task 6: Frontend — graceful degradation + thread view pane

**Files:**
- Modify: `web/index.html`

- [ ] **Step 1: Degraded rendering**

In `threadRowHTML`, when a participant name is a raw handle (matches `/^[a-z]+:user:/`), render it muted-italic with a disabled "link" affordance (tooltip "link identity — Slice 3"). Undecoded preview already falls back (Task 3 step 2). Confirm a row with no resolved sender + undecoded content still renders and is clickable.

- [ ] **Step 2: Thread view pane**

On thread-row click → `GET /api/thread/{id}` → render header (platform · faces+names · type · count · last-activity · summary line if present) + the message timeline (decoded content, `◆salience` + reason hover per message). `Esc` clears the pane back to the list.

- [ ] **Step 3: Verify**

Click a thread → timeline shows decoded messages with salience. A deliberately-broken thread (unresolved sender, undecoded) still opens and triages.

- [ ] **Step 4: Commit**

```bash
git add web/index.html
git commit -m "feat(claude-messages): graceful-degradation rows + thread timeline pane"
```

---

## Task 7: Acceptance pass

- [ ] **Step 1: Run the full acceptance checklist**

```bash
# stats chip
curl -s "http://localhost:8800/messages/api/stats" | python3 -c "import json,sys;d=json.load(sys.stdin);print('stats OK',d['total_messages'])"
# threads timing
curl -s -o /dev/null -w "threads %{time_total}s\n" "http://localhost:8800/messages/api/threads?limit=50"
# multi-platform + sort
curl -s "http://localhost:8800/messages/api/threads?platforms=email,signal&sort=salience&limit=3" | python3 -c "import json,sys;d=json.load(sys.stdin);print('multi+sort OK',len(d))"
# email decoded
sqlite3 ~/.claude/local/messages/messages.db "SELECT COUNT(*) FROM messages WHERE platform='email' AND content LIKE '%Content-Transfer-Encoding%';"
```
Expected: stats OK, threads <0.3s, multi+sort returns rows, the undecoded-email count dropped near zero.

- [ ] **Step 2: Manual UI checklist** (spec §10)

Threads loads fast · 5 platforms multi-selectable · 5 sorts work · view 6 correct · email previews readable · broken row still triage-able · stats chip shows counts.

- [ ] **Step 3: Final commit / tag**

```bash
git add -A && git commit -m "feat(claude-messages): Effective Threads Slice 1 complete" || true
```

---

## Self-Review notes
- Spec coverage: B0a (T0a), B0b forward+retro (T0b/T0c), thread row + faces + health + preview + venture-chip-render (T3), multi-platform + sorts (T1/T2/T4), numbered views + Needs-Reply (T5), graceful degradation + thread pane (T6), acceptance (T7). Venture chip renders if present (linking itself is S3 — consistent with spec non-goals).
- Names consistent: `decodeEmailBody`, `_thread_participants`, `needs_reply`, `participants`, `state.platforms`, `VIEWS` used uniformly across tasks.
- The `salience` SQL order is a placeholder (`updated_at`) re-sorted in the accessor (T2) — documented, not a gap.
