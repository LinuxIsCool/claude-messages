# Effective Threads — Slice 1 Design (Messages Webui Foundation)

- **Date:** 2026-06-14
- **Author:** Matt (session 5ca8176d)
- **Status:** approved (Shawn, 2026-06-14) — architecture: Hybrid; build: Slice 1 first
- **Repo:** `claude-messages` plugin — `web/` (Python kernel webui) + `server/` (TS daemon)
- **Brainstorm (60 concepts):** `~/Workspace/card-ideas/docs/messages-webui-brainstorm.md`
- **Research:**
  - `~/.claude/local/research/2026/06/14/messaging-ux/thread-centric-inbox-ux.md`
  - `~/.claude/local/research/2026/06/14/viz-communications/comms-network-embedding-viz.md`

## 1. Problem
The messages webui is a flat, signal-ranked **message** list. It's slow-feeling,
buggy (stats chip errors, email bodies render as raw base64 MIME), the platform
filter is single-select, sort is fixed, and there's no real thread experience.
Shawn: *"What I really need is effective Threads."*

## 2. Goal
Make the **thread the primary object**, triage-able even when data is incomplete,
in the existing vanilla-JS Python-kernel webui. This is the foundation every
later slice (lanes, linking, viz) sits on.

## 3. Non-goals (later slices)
- Multi-column lanes board, saved-view server persistence (S2)
- Venture-tagging, message→task, outbox round-trip, backlinks (S3)
- Network graph / embedding map / heatmap React islands (S4)
- Reply composer / keyboard send

## 4. Architecture — thread-first 3-pane
Render path: **vanilla JS in `web/index.html`** (hybrid: React viz islands arrive
in S4; S1 is the fast sovereign core). Layout:

```
┌──────┬─────────────────────────┬───────────────────────────┐
│ rail │ THREAD LIST (kbd home)  │ THREAD VIEW (drawer)       │
│ 60px │ 360px                   │ flex                       │
│views │ [filter chips]          │ thread header (inline meta)│
│plat  │ [thread rows]           │ message timeline (decoded) │
│links │                         │ salience per message       │
└──────┴─────────────────────────┴───────────────────────────┘
```
- The thread list is the keyboard focus home. Opening a thread fills the view
  pane (a drawer, not a page nav); `Esc` returns to the list without losing place.
- The current nav (Messages/Search/Stats/Threads) is replaced by: **Threads**
  (primary) + Search + Stats. "Messages" flat feed is retired as the default
  (kept reachable via a view, but Threads is the landing).

## 5. Components

### 5.1 Thread row (`threadRowHTML`)
One row = one thread. Structure, left→right:
- platform glyph(s) — the thread's platform; if cross-platform identity, show primary.
- **participant faces** — up to 3 stacked identicons + "+k", each bordered by
  the participant's Dunbar-tier color (red clique → blue acquaintance).
- **subject/title** (bold) + optional **venture chip** (color-coded; only if linked — S3 wires linking, S1 renders if present).
- **decoded last-message preview** — 1 line, clamped.
- right cluster: `◆ salience` pill · relative time · msg-count · unread dot.
- **left border = thread-health state**: 🔴 needs-reply (last msg not from me) ·
  ⏳ waiting (I sent last, no reply) · ✅ resolved/neutral.

States: default · hover · selected · **degraded** (see 5.4).

### 5.2 Filter bar
- **Platform: multi-select checkboxes** — telegram·signal·email·whatsapp·slack,
  default all on. (Replaces the single dropdown.)
- Filter chips (composable, multi-select): participant · date-range · unread ·
  **needs-reply** · has-attachment · keyword (FTS over thread content).
- Sort menu: **last-activity (default)** · salience · activity-velocity ·
  people (max participant connectedness) · unread-first.
- Clear-all.

### 5.3 Numbered views (`1`–`9`, left rail)
Client-side filter presets, keyboard-jumpable:
`1` All · `2` Telegram · `3` Signal · `4` Email · `5` Slack/WhatsApp ·
`6` **Needs Reply** · `7` Unread · `8`,`9` user-defined (localStorage).
A view = a saved filter+sort state. Active view highlighted.

### 5.4 Graceful degradation (the incomplete-data answer)
Every thread renders and triages regardless of quality:
- missing sender name → raw handle, muted italic, + a "link" affordance (→ MCP `link_identities`, wired S3; S1 shows the affordance disabled/tooltip).
- undecoded/empty content → `⚠ Undecoded · <platform> · <time>`, fall back to the
  subject if present.
- **Never hide a thread because data is incomplete.** Triage (open/mark) always works.

### 5.5 Thread view pane
- header: platform · participants (faces + names) · type · msg-count · last-activity · (LLM summary line if `thread_summaries` has one).
- timeline: messages chrono, decoded, each with its `◆salience` + reason hover + reply-context.

## 6. Data layer

### 6.1 `/api/threads` — extend (the enrich pass is already index-fast)
Add to each thread object:
- `participants`: `[{name, dunbar_layer}]` (≤5) — resolved via identity_links ⋈
  contact_scores, for faces. (Bounded join per page row; cheap with the new indexes.)
- `needs_reply`: bool — true if the thread's last message `direction != 'sent'`.
- `last_content`: **decoded** (post B0b) preview.
- keep: id, platform, title, thread_type, participant_count, message_count,
  updated_at, last_ts, salience.
Query params: add `platforms=a,b,c` (CSV multi-select) + `sort=last_activity|salience|activity|people|unread` + `unread`, `needs_reply`, `has_attachment` filters.

### 6.2 Sorts
- last_activity: `ORDER BY last_ts DESC` (cheap, indexed).
- salience: annotate page, sort desc (current thread salience path).
- activity-velocity: message_count / age — compute on the page.
- people: max participant connectedness — from the participants join.
- unread-first: unread flag then last_ts. (Unread tracking: S1 derives "unread"
  = has messages newer than a per-thread `last_seen` in localStorage; no schema change.)

## 7. Bug fixes (Step 0, prerequisites)
### B0a — stats chip "Error loading stats"
`web/index.html` `apiPath()` strips the leading slash → `/api/stats` fetches
`:8800/api/stats` (404) under the hub mount. Fix: keep the path relative to the
document base (preserve/normalize so it resolves under `/messages/`). 1-line.

### B0b — email bodies are raw base64/MIME (4,462/32,337 = 14%)
- **Forward:** `server/src/adapters/email.ts` — replace the raw-part store with
  `mailparser.simpleParser` → decoded `text` (+ strip quoted chains/signature to a
  collapsible later). Re-sync not required for the fix to take effect on new mail.
- **Retro:** one-off `server/scripts/decode-email-bodies.ts` (or Python) — for
  rows where `content` looks like MIME (`Content-Transfer-Encoding` / base64 /
  quoted-printable), decode in place. Idempotent; dry-run + count first; back up
  the DB (snapshot) before the UPDATE pass. Losslessly decodable for the QP/base64
  majority; rows that aren't → leave + flag for degraded render.

## 8. File change map
- `web/index.html` — apiPath fix · 3-pane layout · threadRowHTML (faces, health,
  decoded preview, venture chip) · multi-select platform filter · sort menu ·
  numbered views · graceful-degradation rendering · thread-view pane.
- `web/messages_data.py` — `list_threads` returns participants + needs_reply +
  decoded preview; `platforms` CSV filter; the 5 sorts.
- `web/messages_accessor.py` — thread participants join (cached set reuse);
  threads() passes new params.
- `server/src/adapters/email.ts` — mailparser decode (forward).
- `server/scripts/decode-email-bodies.ts` — retro decode pass (new).

## 9. Testing
- Python: unit-test `list_threads` param handling (platforms CSV, each sort,
  needs_reply, unread) against a fixture DB; assert participants shape + needs_reply.
- email decode: unit-test the retro decoder on sample base64 + quoted-printable
  bodies → expected plaintext; idempotency (run twice = same).
- API smoke: `/api/threads?platforms=email,signal&sort=needs_reply` returns sane
  rows; timing <300ms warm.
- Manual: load Threads, toggle platforms, each sort, view 6 Needs-Reply correct,
  email previews readable, a deliberately-broken row still triage-able, stats chip OK.

## 10. Acceptance
- Thread list loads <300ms warm.
- All 5 platforms individually multi-selectable; default all on.
- 5 sorts function; Needs-Reply view correct.
- Email previews + thread bodies render as readable text (post B0b).
- No thread hidden by missing/undecoded data; every row triage-able.
- Stats chip shows real counts.

## 11. Rollout
- B0b retro decode: snapshot DB → dry-run count → apply → spot-check. Reversible
  via snapshot.
- Webui changes are read-only render + query; restart `claude-webui-platform.service`.
- Daemon (email adapter) rebuild + the next sync uses decoded bodies.
