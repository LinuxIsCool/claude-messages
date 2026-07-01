# Message Priority Ranking & Attention Awareness — Design Spec

- **Status:** draft (approved architecture; pending spec review)
- **Author:** Shawn Anderson + Claude
- **Created:** 2026-07-01
- **Component:** `claude-messages` plugin (`legion-plugins`)
- **Related:** `[[legion-messages-runtime-setup]]`, `[[telus-free-llm-access]]`

---

## 1. Problem & Goal

The `claude-messages` daemon unifies ~39k messages across Signal, Telegram, and email
(3,200+ contacts, 4,300+ threads). Today it ranks *people* via a fixed 8-factor
`ContactRank` → Dunbar layer model, but it does **not** rank *messages* by importance,
and has **no way to make the user aware** when something important arrives.

The user's mental model of the message corpus is a steep Pareto:

| Class | Est. share | Est. count (of ~39k) | Example |
|---|---:|---:|---|
| **critical** | ~0.1% | ~40 | GPU thread (Signal), live Telus decision |
| **exceptional** | ~0.9% | ~350 | Regen Gaia (Telegram), Carole Anne, Capstone students |
| **somewhat** | ~4% | ~1,600 | routine work threads |
| **irrelevant** | ~95% | ~37,000 | newsletters, group noise, automated mail |

**Goal:** accurately place every message into these tiers, surface a blended
*attention* ranking, and make high-importance arrivals *aware* to the user through
three channels — while never blocking the sync loop and never missing a message from
an explicitly-declared important thread/person.

### Non-goals (this spec)
- Sub-project **B** (people-attention re-ranking) and **C** (WhatsApp / Messenger / SMS
  ingestion) are **separate specs**. They are referenced here because A's engine feeds B
  and A's accuracy depends on C's data, but they are out of scope for implementation here.
- Phone push notifications (deferred by user).

---

## 2. Key Definitions

- **importance** ∈ [0,1] — how much this message matters *intrinsically* (who/what it is
  about). Stable; does not drop when you reply. Drives **awareness** (notifications).
- **urgency** ∈ [0,1] — how much this message needs *your action now*. Decays once
  answered/resolved.
- **attention** ∈ [0,1] — the blended ranking score used to order the priority inbox:
  `attention = 0.6 * importance + 0.4 * urgency` (weights tunable in config).
- **tier** ∈ {critical, exceptional, somewhat, irrelevant} — a calibrated bucket derived
  from **importance** (not attention), so tiering is stable and matches the Pareto.

> **Design rule:** awareness (desktop/statusline) fires on **importance/tier**;
> inbox *ordering* uses **attention**. This is why the GPU thread pings you even when
> there is nothing to do, and why mom (high importance, low urgency) never disappears.

---

## 3. Architecture — Two-Speed Scoring Engine

Scoring is split because requirements conflict: notifications must fire in
**milliseconds** on new messages, but accurate judgment is best made by an LLM reading
the message — too slow to run inline. Nothing that calls the LLM may block the daemon's
sync loop (same failure mode as the Telegram MTProto hang: a slow call stalling
everything).

```
        NEW MESSAGE (daemon inline hook, per synced message)
                     │
   ┌─────────────────▼── FAST PATH (<10ms, pure-local, no network) ──────────┐
   │  1. RULES engine      → importance FLOOR (hard override)                 │
   │  2. relationship       → sender ContactRank composite + Dunbar layer     │
   │  3. reply-state        → unanswered inbound from known contact ⇒ urgency │
   │  4. question heuristic → '?', imperative, deadline words ⇒ urgency       │
   │  ⇒ provisional {importance, urgency, tier, source='rule'}                │
   └─────────────────┬───────────────────────────────────────────────────────┘
                     │ write message_priority row + enqueue for warm/slow paths
        ┌────────────┼─────────────────────────────┐
        ▼            ▼                               ▼
   AWARENESS    WARM PATH (async, seconds)      SLOW PATH (async, batched)
   • desktop    • embed message (TELUS e5)      • LLM-as-judge (TELUS Gemma)
     (critical)   → cosine to "important          reads message + thread context,
   • statusline    centroids"                     returns {importance, urgency,
     (count)     → refine importance              tier, rationale, suggested_cohort}
   • inbox       source='embed'                  • calibrated to Pareto (§6.3)
     (order by                                   • source='llm'; becomes TRAINING LABEL
      attention)                                  └──────────────┬────────────────────┘
                                                                 ▼ (once labels accumulate)
                                            LEARNED RANKER (Phase A3) — §7
                                            GBM on embeddings + ContactRank + thread
                                            signals; replaces LLM judge at runtime;
                                            user corrections → retrain (active learning)
```

### 3.1 Why three speeds, not two
The narrative is "fast vs slow," but embedding-similarity sits between: it needs one
TELUS network call (too slow for the truly-inline hook, fast enough to run seconds
later). So concretely: **inline** (rules + local heuristics, always) → **warm**
(embedding refine, best-effort) → **slow** (LLM judge, batched). Awareness triggers off
the inline result so it is never delayed; warm/slow only *improve* the score.

### 3.2 Rules + learning coexist permanently
The RULES engine is a **hard floor**, not a feature. A learned model can *raise* a
score but can **never demote** something the user declared important. Rules cover the
enumerable few; ML covers the ambiguous 99%.

---

## 4. Data Model

New tables in `messages.db` (SQLite). All timestamps ISO-8601 UTC.

```sql
-- 4.1 User's explicit deterministic signals (the "floor")
CREATE TABLE priority_rules (
  id           INTEGER PRIMARY KEY,
  rule_type    TEXT NOT NULL,          -- 'thread' | 'identity' | 'cohort' | 'platform_folder' | 'keyword'
  match_value  TEXT NOT NULL,          -- thread_id | identity_id | cohort_id | 'email:INBOX/Telus' | regex
  importance_floor REAL NOT NULL,      -- 0..1; tier floor this rule guarantees
  tier_floor   TEXT NOT NULL,          -- redundant human-readable: 'critical' etc.
  note         TEXT,
  enabled      INTEGER NOT NULL DEFAULT 1,
  created_at   TEXT NOT NULL
);

-- 4.2 Named groups of people ("Telus", "Indigenomics Capstone")
CREATE TABLE cohorts (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  description TEXT,
  created_at  TEXT NOT NULL
);
CREATE TABLE cohort_members (
  cohort_id   INTEGER NOT NULL REFERENCES cohorts(id),
  identity_id TEXT    NOT NULL REFERENCES identities(id),
  PRIMARY KEY (cohort_id, identity_id)
);

-- 4.3 Per-message scores (the engine's output)
CREATE TABLE message_priority (
  message_id   TEXT PRIMARY KEY REFERENCES messages(id),
  importance   REAL NOT NULL,
  urgency      REAL NOT NULL,
  attention    REAL NOT NULL,
  tier         TEXT NOT NULL,          -- critical | exceptional | somewhat | irrelevant
  source       TEXT NOT NULL,          -- rule | embed | llm | model | feedback
  model_version TEXT,
  rationale    TEXT,                   -- one-line 'why' (from LLM or rule name)
  needs_llm    INTEGER NOT NULL DEFAULT 1,  -- slow-path queue flag
  seen         INTEGER NOT NULL DEFAULT 0,  -- for awareness dedup
  scored_at    TEXT NOT NULL
);
CREATE INDEX idx_mp_tier      ON message_priority(tier);
CREATE INDEX idx_mp_attention ON message_priority(attention DESC);
CREATE INDEX idx_mp_needs_llm ON message_priority(needs_llm) WHERE needs_llm = 1;
CREATE INDEX idx_mp_unseen    ON message_priority(seen, tier) WHERE seen = 0;

-- 4.4 Active-learning corrections (highest-authority label)
CREATE TABLE priority_feedback (
  id          INTEGER PRIMARY KEY,
  message_id  TEXT NOT NULL REFERENCES messages(id),
  user_tier   TEXT,                    -- explicit tier the user asserts
  user_importance REAL,
  user_urgency    REAL,
  note        TEXT,
  created_at  TEXT NOT NULL
);

-- 4.5 Cached embeddings (reused by warm path + learned ranker + search)
CREATE TABLE message_embeddings (
  message_id  TEXT PRIMARY KEY REFERENCES messages(id),
  dim         INTEGER NOT NULL,        -- 1024 for nv-embedqa-e5-v5
  vector      BLOB NOT NULL,           -- float32 packed
  model       TEXT NOT NULL,
  embedded_at TEXT NOT NULL
);
```

**Label authority order** (highest wins): `feedback` > `rule` > `llm`/`model` >
`embed` > inline-heuristic. Stored in `source`; the composer (§5) resolves conflicts.

---

## 5. Scoring Composition

For a message `m`:

```
imp_rule     = max importance_floor over matching enabled rules (or 0)
imp_rel      = f(ContactRank composite of sender, Dunbar layer)      # 0..1
imp_semantic = cosine(embed(m), importance_centroids)  [warm]        # 0..1
imp_model    = learned ranker importance head          [A3, if trained]

importance = max( imp_rule,                       # hard floor — never demoted
                  clamp( w_rel*imp_rel
                       + w_sem*imp_semantic
                       + w_mdl*imp_model ) )

urg_reply   = 1 if last thread msg is inbound & unanswered by user else decayed
urg_signal  = question/imperative/deadline heuristic (inline) or LLM urgency
urgency     = clamp( w_r*urg_reply + w_s*urg_signal )

attention   = 0.6*importance + 0.4*urgency
tier        = calibrated_bucket(importance)         # §6.3
```

- `feedback` rows override `importance`/`urgency`/`tier` directly and are pinned until
  contradicted by a newer feedback row.
- All weights live in a `priority_config` row (JSON) so tuning needs no code change.
- **importance_centroids**: mean embedding vectors of (a) all rule-matched messages and
  (b) all feedback-confirmed critical/exceptional messages. Recomputed nightly.

---

## 6. The Slow Path — LLM-as-Judge (the labeler)

### 6.1 Role
The LLM is the **labeler that manufactures the training set**, not the runtime engine.
This is the mechanism that dissolves the cold-start problem: 39k unlabeled messages
become LLM-judged labeled examples the learned ranker (A3) trains on.

### 6.2 Call details (TELUS, free / $0 — see `[[telus-free-llm-access]]`)
- **Judge model:** `TELUS_GEMMA_*` (chat, OpenAI-compatible `/v1/chat/completions`).
- **Embeddings:** `nvidia/nv-embedqa-e5-v5`, 1024-dim, **asymmetric** — MUST send
  `input_type: "passage"` for stored messages / `"query"` for searches, else HTTP 400.
- **Credentials:** `~/.claude/local/secrets/telus-api.env` (0600).
- **Reference client:** `~/Workspace/cie-lab/analysis/telus.py` (stdlib, no deps).
- **Privacy note:** message *content* is sent to the TELUS endpoint. It is the user's
  own sovereign infra, but a `sensitive` flag on threads/rules MUST be honored: sensitive
  threads are scored by **rules + local heuristics only** and never sent to the LLM. See
  Risks (§10).

### 6.3 Calibration (critical)
A raw LLM will call far too many messages "important." We **calibrate** its outputs to
the Pareto so tiers stay meaningful:
- Judge returns a raw importance ∈ [0,1] + rationale.
- Maintain a rolling distribution of raw scores; assign tiers by **quantile** targeting
  ≈ {0.1%, 0.9%, 4%, 95%} — with rule-floored messages exempt (always ≥ their floor).
- Re-fit quantile thresholds nightly; store in `priority_config`.

### 6.4 Queue & throughput
- Slow path drains `message_priority WHERE needs_llm=1`, oldest-first for new arrivals,
  then a historical backfill sweep (39k messages) at a rate-limited trickle.
- Runs in a **separate worker** from the sync loop (own async task / process). Falling
  behind degrades label freshness only — never sync or awareness.

---

## 7. The Learned Ranker (Phase A3 — the "Kaggle" model, done right)

Once enough LLM + feedback labels accumulate (target ≥ ~3–5k judged, ≥ a few hundred
per non-irrelevant tier), train a model to replace the LLM judge at runtime (faster,
consistent, offline-capable).

- **Features:** message embedding (1024-d), sender's 8 ContactRank factors + Dunbar
  layer, thread features (type, size, user participation rate, age), reply-state,
  temporal (hour/day, recency), rule-match flags, cohort membership one-hots.
- **Targets:** ordinal tier (importance) + regression urgency head.
- **Model:** gradient-boosted trees — **LightGBM** (primary) / XGBoost; optional
  `LGBMRanker` for pairwise ordering. AutoML (TPOT/AutoGluon) as an *exploration* pass,
  not the shipped artifact. For text, **embeddings + GBM** beats hand-engineered features.
- **Evaluation (the discipline that makes this real):**
  - **Time-based split** — train on older messages, validate on the most recent window
    (prevents leakage; matches deployment).
  - **Rank metrics, not accuracy** — because critical is ~0.1%, report **Precision@k**
    and **NDCG@k** on the critical/exceptional tiers, plus recall on the *named* rule
    set (must be ~1.0 — never miss the GPU thread).
  - **Calibration curve** — verify predicted tier distribution ≈ Pareto.
- **Active learning:** surface **disagreements** (model vs rule) and **boundary cases**
  (mid-confidence near tier cutoffs) in the inbox for one-tap correction; corrections
  land in `priority_feedback` (weighted higher) and trigger periodic retrain.

---

## 8. Awareness Layer (three channels)

| Channel | Trigger | Behavior |
|---|---|---|
| **Desktop notification** | new **critical** (importance tier) message, `seen=0` | `notify-send` on legion2; **deduped per thread** (one ping per thread per cooldown window); respects quiet hours from config. |
| **Statusline** | any unseen critical/exceptional | `claude-statusline` segment: `⚡{crit}!{exc}` count; click/skill opens inbox. Ambient, non-interrupting. |
| **Priority inbox** | on-demand | MCP tool + `messages-web` page: messages grouped by tier, ordered by **attention**, each with rationale + factor breakdown. Marking seen clears awareness. |

Phone push (ntfy/Pushover) is **deferred**; the awareness emitter is built with a
pluggable sink interface so adding it later is a new sink, not a rewrite.

---

## 9. MCP Surface (new tools)

- `priority_inbox(tier?, limit)` — ranked list grouped by tier (ordered by attention).
- `priority_explain(message_id)` — importance/urgency/attention + factor & rule breakdown + rationale.
- `priority_feedback(message_id, tier|importance|urgency, note?)` — active-learning correction.
- `priority_mark_seen(message_id|thread_id)` — clear awareness state.
- `priority_rule_add(rule_type, match_value, tier_floor, note?)` / `priority_rule_list` / `priority_rule_disable(id)`.
- `cohort_create(name, description?)` / `cohort_add_member(cohort, identity_id)` / `cohort_list`.
- `priority_stats()` — tier distribution, queue depth, label counts, model version/metrics.

---

## 10. Encoding the User's Declared Signals (seed configuration)

These are created as `priority_rules` / `cohorts` on first setup. Names must be resolved
to `identity_id`s (disambiguate interactively where ambiguous).

| Declared signal | Encoding | Floor |
|---|---|---|
| **GPU thread (Signal)** — #1 | `thread` rule on that Signal thread_id | **critical** |
| Regen Gaia (Telegram) | `thread` rule on that Telegram thread_id | **exceptional** |
| Telus people (email) | `cohort` "Telus" + `platform_folder`/sender-cohort rule | **exceptional→critical** |
| Indigenomics Capstone students | `cohort` "Indigenomics Capstone" | **exceptional** |
| Carole Anne Hilton | `identity` rule | **exceptional/critical** |
| Yvonne, Eve, Pravin, Darren Zal | `identity` rules | **exceptional** |
| Mom | `identity` rule (+ relationship boost) | **exceptional** |

> Open task at setup: resolve each name → identity across platforms; some (e.g. Carole
> Anne / "Carol Anne") need identity-merge first. Cohort membership for Telus & Capstone
> must be enumerated with the user.

---

## 11. Build Phasing (sub-project A)

| Phase | Delivers | Value |
|---|---|---|
| **A0** | Schema (§4) + rules engine + seed config (§10) + `priority_inbox`/rule/cohort MCP tools. Rules-only scoring. | Ships the #1 want: GPU thread & Telus surface immediately, deterministically. |
| **A1** | Awareness emitter (desktop + statusline) wired to A0 scores; pluggable sink interface. | Instant "made aware" for critical arrivals. |
| **A2** | Warm path (embeddings + centroids) + slow path (LLM judge + calibration) + async worker + historical backfill. | The ambiguous 99% gets scored; training labels accumulate. |
| **A3** | Learned ranker (LightGBM) + eval harness + active-learning loop. | Fast, consistent, self-improving; the "Kaggle" model. |

Each phase is independently shippable and useful. A0+A1 alone satisfy the user's primary
request; A2/A3 raise accuracy on the long tail.

---

## 12. Success Criteria

- **Recall on declared signals = ~1.0** — the system must *never* miss a GPU-thread or
  Telus message. (Guaranteed by rules; verified by test.)
- **Critical-tier precision high** — few false desktop interrupts (target: user dismisses
  < ~1 in 10 as "shouldn't have pinged").
- **Distribution holds** — after calibration, tier shares ≈ the Pareto (§1).
- **Correcting is fast & sticky** — one MCP/inbox action re-tiers a message in < 5s and
  persists across re-scores.
- **No sync-loop regression** — daemon cycle time unchanged; LLM/embly work is off-loop.

---

## 13. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| **Identity resolution gaps** (name→identity, cross-platform) mis-route rules | Resolve at setup with user; cohort membership explicit; `priority_explain` exposes which identity matched. |
| **LLM over-flags importance** | Quantile calibration to Pareto (§6.3); rule floors exempt; nightly re-fit. |
| **Slow path stalls sync** (the Telegram lesson) | LLM/embedding work in a separate worker; sync loop never awaits it; queue may lag safely. |
| **Incomplete data** (mom on WhatsApp) skews people-ranking | Sub-project **C** (WhatsApp first — adapter already ~485 LoC) runs in parallel. |
| **Privacy** — content sent to TELUS judge | `sensitive` flag → rules+local only, never sent; endpoint is user's own sovereign infra. |
| **Class imbalance** breaks naive training | Rank metrics (P@k/NDCG), time-split, feedback up-weighting; rules carry the rarest class regardless. |
| **Notification fatigue** | Desktop reserved for critical only; per-thread dedup; quiet hours; everything else ambient/on-demand. |

---

## 14. Open Questions (resolve during planning)

1. Exact Signal thread_id for the GPU thread and Telegram thread_id for Regen Gaia
   (look up at setup).
2. Telus & Capstone cohort membership — enumerate with user.
3. Where the async worker lives — extra task in the existing daemon process vs a separate
   systemd unit (leaning: separate unit, for the same isolation reason as §13 row 3).
4. Statusline integration contract with `claude-statusline` (segment API vs file poll).
5. Retrain cadence & label-count trigger for A3.
