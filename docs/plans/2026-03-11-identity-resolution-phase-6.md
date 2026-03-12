# Identity Resolution Phase 6: Quick Wins Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix pass ordering bug, backfill Signal phone numbers from recipients table, and detect duplicate Signal UUIDs via shared group membership — raising identity coverage from 17.1% toward ~22%.

**Architecture:** Three independent changes to the identity resolution pipeline. Task 1 reorders existing autoResolve passes so identity-creating passes run before matching passes. Task 2 adds a recipients table lookup to the Signal adapter to surface phone numbers missing from the conversations table. Task 3 adds a new autoResolve Pass 5 that detects same-person Signal UUID pairs via shared group thread participation.

**Tech Stack:** TypeScript, better-sqlite3, sqlcipher (Signal adapter), no new dependencies.

---

## File Map

| File | Responsibility | Tasks |
|------|----------------|-------|
| `server/src/db.ts` | All identity resolution logic, SQL queries | 1, 3 |
| `server/src/types.ts` | TypeScript interfaces for AutoResolveReport | 3 |
| `server/src/mcp.ts` | MCP tool descriptions | 3 |
| `server/src/adapters/signal.ts` | Signal Desktop DB reading, contact extraction | 2 |

All paths relative to `~/.claude/plugins/local/legion-plugins/plugins/claude-messages/`.

---

## Chunk 1: Pass Ordering + Signal Phone Backfill

### Task 1: Fix autoResolve Pass Ordering

**Problem:** Pass 2 (email name match) runs before Pass 3 (single-platform phone identity creation). Pass 2 matches email contacts against existing identity display_names, but the ~920 identities created by Pass 3 don't exist yet when Pass 2 runs.

**Fix:** Swap Pass 2 and Pass 3. The new order:
- Pass 1: Cross-platform phone (creates identities for multi-platform phone groups)
- Pass 2 (was 3): Single-platform phone (creates identities for all remaining phone contacts)
- Pass 3 (was 2): Email name match (NOW has ~1,000 identities to match against)
- Pass 4: Cross-platform name match (unchanged)

**Files:**
- Modify: `server/src/db.ts` (the `autoResolve()` method, lines 832-1087)

- [ ] **Step 1: Read the current autoResolve() method**

Open `server/src/db.ts` and identify the four pass blocks:
- Pass 1 (cross-platform phone): lines 846-918, comment `// Get all contacts with phone numbers`
- Pass 2 (email name match): lines 920-963, comment `// Pass 2: Email name matching`
- Pass 3 (single-platform phone): lines 965-1007, comment `// Pass 3: Single-platform phone identities`
- Pass 4 (cross-platform name): lines 1009-1083, comment `// Pass 4: Cross-platform name matching`

- [ ] **Step 2: Move the Pass 3 block (single-platform phone) to before Pass 2 (email name match)**

Cut the entire block from the comment `// Pass 3: Single-platform phone identities` through the closing brace of its for loop (lines 965-1007), and paste it immediately after Pass 1's closing brace (after line 918, before the current `// Pass 2: Email name matching` comment).

Update the comments to reflect the new numbering:
- The moved block becomes `// Pass 2: Single-platform phone identities`
- The email name match block becomes `// Pass 3: Email name matching`
- Pass 4 comment stays as `// Pass 4: Cross-platform name matching`

The code itself doesn't change — only the physical ordering within the transaction.

- [ ] **Step 3: Build and smoke test**

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server && npm run build
timeout 3 node build/mcp.mjs 2>/dev/null || true
```

Expected: Build succeeds, no runtime errors.

- [ ] **Step 4: Verify pass ordering via auto_resolve report**

Since all contacts are already linked from the previous run, the report should show zeros for new links but no errors. The pass ordering change is structural — it only matters on a fresh DB or when new contacts arrive.

To verify the ordering is correct, read the built mcp.mjs and confirm the single-platform phone block appears before the email name match block.

- [ ] **Step 5: Commit**

```bash
cd ~/.claude/plugins/local/legion-plugins
git add plugins/claude-messages/server/src/db.ts
git commit -m "fix(messages): reorder autoResolve passes — phone identity creation before email name matching

Pass 2 (email name match) was running before Pass 3 (single-platform phone identity creation),
so email contacts couldn't match against the ~920 identities created by Pass 3. Swap order so
all identity-creating passes run before matching passes.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Signal Recipients Table Phone Backfill

**Problem:** The Signal adapter reads `conversations.e164` for phone numbers, but some contacts (especially those only seen in group contexts) have `e164: null`. Signal Desktop's `recipients` table may contain phone→ACI mappings that aren't in `conversations`.

**Approach:** After reading conversations, query the `recipients` table for any phone numbers not already captured. For each recipient with an `e164` that maps to an existing contact (via `serviceId`), update that contact's phone field.

**Files:**
- Modify: `server/src/adapters/signal.ts` (the `syncConversations()` method, lines 228-301)

- [ ] **Step 1: Investigate the recipients table schema**

Before writing code, we need to know if the `recipients` table exists and what columns it has. Add a diagnostic query to the Signal adapter's `init()` method temporarily:

Add after line 131 (after `this.ready = true;`):

```typescript
// Diagnostic: check for recipients table
try {
  const tables = this.queryDb("SELECT name FROM sqlite_master WHERE type='table' AND name='recipients';");
  if (tables.length > 0) {
    const schema = this.queryDb("PRAGMA table_info(recipients);");
    this.log(`[signal] Recipients table found with columns: ${schema.map((r: Record<string, unknown>) => r.name).join(', ')}`);
    const sample = this.queryDb("SELECT * FROM recipients WHERE e164 IS NOT NULL LIMIT 3;");
    this.log(`[signal] Recipients with phone: ${JSON.stringify(sample)}`);
  } else {
    this.log('[signal] No recipients table found');
  }
} catch (err) {
  this.log(`[signal] Recipients table probe failed: ${err}`);
}
```

Build and restart the daemon:
```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server && npm run build
systemctl --user restart legion-messages
sleep 2
journalctl --user -u legion-messages --since "2 min ago" | grep -i recipient
```

This will tell us:
- Whether `recipients` table exists
- What columns it has
- Whether it contains `e164` phone numbers

**IMPORTANT:** The rest of this task depends on what the diagnostic reveals. If the recipients table doesn't exist or doesn't have e164, skip to Step 5 (remove diagnostic, commit what we have).

- [ ] **Step 2: Add recipients phone lookup to syncConversations()**

If the diagnostic confirms `recipients` table has `e164` and `serviceId` columns, add a phone backfill after the main conversation loop.

In `syncConversations()`, after the existing `for (const conv of conversations)` loop ends (after line 298), add:

```typescript
// Backfill phone numbers from recipients table for contacts that lack them
try {
  const recipients = this.queryDb(
    `SELECT e164, serviceId FROM recipients WHERE e164 IS NOT NULL AND serviceId IS NOT NULL;`
  ) as unknown as Array<{ e164: string; serviceId: string }>;

  let phonesBackfilled = 0;
  for (const r of recipients) {
    // Find if we have a contact for this serviceId without a phone
    const contactId = `signal:user:${r.serviceId}`;
    // Yield a contact update with the phone backfilled
    // The daemon's upsertContact uses COALESCE so it won't overwrite existing phones
    const contact: Contact = {
      id: contactId,
      platform: 'signal',
      display_name: null,  // COALESCE preserves existing
      username: null,
      phone: r.e164,
      metadata: { serviceId: r.serviceId, phoneSource: 'recipients' },
      first_seen: now.toISOString(),
      last_seen: now.toISOString(),
    };
    yield { type: 'contact' as const, data: contact };
    phonesBackfilled++;
  }

  if (phonesBackfilled > 0) {
    this.log(`[signal] Backfilled ${phonesBackfilled} phone numbers from recipients table`);
  }
} catch (err) {
  this.log(`[signal] Recipients phone backfill failed (table may not exist): ${err}`);
}
```

Note: The `upsertContact` in `db.ts` uses `COALESCE(excluded.phone, phone)` — it only sets phone if the new value is non-null, and preserves the existing value otherwise. The `display_name: null` won't overwrite existing names for the same reason.

- [ ] **Step 3: Remove the diagnostic probe from init()**

Delete the temporary diagnostic block added in Step 1 (the `// Diagnostic: check for recipients table` block).

- [ ] **Step 4: Build and verify**

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server && npm run build
systemctl --user restart legion-messages
sleep 3
journalctl --user -u legion-messages --since "1 min ago" | grep -i "phone\|recipient\|backfill"
```

Expected: Log line showing how many phones were backfilled (could be 0 if recipients table doesn't have useful data, or 50-200+ if it does).

After backfill, check if any previously phone-less Signal contacts now have phones:
```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"identity_health","arguments":{}}}' | timeout 10 node build/mcp.mjs 2>/dev/null | head -1 | python3 -c "import sys,json; d=json.load(sys.stdin); h=json.loads(d['result']['content'][0]['text']); print(f'unlinked_with_phone: {h[\"unlinked_with_phone\"]}')"
```

If `unlinked_with_phone` > 0, re-running `auto_resolve` will pick them up.

- [ ] **Step 5: Commit**

```bash
cd ~/.claude/plugins/local/legion-plugins
git add plugins/claude-messages/server/src/adapters/signal.ts
git commit -m "feat(messages): backfill Signal phone numbers from recipients table

Query Signal Desktop's recipients table for e164→serviceId mappings
that aren't in the conversations table. Yields contact updates with
phones that upsertContact merges via COALESCE.

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Chunk 2: Signal UUID Deduplication

### Task 3: Signal UUID Dedup (autoResolve Pass 5)

**Problem:** Signal Desktop creates duplicate conversation records when a user re-registers. Example: "UserA" (UUID 1e8a6f8d, 1,704 messages) and "UserA El" (UUID f0ca4cb8, 1,667 messages) share 15 group threads but are treated as separate contacts.

**Approach:** Add Pass 5 to autoResolve that:
1. Finds Signal sender_id pairs sharing 3+ group threads
2. Verifies they never message each other directly (same person can't DM themselves)
3. Auto-links with confidence 0.85 and source `signal_uuid_dedup`
4. Tracks in report as `signal_uuid_dedup_matches`

**Files:**
- Modify: `server/src/types.ts` (extend AutoResolveReport)
- Modify: `server/src/db.ts` (add Pass 5 to autoResolve)
- Modify: `server/src/mcp.ts` (update auto_resolve tool description)

- [ ] **Step 1: Extend AutoResolveReport in types.ts**

In `server/src/types.ts`, add `signal_uuid_dedup_matches: number` to the `AutoResolveReport` interface (after `skipped_ambiguous_names` on line 126):

```typescript
export interface AutoResolveReport {
  identities_created: number;
  links_created: number;
  phone_matches: number;
  name_matches: number;
  single_platform_created: number;
  cross_platform_name_matches: number;
  skipped_ambiguous_names: number;
  signal_uuid_dedup_matches: number;
  skipped_already_linked: number;
  details: Array<{
    phone?: string;
    identity_id: string;
    action: 'created' | 'extended' | 'name_matched' | 'single_platform' | 'cross_platform_name' | 'signal_uuid_dedup';
    contacts_linked: string[];
  }>;
}
```

- [ ] **Step 2: Initialize the new report field in autoResolve()**

In `server/src/db.ts`, add `signal_uuid_dedup_matches: 0` to the report initialization (after `skipped_ambiguous_names: 0` around line 840):

```typescript
const report: AutoResolveReport = {
  identities_created: 0,
  links_created: 0,
  phone_matches: 0,
  name_matches: 0,
  single_platform_created: 0,
  cross_platform_name_matches: 0,
  skipped_ambiguous_names: 0,
  signal_uuid_dedup_matches: 0,
  skipped_already_linked: 0,
  details: [],
};
```

- [ ] **Step 3: Add Pass 5 — Signal UUID dedup**

In `server/src/db.ts`, add the following after Pass 4's closing brace (after the `cross_platform_name` details push, before the transaction-closing `});`):

```typescript
      // Pass 5: Signal UUID deduplication
      // Detect same-person duplicate Signal UUIDs via shared group membership
      const signalUuidPairs = this.db.prepare(`
        SELECT
          m1.sender_id as uuid_a,
          m2.sender_id as uuid_b,
          COUNT(DISTINCT m1.thread_id) as shared_groups
        FROM messages m1
        INNER JOIN messages m2 ON m1.thread_id = m2.thread_id
        WHERE m1.platform = 'signal'
          AND m2.platform = 'signal'
          AND m1.sender_id < m2.sender_id
          AND m1.sender_id LIKE 'signal:user:%'
          AND m2.sender_id LIKE 'signal:user:%'
          AND m1.sender_id != 'signal:user:self'
          AND m2.sender_id != 'signal:user:self'
          AND m1.thread_id IN (
            SELECT id FROM threads WHERE platform = 'signal' AND thread_type = 'group'
          )
        GROUP BY m1.sender_id, m2.sender_id
        HAVING shared_groups >= 3
      `).all() as Array<{ uuid_a: string; uuid_b: string; shared_groups: number }>;

      for (const pair of signalUuidPairs) {
        // Negative filter: if they ever DM each other, they're different people
        const hasDM = this.db.prepare(`
          SELECT 1 FROM threads t
          WHERE t.platform = 'signal' AND t.thread_type = 'dm'
            AND EXISTS (SELECT 1 FROM messages WHERE thread_id = t.id AND sender_id = ?)
            AND EXISTS (SELECT 1 FROM messages WHERE thread_id = t.id AND sender_id = ?)
          LIMIT 1
        `).get(pair.uuid_a, pair.uuid_b);
        if (hasDM) continue;

        // Split sender_ids into platform + platform_id
        const refA = { platform: 'signal', platform_id: pair.uuid_a.substring('signal:'.length) };
        const refB = { platform: 'signal', platform_id: pair.uuid_b.substring('signal:'.length) };

        // Check if either is already linked
        const existingA = this.db.prepare(
          'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
        ).get(refA.platform, refA.platform_id) as { identity_id: string } | undefined;
        const existingB = this.db.prepare(
          'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
        ).get(refB.platform, refB.platform_id) as { identity_id: string } | undefined;

        if (existingA && existingB) {
          // Both already linked — if to different identities, skip (merge_suggestions will catch it)
          if (existingA.identity_id !== existingB.identity_id) continue;
          // Same identity — already done
          report.skipped_already_linked++;
          continue;
        }

        // Get display names for identity creation
        const contactA = this.db.prepare('SELECT display_name FROM contacts WHERE id = ?').get(pair.uuid_a) as { display_name: string | null } | undefined;
        const contactB = this.db.prepare('SELECT display_name FROM contacts WHERE id = ?').get(pair.uuid_b) as { display_name: string | null } | undefined;

        let identityId: string;
        const linkedContacts: string[] = [];

        if (existingA) {
          identityId = existingA.identity_id;
        } else if (existingB) {
          identityId = existingB.identity_id;
        } else {
          // Neither linked — create new identity
          const bestName = contactA?.display_name ?? contactB?.display_name ?? pair.uuid_a;
          const identity = this.createIdentity(bestName);
          identityId = identity.id;
          report.identities_created++;
        }

        // Link whichever isn't linked yet
        if (!existingA) {
          this.linkContact(identityId, refA.platform, refA.platform_id, 0.85, 'signal_uuid_dedup', contactA?.display_name ?? undefined);
          report.links_created++;
          linkedContacts.push(pair.uuid_a);
        }
        if (!existingB) {
          this.linkContact(identityId, refB.platform, refB.platform_id, 0.85, 'signal_uuid_dedup', contactB?.display_name ?? undefined);
          report.links_created++;
          linkedContacts.push(pair.uuid_b);
        }

        if (linkedContacts.length > 0) {
          report.signal_uuid_dedup_matches++;
          report.details.push({ identity_id: identityId, action: 'signal_uuid_dedup', contacts_linked: linkedContacts });
        }
      }
```

- [ ] **Step 4: Update MCP tool description**

In `server/src/mcp.ts`, update the `auto_resolve` tool description (line 333):

```typescript
'Run cross-platform identity matching — phone grouping, single-platform phone, email name match, cross-platform name match, Signal UUID dedup',
```

- [ ] **Step 5: Build and smoke test**

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server && npm run build
timeout 3 node build/mcp.mjs 2>/dev/null || true
```

Expected: Build succeeds, no runtime errors.

- [ ] **Step 6: Restart daemon and run auto_resolve**

```bash
systemctl --user restart legion-messages
sleep 2
```

Then run auto_resolve via MCP to see the UUID dedup results:

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"auto_resolve","arguments":{}}}' | timeout 30 node build/mcp.mjs 2>/dev/null | head -1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = json.loads(d['result']['content'][0]['text'])
print(f'signal_uuid_dedup_matches: {r[\"signal_uuid_dedup_matches\"]}')
print(f'identities_created: {r[\"identities_created\"]}')
print(f'links_created: {r[\"links_created\"]}')
# Show dedup details
for detail in r['details']:
    if detail['action'] == 'signal_uuid_dedup':
        print(f'  dedup: {detail[\"identity_id\"][:8]}... → {detail[\"contacts_linked\"]}')
"
```

Expected: `signal_uuid_dedup_matches` ≥ 1 (at minimum the UserA/UserA El pair). The exact count depends on how many re-registrations exist in the Signal data.

- [ ] **Step 7: Verify UserA specifically**

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"who_is","arguments":{"query":"UserA","limit":5}}}' | timeout 10 node build/mcp.mjs 2>/dev/null | head -1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
results = json.loads(d['result']['content'][0]['text'])
for r in results:
    if isinstance(r, dict) and 'platforms' in r:
        print(f'{r[\"display_name\"]} — {len(r[\"platforms\"])} platforms, {r[\"stats\"][\"total_messages\"]} messages')
        for p in r['platforms']:
            print(f'  {p[\"platform\"]}:{p[\"platform_id\"][:20]}... ({p[\"source\"]}, {p[\"confidence\"]})')
"
```

Expected: The UserA identity should now have both Signal UUIDs linked (plus Telegram), with total_messages reflecting all three sender_ids combined (~3,400+ messages).

- [ ] **Step 8: Run identity_health to verify coverage improvement**

```bash
cd ~/.claude/plugins/local/legion-plugins/plugins/claude-messages/server
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"identity_health","arguments":{}}}' | timeout 10 node build/mcp.mjs 2>/dev/null | head -1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
h = json.loads(d['result']['content'][0]['text'])
print(f'Coverage: {h[\"coverage_pct\"]}% ({h[\"contacts_linked\"]}/{h[\"total_contacts\"]})')
print(f'Identities: {h[\"total_identities\"]}')
print(f'Links: {h[\"total_links\"]}')
print(f'Links by source: {h[\"links_by_source\"]}')
print(f'Unlinked with phone: {h[\"unlinked_with_phone\"]}')
print(f'Orphaned: {h[\"orphaned_identities\"]}')
"
```

Expected: Coverage > 17.1%. `links_by_source` should now include `signal_uuid_dedup`. If Signal phone backfill (Task 2) found phones, `unlinked_with_phone` may have changed.

- [ ] **Step 9: Commit**

```bash
cd ~/.claude/plugins/local/legion-plugins
git add plugins/claude-messages/server/src/types.ts plugins/claude-messages/server/src/db.ts plugins/claude-messages/server/src/mcp.ts
git commit -m "feat(messages): add Signal UUID dedup pass to autoResolve

Pass 5 detects same-person Signal UUIDs by finding sender_id pairs that
share 3+ group threads and never DM each other. Links them at confidence
0.85 with source 'signal_uuid_dedup'. Catches re-registration cases
like UserA/UserA El (3,371 combined messages, 15 shared groups).

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Verification Checklist

After all 3 tasks are complete:

- [ ] `npm run build` succeeds with no errors
- [ ] `timeout 3 node build/mcp.mjs` starts without crashing
- [ ] `systemctl --user status legion-messages` shows active
- [ ] `auto_resolve` runs without errors and reports `signal_uuid_dedup_matches`
- [ ] `identity_health` shows coverage > 17.1%
- [ ] `who_is UserA` shows a single identity with both Signal UUIDs + Telegram linked
- [ ] Existing tools (`search_messages`, `get_thread`, `list_threads`) still work
- [ ] No orphaned identities (`orphaned_identities: 0`)
- [ ] `links_by_source` includes `signal_uuid_dedup` entry

## Risks

| Risk | Mitigation |
|------|-----------|
| `recipients` table doesn't exist in this Signal version | Step 1 diagnostic probe checks first. Wrapped in try/catch — graceful fallback. |
| UUID dedup query slow on 63K messages | Self-join on messages is O(n²) in worst case, but the GROUP BY + HAVING 3 prunes aggressively. On 63K messages with ~30K Signal, expect <5s. If slow, add `AND m1.sender_id IN (SELECT DISTINCT sender_id FROM messages WHERE platform = 'signal')` as a pre-filter. |
| False positive UUID dedup (two different people in same 3 groups) | Mitigated by: requiring 3+ shared groups, DM negative filter, and confidence 0.85 (not 1.0). `unlink_identity` available for corrections. |
| Pass reordering changes existing behavior | Pass 2→3 swap only affects the order within the same transaction. All passes are idempotent (skip already-linked contacts). Running auto_resolve twice produces the same result. |
