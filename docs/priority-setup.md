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
