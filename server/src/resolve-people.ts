/**
 * The people pass (task-885 N4/W3/W4).
 *
 * One ordered run over the corpus that ends with every observed person holding
 * a row, every row the evidence can join joined, and the ranking recomputed
 * from what is actually there. Built to be run on a rhythm (task-885 D6): the
 * routes read what this leaves behind, they never compute it.
 *
 * Order is the whole design, and each step depends on the one before:
 *
 *   1. autoResolve   link what can be linked while contacts are still unlinked.
 *                    Its passes read `identity_links IS NULL`, so admission
 *                    must not run first or they would all find nothing.
 *   2. admit         give every remaining observed person a row. Identity
 *                    stops being a precondition for being counted.
 *   3. merge         admission splits one person across their platforms, so the
 *                    merge pass belongs in the same run and never a later one
 *                    (task-885 R1). It works on identities because after step 2
 *                    there are no unlinked contacts left for the old
 *                    contact-level suggester to see.
 *   4. score         recompute, and retract what the run no longer produces.
 *
 * Usage:
 *   node build/resolve-people.mjs [--dry-run] [--json] [--no-merge] [--skip-auto-resolve]
 */

import path from 'node:path';
import { MessageDB } from './db.js';

const DB_PATH = path.join(process.env.HOME ?? '', '.claude/local/messages/messages.db');

const has = (f: string) => process.argv.includes(`--${f}`);

function main(): void {
  const dryRun = has('dry-run');
  const db = new MessageDB(DB_PATH);
  const count = (sql: string) => ((db as any).db.prepare(sql).get() as { c: number }).c;

  const before = {
    identities: count('SELECT COUNT(*) c FROM identities'),
    scored: count('SELECT COUNT(*) c FROM contact_scores'),
    unlinked_senders: count(`SELECT COUNT(*) c FROM (
      SELECT m.sender_id FROM messages m
      LEFT JOIN identity_links il ON m.sender_id = il.platform || ':' || il.platform_id
      WHERE il.id IS NULL AND m.sender_id IS NOT NULL GROUP BY m.sender_id)`),
  };

  const report: Record<string, unknown> = { dry_run: dryRun, before };
  let t = Date.now();

  if (!has('skip-auto-resolve') && !dryRun) {
    const r = db.autoResolve();
    report.auto_resolve = {
      identities_created: r.identities_created, links_created: r.links_created,
      phone_matches: r.phone_matches, name_matches: r.name_matches,
      fuzzy_matches: r.fuzzy_matches, ms: Date.now() - t,
    };
  }

  t = Date.now();
  const admitted = db.admitObserved({ dryRun });
  report.admit = { ...admitted, ms: Date.now() - t };

  if (!has('no-merge')) {
    t = Date.now();
    const merges = db.applyEvidencedMerges({ dryRun });
    report.merge = {
      merged: merges.merged.length,
      by_kind: merges.merged.reduce((m: Record<string, number>, x) => { m[x.kind] = (m[x.kind] ?? 0) + 1; return m; }, {}),
      held: merges.held.length,
      ms: Date.now() - t,
      examples: merges.merged.slice(0, 12),
    };
  }

  if (!dryRun) {
    t = Date.now();
    const s = db.computeAllScores();
    report.score = { ...s, ms: Date.now() - t };
  }

  report.after = {
    identities: count('SELECT COUNT(*) c FROM identities'),
    scored: count('SELECT COUNT(*) c FROM contact_scores'),
    unlinked_senders: count(`SELECT COUNT(*) c FROM (
      SELECT m.sender_id FROM messages m
      LEFT JOIN identity_links il ON m.sender_id = il.platform || ':' || il.platform_id
      WHERE il.id IS NULL AND m.sender_id IS NOT NULL GROUP BY m.sender_id)`),
  };

  if (has('json')) { console.log(JSON.stringify(report, null, 2)); return; }

  const a = report.after as typeof before;
  console.log(dryRun ? 'People pass (dry run)' : 'People pass');
  if (report.auto_resolve) {
    const ar = report.auto_resolve as any;
    console.log(`  resolve       ${ar.identities_created} identities, ${ar.links_created} links  (${ar.ms} ms)`);
  }
  console.log(`  admit         ${admitted.from_contacts} from contacts, ${admitted.from_senders} from senders with no contact row`);
  console.log(`                ${Object.entries(admitted.by_platform).map(([k, v]) => `${k} ${v}`).join(', ')}`);
  if (report.merge) {
    const mg = report.merge as any;
    console.log(`  merge         ${mg.merged} applied ${JSON.stringify(mg.by_kind)}, ${mg.held} held below the floor  (${mg.ms} ms)`);
    for (const e of mg.examples) console.log(`      ${e.kept}  <-  ${e.absorbed.join(', ')}   [${e.kind}]`);
  }
  if (report.score) {
    const sc = report.score as any;
    console.log(`  score         ${sc.computed} scored, ${sc.retracted} retracted  (${sc.ms} ms)`);
  }
  console.log(`  identities    ${before.identities} -> ${a.identities}`);
  console.log(`  scored        ${before.scored} -> ${a.scored}`);
  console.log(`  senders with no identity  ${before.unlinked_senders} -> ${a.unlinked_senders}`);
}

main();
