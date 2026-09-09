/**
 * Repair email bodies stored before the adapter decoded them (task-885 N2).
 *
 * The MIME headers are gone by the time a row is in the database, so this reads
 * the text itself and rewrites only what is unambiguously an encoded body: it
 * decodes cleanly to valid UTF-8, it is mostly printable, and it holds real
 * whitespace. Anything short of that is left alone, because an unreadable row
 * is a smaller harm than a readable one replaced with noise.
 *
 * The FTS index follows automatically: `messages_au` reindexes on update, so a
 * repaired body becomes searchable in the same statement.
 *
 * Usage: node build/repair-email-bodies.mjs [--dry-run] [--limit N]
 */

import path from 'node:path';
import Database from 'better-sqlite3';
import { repairStoredBody } from './email-body.js';

const DB_PATH = path.join(process.env.HOME ?? '', '.claude/local/messages/messages.db');

function main(): void {
  const dryRun = process.argv.includes('--dry-run');
  const li = process.argv.indexOf('--limit');
  const limit = li >= 0 ? Number(process.argv[li + 1]) : 0;

  const db = new Database(DB_PATH);
  db.pragma('busy_timeout = 60000');

  const rows = db.prepare(
    `SELECT id, content FROM messages
      WHERE platform = 'email' AND content IS NOT NULL AND LENGTH(content) > 24
      ${limit ? 'LIMIT ' + Number(limit) : ''}`
  ).all() as Array<{ id: string; content: string }>;

  const update = db.prepare('UPDATE messages SET content = ? WHERE id = ?');
  let repaired = 0;
  const samples: Array<{ before: string; after: string }> = [];

  const run = db.transaction(() => {
    for (const r of rows) {
      const fixed = repairStoredBody(r.content);
      if (fixed === null) continue;
      repaired++;
      if (samples.length < 5) {
        samples.push({ before: r.content.slice(0, 60).replace(/\s+/g, ' '), after: fixed.slice(0, 60).replace(/\s+/g, ' ') });
      }
      if (!dryRun) update.run(fixed, r.id);
    }
  });
  run();

  console.log(`${dryRun ? 'Would repair' : 'Repaired'} ${repaired} of ${rows.length} email bodies`);
  for (const s of samples) console.log(`    ${s.before}\n  ->${s.after}`);
}

main();
