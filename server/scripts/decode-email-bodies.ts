/**
 * One-off retro-decode script for raw-MIME email rows.
 *
 * Usage:
 *   npx tsx scripts/decode-email-bodies.ts           # dry-run
 *   npx tsx scripts/decode-email-bodies.ts --apply   # write changes
 */

import Database from 'better-sqlite3';
import path from 'node:path';
import { decodeEmailBody } from '../src/adapters/email.js';

const DB_PATH = path.join(
  process.env.HOME ?? '',
  '.claude', 'local', 'messages', 'messages.db',
);

const APPLY = process.argv.includes('--apply');

interface Row {
  id: string;
  content: string;
}

async function main(): Promise<void> {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Predicate: email rows whose content still looks like raw MIME
  const rows = db.prepare<[], Row>(`
    SELECT id, content
    FROM messages
    WHERE platform = 'email'
      AND content IS NOT NULL
      AND (
        content LIKE '%Content-Transfer-Encoding%'
        OR content LIKE '%Content-Type: %'
      )
  `).all();

  console.log(`${rows.length} candidate rows`);

  if (rows.length === 0) {
    console.log('Nothing to decode.');
    return;
  }

  const update = APPLY
    ? db.prepare('UPDATE messages SET content = ? WHERE id = ?')
    : null;

  let changed = 0;
  let sampled = 0;

  for (const row of rows) {
    const decoded = (await decodeEmailBody(row.content)).trim();
    if (!decoded || decoded === row.content) continue;

    if (sampled < 3) {
      console.log(`\n--- sample ${sampled + 1} (id=${row.id}) ---`);
      console.log('  RAW:    ', row.content.slice(0, 120).replace(/\n/g, '\\n'));
      console.log('  DECODED:', decoded.slice(0, 120).replace(/\n/g, '\\n'));
      sampled++;
    }

    if (update) {
      update.run(decoded, row.id);
    }
    changed++;
  }

  if (APPLY) {
    console.log(`\n${changed} rows UPDATED`);
  } else {
    console.log(`\n${changed} rows would change (dry-run; pass --apply)`);
  }
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
