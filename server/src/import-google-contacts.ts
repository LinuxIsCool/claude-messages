/**
 * Google Contacts importer (task-885 W2c / N1).
 *
 * Reads a Google Contacts CSV export into `address_book`, then optionally uses
 * it to name the identities whose display name names nobody.
 *
 * This is not another `contacts` row. A `contacts` row is what a platform says
 * about a person, and there are already three of those per person disagreeing
 * with each other. An address book entry is what Shawn calls them, which under
 * task-885 D3 is a `curated` claim and outranks all of them.
 *
 * Usage:
 *   node build/import-google-contacts.mjs <contacts.csv> [options]
 *
 *   --source <name>   store under this source name        (default: google)
 *   --apply-names     rename placeholder-named identities (default: report only)
 *   --dry-run         parse and report, write nothing
 *   --json            machine-readable report on stdout
 *
 * Export the file from https://contacts.google.com > Export > Google CSV.
 */

import fs from 'node:fs';
import path from 'node:path';
import { MessageDB } from './db.js';
import { parseCsv, entriesFromGoogleRows } from './address-book.js';

const DB_PATH = path.join(process.env.HOME ?? '', '.claude/local/messages/messages.db');

function flag(name: string): boolean {
  return process.argv.includes(`--${name}`);
}

function option(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

function main(): void {
  const file = process.argv[2];
  if (!file || file.startsWith('--')) {
    console.error('Usage: import-google-contacts.mjs <contacts.csv> [--source google] [--apply-names] [--dry-run] [--json]');
    process.exit(1);
  }
  if (!fs.existsSync(file)) {
    console.error(`No such file: ${file}`);
    process.exit(1);
  }

  const source = option('source', 'google');
  const dryRun = flag('dry-run');
  const applyNames = flag('apply-names');
  const asJson = flag('json');

  const rows = parseCsv(fs.readFileSync(file, 'utf-8'));
  const entries = entriesFromGoogleRows(rows);
  const skipped = rows.length - entries.length;
  const phones = entries.reduce((n, e) => n + e.handles.filter(h => h.kind === 'phone').length, 0);
  const emails = entries.reduce((n, e) => n + e.handles.filter(h => h.kind === 'email').length, 0);

  const db = new MessageDB(DB_PATH);
  const stored = dryRun
    ? { entries: entries.length, handles: phones + emails, replaced: 0 }
    : db.replaceAddressBook(source, entries);

  // The naming pass reads the table, so on a dry run it would read the previous
  // import rather than this file. Reported as such instead of pretending.
  const naming = (applyNames || !dryRun)
    ? db.applyAddressBookNames({ dryRun: dryRun || !applyNames })
    : null;

  const report = {
    file,
    source,
    dry_run: dryRun,
    apply_names: applyNames,
    parsed: { rows: rows.length, entries: entries.length, skipped_without_name: skipped, phones, emails },
    stored,
    naming: naming && {
      matched_identities: naming.matched_identities,
      renamed: naming.renamed.length,
      disagreements: naming.disagreements.length,
      already_named: naming.already_named,
      protected_curated: naming.protected_curated,
    },
  };

  if (asJson) {
    console.log(JSON.stringify({ ...report, renamed: naming?.renamed ?? [], disagreements: naming?.disagreements ?? [] }, null, 2));
    return;
  }

  console.log(`Address book: ${source}`);
  console.log(`  parsed        ${entries.length} entries from ${rows.length} rows (${skipped} without a usable name)`);
  console.log(`  handles       ${phones} phone, ${emails} email`);
  console.log(dryRun ? '  stored        nothing (--dry-run)' : `  stored        ${stored.entries} entries, ${stored.handles} handles (replaced ${stored.replaced})`);
  if (naming) {
    const verb = (applyNames && !dryRun) ? 'renamed' : 'would rename';
    console.log(`  identities    ${naming.matched_identities} matched by handle`);
    console.log(`  ${verb.padEnd(13)} ${naming.renamed.length} placeholder-named identities`);
    console.log(`  already named ${naming.already_named}, curated elsewhere ${naming.protected_curated}`);
    for (const r of naming.renamed.slice(0, 20)) console.log(`      ${r.from}  ->  ${r.to}`);
    if (naming.renamed.length > 20) console.log(`      ... and ${naming.renamed.length - 20} more`);
    if (naming.disagreements.length) {
      console.log(`  disagreements ${naming.disagreements.length} (not acted on, for merge review)`);
      for (const d of naming.disagreements.slice(0, 20)) console.log(`      ${d.current}  vs  ${d.address_book}   (${d.via})`);
      if (naming.disagreements.length > 20) console.log(`      ... and ${naming.disagreements.length - 20} more`);
    }
    if (!applyNames && !dryRun) console.log('  (report only; pass --apply-names to write the names)');
  }
}

main();
