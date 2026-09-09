/**
 * Email Sent-folder backfill without disturbing the daemon's live cursor.
 *
 * Usage:
 *   node build/backfill-email.mjs                 # all configured accounts
 *   node build/backfill-email.mjs --account work  # one account
 *   node build/backfill-email.mjs --days 3650     # bounded history
 *
 * Stop legion-messages first so this job has sole custody of the IMAP
 * connections and messages.db. The adapter runs with a fresh in-memory cursor;
 * its resulting cursor is deliberately not persisted.
 */

import fs from 'node:fs';
import path from 'node:path';
import { parse as parseYaml } from 'yaml';
import { EmailAdapter } from './adapters/email.js';
import { MessageDB } from './db.js';
import type { AdapterConfig, AppConfig, Contact, Message, Thread } from './types.js';

function resolveHome(p: string): string {
  return p.startsWith('~/') ? path.join(process.env.HOME ?? '', p.slice(2)) : p;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const accountIdx = args.indexOf('--account');
  const accountId = accountIdx >= 0 ? args[accountIdx + 1] : undefined;
  const daysIdx = args.indexOf('--days');
  const days = daysIdx >= 0 ? Number.parseInt(args[daysIdx + 1], 10) : 36500;
  if (!Number.isFinite(days) || days <= 0) throw new Error('--days must be positive');

  const configPath = resolveHome('~/.claude/local/messages/config.yml');
  const config = parseYaml(fs.readFileSync(configPath, 'utf8')) as AppConfig;
  const emailConfig = config.adapters.email as AdapterConfig | undefined;
  if (!emailConfig?.enabled) throw new Error('email adapter is not enabled');

  const configuredAccounts = (emailConfig.accounts as Array<{ id: string }> | undefined) ?? [];
  const accounts = accountId
    ? configuredAccounts.filter(account => account.id === accountId)
    : configuredAccounts;
  if (!accounts.length) throw new Error(`no matching email account${accountId ? `: ${accountId}` : 's'}`);

  const dataDir = resolveHome(config.data_dir);
  const db = new MessageDB(path.join(dataDir, 'messages.db'));
  const log = (message: string) => console.log(`[${new Date().toISOString()}] ${message}`);
  const adapter = new EmailAdapter(log);
  await adapter.init({
    ...emailConfig,
    accounts,
    data_dir: dataDir,
    folders: ['[Gmail]/Sent Mail', 'Sent', 'Sent Items'],
    initial_days: days,
  });

  let processed = 0;
  let inserted = 0;
  let contacts = 0;
  let threads = 0;
  try {
    for await (const event of adapter.sync(null)) {
      if (event.type === 'contact') {
        db.upsertContact(event.data as Contact);
        contacts++;
      } else if (event.type === 'thread') {
        db.upsertThread(event.data as Thread);
        threads++;
      } else if (event.type === 'message') {
        processed++;
        if (db.insertMessage(event.data as Message)) inserted++;
        if (processed % 500 === 0) log(`processed ${processed} sent messages`);
      }
    }
  } finally {
    await adapter.shutdown();
    db.close();
  }

  log(`complete: processed=${processed} inserted=${inserted} existing=${processed - inserted} contacts=${contacts} threads=${threads}`);
  log('live email cursor preserved');
}

main().catch(error => {
  console.error(`Fatal: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});

