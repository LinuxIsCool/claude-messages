/**
 * Telegram backfill script — fetches full message history for specified dialogs.
 *
 * Usage:
 *   node build/backfill.mjs                    # backfill all dialogs (DMs only by default)
 *   node build/backfill.mjs <dialog_id>        # backfill specific dialog
 *   node build/backfill.mjs <id1> <id2>        # multiple dialogs
 *   node build/backfill.mjs --all              # backfill ALL dialogs including groups
 *   node build/backfill.mjs --dms              # backfill all DM dialogs
 *   node build/backfill.mjs --max 5000         # limit messages per dialog
 *   node build/backfill.mjs --force            # re-backfill even completed dialogs
 *
 * IMPORTANT: Stop the daemon first to avoid Telegram session conflicts:
 *   systemctl --user stop legion-messages
 */

import fs from 'node:fs';
import path from 'node:path';
import { parse as parseYaml } from 'yaml';
import { MessageDB } from './db.js';
import { EventLog } from './events.js';
import { TelegramAdapter } from './adapters/telegram.js';
import type { Contact, Thread, Message, AppConfig, AdapterConfig } from './types.js';

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

async function main() {
  const args = process.argv.slice(2);

  // Parse flags
  const allDialogs = args.includes('--all');
  const dmsOnly = args.includes('--dms') || (!allDialogs && args.filter(a => !a.startsWith('--')).length === 0);
  const forceRebackfill = args.includes('--force');
  const maxIdx = args.indexOf('--max');
  const maxPerDialog = maxIdx !== -1 ? parseInt(args[maxIdx + 1], 10) : undefined;
  const dialogIds = args.filter(a => !a.startsWith('--') && (maxIdx === -1 || args.indexOf(a) !== maxIdx + 1));

  // Load config
  const configPath = resolveHome('~/.claude/local/messages/config.yml');
  const config = parseYaml(fs.readFileSync(configPath, 'utf-8')) as AppConfig;
  const dataDir = resolveHome(config.data_dir);

  // Init DB and event log
  const db = new MessageDB(path.join(dataDir, 'messages.db'));
  const eventLog = new EventLog(path.join(dataDir, 'events'));

  const log = (msg: string) => {
    const line = `[${new Date().toISOString()}] ${msg}`;
    console.log(line);
  };

  // Init Telegram adapter
  const adapter = new TelegramAdapter(log);
  const adapterConfig = config.adapters.telegram;
  if (!adapterConfig?.enabled) {
    console.error('Telegram adapter not enabled in config');
    process.exit(1);
  }
  await adapter.init({ ...adapterConfig, data_dir: dataDir } as AdapterConfig);

  // Load existing cursor
  const cursorStr = db.getCursor('telegram');
  if (cursorStr) {
    // Set adapter's internal cursor state
    (adapter as unknown as { currentCursor: unknown }).currentCursor = JSON.parse(cursorStr);
  }

  // Determine which dialogs to backfill
  let targetIds: string[] | undefined;
  if (dialogIds.length > 0) {
    targetIds = dialogIds;
    log(`Backfilling ${dialogIds.length} specific dialog(s): ${dialogIds.join(', ')}`);
  } else if (dmsOnly) {
    log('Backfilling all DM dialogs (use --all for groups too)');
    // targetIds left undefined — adapter will iterate all, we filter below
  } else {
    log('Backfilling ALL dialogs');
  }

  if (maxPerDialog) {
    log(`Max ${maxPerDialog} messages per dialog`);
  } else {
    log('No message limit — fetching full history');
  }

  // Run backfill
  let totalMsgs = 0;
  let totalContacts = 0;
  let totalThreads = 0;
  let newMsgs = 0;
  let skippedMsgs = 0;
  let skippedComplete = 0;
  const skippedThreads = new Set<string>();
  const dialogStats: Array<{ dialog: string; fetched: number }> = [];

  // Track current dialog for state recording
  let currentThreadId: string | null = null;
  let currentDialogMsgs = 0;

  for await (const event of adapter.backfill(targetIds, maxPerDialog)) {
    // Track progress events — marks end of a dialog
    if ('_backfill_progress' in event && event._backfill_progress) {
      dialogStats.push(event._backfill_progress);
      if (currentThreadId && !skippedThreads.has(currentThreadId)) {
        db.markBackfillComplete(currentThreadId, currentDialogMsgs);
      }
      currentThreadId = null;
      currentDialogMsgs = 0;
      continue;
    }

    switch (event.type) {
      case 'contact':
        db.upsertContact(event.data as Contact);
        totalContacts++;
        break;
      case 'thread': {
        const thread = event.data as Thread;
        // Filter DMs only if --dms mode and no specific IDs
        if (dmsOnly && !targetIds && thread.thread_type !== 'dm') {
          skippedThreads.add(thread.id);
          continue;
        }
        // Skip already-completed dialogs unless --force
        if (!forceRebackfill && db.isBackfillComplete(thread.id)) {
          skippedThreads.add(thread.id);
          skippedComplete++;
          continue;
        }
        db.upsertThread(thread);
        db.markBackfillStart(thread.id, {
          platform: thread.platform,
          title: thread.title ?? undefined,
          type: thread.thread_type ?? undefined,
        });
        currentThreadId = thread.id;
        currentDialogMsgs = 0;
        totalThreads++;
        break;
      }
      case 'message': {
        const msg = event.data as Message;
        if (!msg.id) continue; // Skip empty progress markers
        if (msg.thread_id && skippedThreads.has(msg.thread_id)) {
          skippedMsgs++;
          continue;
        }
        const inserted = db.insertMessage(msg);
        totalMsgs++;
        currentDialogMsgs++;
        if (inserted) newMsgs++;
        break;
      }
    }
  }

  // Save updated cursor
  const newCursor = adapter.getCursor();
  if (newCursor) {
    db.updateCursor('telegram', newCursor);
  }

  // Summary
  log('\n=== Backfill Complete ===');
  log(`Total messages processed: ${totalMsgs}`);
  log(`New messages inserted: ${newMsgs}`);
  log(`Duplicate/existing skipped: ${totalMsgs - newMsgs}`);
  if (skippedMsgs > 0) {
    log(`Messages from non-DM threads skipped: ${skippedMsgs}`);
  }
  if (skippedComplete > 0) {
    log(`Already-complete dialogs skipped: ${skippedComplete} (use --force to re-backfill)`);
  }
  log(`Contacts: ${totalContacts}, Threads: ${totalThreads}`);

  if (dialogStats.length > 0) {
    log('\nPer-dialog results:');
    for (const s of dialogStats) {
      log(`  ${s.dialog}: ${s.fetched} messages`);
    }
  }

  // Cleanup
  await adapter.shutdown();
  db.close();
  log('\nDone. Restart daemon: systemctl --user start legion-messages');
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
