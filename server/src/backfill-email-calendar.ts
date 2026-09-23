import fs from 'node:fs';
import path from 'node:path';
import Database from 'better-sqlite3';
import { ImapFlow, type MessageStructureObject } from 'imapflow';
import { parse as parseYaml } from 'yaml';
import { calendarPartNumbers, captureCalendarSource } from './adapters/email-calendar.js';

interface AccountConfig {
  id: string;
  name?: string;
}

interface StoredMessage {
  id: string;
  metadata: string;
  platform_ts: string;
}

interface Candidate {
  messageId: string;
  accountId: string;
  folder: string;
  uid: number;
  metadata: Record<string, unknown>;
  platformTs: string;
}

interface ScanReceipt {
  status: 'ok' | 'degraded';
  started_at: string;
  completed_at: string;
  since: string;
  candidates: number;
  scanned: number;
  no_calendar: number;
  captured_messages: number;
  captured_events: number;
  missing_at_source: number;
  errors: number;
}

function argValue(name: string, fallback: string): string {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function resolveHome(value: string): string {
  return value.startsWith('~/') ? path.join(process.env.HOME ?? '', value.slice(2)) : value;
}

function loadEnv(filePath: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
    const value = line.trim();
    if (!value || value.startsWith('#')) continue;
    const equals = value.indexOf('=');
    if (equals > 0) env[value.slice(0, equals)] = value.slice(equals + 1);
  }
  return env;
}

function parseUid(messageId: string): number | null {
  const value = Number.parseInt(messageId.split(':').at(-1) ?? '', 10);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function batches<T>(items: T[], size: number): T[][] {
  const output: T[][] = [];
  for (let index = 0; index < items.length; index += size) output.push(items.slice(index, index + size));
  return output;
}

async function resolveFolder(client: ImapFlow, requested: string): Promise<string | null> {
  const mailboxes = await client.list();
  if (requested === 'INBOX') return 'INBOX';
  const exact = mailboxes.find(mailbox => mailbox.path === requested);
  if (exact) return exact.path;
  if (requested.toLowerCase().includes('sent')) {
    return mailboxes.find(mailbox => mailbox.specialUse === '\\Sent')?.path ?? null;
  }
  return null;
}

function initializeState(state: Database.Database): void {
  state.pragma('journal_mode = WAL');
  state.exec(`
    CREATE TABLE IF NOT EXISTS scans (
      message_id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      folder TEXT NOT NULL,
      uid INTEGER NOT NULL,
      status TEXT NOT NULL,
      calendar_events INTEGER NOT NULL DEFAULT 0,
      raw_ref TEXT,
      error TEXT,
      scanned_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_scans_source ON scans(account_id, folder, uid);
  `);
}

function atomicWriteJson(filePath: string, value: unknown): void {
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  try {
    fs.writeFileSync(tempPath, `${JSON.stringify(value, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
    fs.renameSync(tempPath, filePath);
  } catch (error) {
    if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
    throw error;
  }
}

async function main(): Promise<void> {
  const startedAt = new Date().toISOString();
  const home = process.env.HOME ?? '';
  const dataDir = resolveHome(argValue('--data-dir', path.join(home, '.claude', 'local', 'messages')));
  const since = argValue('--since', `${new Date().getUTCFullYear()}-01-01T00:00:00.000Z`);
  const batchSize = Number.parseInt(argValue('--batch-size', '250'), 10);
  if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 1000) throw new Error('Invalid --batch-size');

  const config = parseYaml(fs.readFileSync(path.join(dataDir, 'config.yml'), 'utf8')) as {
    adapters?: { email?: { accounts?: AccountConfig[] } };
  };
  const accounts = config.adapters?.email?.accounts ?? [];
  const env = loadEnv(path.join(dataDir, 'secrets', 'email.env'));
  const messages = new Database(path.join(dataDir, 'messages.db'));
  messages.pragma('busy_timeout = 10000');
  const state = new Database(path.join(dataDir, 'email-calendar-backfill.db'));
  initializeState(state);

  const storedRows = messages.prepare(`
    SELECT id, metadata, platform_ts
    FROM messages
    WHERE platform = 'email'
      AND platform_ts >= ?
      AND json_type(metadata, '$.calendar_events') IS NULL
    ORDER BY platform_ts, id
  `).all(since) as StoredMessage[];
  const scannedIds = new Set(
    (state.prepare("SELECT message_id FROM scans WHERE status IN ('no-calendar', 'captured')").all() as { message_id: string }[])
      .map(row => row.message_id),
  );
  const candidates: Candidate[] = [];
  for (const row of storedRows) {
    if (scannedIds.has(row.id)) continue;
    const metadata = JSON.parse(row.metadata) as Record<string, unknown>;
    const accountId = typeof metadata.account === 'string' ? metadata.account : '';
    const folder = typeof metadata.folder === 'string' ? metadata.folder : 'INBOX';
    const uid = parseUid(row.id);
    if (!accountId || !uid) continue;
    candidates.push({
      messageId: row.id,
      accountId,
      folder,
      uid,
      metadata,
      platformTs: row.platform_ts,
    });
  }

  const receipt: ScanReceipt = {
    status: 'ok',
    started_at: startedAt,
    completed_at: '',
    since,
    candidates: candidates.length,
    scanned: 0,
    no_calendar: 0,
    captured_messages: 0,
    captured_events: 0,
    missing_at_source: 0,
    errors: 0,
  };
  const markScan = state.prepare(`
    INSERT INTO scans
      (message_id, account_id, folder, uid, status, calendar_events, raw_ref, error, scanned_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(message_id) DO UPDATE SET
      status = excluded.status,
      calendar_events = excluded.calendar_events,
      raw_ref = excluded.raw_ref,
      error = excluded.error,
      scanned_at = excluded.scanned_at
  `);
  const updateMessage = messages.prepare('UPDATE messages SET metadata = ? WHERE id = ?');

  try {
    for (const accountConfig of accounts) {
      const accountCandidates = candidates.filter(candidate => candidate.accountId === accountConfig.id);
      if (!accountCandidates.length) continue;
      const prefix = accountConfig.id.toUpperCase();
      const host = env[`IMAP_${prefix}_HOST`];
      const user = env[`IMAP_${prefix}_USER`];
      const password = env[`IMAP_${prefix}_PASSWORD`];
      if (!host || !user || !password) {
        receipt.errors += accountCandidates.length;
        receipt.status = 'degraded';
        continue;
      }

      const client = new ImapFlow({
        host,
        port: 993,
        secure: true,
        auth: { user, pass: password },
        logger: false,
        socketTimeout: 300_000,
      });
      await client.connect();
      try {
        const folders = [...new Set(accountCandidates.map(candidate => candidate.folder))];
        for (const requestedFolder of folders) {
          const folder = await resolveFolder(client, requestedFolder);
          const folderCandidates = accountCandidates.filter(candidate => candidate.folder === requestedFolder);
          if (!folder) {
            receipt.errors += folderCandidates.length;
            receipt.status = 'degraded';
            continue;
          }
          const byUid = new Map(folderCandidates.map(candidate => [candidate.uid, candidate]));
          const lock = await client.getMailboxLock(folder);
          try {
            for (const batch of batches(folderCandidates, batchSize)) {
              const found = new Set<number>();
              const calendarStructures = new Map<number, MessageStructureObject>();
              for await (const result of client.fetch(batch.map(candidate => candidate.uid), {
                uid: true,
                bodyStructure: true,
              }, { uid: true })) {
                found.add(result.uid);
                if (result.bodyStructure && calendarPartNumbers(result.bodyStructure).length) {
                  calendarStructures.set(result.uid, result.bodyStructure);
                }
              }

              for (const candidate of batch) {
                const scannedAt = new Date().toISOString();
                if (!found.has(candidate.uid)) {
                  markScan.run(candidate.messageId, candidate.accountId, candidate.folder, candidate.uid,
                    'missing', 0, null, 'message UID unavailable at source', scannedAt);
                  receipt.missing_at_source++;
                  receipt.status = 'degraded';
                  continue;
                }
                if (!calendarStructures.has(candidate.uid)) {
                  markScan.run(candidate.messageId, candidate.accountId, candidate.folder, candidate.uid,
                    'no-calendar', 0, null, null, scannedAt);
                  receipt.no_calendar++;
                  receipt.scanned++;
                  continue;
                }
                try {
                  const raw = await client.fetchOne(`${candidate.uid}`, { uid: true, source: true }, { uid: true });
                  if (!raw || !raw.source) throw new Error('raw source unavailable');
                  const capture = await captureCalendarSource(
                    dataDir,
                    candidate.accountId,
                    candidate.folder,
                    candidate.uid,
                    raw.source,
                  );
                  const metadata = {
                    ...candidate.metadata,
                    calendar_capture: {
                      raw_ref: capture.raw_ref,
                      raw_sha256: capture.raw_sha256,
                      raw_bytes: capture.raw_bytes,
                      calendar_attachments: capture.calendar_attachments,
                      parse_issues: capture.parse_issues,
                    },
                    calendar_events: capture.calendar_events,
                  };
                  const update = messages.transaction(() => {
                    updateMessage.run(JSON.stringify(metadata), candidate.messageId);
                    markScan.run(candidate.messageId, candidate.accountId, candidate.folder, candidate.uid,
                      'captured', capture.calendar_events.length, capture.raw_ref, null, scannedAt);
                  });
                  update();
                  receipt.captured_messages++;
                  receipt.captured_events += capture.calendar_events.length;
                  receipt.scanned++;
                  if (capture.parse_issues.length) receipt.status = 'degraded';
                } catch (error) {
                  const message = error instanceof Error ? error.message : String(error);
                  markScan.run(candidate.messageId, candidate.accountId, candidate.folder, candidate.uid,
                    'error', 0, null, message, scannedAt);
                  receipt.errors++;
                  receipt.status = 'degraded';
                }
              }
            }
          } finally {
            lock.release();
          }
        }
      } finally {
        await client.logout();
      }
    }
  } finally {
    receipt.completed_at = new Date().toISOString();
    atomicWriteJson(path.join(dataDir, 'email-calendar-backfill-receipt.json'), receipt);
    messages.close();
    state.close();
  }

  console.log(JSON.stringify(receipt));
  if (receipt.errors) process.exitCode = 1;
}

await main();
