import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import type { Adapter } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';

interface SignalCursor {
  lastSentAt: number;  // epoch ms from messages.sent_at
}

interface SignalDbMessage {
  id: string;
  body: string | null;
  json: string;
  sent_at: number;
  type: string;
  conversationId: string;
  sourceServiceId: string | null;
  source: string | null;
  hasAttachments: number;
  isViewOnce: number;
}

interface SignalDbConversation {
  id: string;
  type: string;
  name: string | null;
  profileName: string | null;
  profileFamilyName: string | null;
  e164: string | null;
  serviceId: string | null;
  groupId: string | null;
  active_at: number | null;
}

interface SignalMessageJson {
  reactions?: Array<{
    emoji?: string;
    fromId?: string;
    targetTimestamp?: number;
    timestamp?: number;
  }>;
  preview?: Array<{
    url?: string;
    title?: string;
    description?: string;
  }>;
  bodyRanges?: Array<Record<string, unknown>>;
  quote?: {
    id?: number;
    authorAci?: string;
    text?: string;
  };
  sticker?: { packId?: string; stickerId?: number };
}

function loadEnv(envPath: string): Record<string, string> {
  const content = fs.readFileSync(envPath, 'utf-8');
  const env: Record<string, string> = {};
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    env[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
  }
  return env;
}

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

const BATCH_SIZE = 500;

export class SignalAdapter implements Adapter {
  platform = 'signal';
  private dbPath: string = '';
  private dbKey: string = '';
  private currentCursor: SignalCursor = { lastSentAt: 0 };
  private log: (msg: string) => void;
  private ready = false;

  constructor(log: (msg: string) => void = console.log) {
    this.log = log;
  }

  async init(config: AdapterConfig): Promise<void> {
    const dataDir = (config as unknown as { data_dir?: string }).data_dir ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');

    // Resolve DB path from config or use default
    this.dbPath = resolveHome(
      (config as unknown as { db_path?: string }).db_path ?? '~/.config/Signal/sql/db.sqlite'
    );

    // Check sqlcipher is available
    try {
      execFileSync('sqlcipher', ['--version'], { timeout: 5000, stdio: 'pipe' });
    } catch {
      this.log('[signal] sqlcipher not installed — run: sudo pacman -S sqlcipher');
      return;
    }

    // Check DB exists
    if (!fs.existsSync(this.dbPath)) {
      this.log(`[signal] Database not found at ${this.dbPath} — is Signal Desktop installed?`);
      return;
    }

    // Load encryption key from signal.env
    const envPath = path.join(dataDir, 'secrets', 'signal.env');
    if (!fs.existsSync(envPath)) {
      this.log('[signal] No signal.env found — run: electron --no-sandbox ~/.claude/local/scripts/extract-signal-key.js');
      return;
    }

    const env = loadEnv(envPath);
    this.dbKey = env.SIGNAL_DB_KEY ?? '';
    if (!this.dbKey) {
      this.log('[signal] SIGNAL_DB_KEY not set in signal.env — adapter disabled');
      return;
    }

    // Verify key works by running a test query
    try {
      const result = this.queryDb('SELECT COUNT(*) as count FROM messages;');
      const count = result[0]?.count ?? 0;
      this.log(`[signal] DB access verified — ${count} messages in Signal Desktop`);
      this.ready = true;
    } catch (err) {
      this.log(`[signal] DB access failed (wrong key?): ${err}`);
      return;
    }
  }

  private queryDb(sql: string): Record<string, unknown>[] {
    // Build stdin: set key, enable JSON mode, run query
    const stdin = [
      `PRAGMA key = "${this.dbKey}";`,
      '.mode json',
      sql,
    ].join('\n');

    const output = execFileSync('sqlcipher', [this.dbPath], {
      input: stdin,
      timeout: 30000,
      maxBuffer: 50 * 1024 * 1024,  // 50MB for large result sets
      stdio: ['pipe', 'pipe', 'pipe'],
      encoding: 'utf-8',
    });

    const trimmed = output.trim();
    if (!trimmed) return [];

    // sqlcipher outputs: "ok\n" from PRAGMA key, then the JSON array from the query.
    // The JSON array may span multiple lines (one object per line for large results),
    // so we can't scan line-by-line. Instead, find the first '[' after the PRAGMA
    // response and parse everything from there to the end.
    const lines = trimmed.split('\n');
    let dataStart = -1;
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      // Skip the PRAGMA "ok" response
      if (line === 'ok' || line === '[{"ok":"ok"}]') continue;
      if (line.startsWith('[')) {
        dataStart = i;
        break;
      }
    }

    if (dataStart === -1) return [];

    // Join from the data start line to the end and parse as one JSON array
    const jsonStr = lines.slice(dataStart).join('\n').trim();
    try {
      return JSON.parse(jsonStr);
    } catch {
      return [];
    }
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    if (!this.ready) return;

    this.currentCursor = cursorStr ? JSON.parse(cursorStr) : { lastSentAt: 0 };
    const now = new Date();
    const isFirstSync = !cursorStr;

    // On first sync, yield contacts and threads from conversations table
    if (isFirstSync) {
      yield* this.syncConversations(now);
    }

    // Sync messages (batched)
    let totalMessages = 0;
    let hasMore = true;

    while (hasMore) {
      const rows = this.queryDb(
        `SELECT id, body, json, sent_at, type, conversationId, sourceServiceId, source, hasAttachments, isViewOnce
         FROM messages
         WHERE sent_at > ${this.currentCursor.lastSentAt}
         ORDER BY sent_at ASC
         LIMIT ${BATCH_SIZE};`
      ) as unknown as SignalDbMessage[];

      if (rows.length === 0) {
        hasMore = false;
        break;
      }

      for (const row of rows) {
        const event = this.rowToMessage(row, now);
        if (event) {
          yield event;
          totalMessages++;
        }
        // Always advance cursor even for skipped rows
        if (row.sent_at > this.currentCursor.lastSentAt) {
          this.currentCursor.lastSentAt = row.sent_at;
        }
      }

      hasMore = rows.length === BATCH_SIZE;

      if (totalMessages > 0 && totalMessages % 5000 === 0) {
        this.log(`[signal] Progress: ${totalMessages} messages synced so far...`);
      }
    }

    if (totalMessages > 0) {
      this.log(`[signal] Synced ${totalMessages} messages`);
    }
  }

  private *syncConversations(now: Date): Generator<SyncEvent> {
    const conversations = this.queryDb(
      `SELECT id, type, name, profileName, profileFamilyName, e164, serviceId, groupId, active_at
       FROM conversations
       ORDER BY active_at DESC;`
    ) as unknown as SignalDbConversation[];

    let contactCount = 0;
    let threadCount = 0;

    for (const conv of conversations) {
      if (conv.type === 'private') {
        // Yield contact
        const displayName = [conv.profileName, conv.profileFamilyName].filter(Boolean).join(' ')
          || conv.name
          || null;

        const contactId = conv.serviceId
          ? `signal:user:${conv.serviceId}`
          : `signal:user:${conv.id}`;

        const contact: Contact = {
          id: contactId,
          platform: 'signal',
          display_name: displayName,
          username: null,
          phone: conv.e164 ?? null,
          metadata: {
            serviceId: conv.serviceId,
            conversationId: conv.id,
          },
          first_seen: now.toISOString(),
          last_seen: now.toISOString(),
        };
        yield { type: 'contact', data: contact };
        contactCount++;

        // DM thread
        const thread: Thread = {
          id: `signal:conv:${conv.id}`,
          platform: 'signal',
          title: displayName || conv.e164 || 'Unknown',
          thread_type: 'dm',
          participants: [contactId],
          metadata: {
            serviceId: conv.serviceId,
            e164: conv.e164,
          },
          created_at: conv.active_at ? new Date(conv.active_at).toISOString() : now.toISOString(),
          updated_at: now.toISOString(),
        };
        yield { type: 'thread', data: thread };
        threadCount++;

      } else if (conv.type === 'group') {
        const thread: Thread = {
          id: `signal:conv:${conv.id}`,
          platform: 'signal',
          title: conv.name || 'Unnamed Group',
          thread_type: 'group',
          participants: [],  // Group members aren't in the conversations table
          metadata: {
            groupId: conv.groupId,
          },
          created_at: conv.active_at ? new Date(conv.active_at).toISOString() : now.toISOString(),
          updated_at: now.toISOString(),
        };
        yield { type: 'thread', data: thread };
        threadCount++;
      }
    }

    this.log(`[signal] Yielded ${contactCount} contacts, ${threadCount} threads`);

    // Backfill phone numbers from recipients table for contacts that lack them.
    // Signal Desktop may have e164→serviceId mappings here for contacts only seen
    // in group contexts (conversations.e164 is null for them).
    // Wrapped in try/catch: the table may not exist in all Signal Desktop versions.
    try {
      const recipients = this.queryDb(
        `SELECT e164, serviceId FROM recipients WHERE e164 IS NOT NULL AND serviceId IS NOT NULL;`
      ) as unknown as Array<{ e164: string; serviceId: string }>;

      let phonesBackfilled = 0;
      for (const r of recipients) {
        const contactId = `signal:user:${r.serviceId}`;
        const contact: Contact = {
          id: contactId,
          platform: 'signal',
          display_name: null,  // COALESCE in upsertContact preserves existing name
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
      // Table doesn't exist in this Signal Desktop version — that's fine
      this.log(`[signal] Recipients phone backfill skipped (table may not exist): ${err}`);
    }
  }

  private rowToMessage(row: SignalDbMessage, now: Date): SyncEvent | null {
    // Skip non-message types (key changes, profile updates, etc.)
    const validTypes = ['incoming', 'outgoing', 'story'];
    if (!validTypes.includes(row.type)) return null;

    // Parse the JSON column for supplementary data (reactions, previews, quotes)
    let parsed: SignalMessageJson = {};
    try {
      if (row.json && row.json !== '{}') {
        parsed = JSON.parse(row.json);
      }
    } catch {
      // json column might not always be valid
    }

    // Body is a top-level column, not in json
    const body = row.body ?? null;

    // Determine content type from top-level columns
    let contentType: Message['content_type'] = 'text';
    const metadata: Record<string, unknown> = {};

    if (row.hasAttachments) {
      contentType = 'document';  // Best we can do without attachment details
    }

    if (parsed.sticker) {
      contentType = 'sticker';
      metadata.sticker = parsed.sticker;
    }

    if (parsed.quote) {
      metadata.quote = parsed.quote;
    }

    if (parsed.reactions?.length) {
      metadata.reactions = parsed.reactions;
    }

    if (parsed.preview?.length) {
      metadata.preview = parsed.preview;
    }

    if (row.isViewOnce) {
      metadata.view_once = true;
    }

    if (row.type === 'outgoing') {
      metadata.sent_by_self = true;
    }

    // Determine sender
    const senderId = row.sourceServiceId
      ? `signal:user:${row.sourceServiceId}`
      : row.type === 'outgoing'
        ? 'signal:user:self'
        : `signal:user:${row.source ?? 'unknown'}`;

    // Build reply_to from quote
    let replyTo: string | null = null;
    if (parsed.quote?.id && parsed.quote?.authorAci) {
      replyTo = `signal:msg:${parsed.quote.id}`;
    }

    const message: Message = {
      id: `signal:msg:${row.id}`,
      platform: 'signal',
      thread_id: `signal:conv:${row.conversationId}`,
      sender_id: senderId,
      content: body,
      content_type: contentType,
      reply_to: replyTo,
      metadata,
      platform_ts: new Date(row.sent_at).toISOString(),
      synced_at: now.toISOString(),
    };

    return { type: 'message', data: message };
  }

  getCursor(): string | null {
    return JSON.stringify(this.currentCursor);
  }

  async shutdown(): Promise<void> {
    this.log('[signal] Adapter shutdown');
  }
}
