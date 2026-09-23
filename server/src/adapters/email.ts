import { ImapFlow } from 'imapflow';
import { simpleParser } from 'mailparser';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Adapter, SourceObservation } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';
import { calendarPartNumbers, captureCalendarSource, type CalendarCapture } from './email-calendar.js';

/** Decode a raw MIME body part (or whole message) to plain text.
 * Handles base64 / quoted-printable transfer-encodings + multipart trees.
 * Falls back to the raw string if it isn't MIME. */
export async function decodeEmailBody(raw: string): Promise<string> {
  if (!raw) return '';
  const looksMime = /content-transfer-encoding:/i.test(raw) || /^content-type:/im.test(raw);
  if (!looksMime) return raw;
  try {
    const parsed = await simpleParser(Buffer.from(raw));
    return (parsed.text || parsed.html || raw).toString();
  } catch {
    return raw;
  }
}

interface AccountConfig {
  id: string;
  name: string;
  host?: string;
  user?: string;
  password?: string;
}

interface AccountConnection {
  id: string;
  name: string;
  client: ImapFlow | null;
  host: string;
  user: string;
  password: string;
  idleTimer: ReturnType<typeof setTimeout> | null;
  hasNewMail: boolean;
  resolvedFolders: string[];
}

// --- Cursor format v2: per-folder UID tracking ---
interface FolderCursor {
  lastUid: number;
}

interface AccountCursorV2 {
  folders: Record<string, FolderCursor>;
}

interface EmailCursor {
  accounts: Record<string, AccountCursorV2 | { lastUid: number }>;
  version?: 2;
}

// Map message-id header → thread ID for resolving In-Reply-To chains
type ThreadIndex = Map<string, string>;

// Sent folder detection
const SENT_FOLDER_NAMES = ['Sent', 'Sent Mail', 'Sent Items', '[Gmail]/Sent Mail', 'INBOX.Sent'];

function isSentFolder(folder: string): boolean {
  const lower = folder.toLowerCase();
  return lower === 'sent' || lower.includes('sent mail') || lower.includes('sent items');
}

function folderSlug(folder: string): string {
  return folder.toLowerCase().replace(/[\[\]\/\s]+/g, '-').replace(/^-|-$/g, '');
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

function hashMessageId(messageId: string): string {
  return crypto.createHash('sha256').update(messageId).digest('hex').slice(0, 16);
}

function extractEmailAddress(addr: { address?: string; name?: string } | undefined): string | null {
  return addr?.address?.toLowerCase() ?? null;
}

export class EmailAdapter implements Adapter {
  platform = 'email';
  private accounts: AccountConnection[] = [];
  private currentCursor: EmailCursor = { accounts: {} };
  private folders: string[] = ['INBOX'];
  private initialDays: number = 30;
  private threadIndex: ThreadIndex = new Map();
  private knownContacts: Set<string> = new Set();
  private selfAddresses: Set<string> = new Set();
  private dataDir: string = path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
  private lastSourceObservation: SourceObservation | null = null;
  private log: (msg: string) => void;

  constructor(log: (msg: string) => void = console.log) {
    this.log = log;
  }

  async init(config: AdapterConfig): Promise<void> {
    const dataDir = (config as unknown as { data_dir?: string }).data_dir ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
    this.dataDir = dataDir;
    const secretsDir = path.join(dataDir, 'secrets');
    this.initialDays = config.initial_days ?? 30;

    // Support both old `folder` (string) and new `folders` (array) config
    const configFolders = config.folders ?? config.folder;
    if (Array.isArray(configFolders)) {
      this.folders = configFolders as string[];
    } else if (typeof configFolders === 'string') {
      this.folders = [configFolders];
    }

    const envPath = path.join(secretsDir, 'email.env');
    if (!fs.existsSync(envPath)) {
      throw new Error(`Email adapter is not ready: missing ${envPath}`);
    }

    const env = loadEnv(envPath);
    const accountConfigs = (config.accounts as AccountConfig[]) ?? [];

    if (!accountConfigs.length) {
      throw new Error('Email adapter is not ready: zero configured accounts');
    }

    const connectionErrors: string[] = [];
    for (const acct of accountConfigs) {
      const prefix = acct.id.toUpperCase();
      const host = acct.host ?? env[`IMAP_${prefix}_HOST`];
      const user = acct.user ?? env[`IMAP_${prefix}_USER`];
      const password = acct.password ?? env[`IMAP_${prefix}_PASSWORD`];

      if (!host || !user || !password) {
        connectionErrors.push(`${acct.id}: missing credentials`);
        this.log(`[email] Missing credentials for account ${acct.id}`);
        continue;
      }

      const conn: AccountConnection = {
        id: acct.id,
        name: acct.name ?? acct.id,
        client: null,
        host,
        user,
        password,
        idleTimer: null,
        hasNewMail: false,
        resolvedFolders: [],
      };
      this.accounts.push(conn);
      this.selfAddresses.add(user.toLowerCase());

      if (!await this.connectAccount(conn)) {
        connectionErrors.push(`${acct.id}: connection failed`);
      }
    }

    if (!this.accounts.length) {
      throw new Error('Email adapter has zero configured accounts with complete credentials');
    }
    if (!this.accounts.some(account => account.client)) {
      throw new Error(`Email adapter initialization failed: ${connectionErrors.join('; ')}`);
    }
  }

  private async connectAccount(acct: AccountConnection): Promise<boolean> {
    let client: ImapFlow | null = null;
    try {
      client = new ImapFlow({
        host: acct.host,
        port: 993,
        secure: true,
        auth: { user: acct.user, pass: acct.password },
        logger: false,
        socketTimeout: 300_000,
      });
      await client.connect();

      acct.client = client;
      acct.resolvedFolders = await this.resolveFolders(client, this.folders);
      client.on('exists', () => { acct.hasNewMail = true; });
      client.on('error', (err: Error) => {
        this.log(`[email] ${acct.id} socket error: ${err.message}`);
      });
      this.log(`[email] Connected to ${acct.id} (${acct.user}@${acct.host})`);
      this.log(`[email] ${acct.id} resolved folders: ${acct.resolvedFolders.join(', ')}`);
      return true;
    } catch (err) {
      try {
        await client?.close();
      } catch { /* ignore cleanup failure */ }
      acct.client = null;
      acct.resolvedFolders = [];
      this.log(`[email] Failed to connect account ${acct.id}: ${err}`);
      return false;
    }
  }

  /**
   * Resolve configured folder names to actual folders on the IMAP server.
   * Uses RFC 6154 special-use flags first, then falls back to name matching.
   */
  private async resolveFolders(client: ImapFlow, configured: string[]): Promise<string[]> {
    const resolved: string[] = [];
    const mailboxes = await client.list();

    for (const folder of configured) {
      if (folder === 'INBOX') {
        // INBOX always exists
        resolved.push('INBOX');
        continue;
      }

      // Check if this exact folder name exists
      const exact = mailboxes.find(mb => mb.path === folder);
      if (exact) {
        resolved.push(exact.path);
        continue;
      }

      // For sent folder names, try RFC 6154 special-use discovery
      if (isSentFolder(folder)) {
        const sentByFlag = mailboxes.find(mb => mb.specialUse === '\\Sent');
        if (sentByFlag) {
          resolved.push(sentByFlag.path);
          continue;
        }
        // Try other common sent folder names
        const sentByName = mailboxes.find(mb => SENT_FOLDER_NAMES.includes(mb.path));
        if (sentByName) {
          resolved.push(sentByName.path);
          continue;
        }
      }

      this.log(`[email] Folder "${folder}" not found on server — skipping`);
    }

    // Deduplicate (e.g. if config lists both "Sent" and "[Gmail]/Sent Mail" and they resolve to the same thing)
    return [...new Set(resolved)];
  }

  /**
   * Migrate old cursor format (per-account lastUid) to v2 (per-account per-folder lastUid).
   */
  private migrateCursor(cursor: EmailCursor): EmailCursor {
    for (const [acctId, acctCursor] of Object.entries(cursor.accounts)) {
      if ('lastUid' in acctCursor && !('folders' in acctCursor)) {
        // Old format: migrate INBOX lastUid into folders structure
        cursor.accounts[acctId] = {
          folders: { 'INBOX': { lastUid: (acctCursor as { lastUid: number }).lastUid } }
        };
      }
    }
    cursor.version = 2;
    return cursor;
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    this.currentCursor = cursorStr ? JSON.parse(cursorStr) : { accounts: {} };
    // Migrate old cursor format if needed
    if (!this.currentCursor.version) {
      this.currentCursor = this.migrateCursor(this.currentCursor);
    }
    if (!this.accounts.length) {
      throw new Error('Email adapter is not ready: zero configured accounts');
    }

    const disconnected = this.accounts.filter(account => !account.client);
    for (const acct of disconnected) {
      await this.connectAccount(acct);
    }

    const unavailable = this.accounts.filter(account => !account.client);
    if (unavailable.length === this.accounts.length) {
      throw new Error(`Email adapter is not ready: zero connected accounts (${unavailable.map(a => a.id).join(', ')})`);
    }

    const now = new Date();
    const syncErrors: string[] = unavailable.map(account => `${account.id}: disconnected`);

    for (const acct of this.accounts) {
      if (!acct.client) continue;
      try {
        yield* this.syncAccount(acct, now);
      } catch (err) {
        this.log(`[email] Error syncing account ${acct.id}: ${err}`);
        await this.reconnectAccount(acct);
        syncErrors.push(`${acct.id}: ${String(err)}`);
      }
    }

    if (syncErrors.length) {
      throw new Error(`Email sync incomplete: ${syncErrors.join('; ')}`);
    }
    this.lastSourceObservation = {
      observed_at: new Date().toISOString(),
      evidence: 'IMAP successful folder scan',
    };
  }

  getSourceObservation(): SourceObservation | null {
    return this.lastSourceObservation;
  }

  private async reconnectAccount(acct: AccountConnection): Promise<boolean> {
    try {
      await acct.client?.close();
    } catch { /* ignore */ }
    acct.client = null;
    const connected = await this.connectAccount(acct);
    if (connected) {
      this.log(`[email] Reconnected ${acct.id}`);
    }
    return connected;
  }

  /**
   * Sync all resolved folders for an account.
   */
  private async *syncAccount(acct: AccountConnection, now: Date): AsyncGenerator<SyncEvent> {
    const folderErrors: string[] = [];
    for (const folder of acct.resolvedFolders) {
      try {
        yield* this.syncAccountFolder(acct, folder, now);
      } catch (err) {
        this.log(`[email] Error syncing ${acct.id}/${folder}: ${err}`);
        folderErrors.push(`${folder}: ${String(err)}`);
      }
    }
    if (folderErrors.length) {
      throw new Error(folderErrors.join('; '));
    }
  }

  /**
   * Sync a single folder for an account. Extracted from the old single-folder syncAccount().
   */
  private async *syncAccountFolder(acct: AccountConnection, folder: string, now: Date): AsyncGenerator<SyncEvent> {
    if (!acct.client) throw new Error(`Account ${acct.id} is disconnected`);
    const acctCursor = this.currentCursor.accounts[acct.id] as AccountCursorV2 | undefined;
    const folderCursor = acctCursor?.folders?.[folder] ?? { lastUid: 0 };

    let lock;
    try {
      lock = await acct.client.getMailboxLock(folder);
    } catch {
      // Connection likely died — try to reconnect once
      this.log(`[email] Connection lost for ${acct.id}/${folder}, reconnecting...`);
      if (!await this.reconnectAccount(acct)) {
        throw new Error(`Could not reconnect ${acct.id} while opening ${folder}`);
      }
      try {
        lock = await acct.client!.getMailboxLock(folder);
      } catch (err) {
        throw new Error(`Could not open ${folder} for ${acct.id} after reconnect: ${err}`);
      }
    }

    try {
      // Determine search range
      let searchQuery: Record<string, unknown>;
      if (folderCursor.lastUid > 0) {
        // Incremental: fetch UIDs after last known
        searchQuery = { uid: `${folderCursor.lastUid + 1}:*` };
      } else {
        // Initial sync: last N days
        const cutoff = new Date(now.getTime() - this.initialDays * 24 * 60 * 60 * 1000);
        searchQuery = { since: cutoff };
      }

      let maxUid = folderCursor.lastUid;
      let msgCount = 0;

      // Determine direction for this folder
      const folderIsSent = isSentFolder(folder);

      // Freeze one UID snapshot before fetching content. Calendar MIME messages get
      // a second, exact raw-source fetch before any event or cursor is emitted.
      const observedUids: number[] = [];
      const calendarUids: number[] = [];
      for await (const probe of acct.client!.fetch(searchQuery, {
        uid: true,
        bodyStructure: true,
      })) {
        if (probe.uid <= folderCursor.lastUid) continue;
        observedUids.push(probe.uid);
        if (calendarPartNumbers(probe.bodyStructure).length) calendarUids.push(probe.uid);
      }

      if (!observedUids.length) {
        acct.hasNewMail = false;
        return;
      }

      const calendarCaptures = new Map<number, CalendarCapture>();
      for (const uid of calendarUids) {
        const rawMessage = await acct.client!.fetchOne(`${uid}`, { uid: true, source: true }, { uid: true });
        if (!rawMessage || !rawMessage.source) {
          throw new Error(`Calendar source fetch returned no data for ${acct.id}/${folder} UID ${uid}`);
        }
        const capture = await captureCalendarSource(this.dataDir, acct.id, folder, uid, rawMessage.source);
        calendarCaptures.set(uid, capture);
        this.log(`[email] Preserved calendar source ${capture.raw_ref} with ${capture.calendar_events.length} event(s)`);
      }

      // Envelope and first text part remain the normal message path. Calendar raw
      // source is fetched only for the UIDs identified above.
      for await (const msg of acct.client!.fetch(observedUids, {
        uid: true,
        envelope: true,
        headers: ['references'],
        bodyParts: ['1'],
      }, { uid: true })) {
        // Skip messages we've already processed (IMAP UID ranges can re-include boundary)
        if (msg.uid <= folderCursor.lastUid) continue;

        const env = msg.envelope;
        if (!env) continue;

        // Extract addresses from envelope
        const fromAddrs = env.from ?? [];
        const toAddrs = env.to ?? [];
        const allAddresses = [...fromAddrs, ...toAddrs];

        for (const addr of allAddresses) {
          const email = addr.address?.toLowerCase();
          if (!email) continue;
          const name = addr.name || null;
          // Skip if already seen AND we don't have a new name to offer
          if (this.knownContacts.has(email) && !name) continue;
          this.knownContacts.add(email);

          const contact: Contact = {
            id: `email:user:${email}`,
            platform: 'email',
            display_name: name,
            username: email,
            phone: null,
            metadata: { account: acct.id },
            first_seen: now.toISOString(),
            last_seen: now.toISOString(),
          };
          yield { type: 'contact', data: contact };
        }

        // Extract threading info — envelope has messageId and inReplyTo,
        // headers buffer has References for full chain resolution
        const messageId = env.messageId ?? `${acct.id}-uid-${msg.uid}`;
        const inReplyTo = env.inReplyTo ?? undefined;

        // Parse References from raw header buffer
        let references: string | undefined;
        if (msg.headers) {
          const headerStr = msg.headers.toString();
          const refMatch = headerStr.match(/^References:\s*(.+?)(?:\r?\n(?!\s)|\r?\n$)/ims);
          if (refMatch) {
            references = refMatch[1].replace(/\r?\n\s+/g, ' ').trim();
          }
        }

        const threadId = this.resolveThread(acct.id, messageId, inReplyTo, references);

        // Yield thread
        const thread: Thread = {
          id: threadId,
          platform: 'email',
          title: env.subject ?? null,
          thread_type: 'dm',
          participants: allAddresses
            .filter(a => a.address)
            .map(a => `email:user:${a.address!.toLowerCase()}`),
          metadata: {
            account: acct.id,
            account_name: acct.name,
            folder,
          },
          created_at: (env.date ? new Date(env.date) : now).toISOString(),
          updated_at: now.toISOString(),
        };
        yield { type: 'thread', data: thread };

        // Get text content from first body part, decoded via mailparser
        const textPart = msg.bodyParts?.get('1');
        const textContent = textPart ? await decodeEmailBody(textPart.toString()) : null;

        const senderEmail = fromAddrs[0]?.address?.toLowerCase() ?? null;
        const metadata: Record<string, unknown> = {
          account: acct.id,
          subject: env.subject,
          message_id: messageId,
          folder,
        };

        const envToAddrs = toAddrs.map(a => a.address).filter(Boolean);
        if (envToAddrs.length) metadata.to = envToAddrs;

        const ccAddrs = (env.cc ?? []).map((a: { address?: string }) => a.address).filter(Boolean);
        if (ccAddrs.length) metadata.cc = ccAddrs;

        const calendarCapture = calendarCaptures.get(msg.uid);
        if (calendarCapture) {
          metadata.calendar_capture = {
            raw_ref: calendarCapture.raw_ref,
            raw_sha256: calendarCapture.raw_sha256,
            raw_bytes: calendarCapture.raw_bytes,
            calendar_attachments: calendarCapture.calendar_attachments,
            parse_issues: calendarCapture.parse_issues,
          };
          metadata.calendar_events = calendarCapture.calendar_events;
        }

        // Determine direction: sent folder -> sent, inbox from self -> sent, otherwise received
        let direction: 'sent' | 'received' | 'unknown' = 'unknown';
        if (folderIsSent) {
          direction = 'sent';
        } else if (this.selfAddresses.has(senderEmail ?? '')) {
          direction = 'sent';
        } else {
          direction = 'received';
        }

        // Message ID: INBOX keeps old format for backward compat, other folders include slug
        const msgId = folder === 'INBOX'
          ? `email:msg:${acct.id}:${msg.uid}`
          : `email:msg:${acct.id}:${folderSlug(folder)}:${msg.uid}`;

        const message: Message = {
          id: msgId,
          platform: 'email',
          thread_id: threadId,
          sender_id: senderEmail ? `email:user:${senderEmail}` : `email:system:${acct.id}`,
          content: textContent,
          content_type: 'text',
          reply_to: null,  // Threading handled via threadId
          direction,
          metadata,
          platform_ts: (env.date ? new Date(env.date) : now).toISOString(),
          synced_at: now.toISOString(),
        };
        yield { type: 'message', data: message };

        if (msg.uid > maxUid) maxUid = msg.uid;
        msgCount++;
      }

      // Update cursor — per-folder
      if (maxUid > folderCursor.lastUid) {
        const acctCursorObj = (this.currentCursor.accounts[acct.id] as AccountCursorV2) ?? { folders: {} };
        if (!acctCursorObj.folders) (acctCursorObj as AccountCursorV2).folders = {};
        acctCursorObj.folders[folder] = { lastUid: maxUid };
        this.currentCursor.accounts[acct.id] = acctCursorObj;
      }

      acct.hasNewMail = false;

      if (msgCount > 0) {
        this.log(`[email] ${acct.id}/${folder}: synced ${msgCount} messages (maxUid: ${maxUid})`);
      }
    } finally {
      lock.release();
    }

    // Start IDLE for INBOX only (Sent folders don't need real-time push)
    if (folder === 'INBOX') {
      this.startIdle(acct);
    }
  }

  private resolveThread(
    accountId: string,
    messageId: string,
    inReplyTo?: string,
    references?: string | string[],
  ): string {
    // Check if this message is already part of a known thread
    if (this.threadIndex.has(messageId)) {
      return this.threadIndex.get(messageId)!;
    }

    // Check if we're replying to a known message
    if (inReplyTo && this.threadIndex.has(inReplyTo)) {
      const threadId = this.threadIndex.get(inReplyTo)!;
      this.threadIndex.set(messageId, threadId);
      return threadId;
    }

    // Check references chain (oldest first)
    const refs = typeof references === 'string'
      ? references.split(/\s+/).filter(Boolean)
      : (references ?? []);

    for (const ref of refs) {
      if (this.threadIndex.has(ref)) {
        const threadId = this.threadIndex.get(ref)!;
        this.threadIndex.set(messageId, threadId);
        return threadId;
      }
    }

    // New thread — use the first reference (root) or current message
    const rootId = refs[0] ?? messageId;
    const threadId = `email:thread:${accountId}:${hashMessageId(rootId)}`;
    this.threadIndex.set(messageId, threadId);
    // Also index all references to this thread
    for (const ref of refs) {
      this.threadIndex.set(ref, threadId);
    }

    return threadId;
  }

  private startIdle(acct: AccountConnection): void {
    // Clear any existing IDLE timer
    if (acct.idleTimer) {
      clearTimeout(acct.idleTimer);
      acct.idleTimer = null;
    }

    // Restart IDLE every 25 minutes (Gmail has a 29-minute limit)
    acct.idleTimer = setTimeout(() => {
      this.startIdle(acct);
    }, 25 * 60 * 1000);
  }

  getCursor(): string | null {
    return JSON.stringify(this.currentCursor);
  }

  async shutdown(): Promise<void> {
    for (const acct of this.accounts) {
      if (acct.idleTimer) {
        clearTimeout(acct.idleTimer);
      }
      try {
        await acct.client?.logout();
        this.log(`[email] Disconnected ${acct.id}`);
      } catch {
        // Connection may already be dead
      }
    }
    this.accounts = [];
  }
}
