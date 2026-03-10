import { ImapFlow } from 'imapflow';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import type { Adapter } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';

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
  client: ImapFlow;
  host: string;
  user: string;
  password: string;
  idleTimer: ReturnType<typeof setTimeout> | null;
  hasNewMail: boolean;
}

interface EmailCursor {
  accounts: Record<string, { lastUid: number }>;
}

// Map message-id header → thread ID for resolving In-Reply-To chains
type ThreadIndex = Map<string, string>;

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
  private folder: string = 'INBOX';
  private initialDays: number = 30;
  private threadIndex: ThreadIndex = new Map();
  private knownContacts: Set<string> = new Set();
  private log: (msg: string) => void;

  constructor(log: (msg: string) => void = console.log) {
    this.log = log;
  }

  async init(config: AdapterConfig): Promise<void> {
    const dataDir = (config as unknown as { data_dir?: string }).data_dir ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
    const secretsDir = path.join(dataDir, 'secrets');
    this.folder = (config.folder as string) ?? 'INBOX';
    this.initialDays = config.initial_days ?? 30;

    const envPath = path.join(secretsDir, 'email.env');
    if (!fs.existsSync(envPath)) {
      this.log('[email] No email.env found — adapter disabled');
      return;
    }

    const env = loadEnv(envPath);
    const accountConfigs = (config.accounts as AccountConfig[]) ?? [];

    if (!accountConfigs.length) {
      this.log('[email] No accounts configured — adapter disabled');
      return;
    }

    for (const acct of accountConfigs) {
      const prefix = acct.id.toUpperCase();
      const host = acct.host ?? env[`IMAP_${prefix}_HOST`];
      const user = acct.user ?? env[`IMAP_${prefix}_USER`];
      const password = acct.password ?? env[`IMAP_${prefix}_PASSWORD`];

      if (!host || !user || !password) {
        this.log(`[email] Missing credentials for account ${acct.id} — skipping`);
        continue;
      }

      try {
        const client = new ImapFlow({
          host,
          port: 993,
          secure: true,
          auth: { user, pass: password },
          logger: false,
          socketTimeout: 300_000,  // 5 min — large mailbox fetches need time
        });

        await client.connect();
        this.log(`[email] Connected to ${acct.id} (${user}@${host})`);

        const conn: AccountConnection = {
          id: acct.id,
          name: acct.name ?? acct.id,
          client,
          host,
          user,
          password,
          idleTimer: null,
          hasNewMail: false,
        };

        // Listen for new mail events
        client.on('exists', () => {
          conn.hasNewMail = true;
        });

        // Prevent unhandled 'error' event crash
        client.on('error', (err: Error) => {
          this.log(`[email] ${acct.id} socket error: ${err.message}`);
        });

        this.accounts.push(conn);
      } catch (err) {
        this.log(`[email] Failed to connect account ${acct.id}: ${err}`);
      }
    }

    if (!this.accounts.length) {
      this.log('[email] No accounts connected — adapter effectively disabled');
    }
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    this.currentCursor = cursorStr ? JSON.parse(cursorStr) : { accounts: {} };
    const now = new Date();

    for (const acct of this.accounts) {
      try {
        yield* this.syncAccount(acct, now);
      } catch (err) {
        this.log(`[email] Error syncing account ${acct.id}: ${err}`);

        // Try to reconnect for next time
        try {
          await acct.client.close();
        } catch { /* ignore */ }
        try {
          const client = new ImapFlow({
            host: acct.host,
            port: 993,
            secure: true,
            auth: { user: acct.user, pass: acct.password },
            logger: false,
            socketTimeout: 300_000,
          });
          await client.connect();
          client.on('exists', () => { acct.hasNewMail = true; });
          client.on('error', (err: Error) => {
            this.log(`[email] ${acct.id} socket error: ${err.message}`);
          });
          acct.client = client;
          this.log(`[email] Reconnected account ${acct.id}`);
        } catch (reconnErr) {
          this.log(`[email] Reconnect failed for ${acct.id}: ${reconnErr}`);
        }
      }
    }
  }

  private async *syncAccount(acct: AccountConnection, now: Date): AsyncGenerator<SyncEvent> {
    const acctCursor = this.currentCursor.accounts[acct.id] ?? { lastUid: 0 };

    let lock;
    try {
      lock = await acct.client.getMailboxLock(this.folder);
    } catch (err) {
      this.log(`[email] Could not open ${this.folder} for ${acct.id}: ${err}`);
      return;
    }

    try {
      // Determine search range
      let searchQuery: Record<string, unknown>;
      if (acctCursor.lastUid > 0) {
        // Incremental: fetch UIDs after last known
        searchQuery = { uid: `${acctCursor.lastUid + 1}:*` };
      } else {
        // Initial sync: last N days
        const cutoff = new Date(now.getTime() - this.initialDays * 24 * 60 * 60 * 1000);
        searchQuery = { since: cutoff };
      }

      let maxUid = acctCursor.lastUid;
      let msgCount = 0;

      // Envelope-only fetch for speed — avoids downloading full message bodies.
      // headers returns raw buffer; we parse References from it for threading.
      // bodyParts['1'] gets the first MIME part (usually text/plain).
      for await (const msg of acct.client.fetch(searchQuery, {
        uid: true,
        envelope: true,
        headers: ['references'],
        bodyParts: ['1'],
      })) {
        // Skip messages we've already processed (IMAP UID ranges can re-include boundary)
        if (msg.uid <= acctCursor.lastUid) continue;

        const env = msg.envelope;
        if (!env) continue;

        // Extract addresses from envelope
        const fromAddrs = env.from ?? [];
        const toAddrs = env.to ?? [];
        const allAddresses = [...fromAddrs, ...toAddrs];

        for (const addr of allAddresses) {
          const email = addr.address?.toLowerCase();
          if (!email || this.knownContacts.has(email)) continue;
          this.knownContacts.add(email);

          const contact: Contact = {
            id: `email:user:${email}`,
            platform: 'email',
            display_name: addr.name || null,
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
          },
          created_at: (env.date ? new Date(env.date) : now).toISOString(),
          updated_at: now.toISOString(),
        };
        yield { type: 'thread', data: thread };

        // Get text content from first body part
        const textPart = msg.bodyParts?.get('1');
        const textContent = textPart ? textPart.toString() : null;

        const senderEmail = fromAddrs[0]?.address?.toLowerCase() ?? null;
        const metadata: Record<string, unknown> = {
          account: acct.id,
          subject: env.subject,
          message_id: messageId,
        };

        const envToAddrs = toAddrs.map(a => a.address).filter(Boolean);
        if (envToAddrs.length) metadata.to = envToAddrs;

        const ccAddrs = (env.cc ?? []).map((a: { address?: string }) => a.address).filter(Boolean);
        if (ccAddrs.length) metadata.cc = ccAddrs;

        const message: Message = {
          id: `email:msg:${acct.id}:${msg.uid}`,
          platform: 'email',
          thread_id: threadId,
          sender_id: senderEmail ? `email:user:${senderEmail}` : `email:system:${acct.id}`,
          content: textContent,
          content_type: 'text',
          reply_to: null,  // Threading handled via threadId
          metadata,
          platform_ts: (env.date ? new Date(env.date) : now).toISOString(),
          synced_at: now.toISOString(),
        };
        yield { type: 'message', data: message };

        if (msg.uid > maxUid) maxUid = msg.uid;
        msgCount++;
      }

      // Update cursor
      if (maxUid > acctCursor.lastUid) {
        this.currentCursor.accounts[acct.id] = { lastUid: maxUid };
      }

      acct.hasNewMail = false;

      if (msgCount > 0) {
        this.log(`[email] ${acct.id}: synced ${msgCount} messages (maxUid: ${maxUid})`);
      }
    } finally {
      lock.release();
    }

    // Start IDLE for real-time notifications (non-blocking)
    this.startIdle(acct);
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
        await acct.client.logout();
        this.log(`[email] Disconnected ${acct.id}`);
      } catch {
        // Connection may already be dead
      }
    }
    this.accounts = [];
  }
}
