/**
 * WhatsApp Adapter — buffer-and-drain pattern
 *
 * Baileys (WhatsApp Web) is event-driven: messages arrive via WebSocket push.
 * Our daemon is poll-based. Solution: Baileys events fill an in-memory buffer;
 * each sync() call drains it.
 *
 * init()     → connect socket, register event handlers, buffer starts filling
 * sync()     → drain buffer, yield SyncEvents, update cursor
 * shutdown() → disconnect socket
 */

import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState,
  type WASocket,
  type ConnectionState,
  type BaileysEventMap,
  type proto,
} from '@whiskeysockets/baileys';
// @ts-ignore — qrcode-terminal has no types
import qrcode from 'qrcode-terminal';
import path from 'node:path';
import type { Adapter } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';

// --- Constants ---

const MAX_RECONNECT_ATTEMPTS = 10;
const RECONNECT_BASE_DELAY_MS = 1000;
const RECONNECT_MAX_DELAY_MS = 30000;
const CONNECTION_TIMEOUT_MS = 60000;
const KEEPALIVE_INTERVAL_MS = 25000;
/** Safety cap on buffer size — prevents unbounded growth during extended auth failures */
const MAX_BUFFER_SIZE = 50000;

// --- Cursor ---

interface WhatsAppCursor {
  lastMessageTs: string;    // ISO 8601, newest message yielded
  totalMessages: number;    // diagnostic counter — messages only (not contacts/threads)
}

// --- Helpers ---

/** Extract phone number from JID (e.g. "15551234567@s.whatsapp.net" → "15551234567") */
function phoneFromJid(jid: string): string {
  if (!jid) return 'unknown';
  return jid.split('@')[0].split(':')[0];
}

/** Determine if a JID is a group */
function isGroupJid(jid: string): boolean {
  return jid.endsWith('@g.us');
}

/** Build our platform-specific contact ID */
function contactId(jid: string): string {
  return `whatsapp:user:${phoneFromJid(jid)}`;
}

/** Build our platform-specific thread ID */
function threadId(jid: string): string {
  if (isGroupJid(jid)) {
    const groupId = jid.split('@')[0];
    return `whatsapp:group:${groupId}`;
  }
  return `whatsapp:chat:${phoneFromJid(jid)}`;
}

/** Build our platform-specific message ID */
function messageId(chatJid: string, msgId: string): string {
  const chatPart = isGroupJid(chatJid) ? chatJid.split('@')[0] : phoneFromJid(chatJid);
  return `whatsapp:msg:${chatPart}:${msgId}`;
}

/** Determine content type from Baileys message */
function getContentType(msg: proto.IMessage | null | undefined): Message['content_type'] {
  if (!msg) return 'text';
  if (msg.conversation || msg.extendedTextMessage) return 'text';
  if (msg.imageMessage) return 'photo';
  if (msg.videoMessage) return 'video';
  // ptt=true is a voice note; ptt=false is an audio file share (treat as document)
  if (msg.audioMessage) return msg.audioMessage.ptt ? 'voice' : 'document';
  if (msg.documentMessage) return 'document';
  if (msg.stickerMessage) return 'sticker';
  return 'other';
}

/** Extract text content from Baileys message */
function extractText(msg: proto.IMessage | null | undefined): string | null {
  if (!msg) return null;
  if (msg.conversation) return msg.conversation;
  if (msg.extendedTextMessage?.text) return msg.extendedTextMessage.text;
  if (msg.imageMessage?.caption) return msg.imageMessage.caption;
  if (msg.videoMessage?.caption) return msg.videoMessage.caption;
  if (msg.documentMessage?.fileName) return `[Document: ${msg.documentMessage.fileName}]`;
  if (msg.locationMessage) {
    return `Location: ${msg.locationMessage.degreesLatitude}, ${msg.locationMessage.degreesLongitude}`;
  }
  if (msg.contactMessage?.displayName) return `[Contact: ${msg.contactMessage.displayName}]`;
  if (msg.reactionMessage?.text) return msg.reactionMessage.text;
  return null;
}

// --- Adapter ---

export class WhatsAppAdapter implements Adapter {
  platform = 'whatsapp';

  private socket: WASocket | null = null;
  private buffer: SyncEvent[] = [];
  private currentCursor: WhatsAppCursor = { lastMessageTs: '', totalMessages: 0 };
  private authenticated = false;
  private reconnectAttempts = 0;
  private intentionalDisconnect = false;
  private dataDir = '';
  private authDir = '';
  private log: (msg: string) => void;
  private myJid: string | null = null;

  constructor(log: (msg: string) => void = console.log) {
    this.log = (msg: string) => log(`[whatsapp] ${msg}`);
  }

  async init(config: AdapterConfig): Promise<void> {
    this.dataDir = (config as unknown as { data_dir?: string }).data_dir ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
    this.authDir = path.join(this.dataDir, 'whatsapp-auth');

    await this.connectSocket();
  }

  /**
   * Create (or recreate) the Baileys socket and register event handlers.
   * Returns a promise that resolves when connected or times out.
   * Safe to call multiple times — old socket is cleaned up first.
   */
  private async connectSocket(): Promise<void> {
    // Clean up old socket — remove listeners first to stop in-flight events writing creds
    if (this.socket) {
      try { this.socket.ev.removeAllListeners(); } catch { /* ignore */ }
      try { this.socket.end(undefined); } catch { /* ignore */ }
      this.socket = null;
    }

    // Load auth state (creates authDir if needed)
    const { state, saveCreds } = await useMultiFileAuthState(this.authDir);

    // Get latest Baileys version
    const { version } = await fetchLatestBaileysVersion();

    // Minimal logger — suppress Baileys noise
    const logger = {
      level: 'silent' as const,
      child: () => logger,
      trace: () => {},
      debug: () => {},
      info: () => {},
      warn: (...args: unknown[]) => this.log(`warn: ${args.join(' ')}`),
      error: (...args: unknown[]) => this.log(`error: ${args.join(' ')}`),
      fatal: (...args: unknown[]) => this.log(`fatal: ${args.join(' ')}`),
    };

    this.socket = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger as any),
      },
      generateHighQualityLinkPreview: false,
      logger: logger as any,
      markOnlineOnConnect: false,  // Don't broadcast daemon presence to contacts
      syncFullHistory: true,  // Request full history push from WhatsApp servers
      connectTimeoutMs: CONNECTION_TIMEOUT_MS,
      defaultQueryTimeoutMs: CONNECTION_TIMEOUT_MS,
      keepAliveIntervalMs: KEEPALIVE_INTERVAL_MS,
      retryRequestDelayMs: 250,
    }) as unknown as WASocket;

    // --- One-shot connection promise ---
    const { promise: connected, resolve: resolveConnected } = promiseWithResolvers<void>();

    // --- Event handlers ---

    this.socket.ev.on('connection.update', (update: Partial<ConnectionState>) => {
      const { connection, lastDisconnect, qr } = update;

      // QR code for first-time auth
      if (qr) {
        this.log('QR code generated — scan with WhatsApp mobile to authenticate');
        qrcode.generate(qr, { small: true }, (output: string) => {
          for (const line of output.split('\n')) {
            this.log(`QR| ${line}`);
          }
        });
      }

      if (connection === 'open') {
        this.authenticated = true;
        this.reconnectAttempts = 0;
        this.myJid = this.socket?.user?.id ?? null;
        const phone = this.myJid ? phoneFromJid(this.myJid) : 'unknown';
        this.log(`Connected as ${phone}`);
        resolveConnected();
      }

      if (connection === 'close') {
        this.authenticated = false;
        const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
        this.log(`Connection closed (status: ${statusCode ?? 'unknown'})`);

        // Always resolve the init promise so we don't block the daemon
        resolveConnected();

        if (this.intentionalDisconnect) return;

        // On 401/loggedOut: don't clear session, just log
        if (statusCode === DisconnectReason.loggedOut) {
          this.log('Logged out — session invalid. Re-enable to trigger new QR auth.');
          return;
        }

        // Auto-reconnect with exponential backoff
        if (this.reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
          this.reconnectAttempts++;
          const delay = Math.min(
            RECONNECT_BASE_DELAY_MS * Math.pow(2, this.reconnectAttempts - 1),
            RECONNECT_MAX_DELAY_MS,
          );
          this.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
          setTimeout(() => {
            this.connectSocket().catch(err => {
              this.log(`Reconnection failed: ${err}`);
            });
          }, delay);
        } else {
          this.log(`Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached — giving up`);
        }
      }
    });

    // Credential persistence
    this.socket.ev.on('creds.update', saveCreds);

    // History sync (initial bulk load)
    this.socket.ev.on('messaging-history.set', ({ chats, contacts, messages }) => {
      this.log(`History sync: ${chats.length} chats, ${contacts.length} contacts, ${messages.length} messages`);
      const now = new Date().toISOString();

      for (const contact of contacts) {
        if (!contact.id) continue;
        this.pushToBuffer({ type: 'contact', data: this.transformContact(contact, now) });
      }
      for (const chat of chats) {
        if (!chat.id) continue;
        this.pushToBuffer({ type: 'thread', data: this.transformChat(chat, now) });
      }
      for (const msg of messages) {
        if (!msg.message || !msg.key?.remoteJid) continue;
        if (msg.key.remoteJid === 'status@broadcast') continue;
        this.pushToBuffer({ type: 'message', data: this.transformMessage(msg, now) });
      }
    });

    // Real-time messages
    this.socket.ev.on('messages.upsert', ({ messages, type }: BaileysEventMap['messages.upsert']) => {
      const now = new Date().toISOString();
      for (const msg of messages) {
        if (!msg.message || !msg.key?.remoteJid) continue;
        if (msg.key.remoteJid === 'status@broadcast') continue;
        this.pushToBuffer({ type: 'message', data: this.transformMessage(msg, now) });
      }
      if (messages.length > 0) {
        this.log(`Buffered ${messages.length} messages (${type})`);
      }
    });

    // New/updated chats
    this.socket.ev.on('chats.upsert', (chats: any[]) => {
      const now = new Date().toISOString();
      for (const chat of chats) {
        if (!chat.id) continue;
        this.pushToBuffer({ type: 'thread', data: this.transformChat(chat, now) });
      }
    });

    // New/updated contacts
    this.socket.ev.on('contacts.upsert', (contacts: any[]) => {
      const now = new Date().toISOString();
      for (const contact of contacts) {
        if (!contact.id) continue;
        this.pushToBuffer({ type: 'contact', data: this.transformContact(contact, now) });
      }
    });

    // Wait for connection (with timeout)
    const timeout = setTimeout(() => {
      this.log('Connection timeout — continuing without connection. Will reconnect automatically.');
      resolveConnected();
    }, CONNECTION_TIMEOUT_MS);

    try {
      await connected;
    } finally {
      clearTimeout(timeout);
    }
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    if (!this.authenticated) return;

    const cursor: WhatsAppCursor = cursorStr
      ? JSON.parse(cursorStr)
      : { lastMessageTs: '', totalMessages: 0 };

    // Drain buffer
    const events = this.buffer.splice(0);
    let yielded = 0;
    let msgCount = 0;
    let newestTs = cursor.lastMessageTs;

    for (const event of events) {
      // Dedup: skip messages older than cursor (handles restart overlap)
      if (event.type === 'message' && cursor.lastMessageTs) {
        const msg = event.data as Message;
        if (msg.platform_ts <= cursor.lastMessageTs) continue;
      }

      yield event;
      yielded++;

      if (event.type === 'message') {
        msgCount++;
        const msg = event.data as Message;
        if (msg.platform_ts > newestTs) newestTs = msg.platform_ts;
      }
    }

    this.currentCursor = {
      lastMessageTs: newestTs || cursor.lastMessageTs,
      totalMessages: cursor.totalMessages + msgCount,
    };

    if (yielded > 0) {
      this.log(`Drained ${yielded} events (${events.length - yielded} deduped)`);
    }
  }

  /** Push to buffer with size cap to prevent unbounded growth during auth failures */
  private pushToBuffer(event: SyncEvent): void {
    if (this.buffer.length >= MAX_BUFFER_SIZE) {
      this.log(`Buffer cap (${MAX_BUFFER_SIZE}) reached — dropping oldest event`);
      this.buffer.shift();
    }
    this.buffer.push(event);
  }

  getCursor(): string | null {
    return JSON.stringify(this.currentCursor);
  }

  async shutdown(): Promise<void> {
    this.intentionalDisconnect = true;
    if (this.socket) {
      try { this.socket.end(undefined); } catch { /* ignore */ }
      this.socket = null;
    }
    this.authenticated = false;
    this.log('Disconnected');
  }

  // --- Private: Transform Baileys types → our SyncEvent types ---

  private transformContact(contact: any, now: string): Contact {
    const jid: string = contact.id ?? '';
    const phone = phoneFromJid(jid);
    const name = contact.notify || contact.name || contact.verifiedName || phone;

    return {
      id: contactId(jid),
      platform: 'whatsapp',
      display_name: name,
      username: null,
      phone: phone !== 'unknown' ? phone : null,
      metadata: {},
      first_seen: now,
      last_seen: now,
    };
  }

  private transformChat(chat: any, now: string): Thread {
    const jid: string = chat.id ?? '';
    const isGroup = isGroupJid(jid);

    return {
      id: threadId(jid),
      platform: 'whatsapp',
      title: chat.name || chat.subject || (isGroup ? jid.split('@')[0] : phoneFromJid(jid)),
      thread_type: isGroup ? 'group' : 'dm',
      participants: [],
      metadata: {
        unread_count: chat.unreadCount ?? 0,
        archived: chat.archived ?? false,
        muted: chat.mute !== undefined,
      },
      created_at: now,
      updated_at: now,
    };
  }

  private transformMessage(msg: proto.IWebMessageInfo, now: string): Message {
    const key = msg.key;
    const chatJid = key.remoteJid!;
    const msgContent = msg.message;

    let senderJid: string;
    if (key.fromMe && this.myJid) {
      senderJid = this.myJid;
    } else {
      senderJid = key.participant || chatJid;
    }

    let replyTo: string | null = null;
    // contextInfo exists on all message types that can be replies
    const contextInfo = msgContent?.extendedTextMessage?.contextInfo
      ?? msgContent?.imageMessage?.contextInfo
      ?? msgContent?.videoMessage?.contextInfo
      ?? msgContent?.audioMessage?.contextInfo
      ?? msgContent?.documentMessage?.contextInfo
      ?? msgContent?.stickerMessage?.contextInfo;
    if (contextInfo?.stanzaId) {
      replyTo = messageId(chatJid, contextInfo.stanzaId);
    }

    const metadata: Record<string, unknown> = {};
    if (key.fromMe) metadata.from_me = true;
    if (msg.pushName) metadata.push_name = msg.pushName;
    if (contextInfo?.isForwarded) metadata.forwarded = true;
    if (msgContent?.reactionMessage) {
      metadata.reaction = msgContent.reactionMessage.text;
      metadata.reaction_to = msgContent.reactionMessage.key?.id;
    }

    const ts = msg.messageTimestamp;
    let platformTs: string;
    if (typeof ts === 'number' && ts > 0) {
      platformTs = new Date(ts * 1000).toISOString();
    } else if (ts !== null && ts !== undefined && typeof ts === 'object' && 'toNumber' in ts) {
      // Protobuf Long — use .toNumber() (handles high+low 32-bit words correctly)
      const n = (ts as { toNumber(): number }).toNumber();
      platformTs = n > 0 ? new Date(n * 1000).toISOString() : now;
      if (n <= 0) this.log(`warn: missing timestamp on message ${key.id}`);
    } else {
      // Unknown type — log and fall back to sync time (affects dedup; prefer explicit warning)
      this.log(`warn: unknown timestamp type for message ${key.id}, using sync time`);
      platformTs = now;
    }

    return {
      id: messageId(chatJid, key.id || `unknown-${Date.now()}`),
      platform: 'whatsapp',
      thread_id: threadId(chatJid),
      sender_id: contactId(senderJid),
      content: extractText(msgContent),
      content_type: getContentType(msgContent),
      reply_to: replyTo,
      metadata,
      platform_ts: platformTs,
      synced_at: now,
    };
  }
}

// --- Utility ---

/** Promise.withResolvers polyfill for Node <22 */
function promiseWithResolvers<T>(): { promise: Promise<T>; resolve: (v: T) => void; reject: (e: unknown) => void } {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}
