import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { Api } from 'telegram/tl/index.js';
import { FloodWaitError } from 'telegram/errors/index.js';
import fs from 'node:fs';
import path from 'node:path';
import type { Adapter } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';

interface TelegramCursor {
  dialogs: Record<string, number>;  // dialogId -> lastMessageId
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

function toIso(date: number | Date): string {
  if (typeof date === 'number') {
    return new Date(date * 1000).toISOString();
  }
  return date instanceof Date ? date.toISOString() : new Date().toISOString();
}

function getUserDisplayName(user: Api.User): string {
  const parts = [user.firstName, user.lastName].filter(Boolean);
  return parts.join(' ') || user.username || `User ${user.id}`;
}

function getDialogType(entity: Api.TypeEntity): Thread['thread_type'] {
  if (entity instanceof Api.User) return 'dm';
  if (entity instanceof Api.Chat) return 'group';
  if (entity instanceof Api.Channel) {
    return entity.megagroup ? 'supergroup' : 'channel';
  }
  return 'group';
}

function getDialogTitle(entity: Api.TypeEntity): string | null {
  if (entity instanceof Api.User) {
    return getUserDisplayName(entity);
  }
  if (entity instanceof Api.Chat || entity instanceof Api.Channel) {
    return entity.title ?? null;
  }
  return null;
}

function getEntityId(entity: Api.TypeEntity): string {
  if (entity instanceof Api.User) return String(entity.id);
  if (entity instanceof Api.Chat) return String(-entity.id);
  if (entity instanceof Api.Channel) return String(-1000000000000 - Number(entity.id));
  return String((entity as { id: bigint }).id);
}

export class TelegramAdapter implements Adapter {
  platform = 'telegram';
  private client: TelegramClient | null = null;
  private secretsDir: string = '';
  private initialDays: number = 30;
  private currentCursor: TelegramCursor = { dialogs: {} };
  private log: (msg: string) => void;

  constructor(log: (msg: string) => void = console.log) {
    this.log = log;
  }

  async init(config: AdapterConfig): Promise<void> {
    const dataDir = (config as unknown as { data_dir?: string }).data_dir ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
    this.secretsDir = path.join(dataDir, 'secrets');
    this.initialDays = config.initial_days ?? 30;

    const envPath = path.join(this.secretsDir, 'telegram.env');
    if (!fs.existsSync(envPath)) {
      throw new Error(`Telegram env not found: ${envPath}`);
    }

    const env = loadEnv(envPath);
    const apiId = parseInt(env.TELEGRAM_API_ID, 10);
    const apiHash = env.TELEGRAM_API_HASH;
    const sessionStr = env.TELEGRAM_STRING_SESSION;

    if (!apiId || !apiHash || !sessionStr) {
      throw new Error('Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_STRING_SESSION in telegram.env');
    }

    const session = new StringSession(sessionStr);
    this.client = new TelegramClient(session, apiId, apiHash, {
      connectionRetries: 5,
      useWSS: false,
    });

    await this.client.connect();
    this.log('[telegram] Connected');
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    if (!this.client) throw new Error('Telegram adapter not initialized');

    this.currentCursor = cursorStr ? JSON.parse(cursorStr) : { dialogs: {} };
    const now = new Date();
    const cutoffDate = new Date(now.getTime() - this.initialDays * 24 * 60 * 60 * 1000);

    // Get all dialogs (paginate until exhausted)
    const dialogs: Awaited<ReturnType<TelegramClient['getDialogs']>> = [];
    let offsetDate = 0;
    let fetchMore = true;

    while (fetchMore) {
      const batch = await this.client.getDialogs({
        limit: 100,
        ...(offsetDate ? { offsetDate } : {}),
      });

      if (batch.length === 0) {
        fetchMore = false;
        break;
      }

      dialogs.push(...batch);

      // Use the last dialog's date as offset for next page
      const lastDialog = batch[batch.length - 1];
      const lastDate = lastDialog.date;
      if (!lastDate || lastDate === offsetDate) {
        fetchMore = false;
      } else {
        offsetDate = lastDate;
      }

      // Safety: avoid infinite loops
      if (dialogs.length > 10000) {
        this.log(`[telegram] Capped at ${dialogs.length} dialogs`);
        fetchMore = false;
      }
    }

    this.log(`[telegram] Found ${dialogs.length} dialogs`);

    for (const dialog of dialogs) {
      if (!dialog.entity) continue;

      const entityId = getEntityId(dialog.entity);
      const threadId = `telegram:chat:${entityId}`;
      const dialogType = getDialogType(dialog.entity);
      const title = getDialogTitle(dialog.entity);

      // Yield contact for DMs
      if (dialog.entity instanceof Api.User && !dialog.entity.bot) {
        const contact: Contact = {
          id: `telegram:user:${dialog.entity.id}`,
          platform: 'telegram',
          display_name: getUserDisplayName(dialog.entity),
          username: dialog.entity.username ?? null,
          phone: dialog.entity.phone ?? null,
          metadata: { bot: dialog.entity.bot ?? false },
          first_seen: now.toISOString(),
          last_seen: now.toISOString(),
        };
        yield { type: 'contact', data: contact };
      }

      // Yield thread
      const thread: Thread = {
        id: threadId,
        platform: 'telegram',
        title,
        thread_type: dialogType,
        participants: [],
        metadata: {
          unread_count: dialog.unreadCount,
          pinned: dialog.pinned,
        },
        created_at: now.toISOString(),
        updated_at: now.toISOString(),
      };
      yield { type: 'thread', data: thread };

      // Fetch messages since cursor or cutoff
      const lastId = this.currentCursor.dialogs[entityId];
      try {
        const messages = await this.client.getMessages(dialog.entity, {
          limit: lastId ? 200 : 100,  // More for incremental, less for initial
          minId: lastId ?? undefined,
        });

        let maxId = lastId ?? 0;
        for (const msg of messages) {
          // Skip messages before cutoff on initial sync
          if (!lastId && msg.date && msg.date < cutoffDate.getTime() / 1000) {
            continue;
          }

          const senderId = msg.senderId ? `telegram:user:${msg.senderId}` : `telegram:system:${entityId}`;

          // Determine content type
          let contentType: Message['content_type'] = 'text';
          const metadata: Record<string, unknown> = {};

          if (msg.photo) {
            contentType = 'photo';
            metadata.has_photo = true;
          } else if (msg.document) {
            contentType = 'document';
            metadata.document_name = (msg.document as Api.Document).mimeType;
          } else if (msg.sticker) {
            contentType = 'sticker';
            metadata.sticker_emoji = (msg.sticker as Api.Document).attributes?.find(
              (a: Api.TypeDocumentAttribute) => a instanceof Api.DocumentAttributeSticker
            )?.alt;
          }

          if (msg.replyTo?.replyToMsgId) {
            metadata.reply_to_msg_id = msg.replyTo.replyToMsgId;
          }
          if (msg.fwdFrom) {
            metadata.forwarded = true;
            metadata.fwd_from_id = msg.fwdFrom.fromId?.toString();
          }
          if (msg.editDate) {
            metadata.edited = true;
            metadata.edit_date = toIso(msg.editDate);
          }

          const message: Message = {
            id: `telegram:msg:${entityId}:${msg.id}`,
            platform: 'telegram',
            thread_id: threadId,
            sender_id: senderId,
            content: msg.message ?? null,
            content_type: contentType,
            reply_to: msg.replyTo?.replyToMsgId ? `telegram:msg:${entityId}:${msg.replyTo.replyToMsgId}` : null,
            metadata,
            platform_ts: toIso(msg.date),
            synced_at: now.toISOString(),
          };
          yield { type: 'message', data: message };

          if (msg.id > maxId) maxId = msg.id;
        }

        // Update internal cursor for this dialog
        if (maxId > 0) {
          this.currentCursor.dialogs[entityId] = maxId;
        }
      } catch (err) {
        if (err instanceof FloodWaitError) {
          this.log(`[telegram] Flood wait: ${err.seconds}s for dialog ${entityId}`);
          await new Promise(resolve => setTimeout(resolve, (err.seconds + 1) * 1000));
        } else {
          this.log(`[telegram] Error syncing dialog ${entityId}: ${err}`);
        }
      }
    }
  }

  getCursor(): string | null {
    return JSON.stringify(this.currentCursor);
  }

  async shutdown(): Promise<void> {
    if (this.client) {
      await this.client.disconnect();
      this.log('[telegram] Disconnected');
    }
  }
}
