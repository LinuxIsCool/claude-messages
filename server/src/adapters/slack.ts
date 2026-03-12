import fs from 'node:fs';
import path from 'node:path';
import { WebClient } from '@slack/web-api';
import type { Adapter } from './base.js';
import type { SyncEvent, AdapterConfig, Contact, Thread, Message } from '../types.js';

// Slack API response types (subset we use)
interface SlackUser {
  id: string;
  name: string;
  real_name?: string;
  profile?: {
    display_name?: string;
    real_name?: string;
    email?: string;
    image_72?: string;
  };
  is_bot?: boolean;
  deleted?: boolean;
}

interface SlackConversation {
  id: string;
  name?: string;
  is_im?: boolean;
  is_mpim?: boolean;
  is_private?: boolean;
  is_channel?: boolean;
  is_group?: boolean;
  is_archived?: boolean;
  user?: string;           // DM partner user ID
  topic?: { value: string };
  purpose?: { value: string };
  num_members?: number;
  created?: number;
  updated?: number;
}

interface SlackBlock {
  type: string;
  elements?: SlackBlock[];
  text?: string | { text: string; type: string };
  url?: string;
  user_id?: string;
  channel_id?: string;
}

interface SlackMessage {
  type?: string;
  subtype?: string;
  ts: string;
  user?: string;
  bot_id?: string;
  bot_profile?: { id: string; name: string };
  text?: string;
  blocks?: SlackBlock[];
  thread_ts?: string;
  reply_count?: number;
  files?: Array<{ name?: string; mimetype?: string; url_private?: string }>;
  attachments?: Array<{ fallback?: string; text?: string; title?: string }>;
  edited?: { user: string; ts: string };
  // message_changed subtype
  message?: SlackMessage;
  previous_message?: SlackMessage;
}

interface SlackCursor {
  channels: Record<string, string>;  // channelId -> lastTs (string, never float)
}

/** System subtypes that don't represent user content */
const SKIP_SUBTYPES = new Set([
  'channel_join', 'channel_leave', 'channel_archive', 'channel_unarchive',
  'channel_name', 'channel_topic', 'channel_purpose', 'channel_posting_permissions',
  'group_join', 'group_leave', 'group_archive', 'group_unarchive',
  'group_name', 'group_topic', 'group_purpose',
  'pinned_item', 'unpinned_item', 'reminder_add',
  'message_replied',  // informational — actual reply arrives as its own message
  'bot_add', 'bot_remove',
  'channel_convert.from_regular', 'channel_convert.to_regular',
]);

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

/** Convert Slack ts (epoch.seq) to ISO 8601. ts is always kept as string. */
function slackTsToIso(ts: string): string {
  const epochSeconds = parseInt(ts.split('.')[0], 10);
  return new Date(epochSeconds * 1000).toISOString();
}

/** Extract readable text from blocks array (recursive rich_text traversal) */
function extractBlockText(blocks: SlackBlock[]): string {
  const parts: string[] = [];

  for (const block of blocks) {
    if (block.type === 'rich_text') {
      if (block.elements) parts.push(extractBlockText(block.elements));
    } else if (block.type === 'rich_text_section' || block.type === 'rich_text_preformatted' ||
               block.type === 'rich_text_quote' || block.type === 'rich_text_list') {
      if (block.elements) parts.push(extractBlockText(block.elements));
    } else if (block.type === 'text') {
      const text = typeof block.text === 'string' ? block.text : block.text?.text;
      if (text) parts.push(text);
    } else if (block.type === 'link') {
      const text = typeof block.text === 'string' ? block.text : block.text?.text;
      parts.push(text || block.url || '');
    } else if (block.type === 'user') {
      parts.push(`<@${block.user_id}>`);
    } else if (block.type === 'channel') {
      parts.push(`<#${block.channel_id}>`);
    } else if (block.type === 'emoji') {
      const name = typeof block.text === 'string' ? block.text : block.text?.text;
      if (name) parts.push(`:${name}:`);
    } else if (block.type === 'section' || block.type === 'context' || block.type === 'header') {
      const text = typeof block.text === 'string' ? block.text : block.text?.text;
      if (text) parts.push(text);
      if (block.elements) parts.push(extractBlockText(block.elements));
    }
  }

  return parts.join('');
}

/** Clean mrkdwn text — resolve basic formatting for storage */
function cleanMrkdwn(text: string, resolveUser: (id: string) => string): string {
  return text
    // Resolve user mentions: <@U123> → @displayname
    .replace(/<@(U[A-Z0-9]+)>/g, (_match, uid: string) => `@${resolveUser(uid)}`)
    // Resolve channel mentions: <#C123|channel-name> → #channel-name
    .replace(/<#[A-Z0-9]+\|([^>]+)>/g, '#$1')
    // Resolve channel mentions without label: <#C123> → #C123
    .replace(/<#([A-Z0-9]+)>/g, '#$1')
    // Resolve links: <url|label> → label, <url> → url
    .replace(/<([^|>]+)\|([^>]+)>/g, '$2')
    .replace(/<([^>]+)>/g, '$1');
}

/** Extract content from a Slack message — blocks first, mrkdwn text fallback */
function extractContent(
  msg: SlackMessage,
  resolveUser: (id: string) => string,
): string | null {
  // Blocks are the canonical rich format
  if (msg.blocks?.length) {
    const blockText = extractBlockText(msg.blocks);
    if (blockText.trim()) return blockText;
  }

  // Fall back to mrkdwn text field
  if (msg.text?.trim()) {
    return cleanMrkdwn(msg.text, resolveUser);
  }

  // Attachment-only messages (links, bot cards)
  if (msg.attachments?.length) {
    const parts = msg.attachments
      .map(a => a.text || a.fallback || a.title || '')
      .filter(Boolean);
    if (parts.length) return parts.join('\n');
  }

  // File-only messages — generate description for FTS5 searchability
  if (msg.files?.length) {
    const descriptions = msg.files
      .map(f => f.name || f.mimetype || 'file')
      .filter(Boolean);
    if (descriptions.length) return `[${descriptions.join(', ')}]`;
  }

  return null;
}

/** Determine content_type from message properties */
function getContentType(msg: SlackMessage): Message['content_type'] {
  if (msg.files?.length) {
    const firstFile = msg.files[0];
    const mime = firstFile.mimetype ?? '';
    if (mime.startsWith('image/')) return 'photo';
    if (mime.startsWith('video/')) return 'video';
    if (mime.startsWith('audio/')) return 'voice';
    return 'document';
  }
  return 'text';
}

/** Map Slack conversation flags to our thread_type */
function getThreadType(conv: SlackConversation): Thread['thread_type'] {
  if (conv.is_im) return 'dm';
  if (conv.is_mpim) return 'group';
  if (conv.is_private || conv.is_group) return 'group';
  return 'channel';
}

/** Tier 3 rate limit — 50 req/min for internal apps. Wait this between bursts. */
const RATE_LIMIT_DELAY_MS = 1200;

export class SlackAdapter implements Adapter {
  private webClient!: WebClient;
  private teamId: string = '';
  private workspaceId: string = '';
  private workspaceName: string = '';
  private userCache: Map<string, SlackUser> = new Map();
  private currentCursor: SlackCursor = { channels: {} };
  private unknownUsersThisCycle = new Set<string>();
  private initialDays: number = 365;
  private log: (msg: string) => void;

  /** Cursor key — unique per workspace so each gets its own sync position */
  get platform(): string {
    return `slack:${this.workspaceId}`;
  }

  constructor(log: (msg: string) => void = console.log) {
    this.log = log;
  }

  async init(config: AdapterConfig): Promise<void> {
    const dataDir = (config as Record<string, unknown>).data_dir as string ??
      path.join(process.env.HOME ?? '', '.claude', 'local', 'messages');
    const secretsDir = path.join(dataDir, 'secrets');

    this.workspaceId = (config as Record<string, unknown>).workspace_id as string;
    this.workspaceName = (config as Record<string, unknown>).workspace_name as string ?? this.workspaceId;
    this.initialDays = config.initial_days ?? 365;

    const envPath = path.join(secretsDir, `slack_${this.workspaceId}.env`);
    if (!fs.existsSync(envPath)) {
      throw new Error(`No slack_${this.workspaceId}.env found at ${secretsDir}`);
    }

    const env = loadEnv(envPath);
    const userToken = env.SLACK_USER_TOKEN;
    if (!userToken) {
      throw new Error(`SLACK_USER_TOKEN not found in slack_${this.workspaceId}.env`);
    }

    // User token sees all channels without needing /invite — ideal for backfill
    this.webClient = new WebClient(userToken, {
      retryConfig: { retries: 3 },
    });

    // Verify auth and get team info
    const authResult = await this.webClient.auth.test();
    this.teamId = authResult.team_id as string;
    const teamName = authResult.team as string;
    this.log(`[slack:${this.workspaceId}] Authenticated to ${teamName} (team: ${this.teamId})`);

    // Populate user cache
    await this.fetchUsers();
  }

  private async fetchUsers(): Promise<void> {
    let cursor: string | undefined;
    let totalUsers = 0;

    do {
      const result = await this.webClient.users.list({
        limit: 200,
        cursor,
      });

      for (const user of (result.members ?? [])) {
        this.userCache.set(user.id!, user as SlackUser);
        totalUsers++;
      }

      cursor = result.response_metadata?.next_cursor || undefined;
    } while (cursor);

    this.log(`[slack:${this.workspaceId}] Cached ${totalUsers} users`);
  }

  /** Resolve a Slack user ID to a display name */
  private resolveUserName(userId: string): string {
    const user = this.userCache.get(userId);
    if (!user) return userId;
    return user.profile?.display_name || user.real_name || user.name || userId;
  }

  async *sync(cursorStr: string | null): AsyncGenerator<SyncEvent> {
    this.currentCursor = cursorStr ? JSON.parse(cursorStr) : { channels: {} };
    this.unknownUsersThisCycle = new Set();
    const now = new Date().toISOString();

    // Phase 1: Yield contacts
    yield* this.syncContacts(now);

    // Phase 2 & 3: Enumerate channels and fetch messages
    yield* this.syncChannelsAndMessages(now);
  }

  private *syncContacts(now: string): Generator<SyncEvent> {
    for (const user of this.userCache.values()) {
      if (user.deleted) continue;

      const metadata: Record<string, unknown> = {};
      if (user.profile?.email) metadata.email = user.profile.email;
      if (user.profile?.image_72) metadata.avatar = user.profile.image_72;
      if (user.is_bot) metadata.is_bot = true;

      const idSuffix = user.is_bot ? `bot:${user.id}` : `user:${user.id}`;

      const contact: Contact = {
        id: `slack:${this.teamId}:${idSuffix}`,
        platform: 'slack',
        display_name: user.profile?.display_name || user.real_name || user.name || null,
        username: user.name ?? null,
        phone: null,
        metadata,
        first_seen: now,
        last_seen: now,
      };
      yield { type: 'contact', data: contact };
    }
  }

  private async *syncChannelsAndMessages(now: string): AsyncGenerator<SyncEvent> {
    // Collect all conversations
    const conversations: SlackConversation[] = [];
    let cursor: string | undefined;

    do {
      const result = await this.webClient.conversations.list({
        types: 'public_channel,private_channel,im,mpim',
        limit: 200,
        cursor,
        exclude_archived: false,
      });

      for (const conv of (result.channels ?? [])) {
        conversations.push(conv as SlackConversation);
      }

      cursor = result.response_metadata?.next_cursor || undefined;
    } while (cursor);

    this.log(`[slack:${this.workspaceId}] Found ${conversations.length} conversations`);

    // Track parent messages that have replies — fetch them after main pass
    const threadsToFetch: Array<{ channelId: string; parentTs: string }> = [];
    const failedChannels = new Set<string>();

    for (const conv of conversations) {
      // Yield thread
      const threadType = getThreadType(conv);
      let title = conv.name ?? null;

      // For DMs, resolve the partner's name
      if (conv.is_im && conv.user) {
        title = this.resolveUserName(conv.user);
      }

      const thread: Thread = {
        id: `slack:${this.teamId}:channel:${conv.id}`,
        platform: 'slack',
        title,
        thread_type: threadType,
        participants: [],  // populated incrementally from messages
        metadata: {
          workspace: this.workspaceId,
          is_archived: conv.is_archived ?? false,
          ...(conv.topic?.value ? { topic: conv.topic.value } : {}),
          ...(conv.purpose?.value ? { purpose: conv.purpose.value } : {}),
          ...(conv.num_members ? { num_members: conv.num_members } : {}),
        },
        created_at: conv.created ? new Date(conv.created * 1000).toISOString() : now,
        updated_at: conv.updated ? new Date(conv.updated * 1000).toISOString() : now,
      };
      yield { type: 'thread', data: thread };

      // Skip archived channels on incremental sync — they rarely get new messages.
      // On first sync (no cursor), still fetch to capture historical messages.
      const channelCursorTs = this.currentCursor.channels[conv.id];
      if (conv.is_archived && channelCursorTs) continue;
      let newestTs = channelCursorTs ?? '';

      try {
        yield* this.fetchChannelMessages(conv.id, channelCursorTs, now, threadsToFetch);

        // Track newest ts from messages yielded (fetchChannelMessages updates currentCursor)
        if (this.currentCursor.channels[conv.id] &&
            this.currentCursor.channels[conv.id] > newestTs) {
          newestTs = this.currentCursor.channels[conv.id];
        }
      } catch (err) {
        const errMsg = String(err);
        const isPermission = errMsg.includes('not_in_channel') || errMsg.includes('channel_not_found') || errMsg.includes('missing_scope');
        this.log(`[slack:${this.workspaceId}] ${isPermission ? 'PERM' : 'ERR'} fetching ${conv.id}: ${err}`);
        failedChannels.add(conv.id);
      }
    }

    // Phase 4: Fetch thread replies (skip channels that failed in Phase 3)
    const validThreads = threadsToFetch.filter(t => !failedChannels.has(t.channelId));
    if (validThreads.length) {
      this.log(`[slack:${this.workspaceId}] Fetching replies for ${validThreads.length} threaded messages`);
      yield* this.fetchThreadReplies(validThreads, now);
    }
  }

  private async *fetchChannelMessages(
    channelId: string,
    lastTs: string | undefined,
    now: string,
    threadsToFetch: Array<{ channelId: string; parentTs: string }>,
  ): AsyncGenerator<SyncEvent> {
    const params: Record<string, unknown> = {
      channel: channelId,
      limit: 200,
    };

    if (lastTs) {
      // Incremental — fetch only newer messages
      params.oldest = lastTs;
      params.inclusive = false;
    } else {
      // First sync — go back initialDays
      const cutoff = Date.now() / 1000 - this.initialDays * 86400;
      params.oldest = cutoff.toString();
    }

    let pageCount = 0;
    let msgCount = 0;
    let paginationCursor: string | undefined;

    do {
      if (paginationCursor) {
        params.cursor = paginationCursor;
      }

      const result = await this.webClient.conversations.history(params as Parameters<WebClient['conversations']['history']>[0]);
      const messages = (result.messages ?? []) as SlackMessage[];

      for (const msg of messages) {
        // Handle message_changed: extract the updated message
        if (msg.subtype === 'message_changed' && !msg.message) continue; // malformed event
        const effectiveMsg = msg.subtype === 'message_changed' && msg.message
          ? msg.message
          : msg;

        // Skip system subtypes
        if (effectiveMsg.subtype && SKIP_SUBTYPES.has(effectiveMsg.subtype)) continue;

        // Skip message_deleted
        if (msg.subtype === 'message_deleted') continue;

        // Lazy-resolve unknown users (joined after init, deactivated, etc.)
        if (effectiveMsg.user && !this.userCache.has(effectiveMsg.user) && !this.unknownUsersThisCycle.has(effectiveMsg.user)) {
          try {
            const result = await this.webClient.users.info({ user: effectiveMsg.user });
            if (result.user) {
              this.userCache.set(result.user.id!, result.user as SlackUser);
            }
          } catch {
            this.unknownUsersThisCycle.add(effectiveMsg.user);
          }
        }

        // Determine sender
        let senderId: string;
        if (effectiveMsg.user) {
          senderId = `slack:${this.teamId}:user:${effectiveMsg.user}`;
        } else if (effectiveMsg.bot_id) {
          senderId = `slack:${this.teamId}:bot:${effectiveMsg.bot_id}`;
        } else {
          senderId = `slack:${this.teamId}:user:UNKNOWN`;
        }

        // Extract content
        const content = extractContent(effectiveMsg, (id) => this.resolveUserName(id));

        // Determine reply_to — if thread_ts differs from ts, this is a threaded reply
        let replyTo: string | null = null;
        if (effectiveMsg.thread_ts && effectiveMsg.thread_ts !== effectiveMsg.ts) {
          replyTo = `slack:${this.teamId}:${channelId}:${effectiveMsg.thread_ts}`;
        }

        // Track threads with replies for Phase 4
        if (effectiveMsg.reply_count && effectiveMsg.reply_count > 0) {
          threadsToFetch.push({ channelId, parentTs: effectiveMsg.ts });
        }

        const metadata: Record<string, unknown> = {
          workspace: this.workspaceId,
        };
        if (effectiveMsg.edited) metadata.edited = true;
        if (effectiveMsg.files?.length) {
          metadata.files = effectiveMsg.files.map(f => ({
            name: f.name, mimetype: f.mimetype,
          }));
        }
        if (effectiveMsg.attachments?.length) {
          metadata.has_attachments = true;
        }
        if (effectiveMsg.reply_count) metadata.reply_count = effectiveMsg.reply_count;
        if (msg.subtype === 'message_changed') metadata.is_edit = true;

        const message: Message = {
          id: `slack:${this.teamId}:${channelId}:${effectiveMsg.ts}`,
          platform: 'slack',
          thread_id: `slack:${this.teamId}:channel:${channelId}`,
          sender_id: senderId,
          content,
          content_type: getContentType(effectiveMsg),
          reply_to: replyTo,
          metadata,
          platform_ts: slackTsToIso(effectiveMsg.ts),
          synced_at: now,
        };
        yield { type: 'message', data: message };
        msgCount++;

        // Track newest ts for cursor
        if (!this.currentCursor.channels[channelId] ||
            effectiveMsg.ts > this.currentCursor.channels[channelId]) {
          this.currentCursor.channels[channelId] = effectiveMsg.ts;
        }
      }

      paginationCursor = result.response_metadata?.next_cursor || undefined;
      pageCount++;

      // Rate limit awareness — pause between pages
      if (paginationCursor) {
        await this.rateLimitDelay();
      }
    } while (paginationCursor);

    if (msgCount > 0) {
      this.log(`[slack:${this.workspaceId}] ${channelId}: ${msgCount} messages (${pageCount} pages)`);
    }
  }

  private async *fetchThreadReplies(
    threads: Array<{ channelId: string; parentTs: string }>,
    now: string,
  ): AsyncGenerator<SyncEvent> {
    let replyCount = 0;

    for (const { channelId, parentTs } of threads) {
      try {
        let cursor: string | undefined;

        do {
          const result = await this.webClient.conversations.replies({
            channel: channelId,
            ts: parentTs,
            limit: 200,
            cursor,
          });

          const messages = (result.messages ?? []) as SlackMessage[];

          for (const msg of messages) {
            // Skip the parent message itself (first in results) — already yielded
            if (msg.ts === parentTs) continue;

            // Skip system subtypes
            if (msg.subtype && SKIP_SUBTYPES.has(msg.subtype)) continue;

            let senderId: string;
            if (msg.user) {
              senderId = `slack:${this.teamId}:user:${msg.user}`;
            } else if (msg.bot_id) {
              senderId = `slack:${this.teamId}:bot:${msg.bot_id}`;
            } else {
              senderId = `slack:${this.teamId}:user:UNKNOWN`;
            }

            const content = extractContent(msg, (id) => this.resolveUserName(id));

            const metadata: Record<string, unknown> = {
              workspace: this.workspaceId,
              is_thread_reply: true,
            };
            if (msg.edited) metadata.edited = true;
            if (msg.files?.length) {
              metadata.files = msg.files.map(f => ({ name: f.name, mimetype: f.mimetype }));
            }

            const message: Message = {
              id: `slack:${this.teamId}:${channelId}:${msg.ts}`,
              platform: 'slack',
              thread_id: `slack:${this.teamId}:channel:${channelId}`,
              sender_id: senderId,
              content,
              content_type: getContentType(msg),
              reply_to: `slack:${this.teamId}:${channelId}:${parentTs}`,
              metadata,
              platform_ts: slackTsToIso(msg.ts),
              synced_at: now,
            };
            yield { type: 'message', data: message };
            replyCount++;
            // NOTE: Do NOT update currentCursor here — reply timestamps can be
            // newer than the latest channel message, which would cause the next
            // incremental sync to skip channel messages between the real latest
            // and the reply timestamp. Only fetchChannelMessages() advances cursors.
          }

          cursor = result.response_metadata?.next_cursor || undefined;
          if (cursor) await this.rateLimitDelay();
        } while (cursor);

        // Rate limit between thread fetches
        await this.rateLimitDelay();
      } catch (err) {
        this.log(`[slack:${this.workspaceId}] Error fetching replies for ${channelId}/${parentTs}: ${err}`);
      }
    }

    if (replyCount > 0) {
      this.log(`[slack:${this.workspaceId}] Fetched ${replyCount} thread replies from ${threads.length} threads`);
    }
  }

  private rateLimitDelay(): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY_MS));
  }

  getCursor(): string | null {
    return JSON.stringify(this.currentCursor);
  }

  async shutdown(): Promise<void> {
    // V1 is poll-based — no persistent connections to close
    this.log(`[slack:${this.workspaceId}] Adapter shut down`);
  }
}
