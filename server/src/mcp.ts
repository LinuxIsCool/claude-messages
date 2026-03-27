import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { MessageDB } from './db.js';
import { formatMessages, formatThreadList, formatThread, formatContext, formatTimeline } from './formatters.js';
import type { OutputFormat } from './formatters.js';
import type { Contact, IdentityCard } from './types.js';
import { parseTemporalRef } from './temporal.js';

const execFileAsync = promisify(execFile);

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

const dataDir = process.env.MESSAGES_DATA_DIR ?? resolveHome('~/.claude/local/messages');
const db = new MessageDB(path.join(dataDir, 'messages.db'));

const server = new McpServer({
  name: 'claude-messages',
  version: '0.2.0',
});

const formatSchema = z.enum(['text', 'json', 'compact']).optional().default('text')
  .describe('Output format: text (readable, default), json (structured), compact (one-line)');

// Tool: search_messages
server.tool(
  'search_messages',
  'Full-text search across all synced messages — returns readable text by default',
  {
    query: z.string().describe('Search query (FTS5 syntax supported)'),
    limit: z.number().optional().default(30).describe('Max results'),
    identity_id: z.string().optional().describe('Filter to messages from this identity across all platforms'),
    direction: z.enum(['sent', 'received', 'unknown']).optional().describe('Filter by message direction (sent/received)'),
    dm_only: z.boolean().optional().default(false).describe('Only return DM messages (filter out group chats)'),
    format: formatSchema,
  },
  async ({ query, limit, identity_id, direction, dm_only, format }) => {
    try {
      const results = identity_id
        ? db.searchMessagesByIdentity(identity_id, query, limit, dm_only || undefined)
        : db.searchMessages(query, limit, direction, dm_only || undefined);

      const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
      const names = db.resolveContactNames(senderIds);
      const threadIds = [...new Set(results.map(m => m.thread_id))];
      const threadInfo = db.getThreadInfoBatch(threadIds);

      let text = formatMessages(results, names, threadInfo, {
        format: format as OutputFormat,
        header: `${results.length} results for "${query}"`,
      });

      // Smart suggestions when few results (Phase 3)
      if (results.length < 3 && format !== 'json') {
        const suggestions = db.getSearchSuggestions(query, results);
        if (suggestions.length > 0) {
          text += '\n\nFew results? Try:\n' + suggestions.map(s => `  - "${s}"`).join('\n');
        }
      }

      return { content: [{ type: 'text' as const, text }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Search error: ${err}` }] };
    }
  }
);

// Tool: recent_messages
server.tool(
  'recent_messages',
  'Get the most recent messages across all platforms — returns readable text by default',
  {
    limit: z.number().optional().default(30).describe('Max results'),
    direction: z.enum(['sent', 'received', 'unknown']).optional().describe('Filter by message direction (sent/received)'),
    dm_only: z.boolean().optional().default(false).describe('Only return DM messages (filter out group chats)'),
    format: formatSchema,
  },
  async ({ limit, direction, dm_only, format }) => {
    const results = db.recentMessages(limit, direction, dm_only || undefined);
    const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    const threadIds = [...new Set(results.map(m => m.thread_id))];
    const threadInfo = db.getThreadInfoBatch(threadIds);

    const text = formatMessages(results, names, threadInfo, {
      format: format as OutputFormat,
      header: `${results.length} recent messages`,
    });

    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: get_thread
server.tool(
  'get_thread',
  'Get messages from a specific thread/conversation with sender names and participant info',
  {
    thread_id: z.string().describe('Thread ID (e.g. telegram:chat:12345)'),
    limit: z.number().optional().default(50).describe('Max messages'),
    format: formatSchema,
  },
  async ({ thread_id, limit, format }) => {
    const thread = db.getThread(thread_id);
    const messages = db.getThreadMessages(thread_id, limit);

    const senderIds = [...new Set(messages.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    const participantNames = thread ? db.resolveContactNames(thread.participants) : new Map<string, string>();

    const text = formatThread(thread, messages, names, participantNames, {
      format: format as OutputFormat,
    });

    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: list_threads
server.tool(
  'list_threads',
  'List conversation threads, optionally filtered by platform or identity',
  {
    platform: z.string().optional().describe('Filter by platform (telegram, signal, email)'),
    identity_id: z.string().optional().describe('Filter to threads involving this identity'),
    limit: z.number().optional().default(30).describe('Max results'),
    format: formatSchema,
  },
  async ({ platform, identity_id, limit, format }) => {
    if (identity_id && platform) {
      return { content: [{ type: 'text' as const, text: 'Error: identity_id and platform are mutually exclusive — use one or the other' }] };
    }
    const threads = identity_id
      ? db.getThreadsByIdentity(identity_id, limit)
      : db.listThreads(platform, limit);

    // Fetch cached summaries for all output formats
    const summaries = new Map<string, string>();
    for (const t of threads) {
      const cached = db.getThreadSummary(t.id);
      if (cached) summaries.set(t.id, cached.summary);
    }

    const text = formatThreadList(
      threads.map(t => ({ id: t.id, title: t.title, thread_type: t.thread_type ?? '', platform: t.platform, updated_at: t.updated_at })),
      { format: format as OutputFormat, header: `${threads.length} threads` },
      summaries,
    );

    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: messages_person (Phase 1 — single-call identity-first search)
server.tool(
  'messages_person',
  'Search messages from/to a specific person — resolves identity automatically. One call replaces who_is + search_messages.',
  {
    person: z.string().describe('Name, email, phone, or username'),
    query: z.string().optional().describe('Search within their messages'),
    dm_only: z.boolean().optional().default(true).describe('Only DMs (default true)'),
    limit: z.number().optional().default(20).describe('Max results'),
    format: formatSchema,
  },
  async ({ person, query, dm_only, limit, format }) => {
    try {
      // 1. Resolve person to identity
      const matches = db.whoIs(person, 3);
      if (!matches.length) {
        return { content: [{ type: 'text' as const, text: `No identity or contact found for "${person}"` }] };
      }

      const best = matches[0];

      // Check if it's an IdentityCard (has platforms array) vs raw Contact
      const isIdentity = 'platforms' in best && Array.isArray((best as IdentityCard).platforms);

      if (isIdentity) {
        const card = best as IdentityCard;
        const results = db.searchMessagesByIdentity(card.id, query, limit, dm_only || undefined);
        const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
        const names = db.resolveContactNames(senderIds);
        const threadIds = [...new Set(results.map(m => m.thread_id))];
        const threadInfo = db.getThreadInfoBatch(threadIds);

        const header = query
          ? `${results.length} results for "${query}" from ${card.display_name}`
          : `${results.length} recent messages with ${card.display_name}`;

        const text = formatMessages(results, names, threadInfo, {
          format: format as OutputFormat,
          header,
        });
        return { content: [{ type: 'text' as const, text }] };
      }

      // Unlinked contact — search by sender_id at SQL level
      const contact = best as Contact;
      const results = db.messagesBySender(contact.id, query, limit, dm_only || undefined);

      const names = db.resolveContactNames([contact.id]);
      const threadIds = [...new Set(results.map(m => m.thread_id))];
      const threadInfo = db.getThreadInfoBatch(threadIds);

      const displayName = contact.display_name ?? contact.id;
      const header = query
        ? `${results.length} results for "${query}" from ${displayName}`
        : `${results.length} recent messages with ${displayName}`;

      const text = formatMessages(results, names, threadInfo, {
        format: format as OutputFormat,
        header,
      });
      return { content: [{ type: 'text' as const, text }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: get_message_context (Phase 1 — context window expansion)
server.tool(
  'get_message_context',
  'Get a message with surrounding conversation context from the same thread',
  {
    message_id: z.string().describe('Message ID from a search result'),
    before: z.number().optional().default(5).describe('Messages before'),
    after: z.number().optional().default(5).describe('Messages after'),
    format: formatSchema,
  },
  async ({ message_id, before, after, format }) => {
    const ctx = db.getMessageContext(message_id, before, after);
    if (!ctx) {
      return { content: [{ type: 'text' as const, text: `Message ${message_id} not found` }] };
    }

    const { target, messages, thread } = ctx;
    const senderIds = [...new Set(messages.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);

    const text = formatContext(target, messages, thread, names, {
      format: format as OutputFormat,
    });

    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: thread_messages (Phase 3 — temporal thread navigation)
server.tool(
  'thread_messages',
  'Get messages from a thread with temporal navigation — slice by time range, anchor point, or latest',
  {
    thread_id: z.string().describe('Thread ID'),
    around: z.string().optional().describe('Timestamp or natural date — center window here'),
    after: z.string().optional().describe('Timestamp or natural date — messages after this point'),
    before: z.string().optional().describe('Timestamp or natural date — messages before this point'),
    limit: z.number().optional().default(50).describe('Max messages'),
    format: formatSchema,
  },
  async ({ thread_id, around, after, before, limit, format }) => {
    const parsedAround = around ? parseTemporalRef(around) : undefined;
    const parsedAfter = after ? parseTemporalRef(after) : undefined;
    const parsedBefore = before ? parseTemporalRef(before) : undefined;

    if (around && !parsedAround) return { content: [{ type: 'text' as const, text: `Could not parse date: "${around}"` }] };
    if (after && !parsedAfter) return { content: [{ type: 'text' as const, text: `Could not parse date: "${after}"` }] };
    if (before && !parsedBefore) return { content: [{ type: 'text' as const, text: `Could not parse date: "${before}"` }] };

    const thread = db.getThread(thread_id);
    const messages = db.getThreadMessagesTemporally(thread_id, {
      around: parsedAround,
      after: parsedAfter,
      before: parsedBefore,
      limit,
    });

    const senderIds = [...new Set(messages.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    const participantNames = thread ? db.resolveContactNames(thread.participants) : new Map<string, string>();

    let headerDesc = `${messages.length} messages`;
    if (parsedAround) headerDesc += ` around ${parsedAround.slice(0, 16)}`;
    else if (parsedAfter && parsedBefore) headerDesc += ` between ${parsedAfter.slice(0, 10)} and ${parsedBefore.slice(0, 10)}`;
    else if (parsedAfter) headerDesc += ` after ${parsedAfter.slice(0, 10)}`;
    else if (parsedBefore) headerDesc += ` before ${parsedBefore.slice(0, 10)}`;

    const text = formatThread(thread, messages, names, participantNames, {
      format: format as OutputFormat,
      header: headerDesc,
    });

    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: messages_timeframe (Phase 3 — global temporal search)
server.tool(
  'messages_timeframe',
  'Search messages within a time window — global or filtered by person/query',
  {
    start: z.string().describe('Start date (ISO or natural: "last week", "March 1", "3 days ago")'),
    end: z.string().optional().describe('End date (default: now)'),
    person: z.string().optional().describe('Name to auto-resolve to identity'),
    query: z.string().optional().describe('Search within time window (FTS5)'),
    dm_only: z.boolean().optional().default(false),
    limit: z.number().optional().default(30),
    format: formatSchema,
  },
  async ({ start, end, person, query, dm_only, limit, format }) => {
    const parsedStart = parseTemporalRef(start);
    if (!parsedStart) return { content: [{ type: 'text' as const, text: `Could not parse start date: "${start}"` }] };
    const parsedEnd = end ? parseTemporalRef(end) : undefined;
    if (end && !parsedEnd) return { content: [{ type: 'text' as const, text: `Could not parse end date: "${end}"` }] };

    // Resolve person name to identity_id
    let identityId: string | undefined;
    let personLabel = '';
    if (person) {
      const matches = db.whoIs(person, 1);
      if (matches.length && 'platforms' in matches[0]) {
        const card = matches[0] as IdentityCard;
        identityId = card.id;
        personLabel = card.display_name;
      } else {
        personLabel = person;
      }
    }

    const results = db.searchMessagesInTimeframe({
      start: parsedStart,
      end: parsedEnd ?? undefined,
      query,
      person: identityId,
      dmOnly: dm_only || undefined,
      limit,
    });

    const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    const threadIds = [...new Set(results.map(m => m.thread_id))];
    const threadInfo = db.getThreadInfoBatch(threadIds);

    let header = `${results.length} messages`;
    if (parsedEnd) header += ` between ${parsedStart.slice(0, 10)} and ${parsedEnd.slice(0, 10)}`;
    else header += ` since ${parsedStart.slice(0, 10)}`;
    if (personLabel) header += ` from ${personLabel}`;
    if (query) header += ` matching "${query}"`;

    const text = formatMessages(results, names, threadInfo, {
      format: format as OutputFormat,
      header,
    });
    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: get_thread_summary (Phase 3 — LLM thread summaries)
server.tool(
  'get_thread_summary',
  'Get or generate an LLM summary of a thread — cached, regenerated when stale',
  {
    thread_id: z.string().describe('Thread ID'),
    force: z.boolean().optional().default(false).describe('Force regeneration even if cached'),
  },
  async ({ thread_id, force }) => {
    // Check cache
    if (!force) {
      const cached = db.getThreadSummary(thread_id);
      if (cached && !cached.stale) {
        return { content: [{ type: 'text' as const, text:
          `Summary (${cached.message_count} messages, cached ${cached.generated_at}):\n${cached.summary}`
        }] };
      }
    }

    // Generate: pull last 100 messages, send to summarize.py
    const thread = db.getThread(thread_id);
    const messages = db.getThreadMessages(thread_id, 100);
    if (messages.length === 0) {
      return { content: [{ type: 'text' as const, text: 'Thread has no messages' }] };
    }

    const senderIds = [...new Set(messages.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);

    const input = JSON.stringify({
      messages: messages.reverse().map(m => ({
        sender_id: m.sender_id,
        sender_name: names.get(m.sender_id) ?? m.sender_id,
        content: m.content,
        platform_ts: m.platform_ts,
      })),
    });

    try {
      const currentDir = path.dirname(fileURLToPath(import.meta.url));
      const scriptPath = path.join(currentDir, '..', 'scripts', 'summarize.py');
      const { stdout } = await execFileAsync('uv', ['run', '--script', scriptPath], {
        input,
        env: { ...process.env },
        timeout: 45000,
      });

      const result = JSON.parse(stdout);
      const lastTs = messages[messages.length - 1]?.platform_ts ?? new Date().toISOString();
      db.upsertThreadSummary(thread_id, result.summary, messages.length, lastTs, result.model);

      const threadTitle = thread?.title ?? thread_id;
      return { content: [{ type: 'text' as const, text:
        `=== ${threadTitle} ===\nSummary (${messages.length} messages):\n${result.summary}`
      }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Summary generation failed: ${err}` }] };
    }
  }
);

// Tool: message_timeline (Phase 3 — activity visualization)
server.tool(
  'message_timeline',
  'Show message activity over time — ASCII bar chart by month, optionally filtered by person or thread',
  {
    person: z.string().optional().describe('Person name (auto-resolved to identity)'),
    thread_id: z.string().optional().describe('Thread ID'),
    months: z.number().optional().default(12).describe('How many months back'),
    format: formatSchema,
  },
  async ({ person, thread_id, months, format }) => {
    let identityId: string | undefined;
    let displayName = 'All Messages';

    if (person) {
      const matches = db.whoIs(person, 1);
      if (matches.length && 'platforms' in matches[0]) {
        const card = matches[0] as IdentityCard;
        identityId = card.id;
        displayName = card.display_name;
      } else {
        displayName = person;
      }
    }

    const data = db.getMessageTimeline({
      identityId,
      threadId: thread_id,
      months,
    });

    const text = formatTimeline(data, {
      format: format as OutputFormat,
      header: `${displayName} — Message Timeline (${months} months)`,
    });
    return { content: [{ type: 'text' as const, text }] };
  }
);

// Tool: message_stats
server.tool(
  'message_stats',
  'Get message statistics across all platforms',
  {},
  async () => {
    const stats = db.getStats();
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(stats, null, 2),
      }],
    };
  }
);

// Tool: resolve_contact
server.tool(
  'resolve_contact',
  'Given a platform contact, return their unified identity card (if linked)',
  {
    platform: z.string().describe('Platform (telegram, signal, email)'),
    platform_id: z.string().describe('Platform-specific ID (e.g. user:458825601)'),
  },
  async ({ platform, platform_id }) => {
    const card = db.resolveContact(platform, platform_id);
    if (!card) {
      return { content: [{ type: 'text' as const, text: `No identity found for ${platform}:${platform_id}` }] };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
  }
);

// Tool: who_is
server.tool(
  'who_is',
  'Fuzzy search across all names, phones, emails, usernames — returns identity cards for linked contacts, raw contacts for unlinked',
  {
    query: z.string().describe('Search query (name, phone, email, username)'),
    limit: z.number().optional().default(10).describe('Max results'),
  },
  async ({ query, limit }) => {
    const results = db.whoIs(query, limit);
    return { content: [{ type: 'text' as const, text: JSON.stringify(results, null, 2) }] };
  }
);

// Tool: link_identities
server.tool(
  'link_identities',
  'Link a platform contact to an identity (creates identity if no identity_id provided)',
  {
    identity_id: z.string().optional().describe('Existing identity ID (omit to create new)'),
    display_name: z.string().optional().describe('Display name for new identity (required if no identity_id)'),
    platform: z.string().describe('Platform (telegram, signal, email)'),
    platform_id: z.string().describe('Platform-specific ID'),
    confidence: z.number().optional().default(1.0).describe('Confidence score 0-1'),
    source: z.string().optional().default('manual').describe('How this link was established'),
    notes: z.string().optional().describe('Notes for new identity'),
  },
  async ({ identity_id, display_name, platform, platform_id, confidence, source, notes }) => {
    try {
      let targetId = identity_id;
      if (!targetId) {
        if (!display_name) {
          return { content: [{ type: 'text' as const, text: 'Error: display_name required when creating new identity' }] };
        }
        const identity = db.createIdentity(display_name, notes);
        targetId = identity.id;
      }
      const link = db.linkContact(targetId, platform, platform_id, confidence, source, display_name);
      const card = db.getIdentityCard(targetId);
      return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: unlink_identity
server.tool(
  'unlink_identity',
  'Remove a platform link from an identity',
  {
    identity_id: z.string().describe('Identity ID'),
    platform: z.string().describe('Platform to unlink'),
    platform_id: z.string().describe('Platform-specific ID to unlink'),
  },
  async ({ identity_id, platform, platform_id }) => {
    const removed = db.unlinkContact(identity_id, platform, platform_id);
    if (!removed) {
      return { content: [{ type: 'text' as const, text: 'No matching link found' }] };
    }
    const card = db.getIdentityCard(identity_id);
    return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
  }
);

// Tool: merge_identities
server.tool(
  'merge_identities',
  'Merge two identities — source is absorbed into target, source is deleted',
  {
    source_id: z.string().describe('Identity to absorb (will be deleted)'),
    target_id: z.string().describe('Identity to keep (receives all links)'),
  },
  async ({ source_id, target_id }) => {
    try {
      const card = db.mergeIdentities(source_id, target_id);
      return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: list_identities
server.tool(
  'list_identities',
  'Browse and search unified identities',
  {
    limit: z.number().optional().default(50).describe('Max results'),
    offset: z.number().optional().default(0).describe('Pagination offset'),
    search: z.string().optional().describe('Filter by display name'),
  },
  async ({ limit, offset, search }) => {
    const result = db.listIdentities(limit, offset, search);
    return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
  }
);

// Tool: get_identity
server.tool(
  'get_identity',
  'Get full identity card with all linked platforms, message stats, and events',
  {
    identity_id: z.string().describe('Identity ID'),
  },
  async ({ identity_id }) => {
    const card = db.getIdentityCard(identity_id);
    if (!card) {
      return { content: [{ type: 'text' as const, text: `Identity ${identity_id} not found` }] };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
  }
);

// Tool: unlinked_contacts
server.tool(
  'unlinked_contacts',
  'Audit unlinked contacts sorted by message activity — surfaces contacts most worth linking to identities',
  {
    platform: z.string().optional().describe('Filter by platform (telegram, signal, email)'),
    limit: z.number().optional().default(50).describe('Max results'),
  },
  async ({ platform, limit }) => {
    const contacts = db.getUnlinkedContacts(platform, limit);
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(contacts.map(c => ({
          id: c.id,
          platform: c.platform,
          display_name: c.display_name,
          username: c.username,
          phone: c.phone,
          message_count: c.message_count,
          last_seen: c.last_seen,
        })), null, 2),
      }],
    };
  }
);

// Tool: auto_resolve
server.tool(
  'auto_resolve',
  'Run cross-platform identity matching — phone grouping, single-platform phone, email name match, cross-platform name match, Signal UUID dedup, nickname + fuzzy name matching, identity-to-identity merging',
  {},
  async () => {
    try {
      const report = db.autoResolve();
      return { content: [{ type: 'text' as const, text: JSON.stringify(report, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: identity_health
server.tool(
  'identity_health',
  'Diagnostic view of identity resolution coverage — linked/unlinked counts, top unlinked contacts, orphaned identities',
  {},
  async () => {
    const health = db.getIdentityHealth();
    return { content: [{ type: 'text' as const, text: JSON.stringify(health, null, 2) }] };
  }
);

// Tool: update_identity
server.tool(
  'update_identity',
  'Update an identity\'s display name or notes',
  {
    identity_id: z.string().describe('Identity ID'),
    display_name: z.string().optional().describe('New display name'),
    notes: z.string().optional().describe('New notes'),
  },
  async ({ identity_id, display_name, notes }) => {
    try {
      if (display_name === undefined && notes === undefined) {
        return { content: [{ type: 'text' as const, text: 'Error: at least one of display_name or notes must be provided' }] };
      }
      const updated = db.updateIdentity(identity_id, { display_name, notes });
      const card = db.getIdentityCard(identity_id);
      return { content: [{ type: 'text' as const, text: JSON.stringify(card, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: cleanup_identities
server.tool(
  'cleanup_identities',
  'Remove orphaned identities (zero links remaining)',
  {},
  async () => {
    const result = db.cleanupOrphanedIdentities();
    return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
  }
);

// Tool: identity_relationships
server.tool(
  'identity_relationships',
  'Who talks to whom — shared thread participation between an identity and others',
  {
    identity_id: z.string().describe('Identity ID'),
    limit: z.number().optional().default(20).describe('Max relationships to return'),
  },
  async ({ identity_id, limit }) => {
    const relationships = db.getRelationships(identity_id, limit);
    return { content: [{ type: 'text' as const, text: JSON.stringify(relationships, null, 2) }] };
  }
);

// Tool: merge_suggestions
server.tool(
  'merge_suggestions',
  'Surface ambiguous name matches (first-name-only, same-platform duplicates) as human-reviewable merge candidates',
  {
    limit: z.number().optional().default(30).describe('Max suggestions'),
  },
  async ({ limit }) => {
    const suggestions = db.getMergeSuggestions(limit);
    return { content: [{ type: 'text' as const, text: JSON.stringify(suggestions, null, 2) }] };
  }
);

// Tool: export_identities
server.tool(
  'export_identities',
  'Bulk export all identities with platforms and message stats — for integration with other plugins',
  {},
  async () => {
    const identities = db.exportIdentities();
    return { content: [{ type: 'text' as const, text: JSON.stringify(identities, null, 2) }] };
  }
);

// Tool: relationship_score
server.tool(
  'relationship_score',
  'Get full ContactRank score breakdown for a contact — all 8 scoring factors, composite score, Dunbar layer, and confidence',
  {
    identity_id: z.string().describe('Identity ID'),
  },
  async ({ identity_id }) => {
    const score = db.getContactScore(identity_id);
    if (!score) {
      return { content: [{ type: 'text' as const, text: `No score found for ${identity_id}. Run refresh_scores first.` }] };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(score, null, 2) }] };
  }
);

// Tool: inner_circle
server.tool(
  'inner_circle',
  'View contacts ranked by relationship strength, grouped by Dunbar layer (support_clique/sympathy_group/affinity_group/active_network/acquaintance)',
  {
    layer: z.enum(['support_clique', 'sympathy_group', 'affinity_group', 'active_network', 'acquaintance']).optional().describe('Filter to a specific Dunbar layer'),
    limit: z.number().optional().default(50).describe('Max results'),
  },
  async ({ layer, limit }) => {
    const scores = db.getInnerCircle(layer, limit);
    if (scores.length === 0) {
      return { content: [{ type: 'text' as const, text: 'No scores computed yet. Run refresh_scores first.' }] };
    }

    // Group by layer for display
    if (!layer) {
      const grouped: Record<string, Array<{ display_name: string; composite: number; confidence: number }>> = {};
      for (const s of scores) {
        const g = grouped[s.dunbar_layer] ??= [];
        g.push({ display_name: s.display_name, composite: Math.round(s.composite * 1000) / 1000, confidence: Math.round(s.confidence * 100) / 100 });
      }
      return { content: [{ type: 'text' as const, text: JSON.stringify(grouped, null, 2) }] };
    }

    return { content: [{ type: 'text' as const, text: JSON.stringify(scores.map(s => ({
      display_name: s.display_name,
      composite: Math.round(s.composite * 1000) / 1000,
      confidence: Math.round(s.confidence * 100) / 100,
      frequency: Math.round(s.frequency * 100) / 100,
      recency: Math.round(s.recency * 100) / 100,
      reciprocity: Math.round(s.reciprocity * 100) / 100,
      dm_ratio: Math.round(s.dm_ratio * 100) / 100,
    })), null, 2) }] };
  }
);

// Tool: fading_relationships
server.tool(
  'fading_relationships',
  'Detect contacts going unusually silent — flags when silence exceeds N times their typical interval. Inner circle contacts shown first.',
  {
    threshold: z.number().optional().default(8).describe('Silence ratio threshold (default: 8x typical interval)'),
    min_layer: z.enum(['support_clique', 'sympathy_group', 'affinity_group', 'active_network', 'acquaintance']).optional().describe('Only show contacts at this Dunbar layer or higher'),
  },
  async ({ threshold, min_layer }) => {
    const results = db.getFadingRelationships(threshold, min_layer);
    if (results.length === 0) {
      return { content: [{ type: 'text' as const, text: 'No fading relationships detected at this threshold.' }] };
    }
    return { content: [{ type: 'text' as const, text: JSON.stringify(results, null, 2) }] };
  }
);

// Tool: refresh_scores
server.tool(
  'refresh_scores',
  'Recompute ContactRank scores for all identities. Stores self_identity_id on first call.',
  {
    self_identity_id: z.string().optional().describe('Your own identity ID (stored for future calls)'),
  },
  async ({ self_identity_id }) => {
    try {
      if (self_identity_id) {
        db.setConfig('self_identity_id', self_identity_id);
      }
      const result = db.computeAllScores();
      return { content: [{ type: 'text' as const, text: JSON.stringify({
        status: 'ok',
        identities_scored: result.computed,
        duration_ms: result.duration_ms,
      }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Tool: set_dunbar_override
server.tool(
  'set_dunbar_override',
  'Manually override a contact\'s Dunbar layer — survives re-scoring. Use for contacts with data gaps (e.g. iMessage-only partners).',
  {
    identity_id: z.string().describe('Identity ID'),
    layer: z.enum(['support_clique', 'sympathy_group', 'affinity_group', 'active_network', 'acquaintance']).describe('Target Dunbar layer'),
    reason: z.string().optional().describe('Why this override exists'),
  },
  async ({ identity_id, layer, reason }) => {
    try {
      db.setTierOverride(identity_id, layer, reason);
      const score = db.getContactScore(identity_id);
      return { content: [{ type: 'text' as const, text: JSON.stringify({
        status: 'override_set',
        identity_id,
        layer,
        reason: reason ?? null,
        current_score: score,
      }, null, 2) }] };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Error: ${err}` }] };
    }
  }
);

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
