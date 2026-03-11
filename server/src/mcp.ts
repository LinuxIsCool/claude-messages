import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import path from 'node:path';
import { MessageDB } from './db.js';

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

const dataDir = process.env.MESSAGES_DATA_DIR ?? resolveHome('~/.claude/local/messages');
const db = new MessageDB(path.join(dataDir, 'messages.db'));

const server = new McpServer({
  name: 'claude-messages',
  version: '0.1.0',
});

// Tool: search_messages
server.tool(
  'search_messages',
  'Full-text search across all synced messages',
  { query: z.string().describe('Search query (FTS5 syntax supported)'), limit: z.number().optional().default(30).describe('Max results') },
  async ({ query, limit }) => {
    try {
      const results = db.searchMessages(query, limit);
      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify(results.map(m => ({
            id: m.id,
            thread: m.thread_id,
            sender: m.sender_id,
            content: m.content,
            type: m.content_type,
            time: m.platform_ts,
          })), null, 2),
        }],
      };
    } catch (err) {
      return { content: [{ type: 'text' as const, text: `Search error: ${err}` }] };
    }
  }
);

// Tool: recent_messages
server.tool(
  'recent_messages',
  'Get the most recent messages across all platforms',
  { limit: z.number().optional().default(30).describe('Max results') },
  async ({ limit }) => {
    const results = db.recentMessages(limit);
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(results.map(m => ({
          id: m.id,
          thread: m.thread_id,
          sender: m.sender_id,
          content: m.content,
          type: m.content_type,
          time: m.platform_ts,
        })), null, 2),
      }],
    };
  }
);

// Tool: get_thread
server.tool(
  'get_thread',
  'Get messages from a specific thread/conversation',
  {
    thread_id: z.string().describe('Thread ID (e.g. telegram:chat:12345)'),
    limit: z.number().optional().default(50).describe('Max messages'),
  },
  async ({ thread_id, limit }) => {
    const thread = db.getThread(thread_id);
    const messages = db.getThreadMessages(thread_id, limit);
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify({
          thread: thread ? { id: thread.id, title: thread.title, type: thread.thread_type, platform: thread.platform } : null,
          messages: messages.map(m => ({
            id: m.id,
            sender: m.sender_id,
            content: m.content,
            type: m.content_type,
            time: m.platform_ts,
          })),
        }, null, 2),
      }],
    };
  }
);

// Tool: list_threads
server.tool(
  'list_threads',
  'List conversation threads, optionally filtered by platform',
  {
    platform: z.string().optional().describe('Filter by platform (telegram, signal, email)'),
    limit: z.number().optional().default(30).describe('Max results'),
  },
  async ({ platform, limit }) => {
    const threads = db.listThreads(platform, limit);
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(threads.map(t => ({
          id: t.id,
          title: t.title,
          type: t.thread_type,
          platform: t.platform,
          updated: t.updated_at,
        })), null, 2),
      }],
    };
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
      const link = db.linkContact(targetId, platform, platform_id, confidence, source);
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

// Tool: auto_resolve
server.tool(
  'auto_resolve',
  'Run phone-based cross-platform identity matching — groups contacts sharing the same phone number into unified identities',
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

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
