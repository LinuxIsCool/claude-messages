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

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
