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
  'Full-text search across all synced messages, optionally filtered by identity',
  {
    query: z.string().describe('Search query (FTS5 syntax supported)'),
    limit: z.number().optional().default(30).describe('Max results'),
    identity_id: z.string().optional().describe('Filter to messages from this identity across all platforms'),
  },
  async ({ query, limit, identity_id }) => {
    try {
      const results = identity_id
        ? db.searchMessagesByIdentity(identity_id, query, limit)
        : db.searchMessages(query, limit);

      const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
      const names = db.resolveContactNames(senderIds);

      return {
        content: [{
          type: 'text' as const,
          text: JSON.stringify(results.map(m => ({
            id: m.id,
            thread: m.thread_id,
            sender: m.sender_id,
            sender_name: names.get(m.sender_id) ?? null,
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
    const senderIds = [...new Set(results.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify(results.map(m => ({
          id: m.id,
          thread: m.thread_id,
          sender: m.sender_id,
          sender_name: names.get(m.sender_id) ?? null,
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
  'Get messages from a specific thread/conversation with sender names and participant info',
  {
    thread_id: z.string().describe('Thread ID (e.g. telegram:chat:12345)'),
    limit: z.number().optional().default(50).describe('Max messages'),
  },
  async ({ thread_id, limit }) => {
    const thread = db.getThread(thread_id);
    const messages = db.getThreadMessages(thread_id, limit);

    const senderIds = [...new Set(messages.map(m => m.sender_id).filter(Boolean))];
    const names = db.resolveContactNames(senderIds);
    const participantNames = thread ? db.resolveContactNames(thread.participants) : new Map<string, string>();

    return {
      content: [{
        type: 'text' as const,
        text: JSON.stringify({
          thread: thread ? {
            id: thread.id,
            title: thread.title,
            type: thread.thread_type,
            platform: thread.platform,
            participants: thread.participants.map(p => ({ id: p, name: participantNames.get(p) ?? null })),
          } : null,
          messages: messages.map(m => ({
            id: m.id,
            sender: m.sender_id,
            sender_name: names.get(m.sender_id) ?? null,
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
  'List conversation threads, optionally filtered by platform or identity',
  {
    platform: z.string().optional().describe('Filter by platform (telegram, signal, email)'),
    identity_id: z.string().optional().describe('Filter to threads involving this identity'),
    limit: z.number().optional().default(30).describe('Max results'),
  },
  async ({ platform, identity_id, limit }) => {
    if (identity_id && platform) {
      return { content: [{ type: 'text' as const, text: 'Error: identity_id and platform are mutually exclusive — use one or the other' }] };
    }
    const threads = identity_id
      ? db.getThreadsByIdentity(identity_id, limit)
      : db.listThreads(platform, limit);
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
