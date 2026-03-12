import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import type { Contact, Thread, Message, Identity, IdentityLink, IdentityEvent, IdentityCard, AutoResolveReport, IdentityHealth, IdentityRelationship, MergeSuggestion, RawMetrics, ScoringContext, ScoringFactor, ScoringConfig, ContactScore, FadingRelationship, DunbarLayer } from './types.js';
import { jaroWinkler, extractFirstName, findBestFuzzyMatch } from './fuzzy.js';
import type { IdentityCandidate } from './fuzzy.js';
// @ts-ignore — esbuild text loader inlines CSV as string
import nicknamesCsv from '../data/nicknames.csv';

export class MessageDB {
  private db: Database.Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this.db.function('jaro_winkler', (a: unknown, b: unknown) =>
      (typeof a === 'string' && typeof b === 'string') ? jaroWinkler(a.toLowerCase(), b.toLowerCase()) : 0
    );
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS contacts (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        display_name TEXT,
        username TEXT,
        phone TEXT,
        metadata TEXT DEFAULT '{}',
        first_seen TEXT NOT NULL,
        last_seen TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS threads (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        title TEXT,
        thread_type TEXT,
        participants TEXT DEFAULT '[]',
        metadata TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        thread_id TEXT,
        sender_id TEXT,
        content TEXT,
        content_type TEXT DEFAULT 'text',
        reply_to TEXT,
        metadata TEXT DEFAULT '{}',
        platform_ts TEXT NOT NULL,
        synced_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS sync_cursors (
        adapter TEXT PRIMARY KEY,
        cursor_value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS extraction_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT,
        message_range TEXT,
        status TEXT DEFAULT 'pending',
        triples_count INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        completed_at TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);
      CREATE INDEX IF NOT EXISTS idx_messages_platform ON messages(platform);
      CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);
      CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(platform_ts);
      CREATE INDEX IF NOT EXISTS idx_contacts_platform ON contacts(platform);
      CREATE INDEX IF NOT EXISTS idx_threads_platform ON threads(platform);

      CREATE TABLE IF NOT EXISTS identities (
        id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        notes TEXT,
        metadata TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS identity_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        identity_id TEXT NOT NULL REFERENCES identities(id) ON DELETE CASCADE,
        platform TEXT NOT NULL,
        platform_id TEXT NOT NULL,
        display_name TEXT,
        username TEXT,
        confidence REAL DEFAULT 1.0,
        source TEXT DEFAULT 'manual',
        metadata TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        UNIQUE(platform, platform_id)
      );

      CREATE TABLE IF NOT EXISTS identity_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL,
        identity_id TEXT NOT NULL,
        details TEXT DEFAULT '{}',
        created_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_identity_links_identity ON identity_links(identity_id);
      CREATE INDEX IF NOT EXISTS idx_identity_links_platform ON identity_links(platform, platform_id);
      CREATE INDEX IF NOT EXISTS idx_identity_events_identity ON identity_events(identity_id);

      CREATE TABLE IF NOT EXISTS nickname_map (
        canonical TEXT NOT NULL,
        nickname TEXT NOT NULL,
        PRIMARY KEY (canonical, nickname)
      );
      CREATE INDEX IF NOT EXISTS idx_nickname_nickname ON nickname_map(nickname);

      CREATE TABLE IF NOT EXISTS backfill_state (
        dialog_id TEXT PRIMARY KEY,
        platform TEXT NOT NULL DEFAULT 'telegram',
        dialog_title TEXT,
        dialog_type TEXT,
        messages_fetched INTEGER DEFAULT 0,
        started_at TEXT,
        completed_at TEXT,
        status TEXT DEFAULT 'pending'
      );

      CREATE TABLE IF NOT EXISTS contact_scores (
        identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
        frequency REAL DEFAULT 0,
        recency REAL DEFAULT 0,
        reciprocity REAL DEFAULT 0,
        channel_diversity REAL DEFAULT 0,
        dm_ratio REAL DEFAULT 0,
        structural REAL DEFAULT 0,
        temporal_regularity REAL DEFAULT 0,
        response_latency REAL DEFAULT 0,
        composite REAL DEFAULT 0,
        dunbar_layer TEXT DEFAULT 'acquaintance',
        confidence REAL DEFAULT 0,
        computed_at TEXT NOT NULL DEFAULT ''
      );

      CREATE TABLE IF NOT EXISTS tier_overrides (
        identity_id TEXT PRIMARY KEY REFERENCES identities(id) ON DELETE CASCADE,
        dunbar_layer TEXT NOT NULL,
        reason TEXT,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);

    this.loadNicknames();

    // FTS5 virtual table (can't use IF NOT EXISTS, so check first)
    const ftsExists = this.db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='messages_fts'"
    ).get();

    if (!ftsExists) {
      this.db.exec(`
        CREATE VIRTUAL TABLE messages_fts USING fts5(
          content,
          content=messages,
          content_rowid=rowid,
          tokenize='porter unicode61'
        );

        CREATE TRIGGER messages_ai AFTER INSERT ON messages BEGIN
          INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;

        CREATE TRIGGER messages_au AFTER UPDATE ON messages BEGIN
          INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
          INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
        END;

        CREATE TRIGGER messages_ad AFTER DELETE ON messages BEGIN
          INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
        END;
      `);
    }
  }

  private loadNicknames(): void {
    const count = (this.db.prepare('SELECT COUNT(*) as c FROM nickname_map').get() as { c: number }).c;
    if (count > 0) return; // already loaded

    const insert = this.db.prepare('INSERT OR IGNORE INTO nickname_map (canonical, nickname) VALUES (?, ?)');
    const txn = this.db.transaction(() => {
      for (const line of nicknamesCsv.split('\n')) {
        const parts = line.trim().toLowerCase().split(',').filter(Boolean);
        if (parts.length < 3) continue;
        // CSV format: canonical,has_nickname,nickname
        if (parts[1] !== 'has_nickname') continue;
        const canonical = parts[0];
        const nickname = parts[2];
        insert.run(canonical, nickname);
        // Self-reference: canonical is also a "nickname" of itself
        insert.run(canonical, canonical);
      }
    });
    txn();
  }

  getNicknameCanonicals(name: string): string[] {
    const rows = this.db.prepare(
      'SELECT DISTINCT canonical FROM nickname_map WHERE nickname = ?'
    ).all(name.toLowerCase()) as { canonical: string }[];
    return rows.map(r => r.canonical);
  }

  getNicknameVariants(canonical: string): string[] {
    const rows = this.db.prepare(
      'SELECT DISTINCT nickname FROM nickname_map WHERE canonical = ?'
    ).all(canonical.toLowerCase()) as { nickname: string }[];
    return rows.map(r => r.nickname);
  }

  // --- WRITES ---

  upsertContact(contact: Contact): void {
    this.db.prepare(`
      INSERT INTO contacts (id, platform, display_name, username, phone, metadata, first_seen, last_seen)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        display_name = COALESCE(excluded.display_name, display_name),
        username = COALESCE(excluded.username, username),
        phone = COALESCE(excluded.phone, phone),
        metadata = excluded.metadata,
        last_seen = excluded.last_seen
    `).run(
      contact.id, contact.platform, contact.display_name, contact.username,
      contact.phone, JSON.stringify(contact.metadata), contact.first_seen, contact.last_seen
    );
  }

  upsertThread(thread: Thread): void {
    this.db.prepare(`
      INSERT INTO threads (id, platform, title, thread_type, participants, metadata, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        title = COALESCE(excluded.title, title),
        thread_type = COALESCE(excluded.thread_type, thread_type),
        participants = excluded.participants,
        metadata = excluded.metadata,
        updated_at = excluded.updated_at
    `).run(
      thread.id, thread.platform, thread.title, thread.thread_type,
      JSON.stringify(thread.participants), JSON.stringify(thread.metadata),
      thread.created_at, thread.updated_at
    );
  }

  insertMessage(msg: Message): boolean {
    // Returns true if actually inserted (not duplicate)
    const result = this.db.prepare(`
      INSERT OR IGNORE INTO messages (id, platform, thread_id, sender_id, content, content_type, reply_to, metadata, platform_ts, synced_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      msg.id, msg.platform, msg.thread_id, msg.sender_id,
      msg.content, msg.content_type, msg.reply_to,
      JSON.stringify(msg.metadata), msg.platform_ts, msg.synced_at
    );
    return result.changes > 0;
  }

  updateCursor(adapter: string, cursorValue: string): void {
    const now = new Date().toISOString();
    this.db.prepare(`
      INSERT INTO sync_cursors (adapter, cursor_value, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(adapter) DO UPDATE SET
        cursor_value = excluded.cursor_value,
        updated_at = excluded.updated_at
    `).run(adapter, cursorValue, now);
  }

  // --- BACKFILL STATE ---

  markBackfillStart(dialogId: string, meta: { platform?: string; title?: string; type?: string }): void {
    const now = new Date().toISOString();
    this.db.prepare(`
      INSERT INTO backfill_state (dialog_id, platform, dialog_title, dialog_type, started_at, status)
      VALUES (?, ?, ?, ?, ?, 'in_progress')
      ON CONFLICT(dialog_id) DO UPDATE SET
        started_at = excluded.started_at,
        status = 'in_progress',
        dialog_title = COALESCE(excluded.dialog_title, dialog_title),
        dialog_type = COALESCE(excluded.dialog_type, dialog_type)
    `).run(dialogId, meta.platform ?? 'telegram', meta.title ?? null, meta.type ?? null, now);
  }

  markBackfillComplete(dialogId: string, messageCount: number): void {
    const now = new Date().toISOString();
    this.db.prepare(`
      UPDATE backfill_state SET
        messages_fetched = ?,
        completed_at = ?,
        status = 'complete'
      WHERE dialog_id = ?
    `).run(messageCount, now, dialogId);
  }

  markBackfillError(dialogId: string, messageCount: number): void {
    this.db.prepare(`
      UPDATE backfill_state SET
        messages_fetched = ?,
        status = 'error'
      WHERE dialog_id = ?
    `).run(messageCount, dialogId);
  }

  getBackfillState(): Array<{ dialog_id: string; platform: string; dialog_title: string | null; dialog_type: string | null; messages_fetched: number; started_at: string | null; completed_at: string | null; status: string }> {
    return this.db.prepare('SELECT * FROM backfill_state ORDER BY completed_at DESC NULLS FIRST').all() as any[];
  }

  isBackfillComplete(dialogId: string): boolean {
    const row = this.db.prepare('SELECT status FROM backfill_state WHERE dialog_id = ?').get(dialogId) as { status: string } | undefined;
    return row?.status === 'complete';
  }

  // --- READS ---

  getCursor(adapter: string): string | null {
    const row = this.db.prepare('SELECT cursor_value FROM sync_cursors WHERE adapter = ?').get(adapter) as { cursor_value: string } | undefined;
    return row?.cursor_value ?? null;
  }

  searchMessages(query: string, limit = 50): Message[] {
    return this.db.prepare(`
      SELECT m.* FROM messages m
      JOIN messages_fts fts ON m.rowid = fts.rowid
      WHERE messages_fts MATCH ?
      ORDER BY rank
      LIMIT ?
    `).all(query, limit).map(this.parseMessage);
  }

  getThread(threadId: string): Thread | null {
    const row = this.db.prepare('SELECT * FROM threads WHERE id = ?').get(threadId);
    return row ? this.parseThread(row as Record<string, unknown>) : null;
  }

  getThreadMessages(threadId: string, limit = 100, offset = 0): Message[] {
    return this.db.prepare(`
      SELECT * FROM messages WHERE thread_id = ?
      ORDER BY platform_ts DESC
      LIMIT ? OFFSET ?
    `).all(threadId, limit, offset).map(this.parseMessage);
  }

  recentMessages(limit = 50): Message[] {
    return this.db.prepare(`
      SELECT * FROM messages ORDER BY platform_ts DESC LIMIT ?
    `).all(limit).map(this.parseMessage);
  }

  listThreads(platform?: string, limit = 50): Thread[] {
    if (platform) {
      return this.db.prepare(`
        SELECT * FROM threads WHERE platform = ? ORDER BY updated_at DESC LIMIT ?
      `).all(platform, limit).map(r => this.parseThread(r as Record<string, unknown>));
    }
    return this.db.prepare(`
      SELECT * FROM threads ORDER BY updated_at DESC LIMIT ?
    `).all(limit).map(r => this.parseThread(r as Record<string, unknown>));
  }

  getContacts(platform?: string): Contact[] {
    if (platform) {
      return this.db.prepare('SELECT * FROM contacts WHERE platform = ? ORDER BY last_seen DESC').all(platform).map(r => this.parseContact(r as Record<string, unknown>));
    }
    return this.db.prepare('SELECT * FROM contacts ORDER BY last_seen DESC').all().map(r => this.parseContact(r as Record<string, unknown>));
  }

  getStats(): { total_messages: number; total_threads: number; total_contacts: number; by_platform: Record<string, number>; date_range: { earliest: string; latest: string } | null } {
    const total_messages = (this.db.prepare('SELECT COUNT(*) as c FROM messages').get() as { c: number }).c;
    const total_threads = (this.db.prepare('SELECT COUNT(*) as c FROM threads').get() as { c: number }).c;
    const total_contacts = (this.db.prepare('SELECT COUNT(*) as c FROM contacts').get() as { c: number }).c;

    const platformRows = this.db.prepare('SELECT platform, COUNT(*) as c FROM messages GROUP BY platform').all() as { platform: string; c: number }[];
    const by_platform: Record<string, number> = {};
    for (const row of platformRows) {
      by_platform[row.platform] = row.c;
    }

    const range = this.db.prepare('SELECT MIN(platform_ts) as earliest, MAX(platform_ts) as latest FROM messages').get() as { earliest: string | null; latest: string | null };
    const date_range = range.earliest ? { earliest: range.earliest, latest: range.latest! } : null;

    return { total_messages, total_threads, total_contacts, by_platform, date_range };
  }

  // --- RELATIONSHIP INTELLIGENCE (Phase 2) ---

  /**
   * Extract raw metrics for ALL identities in batch.
   * selfSenderIds: the sender_id values belonging to the self identity (e.g. ['telegram:user:123', 'signal:uuid:abc'])
   */
  computeRawMetrics(selfSenderIds: string[]): Map<string, RawMetrics> {
    if (selfSenderIds.length === 0) return new Map();

    const selfPlaceholders = selfSenderIds.map(() => '?').join(',');

    // Step 1: Get all identity → sender_id mappings (excluding self)
    const allLinks = this.db.prepare(`
      SELECT identity_id, platform || ':' || platform_id as sender_id, platform
      FROM identity_links WHERE platform != 'phone'
    `).all() as Array<{ identity_id: string; sender_id: string; platform: string }>;

    // Build sender_id → identity_id map, and identity → sender_ids map
    const senderToIdentity = new Map<string, string>();
    const identitySenders = new Map<string, string[]>();
    const identityPlatforms = new Map<string, Set<string>>();
    const selfIdentityId = this.getConfig('self_identity_id');

    for (const link of allLinks) {
      if (link.identity_id === selfIdentityId) continue; // skip self
      senderToIdentity.set(link.sender_id, link.identity_id);
      const senders = identitySenders.get(link.identity_id) ?? [];
      senders.push(link.sender_id);
      identitySenders.set(link.identity_id, senders);
      const platforms = identityPlatforms.get(link.identity_id) ?? new Set();
      platforms.add(link.platform);
      identityPlatforms.set(link.identity_id, platforms);
    }

    // Step 2: Batch query — per-identity message counts with sent/received and DM/group split
    // Group participant counts for denoising
    const groupParticipants = new Map<string, number>();
    const gpRows = this.db.prepare(`
      SELECT thread_id, COUNT(DISTINCT sender_id) as n
      FROM messages
      WHERE thread_id IN (SELECT id FROM threads WHERE thread_type != 'dm')
      GROUP BY thread_id
    `).all() as Array<{ thread_id: string; n: number }>;
    for (const r of gpRows) groupParticipants.set(r.thread_id, r.n);

    // Step 3: Get thread types
    const threadTypes = new Map<string, string>();
    const ttRows = this.db.prepare('SELECT id, thread_type FROM threads').all() as Array<{ id: string; thread_type: string }>;
    for (const r of ttRows) threadTypes.set(r.id, r.thread_type);

    // Step 4: For each identity, compute metrics
    const metrics = new Map<string, RawMetrics>();

    for (const [identityId, senders] of identitySenders) {
      const placeholders = senders.map(() => '?').join(',');

      // Messages sent BY this identity
      const sentRows = this.db.prepare(`
        SELECT thread_id, COUNT(*) as cnt, MAX(platform_ts) as last_ts
        FROM messages WHERE sender_id IN (${placeholders})
        GROUP BY thread_id
      `).all(...senders) as Array<{ thread_id: string; cnt: number; last_ts: string }>;

      // Messages sent TO this identity (in threads where they participate, sent by self)
      const receivedRows = this.db.prepare(`
        SELECT thread_id, COUNT(*) as cnt
        FROM messages
        WHERE sender_id IN (${selfPlaceholders})
          AND thread_id IN (
            SELECT DISTINCT thread_id FROM messages WHERE sender_id IN (${placeholders})
          )
        GROUP BY thread_id
      `).all(...selfSenderIds, ...senders) as Array<{ thread_id: string; cnt: number }>;

      let totalSent = 0, totalReceived = 0, dmMessages = 0, groupMessages = 0;
      let lastTs: string | null = null;
      const sharedGroupIds = new Set<string>();

      for (const r of sentRows) {
        const type = threadTypes.get(r.thread_id);
        if (type === 'dm') {
          dmMessages += r.cnt;
          totalSent += r.cnt;
        } else {
          // Group denoising: weight = 1/N
          const n = groupParticipants.get(r.thread_id) ?? 1;
          const weighted = r.cnt / n;
          groupMessages += weighted;
          totalSent += weighted;
          sharedGroupIds.add(r.thread_id);
        }
        if (!lastTs || r.last_ts > lastTs) lastTs = r.last_ts;
      }

      for (const r of receivedRows) {
        const type = threadTypes.get(r.thread_id);
        if (type === 'dm') {
          totalReceived += r.cnt;
        } else {
          const n = groupParticipants.get(r.thread_id) ?? 1;
          totalReceived += r.cnt / n;
        }
      }

      const totalMessages = totalSent + totalReceived;
      if (totalMessages < 1) continue; // skip zero-activity identities

      // Step 5: Message timestamps for temporal analysis
      const tsRows = this.db.prepare(`
        SELECT platform_ts FROM messages
        WHERE sender_id IN (${placeholders})
           OR (sender_id IN (${selfPlaceholders}) AND thread_id IN (
             SELECT DISTINCT thread_id FROM messages WHERE sender_id IN (${placeholders})
             INTERSECT SELECT id FROM threads WHERE thread_type = 'dm'
           ))
        ORDER BY platform_ts
      `).all(...senders, ...selfSenderIds, ...senders) as Array<{ platform_ts: string }>;

      // Step 6: Response latency in DM threads
      // Use sender alternation: when self sends after other or other sends after self
      const latencies: number[] = [];
      const dmThreadIds = this.db.prepare(`
        SELECT DISTINCT thread_id FROM messages
        WHERE sender_id IN (${placeholders})
          AND thread_id IN (SELECT id FROM threads WHERE thread_type = 'dm')
      `).all(...senders) as Array<{ thread_id: string }>;

      for (const { thread_id } of dmThreadIds) {
        const turns = this.db.prepare(`
          SELECT sender_id, platform_ts FROM messages
          WHERE thread_id = ? AND (sender_id IN (${placeholders}) OR sender_id IN (${selfPlaceholders}))
          ORDER BY platform_ts
        `).all(thread_id, ...senders, ...selfSenderIds) as Array<{ sender_id: string; platform_ts: string }>;

        const selfSet = new Set(selfSenderIds);
        for (let i = 1; i < turns.length; i++) {
          const prev = turns[i - 1];
          const curr = turns[i];
          const prevIsSelf = selfSet.has(prev.sender_id);
          const currIsSelf = selfSet.has(curr.sender_id);
          // Only measure when sender switches (a response)
          if (prevIsSelf !== currIsSelf) {
            const dt = new Date(curr.platform_ts).getTime() - new Date(prev.platform_ts).getTime();
            if (dt > 0 && dt < 7 * 24 * 60 * 60 * 1000) { // cap at 7 days
              latencies.push(dt);
            }
          }
        }
      }

      metrics.set(identityId, {
        identity_id: identityId,
        total_messages: totalMessages,
        sent: totalSent,
        received: totalReceived,
        dm_messages: dmMessages,
        group_messages: groupMessages,
        platforms: [...(identityPlatforms.get(identityId) ?? [])],
        shared_groups: sharedGroupIds.size,
        last_message_ts: lastTs,
        message_timestamps: tsRows.map(r => r.platform_ts),
        response_latencies_ms: latencies,
      });
    }

    return metrics;
  }

  // --- SCORING FUNCTIONS ---

  private static defaultScoringFactors(): ScoringFactor[] {
    return [
      {
        name: 'recency',
        weight: 0.23,
        compute: (m: RawMetrics) => {
          if (!m.last_message_ts) return 0;
          const daysSince = (Date.now() - new Date(m.last_message_ts).getTime()) / (1000 * 60 * 60 * 24);
          return Math.exp(-Math.LN2 / 30 * daysSince); // half-life 30 days
        },
      },
      {
        name: 'frequency',
        weight: 0.15,
        compute: (m: RawMetrics, ctx: ScoringContext) => {
          if (ctx.maxMessages <= 1) return 0;
          return Math.log(1 + m.total_messages) / Math.log(1 + ctx.maxMessages);
        },
      },
      {
        name: 'reciprocity',
        weight: 0.18,
        compute: (m: RawMetrics) => {
          if (m.sent === 0 || m.received === 0) return 0;
          return Math.min(m.sent, m.received) / Math.max(m.sent, m.received);
        },
      },
      {
        name: 'dm_ratio',
        weight: 0.15,
        compute: (m: RawMetrics) => {
          const total = m.dm_messages + m.group_messages;
          if (total === 0) return 0;
          return m.dm_messages / total;
        },
      },
      {
        name: 'channel_diversity',
        weight: 0.10,
        compute: (m: RawMetrics) => {
          const n = m.platforms.length;
          if (n >= 3) return 1.0;
          if (n === 2) return 0.67;
          return 0.33;
        },
      },
      {
        name: 'temporal_regularity',
        weight: 0.01,
        compute: (m: RawMetrics) => {
          if (m.message_timestamps.length < 3) return 0;
          const times = m.message_timestamps.map(t => new Date(t).getTime());
          const intervals: number[] = [];
          for (let i = 1; i < times.length; i++) intervals.push(times[i] - times[i - 1]);
          const mean = intervals.reduce((a, b) => a + b, 0) / intervals.length;
          if (mean === 0) return 0;
          const variance = intervals.reduce((sum, v) => sum + (v - mean) ** 2, 0) / intervals.length;
          const cv = Math.sqrt(variance) / mean; // coefficient of variation
          return 1 / (1 + cv); // gentler decay — moderately regular contacts get partial credit
        },
      },
      {
        name: 'response_latency',
        weight: 0.13,
        compute: (m: RawMetrics, ctx: ScoringContext) => {
          if (m.response_latencies_ms.length === 0) return 0;
          const sorted = [...m.response_latencies_ms].sort((a, b) => a - b);
          const median = sorted[Math.floor(sorted.length / 2)];
          if (ctx.maxMedianLatency <= 0) return 0;
          return Math.max(0, 1 - median / ctx.maxMedianLatency); // fast response = high score
        },
      },
      {
        name: 'structural',
        weight: 0.05,
        compute: (m: RawMetrics, ctx: ScoringContext) => {
          if (ctx.maxSharedGroups <= 0) return 0;
          return m.shared_groups / ctx.maxSharedGroups;
        },
      },
    ];
  }

  private static defaultScoringConfig(): ScoringConfig {
    const factors = MessageDB.defaultScoringFactors();
    return {
      id: 'v1-weighted-sum',
      factors,
      aggregate: (scores, facs) => {
        let total = 0;
        for (const f of facs) total += (scores[f.name] ?? 0) * f.weight;
        return total;
      },
    };
  }

  private static computeConfidence(m: RawMetrics): number {
    let score = 0;
    if (m.total_messages >= 10) score += 0.3;
    else if (m.total_messages >= 3) score += 0.15;
    if (m.platforms.length >= 2) score += 0.2;
    if (m.message_timestamps.length >= 10) score += 0.2;
    if (m.response_latencies_ms.length >= 5) score += 0.15;
    if (m.sent > 0 && m.received > 0) score += 0.15;
    return Math.min(1, score);
  }

  // --- DUNBAR LAYER DETECTION ---

  /**
   * Jenks natural breaks — 1D clustering that minimizes within-class variance.
   * Returns k-1 breakpoints. Values below breaks[0] → class 0, etc.
   */
  static jenksBreaks(values: number[], k: number): number[] {
    const sorted = [...values].sort((a, b) => a - b);
    const n = sorted.length;
    if (n <= k) return sorted.slice(0, -1); // degenerate: each value is its own class

    // Fisher-Jenks algorithm using dynamic programming
    const lowerClassLimits = Array.from({ length: n + 1 }, () => new Float64Array(k + 1));
    const varianceCombinations = Array.from({ length: n + 1 }, () => {
      const arr = new Float64Array(k + 1);
      arr.fill(Infinity);
      return arr;
    });

    for (let i = 1; i <= k; i++) {
      lowerClassLimits[1][i] = 1;
      varianceCombinations[1][i] = 0;
    }

    for (let l = 2; l <= n; l++) {
      let sum = 0, sumSq = 0;
      for (let m = 1; m <= l; m++) {
        const idx = l - m; // 0-based index into sorted
        const val = sorted[idx];
        sum += val;
        sumSq += val * val;
        const variance = sumSq - (sum * sum) / m;
        if (idx > 0) {
          for (let j = 2; j <= k; j++) {
            const candidate = variance + varianceCombinations[l - m][j - 1];
            if (candidate < varianceCombinations[l][j]) {
              lowerClassLimits[l][j] = l - m + 1;
              varianceCombinations[l][j] = candidate;
            }
          }
        }
      }
      lowerClassLimits[l][1] = 1;
      varianceCombinations[l][1] = sumSq - (sum * sum) / l;
    }

    // Extract breakpoints
    const breaks: number[] = [];
    let kk = n;
    for (let j = k; j >= 2; j--) {
      const idx = lowerClassLimits[kk][j] - 1; // 0-based
      if (idx >= 0 && idx < sorted.length) {
        breaks.unshift(sorted[idx]);
      }
      kk = lowerClassLimits[kk][j] - 1;
    }

    return breaks;
  }

  private static assignLayer(composite: number, breaks: number[]): DunbarLayer {
    const layers: DunbarLayer[] = ['acquaintance', 'active_network', 'affinity_group', 'sympathy_group', 'support_clique'];
    if (breaks.length < 4) {
      // Fallback to percentile binning
      // Can't classify well, default to acquaintance
      return 'acquaintance';
    }
    for (let i = breaks.length - 1; i >= 0; i--) {
      if (composite >= breaks[i]) return layers[Math.min(i + 1, layers.length - 1)];
    }
    return 'acquaintance';
  }

  // --- SILENCE DETECTION ---

  getFadingRelationships(threshold = 8, minLayer?: DunbarLayer): FadingRelationship[] {
    const layerOrder: DunbarLayer[] = ['support_clique', 'sympathy_group', 'affinity_group', 'active_network', 'acquaintance'];
    const minLayerIdx = minLayer ? layerOrder.indexOf(minLayer) : layerOrder.length - 1;

    const rows = this.db.prepare(`
      SELECT cs.identity_id, i.display_name, cs.dunbar_layer, cs.composite
      FROM contact_scores cs
      JOIN identities i ON i.id = cs.identity_id
      WHERE cs.composite > 0
      ORDER BY cs.composite DESC
    `).all() as Array<{ identity_id: string; display_name: string; dunbar_layer: string; composite: number }>;

    const results: FadingRelationship[] = [];

    for (const row of rows) {
      const layer = row.dunbar_layer as DunbarLayer;
      const layerIdx = layerOrder.indexOf(layer);
      if (layerIdx > minLayerIdx) continue;

      // Get sender IDs for this identity
      const senders = this.db.prepare(
        "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
      ).all(row.identity_id) as Array<{ sender_id: string }>;
      if (senders.length === 0) continue;

      const placeholders = senders.map(() => '?').join(',');
      const senderIds = senders.map(s => s.sender_id);

      // Get message timestamps — both sides of the conversation in DM threads
      const selfId = this.getConfig('self_identity_id');
      const selfLinks = selfId ? this.db.prepare(
        "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
      ).all(selfId) as Array<{ sender_id: string }> : [];
      const selfSenderIds = selfLinks.map(s => s.sender_id);
      const selfPlaceholders = selfSenderIds.map(() => '?').join(',');

      const tsRows = this.db.prepare(`
        SELECT platform_ts FROM messages
        WHERE sender_id IN (${placeholders})
        ${selfSenderIds.length > 0 ? `
          OR (sender_id IN (${selfPlaceholders}) AND thread_id IN (
            SELECT DISTINCT thread_id FROM messages WHERE sender_id IN (${placeholders})
            INTERSECT SELECT id FROM threads WHERE thread_type = 'dm'
          ))
        ` : ''}
        ORDER BY platform_ts
      `).all(...senderIds, ...(selfSenderIds.length > 0 ? [...selfSenderIds, ...senderIds] : [])) as Array<{ platform_ts: string }>;

      if (tsRows.length < 3) continue;

      const times = tsRows.map(r => new Date(r.platform_ts).getTime());
      const intervals: number[] = [];
      for (let i = 1; i < times.length; i++) intervals.push(times[i] - times[i - 1]);

      // Median interval
      const sorted = [...intervals].sort((a, b) => a - b);
      const medianMs = sorted[Math.floor(sorted.length / 2)];
      const medianDays = medianMs / (1000 * 60 * 60 * 24);
      if (medianDays < 0.1) continue; // less than ~2.4 hours typical interval — too noisy

      const lastTs = tsRows[tsRows.length - 1].platform_ts;
      const daysSinceLast = (Date.now() - new Date(lastTs).getTime()) / (1000 * 60 * 60 * 24);
      const silenceRatio = daysSinceLast / medianDays;

      if (silenceRatio >= threshold) {
        results.push({
          identity_id: row.identity_id,
          display_name: row.display_name,
          dunbar_layer: layer,
          median_interval_days: Math.round(medianDays * 10) / 10,
          days_since_last: Math.round(daysSinceLast * 10) / 10,
          silence_ratio: Math.round(silenceRatio * 10) / 10,
          last_message_ts: lastTs,
          total_messages: tsRows.length,
        });
      }
    }

    // Sort: inner circle first, then by silence_ratio descending
    results.sort((a, b) => {
      const aIdx = layerOrder.indexOf(a.dunbar_layer);
      const bIdx = layerOrder.indexOf(b.dunbar_layer);
      if (aIdx !== bIdx) return aIdx - bIdx;
      return b.silence_ratio - a.silence_ratio;
    });

    return results;
  }

  // --- BATCH SCORING ORCHESTRATOR ---

  computeAllScores(config?: ScoringConfig): { computed: number; duration_ms: number } {
    const start = Date.now();
    const selfId = this.getConfig('self_identity_id');
    if (!selfId) throw new Error('self_identity_id not set in config. Call setConfig("self_identity_id", "<id>") first.');

    // Get self sender IDs
    const selfLinks = this.db.prepare(
      "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
    ).all(selfId) as Array<{ sender_id: string }>;
    const selfSenderIds = selfLinks.map(l => l.sender_id);
    if (selfSenderIds.length === 0) throw new Error('Self identity has no platform links');

    // Get display names
    const nameMap = new Map<string, string>();
    const nameRows = this.db.prepare('SELECT id, display_name FROM identities').all() as Array<{ id: string; display_name: string }>;
    for (const r of nameRows) nameMap.set(r.id, r.display_name);

    // Compute raw metrics
    const rawMetrics = this.computeRawMetrics(selfSenderIds);

    // Build scoring context from dataset maximums
    const scoringConfig = config ?? MessageDB.defaultScoringConfig();
    let maxMessages = 0, maxSharedGroups = 0;
    const medianLatencies: number[] = [];

    for (const m of rawMetrics.values()) {
      if (m.total_messages > maxMessages) maxMessages = m.total_messages;
      if (m.shared_groups > maxSharedGroups) maxSharedGroups = m.shared_groups;
      if (m.response_latencies_ms.length > 0) {
        const sorted = [...m.response_latencies_ms].sort((a, b) => a - b);
        medianLatencies.push(sorted[Math.floor(sorted.length / 2)]);
      }
    }

    const ctx: ScoringContext = {
      maxMessages,
      maxSharedGroups,
      maxMedianLatency: medianLatencies.length > 0 ? Math.max(...medianLatencies) : 1,
    };

    // Score each identity
    const scores: ContactScore[] = [];
    const now = new Date().toISOString();

    for (const [identityId, m] of rawMetrics) {
      const factorScores: Record<string, number> = {};
      for (const factor of scoringConfig.factors) {
        factorScores[factor.name] = factor.compute(m, ctx);
      }
      const composite = scoringConfig.aggregate(factorScores, scoringConfig.factors);
      const confidence = MessageDB.computeConfidence(m);

      scores.push({
        identity_id: identityId,
        display_name: nameMap.get(identityId) ?? identityId,
        frequency: factorScores['frequency'] ?? 0,
        recency: factorScores['recency'] ?? 0,
        reciprocity: factorScores['reciprocity'] ?? 0,
        channel_diversity: factorScores['channel_diversity'] ?? 0,
        dm_ratio: factorScores['dm_ratio'] ?? 0,
        structural: factorScores['structural'] ?? 0,
        temporal_regularity: factorScores['temporal_regularity'] ?? 0,
        response_latency: factorScores['response_latency'] ?? 0,
        composite,
        dunbar_layer: 'acquaintance', // assigned next
        confidence,
        computed_at: now,
      });
    }

    // Assign Dunbar layers via Jenks natural breaks
    if (scores.length >= 5) {
      const composites = scores.map(s => s.composite);
      const rawBreaks = MessageDB.jenksBreaks(composites, 5);
      // Deduplicate breakpoints — duplicate values collapse layers
      const breaks = [...new Set(rawBreaks)].sort((a, b) => a - b);
      if (breaks.length >= 4) {
        for (const s of scores) {
          s.dunbar_layer = MessageDB.assignLayer(s.composite, breaks);
        }
      } else {
        // Fallback: percentile binning (ascending order)
        const sorted = [...composites].sort((a, b) => a - b);
        const pcts = [0.25, 0.5, 0.75, 0.9];
        const fallbackBreaks = pcts.map(p => sorted[Math.floor(sorted.length * p)]);
        for (const s of scores) {
          s.dunbar_layer = MessageDB.assignLayer(s.composite, fallbackBreaks);
        }
      }
    }

    // Apply tier overrides
    const overrides = this.getTierOverrides();
    for (const s of scores) {
      const override = overrides.get(s.identity_id);
      if (override) s.dunbar_layer = override;
    }

    // Write to DB in a single transaction
    const upsert = this.db.prepare(`
      INSERT OR REPLACE INTO contact_scores
        (identity_id, frequency, recency, reciprocity, channel_diversity, dm_ratio,
         structural, temporal_regularity, response_latency, composite, dunbar_layer, confidence, computed_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    this.db.transaction(() => {
      for (const s of scores) {
        upsert.run(
          s.identity_id, s.frequency, s.recency, s.reciprocity, s.channel_diversity,
          s.dm_ratio, s.structural, s.temporal_regularity, s.response_latency,
          s.composite, s.dunbar_layer, s.confidence, s.computed_at,
        );
      }
    })();

    return { computed: scores.length, duration_ms: Date.now() - start };
  }

  // --- SCORE QUERIES ---

  getContactScore(identityId: string): ContactScore | null {
    const row = this.db.prepare(`
      SELECT cs.*, i.display_name
      FROM contact_scores cs
      JOIN identities i ON i.id = cs.identity_id
      WHERE cs.identity_id = ?
    `).get(identityId) as (Record<string, unknown>) | undefined;
    if (!row) return null;
    return {
      identity_id: row.identity_id as string,
      display_name: row.display_name as string,
      frequency: row.frequency as number,
      recency: row.recency as number,
      reciprocity: row.reciprocity as number,
      channel_diversity: row.channel_diversity as number,
      dm_ratio: row.dm_ratio as number,
      structural: row.structural as number,
      temporal_regularity: row.temporal_regularity as number,
      response_latency: row.response_latency as number,
      composite: row.composite as number,
      dunbar_layer: row.dunbar_layer as DunbarLayer,
      confidence: row.confidence as number,
      computed_at: row.computed_at as string,
    };
  }

  getInnerCircle(layer?: DunbarLayer, limit = 50): ContactScore[] {
    let sql: string;
    let params: unknown[];

    if (layer) {
      sql = `
        SELECT cs.*, i.display_name FROM contact_scores cs
        JOIN identities i ON i.id = cs.identity_id
        WHERE cs.dunbar_layer = ?
        ORDER BY cs.composite DESC LIMIT ?
      `;
      params = [layer, limit];
    } else {
      sql = `
        SELECT cs.*, i.display_name FROM contact_scores cs
        JOIN identities i ON i.id = cs.identity_id
        ORDER BY cs.composite DESC LIMIT ?
      `;
      params = [limit];
    }

    const rows = this.db.prepare(sql).all(...params) as Array<Record<string, unknown>>;
    return rows.map(row => ({
      identity_id: row.identity_id as string,
      display_name: row.display_name as string,
      frequency: row.frequency as number,
      recency: row.recency as number,
      reciprocity: row.reciprocity as number,
      channel_diversity: row.channel_diversity as number,
      dm_ratio: row.dm_ratio as number,
      structural: row.structural as number,
      temporal_regularity: row.temporal_regularity as number,
      response_latency: row.response_latency as number,
      composite: row.composite as number,
      dunbar_layer: row.dunbar_layer as DunbarLayer,
      confidence: row.confidence as number,
      computed_at: row.computed_at as string,
    }));
  }

  // --- CONFIG ---

  getConfig(key: string): string | null {
    const row = this.db.prepare('SELECT value FROM config WHERE key = ?').get(key) as { value: string } | undefined;
    return row?.value ?? null;
  }

  setConfig(key: string, value: string): void {
    this.db.prepare('INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)').run(key, value);
  }

  // --- TIER OVERRIDES ---

  setTierOverride(identityId: string, layer: DunbarLayer, reason?: string): void {
    this.db.prepare(`
      INSERT OR REPLACE INTO tier_overrides (identity_id, dunbar_layer, reason, created_at)
      VALUES (?, ?, ?, ?)
    `).run(identityId, layer, reason ?? null, new Date().toISOString());
    // Also update contact_scores if it exists
    this.db.prepare('UPDATE contact_scores SET dunbar_layer = ? WHERE identity_id = ?').run(layer, identityId);
  }

  getTierOverrides(): Map<string, DunbarLayer> {
    const rows = this.db.prepare('SELECT identity_id, dunbar_layer FROM tier_overrides').all() as Array<{ identity_id: string; dunbar_layer: string }>;
    return new Map(rows.map(r => [r.identity_id, r.dunbar_layer as DunbarLayer]));
  }

  close(): void {
    this.db.close();
  }

  // --- PARSERS ---

  private parseMessage = (row: unknown): Message => {
    const r = row as Record<string, unknown>;
    return {
      id: r.id as string,
      platform: r.platform as string,
      thread_id: r.thread_id as string,
      sender_id: r.sender_id as string,
      content: r.content as string | null,
      content_type: (r.content_type as Message['content_type']) ?? 'text',
      reply_to: r.reply_to as string | null,
      metadata: JSON.parse((r.metadata as string) || '{}'),
      platform_ts: r.platform_ts as string,
      synced_at: r.synced_at as string,
    };
  };

  private parseThread(row: Record<string, unknown>): Thread {
    return {
      id: row.id as string,
      platform: row.platform as string,
      title: row.title as string | null,
      thread_type: (row.thread_type as Thread['thread_type']) ?? 'dm',
      participants: JSON.parse((row.participants as string) || '[]'),
      metadata: JSON.parse((row.metadata as string) || '{}'),
      created_at: row.created_at as string,
      updated_at: row.updated_at as string,
    };
  }

  private parseContact(row: Record<string, unknown>): Contact {
    return {
      id: row.id as string,
      platform: row.platform as string,
      display_name: row.display_name as string | null,
      username: row.username as string | null,
      phone: row.phone as string | null,
      metadata: JSON.parse((row.metadata as string) || '{}'),
      first_seen: row.first_seen as string,
      last_seen: row.last_seen as string,
    };
  }

  private parseIdentity(row: Record<string, unknown>): Identity {
    return {
      id: row.id as string,
      display_name: row.display_name as string,
      notes: row.notes as string | null,
      metadata: JSON.parse((row.metadata as string) || '{}'),
      created_at: row.created_at as string,
      updated_at: row.updated_at as string,
    };
  }

  private parseIdentityLink(row: Record<string, unknown>): IdentityLink {
    return {
      id: row.id as number,
      identity_id: row.identity_id as string,
      platform: row.platform as string,
      platform_id: row.platform_id as string,
      display_name: row.display_name as string | null,
      username: row.username as string | null,
      confidence: row.confidence as number,
      source: row.source as string,
      metadata: JSON.parse((row.metadata as string) || '{}'),
      created_at: row.created_at as string,
    };
  }

  private parseIdentityEvent(row: Record<string, unknown>): IdentityEvent {
    return {
      id: row.id as number,
      event_type: row.event_type as string,
      identity_id: row.identity_id as string,
      details: JSON.parse((row.details as string) || '{}'),
      created_at: row.created_at as string,
    };
  }

  // --- Identity Helpers ---

  private normalizePhone(phone: string): string {
    const digits = phone.replace(/\D/g, '');
    return '+' + digits;
  }

  private logIdentityEvent(identityId: string, eventType: string, details: Record<string, unknown>): void {
    this.db.prepare(`
      INSERT INTO identity_events (event_type, identity_id, details, created_at)
      VALUES (?, ?, ?, ?)
    `).run(eventType, identityId, JSON.stringify(details), new Date().toISOString());
  }

  private touchIdentity(identityId: string): void {
    this.db.prepare('UPDATE identities SET updated_at = ? WHERE id = ?')
      .run(new Date().toISOString(), identityId);
  }

  private findExistingIdentityForContacts(contacts: Array<{ platform: string; platform_id: string }>): string | null {
    for (const c of contacts) {
      const row = this.db.prepare(
        'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
      ).get(c.platform, c.platform_id) as { identity_id: string } | undefined;
      if (row) return row.identity_id;
    }
    return null;
  }

  // --- Identity Public Methods ---

  createIdentity(displayName: string, notes?: string): Identity {
    const now = new Date().toISOString();
    const id = crypto.randomUUID();
    this.db.prepare(`
      INSERT INTO identities (id, display_name, notes, metadata, created_at, updated_at)
      VALUES (?, ?, ?, '{}', ?, ?)
    `).run(id, displayName, notes ?? null, now, now);
    this.logIdentityEvent(id, 'created', { display_name: displayName });
    return { id, display_name: displayName, notes: notes ?? null, metadata: {}, created_at: now, updated_at: now };
  }

  linkContact(identityId: string, platform: string, platformId: string, confidence = 1.0, source = 'manual', displayName?: string, username?: string): IdentityLink {
    const now = new Date().toISOString();
    this.db.prepare(`
      INSERT INTO identity_links (identity_id, platform, platform_id, display_name, username, confidence, source, metadata, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, '{}', ?)
      ON CONFLICT(platform, platform_id) DO UPDATE SET
        identity_id = excluded.identity_id,
        display_name = COALESCE(excluded.display_name, display_name),
        username = COALESCE(excluded.username, username),
        confidence = excluded.confidence,
        source = excluded.source
    `).run(identityId, platform, platformId, displayName ?? null, username ?? null, confidence, source, now);
    this.touchIdentity(identityId);
    this.logIdentityEvent(identityId, 'linked', { platform, platform_id: platformId, confidence, source });
    const row = this.db.prepare(
      'SELECT * FROM identity_links WHERE platform = ? AND platform_id = ?'
    ).get(platform, platformId) as Record<string, unknown>;
    return this.parseIdentityLink(row);
  }

  unlinkContact(identityId: string, platform: string, platformId: string): boolean {
    const result = this.db.prepare(
      'DELETE FROM identity_links WHERE identity_id = ? AND platform = ? AND platform_id = ?'
    ).run(identityId, platform, platformId);
    if (result.changes > 0) {
      this.touchIdentity(identityId);
      this.logIdentityEvent(identityId, 'unlinked', { platform, platform_id: platformId });
    }
    return result.changes > 0;
  }

  resolveContact(platform: string, platformId: string): IdentityCard | null {
    const row = this.db.prepare(
      'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
    ).get(platform, platformId) as { identity_id: string } | undefined;
    if (!row) return null;
    return this.getIdentityCard(row.identity_id);
  }

  getIdentityCard(identityId: string): IdentityCard | null {
    const identity = this.db.prepare('SELECT * FROM identities WHERE id = ?').get(identityId) as Record<string, unknown> | undefined;
    if (!identity) return null;
    const parsed = this.parseIdentity(identity);

    const links = this.db.prepare('SELECT * FROM identity_links WHERE identity_id = ?')
      .all(identityId) as Record<string, unknown>[];

    const platforms = links.map(l => {
      const link = this.parseIdentityLink(l);
      return {
        platform: link.platform,
        platform_id: link.platform_id,
        display_name: link.display_name,
        username: link.username,
        confidence: link.confidence,
        source: link.source,
        contact_id: `${link.platform}:${link.platform_id}`,
      };
    });

    // Message stats across all linked sender_ids
    const senderIds = platforms
      .filter(p => p.platform !== 'phone')
      .map(p => p.contact_id);

    let totalMessages = 0;
    let firstSeen: string | null = null;
    let lastSeen: string | null = null;
    const activePlatforms = new Set<string>();

    if (senderIds.length > 0) {
      const placeholders = senderIds.map(() => '?').join(',');
      const statsRow = this.db.prepare(`
        SELECT COUNT(*) as total, MIN(platform_ts) as first_seen, MAX(platform_ts) as last_seen
        FROM messages WHERE sender_id IN (${placeholders})
      `).get(...senderIds) as { total: number; first_seen: string | null; last_seen: string | null };
      totalMessages = statsRow.total;
      firstSeen = statsRow.first_seen;
      lastSeen = statsRow.last_seen;

      const platformRows = this.db.prepare(`
        SELECT DISTINCT platform FROM messages WHERE sender_id IN (${placeholders})
      `).all(...senderIds) as { platform: string }[];
      for (const r of platformRows) activePlatforms.add(r.platform);
    }

    const events = this.db.prepare(
      'SELECT * FROM identity_events WHERE identity_id = ? ORDER BY created_at DESC LIMIT 50'
    ).all(identityId).map(r => this.parseIdentityEvent(r as Record<string, unknown>));

    return {
      id: parsed.id,
      display_name: parsed.display_name,
      notes: parsed.notes,
      platforms,
      stats: {
        total_messages: totalMessages,
        platforms_active: activePlatforms.size,
        first_seen: firstSeen,
        last_seen: lastSeen,
      },
      events,
      created_at: parsed.created_at,
      updated_at: parsed.updated_at,
    };
  }

  whoIs(query: string, limit = 10): Array<IdentityCard | Contact> {
    const pattern = `%${query}%`;
    const results: Array<IdentityCard | Contact> = [];
    const seenIdentities = new Set<string>();

    // Search identity_links first
    const linkRows = this.db.prepare(`
      SELECT DISTINCT identity_id FROM identity_links
      WHERE display_name LIKE ? OR username LIKE ? OR platform_id LIKE ?
      LIMIT ?
    `).all(pattern, pattern, pattern, limit) as { identity_id: string }[];

    for (const r of linkRows) {
      if (!seenIdentities.has(r.identity_id)) {
        seenIdentities.add(r.identity_id);
        const card = this.getIdentityCard(r.identity_id);
        if (card) results.push(card);
      }
    }

    // Search identities by display_name
    const identityRows = this.db.prepare(`
      SELECT id FROM identities WHERE display_name LIKE ? LIMIT ?
    `).all(pattern, limit) as { id: string }[];

    for (const r of identityRows) {
      if (!seenIdentities.has(r.id)) {
        seenIdentities.add(r.id);
        const card = this.getIdentityCard(r.id);
        if (card) results.push(card);
      }
    }

    // Search unlinked contacts (exclude those already linked to an identity)
    if (results.length < limit) {
      const remaining = limit - results.length;
      const contactRows = this.db.prepare(`
        SELECT c.* FROM contacts c
        WHERE (c.display_name LIKE ? OR c.username LIKE ? OR c.phone LIKE ? OR c.id LIKE ?)
          AND NOT EXISTS (
            SELECT 1 FROM identity_links il
            WHERE il.platform || ':' || il.platform_id = c.id
          )
        ORDER BY c.last_seen DESC LIMIT ?
      `).all(pattern, pattern, pattern, pattern, remaining) as Record<string, unknown>[];

      for (const r of contactRows) {
        results.push(this.parseContact(r));
      }
    }

    return results;
  }

  private _mergeIdentityRaw(sourceId: string, targetId: string, source = 'manual'): number {
    const sourceLinks = this.db.prepare('SELECT * FROM identity_links WHERE identity_id = ?')
      .all(sourceId) as Record<string, unknown>[];
    for (const link of sourceLinks) {
      const l = this.parseIdentityLink(link);
      const conflict = this.db.prepare(
        'SELECT id FROM identity_links WHERE platform = ? AND platform_id = ? AND identity_id = ?'
      ).get(l.platform, l.platform_id, targetId);
      if (conflict) {
        this.db.prepare('DELETE FROM identity_links WHERE id = ?').run(l.id);
      } else {
        this.db.prepare('UPDATE identity_links SET identity_id = ? WHERE id = ?').run(targetId, l.id);
      }
    }
    this.logIdentityEvent(targetId, 'merged', { absorbed_identity: sourceId, links_moved: sourceLinks.length, source });
    this.db.prepare('DELETE FROM identities WHERE id = ?').run(sourceId);
    this.touchIdentity(targetId);
    return sourceLinks.length;
  }

  mergeIdentities(sourceId: string, targetId: string): IdentityCard {
    const txn = this.db.transaction(() => {
      this._mergeIdentityRaw(sourceId, targetId);
    });
    txn();
    return this.getIdentityCard(targetId)!;
  }

  listIdentities(limit = 50, offset = 0, search?: string): { identities: Identity[]; total: number } {
    if (search) {
      const pattern = `%${search}%`;
      const total = (this.db.prepare(
        'SELECT COUNT(*) as c FROM identities WHERE display_name LIKE ?'
      ).get(pattern) as { c: number }).c;
      const rows = this.db.prepare(
        'SELECT * FROM identities WHERE display_name LIKE ? ORDER BY updated_at DESC LIMIT ? OFFSET ?'
      ).all(pattern, limit, offset) as Record<string, unknown>[];
      return { identities: rows.map(r => this.parseIdentity(r)), total };
    }
    const total = (this.db.prepare('SELECT COUNT(*) as c FROM identities').get() as { c: number }).c;
    const rows = this.db.prepare(
      'SELECT * FROM identities ORDER BY updated_at DESC LIMIT ? OFFSET ?'
    ).all(limit, offset) as Record<string, unknown>[];
    return { identities: rows.map(r => this.parseIdentity(r)), total };
  }

  updateIdentity(identityId: string, fields: { display_name?: string; notes?: string }): Identity {
    const identity = this.db.prepare('SELECT * FROM identities WHERE id = ?').get(identityId) as Record<string, unknown> | undefined;
    if (!identity) throw new Error(`Identity ${identityId} not found`);

    const updates: string[] = [];
    const params: unknown[] = [];
    if (fields.display_name !== undefined) {
      if (!fields.display_name.trim()) throw new Error('display_name cannot be empty');
      updates.push('display_name = ?');
      params.push(fields.display_name);
    }
    if (fields.notes !== undefined) {
      updates.push('notes = ?');
      params.push(fields.notes);
    }
    if (updates.length === 0) return this.parseIdentity(identity);

    const now = new Date().toISOString();
    updates.push('updated_at = ?');
    params.push(now, identityId);
    this.db.prepare(`UPDATE identities SET ${updates.join(', ')} WHERE id = ?`).run(...params);
    this.logIdentityEvent(identityId, 'updated', fields);

    return this.parseIdentity(
      this.db.prepare('SELECT * FROM identities WHERE id = ?').get(identityId) as Record<string, unknown>
    );
  }

  cleanupOrphanedIdentities(): { removed: number; ids: string[] } {
    const orphans = this.db.prepare(`
      SELECT i.id FROM identities i
      LEFT JOIN identity_links il ON il.identity_id = i.id
      WHERE il.id IS NULL
    `).all() as { id: string }[];

    const ids = orphans.map(r => r.id);
    for (const id of ids) {
      this.logIdentityEvent(id, 'deleted_orphan', {});
      this.db.prepare('DELETE FROM identities WHERE id = ?').run(id);
    }
    return { removed: ids.length, ids };
  }

  getIdentityHealth(): IdentityHealth {
    const total_contacts = (this.db.prepare('SELECT COUNT(*) as c FROM contacts').get() as { c: number }).c;
    const total_identities = (this.db.prepare('SELECT COUNT(*) as c FROM identities').get() as { c: number }).c;
    const total_links = (this.db.prepare('SELECT COUNT(*) as c FROM identity_links').get() as { c: number }).c;

    const contacts_linked = (this.db.prepare(`
      SELECT COUNT(DISTINCT c.id) as c FROM contacts c
      INNER JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
    `).get() as { c: number }).c;

    const contacts_unlinked = total_contacts - contacts_linked;
    const coverage_pct = total_contacts > 0 ? Math.round((contacts_linked / total_contacts) * 1000) / 10 : 0;

    const sourceRows = this.db.prepare(
      'SELECT source, COUNT(*) as c FROM identity_links GROUP BY source'
    ).all() as { source: string; c: number }[];
    const links_by_source: Record<string, number> = {};
    for (const r of sourceRows) links_by_source[r.source] = r.c;

    const unlinked_with_messages = (this.db.prepare(`
      SELECT COUNT(DISTINCT c.id) as c FROM contacts c
      LEFT JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
      WHERE il.id IS NULL AND EXISTS (SELECT 1 FROM messages m WHERE m.sender_id = c.id)
    `).get() as { c: number }).c;

    const unlinked_with_phone = (this.db.prepare(`
      SELECT COUNT(*) as c FROM contacts c
      LEFT JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
      WHERE il.id IS NULL AND c.phone IS NOT NULL AND LENGTH(c.phone) > 0
    `).get() as { c: number }).c;

    const top_unlinked = this.db.prepare(`
      SELECT c.id, c.display_name, c.platform, COUNT(m.id) as message_count
      FROM contacts c
      LEFT JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
      LEFT JOIN messages m ON m.sender_id = c.id
      WHERE il.id IS NULL
      GROUP BY c.id
      ORDER BY message_count DESC
      LIMIT 20
    `).all() as Array<{ id: string; display_name: string | null; platform: string; message_count: number }>;

    const orphaned_identities = (this.db.prepare(`
      SELECT COUNT(*) as c FROM identities i
      LEFT JOIN identity_links il ON il.identity_id = i.id
      WHERE il.id IS NULL
    `).get() as { c: number }).c;

    return {
      total_contacts, total_identities, total_links,
      contacts_linked, contacts_unlinked, coverage_pct,
      links_by_source, unlinked_with_messages, unlinked_with_phone,
      top_unlinked, orphaned_identities,
    };
  }

  tryAutoLink(contact: Contact): { linked: boolean; identity_id?: string } {
    const firstColon = contact.id.indexOf(':');
    const platform = contact.id.substring(0, firstColon);
    const platformId = contact.id.substring(firstColon + 1);

    // Already linked?
    const existing = this.db.prepare(
      'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
    ).get(platform, platformId) as { identity_id: string } | undefined;
    if (existing) return { linked: false };

    // Path 1: phone-based matching
    if (contact.phone && contact.phone.length > 0) {
      const normalized = this.normalizePhone(contact.phone);
      const phoneLink = this.db.prepare(
        "SELECT identity_id FROM identity_links WHERE platform = 'phone' AND platform_id = ?"
      ).get(normalized) as { identity_id: string } | undefined;
      if (phoneLink) {
        this.linkContact(phoneLink.identity_id, platform, platformId, 0.95, 'sync_auto', contact.display_name ?? undefined, contact.username ?? undefined);
        return { linked: true, identity_id: phoneLink.identity_id };
      }
    }

    // Path 2: email username matching — if this is an email contact with a username,
    // check if another email link with the same username already exists
    if (platform === 'email' && contact.username) {
      const emailLink = this.db.prepare(
        "SELECT identity_id FROM identity_links WHERE platform = 'email' AND username = ? AND platform_id != ?"
      ).get(contact.username, platformId) as { identity_id: string } | undefined;
      if (emailLink) {
        this.linkContact(emailLink.identity_id, platform, platformId, 0.9, 'email_match', contact.display_name ?? undefined, contact.username ?? undefined);
        return { linked: true, identity_id: emailLink.identity_id };
      }
    }

    // Path 3: Slack email metadata matching — if contact has email in metadata,
    // check if an email-platform contact with that address exists and is linked
    if (contact.metadata?.email && typeof contact.metadata.email === 'string') {
      const emailAddr = (contact.metadata.email as string).toLowerCase();
      const emailLink = this.db.prepare(
        "SELECT il.identity_id FROM identity_links il WHERE il.platform = 'email' AND il.platform_id = ?"
      ).get(`user:${emailAddr}`) as { identity_id: string } | undefined;
      if (emailLink) {
        this.linkContact(emailLink.identity_id, platform, platformId, 0.9, 'metadata_email_match',
          contact.display_name ?? undefined, contact.username ?? undefined);
        return { linked: true, identity_id: emailLink.identity_id };
      }
    }

    return { linked: false };
  }

  resolveContactNames(senderIds: string[]): Map<string, string> {
    const result = new Map<string, string>();
    if (senderIds.length === 0) return result;

    const placeholders = senderIds.map(() => '?').join(',');
    const rows = this.db.prepare(
      `SELECT id, display_name FROM contacts WHERE id IN (${placeholders}) AND display_name IS NOT NULL`
    ).all(...senderIds) as { id: string; display_name: string }[];

    for (const row of rows) {
      result.set(row.id, row.display_name);
    }
    return result;
  }

  getThreadsByIdentity(identityId: string, limit = 50): Thread[] {
    const linkRows = this.db.prepare(
      "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
    ).all(identityId) as { sender_id: string }[];

    const senderIds = linkRows.map(r => r.sender_id);
    if (senderIds.length === 0) return [];

    const placeholders = senderIds.map(() => '?').join(',');
    return this.db.prepare(`
      SELECT t.* FROM threads t
      WHERE EXISTS (
        SELECT 1 FROM messages m
        WHERE m.thread_id = t.id AND m.sender_id IN (${placeholders})
      )
      ORDER BY t.updated_at DESC LIMIT ?
    `).all(...senderIds, limit).map(r => this.parseThread(r as Record<string, unknown>));
  }

  getUnlinkedContacts(platform?: string, limit = 50): Array<Contact & { message_count: number }> {
    const platformFilter = platform ? 'AND c.platform = ?' : '';
    const params: unknown[] = platform ? [platform, limit] : [limit];

    const rows = this.db.prepare(`
      SELECT c.*, COALESCE(mc.cnt, 0) as message_count
      FROM contacts c
      LEFT JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
      LEFT JOIN (
        SELECT sender_id, COUNT(*) as cnt FROM messages GROUP BY sender_id
      ) mc ON mc.sender_id = c.id
      WHERE il.id IS NULL ${platformFilter}
      ORDER BY message_count DESC
      LIMIT ?
    `).all(...params) as Array<Record<string, unknown>>;

    return rows.map(r => ({
      ...this.parseContact(r),
      message_count: r.message_count as number,
    }));
  }

  searchMessagesByIdentity(identityId: string, query?: string, limit = 50): Message[] {
    const linkRows = this.db.prepare(
      "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
    ).all(identityId) as { sender_id: string }[];

    const senderIds = linkRows.map(r => r.sender_id);
    if (senderIds.length === 0) return [];

    const placeholders = senderIds.map(() => '?').join(',');

    if (query) {
      return this.db.prepare(`
        SELECT m.* FROM messages m
        JOIN messages_fts fts ON m.rowid = fts.rowid
        WHERE messages_fts MATCH ? AND m.sender_id IN (${placeholders})
        ORDER BY rank LIMIT ?
      `).all(query, ...senderIds, limit).map(this.parseMessage);
    }

    return this.db.prepare(`
      SELECT * FROM messages WHERE sender_id IN (${placeholders})
      ORDER BY platform_ts DESC LIMIT ?
    `).all(...senderIds, limit).map(this.parseMessage);
  }

  autoResolve(): AutoResolveReport {
    const report: AutoResolveReport = {
      identities_created: 0,
      links_created: 0,
      phone_matches: 0,
      name_matches: 0,
      single_platform_created: 0,
      cross_platform_name_matches: 0,
      skipped_ambiguous_names: 0,
      signal_uuid_dedup_matches: 0,
      nickname_matches: 0,
      fuzzy_matches: 0,
      identity_merges: 0,
      email_metadata_matches: 0,
      skipped_already_linked: 0,
      details: [],
    };

    const txn = this.db.transaction(() => {
      // Get all contacts with phone numbers
      const contacts = this.db.prepare(
        "SELECT * FROM contacts WHERE phone IS NOT NULL AND length(phone) > 0"
      ).all() as Record<string, unknown>[];

      // Group by normalized phone
      const phoneGroups = new Map<string, Contact[]>();
      for (const row of contacts) {
        const contact = this.parseContact(row);
        const normalized = this.normalizePhone(contact.phone!);
        if (!phoneGroups.has(normalized)) phoneGroups.set(normalized, []);
        phoneGroups.get(normalized)!.push(contact);
      }

      // Process groups spanning 2+ platforms
      for (const [phone, group] of phoneGroups) {
        const platforms = new Set(group.map(c => c.platform));
        if (platforms.size < 2) continue;

        report.phone_matches++;

        // Split contact.id into platform + platform_id
        const contactRefs = group.map(c => {
          const firstColon = c.id.indexOf(':');
          return { platform: c.id.substring(0, firstColon), platform_id: c.id.substring(firstColon + 1) };
        });

        // Check if any contact already linked
        const existingIdentityId = this.findExistingIdentityForContacts(contactRefs);

        if (existingIdentityId) {
          // Extend existing identity with unlinked contacts
          const linkedContacts: string[] = [];
          for (let i = 0; i < contactRefs.length; i++) {
            const ref = contactRefs[i];
            const existing = this.db.prepare(
              'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
            ).get(ref.platform, ref.platform_id) as { identity_id: string } | undefined;

            if (existing) {
              report.skipped_already_linked++;
              continue;
            }

            this.linkContact(existingIdentityId, ref.platform, ref.platform_id, 0.95, 'phone_match', group[i].display_name ?? undefined, group[i].username ?? undefined);
            report.links_created++;
            linkedContacts.push(group[i].id);
          }

          if (linkedContacts.length > 0) {
            report.details.push({ phone, identity_id: existingIdentityId, action: 'extended', contacts_linked: linkedContacts });
          }
        } else {
          // Create new identity
          const bestName = group.find(c => c.display_name)?.display_name ?? phone;
          const identity = this.createIdentity(bestName);
          report.identities_created++;

          // Link the phone number itself
          this.linkContact(identity.id, 'phone', phone, 1.0, 'phone_match');
          report.links_created++;

          // Link all contacts
          const linkedContacts: string[] = [];
          for (let i = 0; i < contactRefs.length; i++) {
            this.linkContact(identity.id, contactRefs[i].platform, contactRefs[i].platform_id, 0.95, 'phone_match', group[i].display_name ?? undefined, group[i].username ?? undefined);
            report.links_created++;
            linkedContacts.push(group[i].id);
          }

          report.details.push({ phone, identity_id: identity.id, action: 'created', contacts_linked: linkedContacts });
        }
      }

      // Pass 2: Single-platform phone identities
      // Contacts with phones that aren't linked to any identity yet (single-platform)
      const unlinkedPhoneContacts = this.db.prepare(`
        SELECT c.* FROM contacts c
        LEFT JOIN identity_links il
          ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
        WHERE c.phone IS NOT NULL AND LENGTH(c.phone) > 0 AND il.id IS NULL
      `).all() as Record<string, unknown>[];

      for (const row of unlinkedPhoneContacts) {
        const contact = this.parseContact(row);
        const normalized = this.normalizePhone(contact.phone!);
        const firstColon = contact.id.indexOf(':');
        const platform = contact.id.substring(0, firstColon);
        const platformId = contact.id.substring(firstColon + 1);

        // Check if a phone link already exists (identity may have been created by another contact with same phone)
        const existingPhoneLink = this.db.prepare(
          "SELECT identity_id FROM identity_links WHERE platform = 'phone' AND platform_id = ?"
        ).get(normalized) as { identity_id: string } | undefined;

        if (existingPhoneLink) {
          // Link to existing identity
          this.linkContact(existingPhoneLink.identity_id, platform, platformId, 0.95, 'phone_match', contact.display_name ?? undefined, contact.username ?? undefined);
          report.links_created++;
          report.single_platform_created++;
          report.details.push({ phone: normalized, identity_id: existingPhoneLink.identity_id, action: 'single_platform', contacts_linked: [contact.id] });
        } else {
          // Create new identity for single-platform contact
          const bestName = contact.display_name ?? normalized;
          const identity = this.createIdentity(bestName);
          report.identities_created++;

          this.linkContact(identity.id, 'phone', normalized, 1.0, 'phone_match');
          report.links_created++;

          this.linkContact(identity.id, platform, platformId, 0.95, 'phone_match', contact.display_name ?? undefined, contact.username ?? undefined);
          report.links_created++;
          report.single_platform_created++;

          report.details.push({ phone: normalized, identity_id: identity.id, action: 'single_platform', contacts_linked: [contact.id] });
        }
      }

      // Pass 3: Email name matching — link unlinked email contacts by display_name
      const identityNames = this.db.prepare('SELECT id, display_name FROM identities').all() as { id: string; display_name: string }[];
      if (identityNames.length > 0) {
        // Build case-insensitive lookup: lowercase name → identity_id (skip ambiguous names)
        const nameIndex = new Map<string, string>();
        const nameConflicts = new Set<string>();
        for (const row of identityNames) {
          const lower = row.display_name.toLowerCase();
          if (lower.length < 3) continue;
          if (nameIndex.has(lower)) {
            nameConflicts.add(lower);
            nameIndex.delete(lower);
          } else if (!nameConflicts.has(lower)) {
            nameIndex.set(lower, row.id);
          }
        }

        // Get unlinked email contacts with display_names
        const emailContacts = this.db.prepare(`
          SELECT c.* FROM contacts c
          LEFT JOIN identity_links il
            ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
          WHERE c.platform = 'email' AND c.display_name IS NOT NULL AND LENGTH(c.display_name) >= 3 AND il.id IS NULL
        `).all() as Record<string, unknown>[];

        for (const row of emailContacts) {
          const contact = this.parseContact(row);
          const matchId = nameIndex.get(contact.display_name!.toLowerCase());
          if (!matchId) continue;

          const firstColon = contact.id.indexOf(':');
          const platform = contact.id.substring(0, firstColon);
          const platformId = contact.id.substring(firstColon + 1);

          this.linkContact(matchId, platform, platformId, 0.65, 'name_match', contact.display_name ?? undefined, contact.username ?? undefined);
          report.name_matches++;
          report.links_created++;
          report.details.push({
            identity_id: matchId,
            action: 'name_matched',
            contacts_linked: [contact.id],
          });
        }
      }

      // Pass 4: Cross-platform name matching
      // Match unlinked contacts by display_name across different platforms
      const unlinkedContacts = this.db.prepare(`
        SELECT c.* FROM contacts c
        LEFT JOIN identity_links il
          ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
        WHERE il.id IS NULL AND c.display_name IS NOT NULL AND LENGTH(c.display_name) >= 2
      `).all() as Record<string, unknown>[];

      const nameGroups = new Map<string, Contact[]>();
      for (const row of unlinkedContacts) {
        const contact = this.parseContact(row);
        const lower = contact.display_name!.toLowerCase().trim();
        if (!nameGroups.has(lower)) nameGroups.set(lower, []);
        nameGroups.get(lower)!.push(contact);
      }

      for (const [name, group] of nameGroups) {
        const platforms = new Set(group.map(c => c.platform));

        // Must span 2+ platforms
        if (platforms.size < 2) continue;

        // Skip first-name-only (no space) — too ambiguous
        if (!name.includes(' ')) {
          report.skipped_ambiguous_names++;
          continue;
        }

        // Skip if any platform has 2+ contacts with this name — ambiguous
        const platformCounts = new Map<string, number>();
        for (const c of group) {
          platformCounts.set(c.platform, (platformCounts.get(c.platform) ?? 0) + 1);
        }
        const hasAmbiguousPlatform = [...platformCounts.values()].some(count => count > 1);
        if (hasAmbiguousPlatform) {
          report.skipped_ambiguous_names++;
          continue;
        }

        // Find or create identity
        const contactRefs = group.map(c => {
          const firstColon = c.id.indexOf(':');
          return { platform: c.id.substring(0, firstColon), platform_id: c.id.substring(firstColon + 1) };
        });
        let identityId = this.findExistingIdentityForContacts(contactRefs);

        if (!identityId) {
          const bestName = group.find(c => c.display_name)?.display_name ?? name;
          const identity = this.createIdentity(bestName);
          identityId = identity.id;
          report.identities_created++;
        }

        const linkedContacts: string[] = [];
        for (let i = 0; i < contactRefs.length; i++) {
          const existing = this.db.prepare(
            'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
          ).get(contactRefs[i].platform, contactRefs[i].platform_id) as { identity_id: string } | undefined;
          if (existing) {
            report.skipped_already_linked++;
            continue;
          }

          const confidence = 0.75; // full name with space
          this.linkContact(identityId, contactRefs[i].platform, contactRefs[i].platform_id, confidence, 'name_match', group[i].display_name ?? undefined, group[i].username ?? undefined);
          report.links_created++;
          linkedContacts.push(group[i].id);
        }

        if (linkedContacts.length > 0) {
          report.cross_platform_name_matches++;
          report.details.push({ identity_id: identityId, action: 'cross_platform_name', contacts_linked: linkedContacts });
        }
      }

      // Pass 4b: Email metadata cross-linking
      // Slack (and other) contacts store email in metadata.email — match against email-platform contacts
      const metadataEmailContacts = this.db.prepare(`
        SELECT c.* FROM contacts c
        LEFT JOIN identity_links il
          ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
        WHERE il.id IS NULL
          AND json_extract(c.metadata, '$.email') IS NOT NULL
      `).all() as Record<string, unknown>[];

      for (const row of metadataEmailContacts) {
        const contact = this.parseContact(row);
        const email = (contact.metadata as Record<string, unknown>)?.email;
        if (!email || typeof email !== 'string') continue;

        const emailAddr = email.toLowerCase();
        const firstColon = contact.id.indexOf(':');
        const platform = contact.id.substring(0, firstColon);
        const platformId = contact.id.substring(firstColon + 1);

        // Check if an email-platform contact with this address is already linked
        const emailLink = this.db.prepare(
          "SELECT il.identity_id FROM identity_links il WHERE il.platform = 'email' AND il.platform_id = ?"
        ).get(`user:${emailAddr}`) as { identity_id: string } | undefined;

        if (emailLink) {
          this.linkContact(emailLink.identity_id, platform, platformId, 0.9, 'metadata_email_match',
            contact.display_name ?? undefined, contact.username ?? undefined);
          report.email_metadata_matches++;
          report.links_created++;
          report.details.push({ identity_id: emailLink.identity_id, action: 'email_metadata', contacts_linked: [contact.id] });
          continue;
        }

        // Check if another unlinked contact shares this metadata email — group them
        // (handled implicitly by tryAutoLink on next sync; autoResolve focuses on existing identities)
      }

      // Pass 5: Signal UUID deduplication
      // Detect same-person duplicate Signal UUIDs via name similarity + shared group membership
      // Re-registration creates a new UUID with the same/similar display name
      const signalUuidPairs = this.db.prepare(`
        WITH signal_users AS (
          SELECT m.sender_id, c.display_name,
            MIN(m.platform_ts) as first_msg,
            MAX(m.platform_ts) as last_msg,
            COUNT(*) as msg_count
          FROM messages m
          JOIN contacts c ON c.id = m.sender_id
          WHERE m.platform = 'signal'
            AND m.sender_id LIKE 'signal:user:%'
            AND m.sender_id != 'signal:user:self'
            AND c.display_name IS NOT NULL
            AND LENGTH(c.display_name) >= 3
          GROUP BY m.sender_id
          HAVING msg_count >= 5
        )
        SELECT a.sender_id as uuid_a, b.sender_id as uuid_b,
          a.display_name as name_a, b.display_name as name_b,
          a.first_msg as first_a, a.last_msg as last_a,
          b.first_msg as first_b, b.last_msg as last_b
        FROM signal_users a, signal_users b
        WHERE a.sender_id < b.sender_id
          AND (
            LOWER(a.display_name) = LOWER(b.display_name)
            OR LOWER(b.display_name) LIKE LOWER(a.display_name) || '%'
            OR LOWER(a.display_name) LIKE LOWER(b.display_name) || '%'
          )
      `).all() as Array<{
        uuid_a: string; uuid_b: string;
        name_a: string; name_b: string;
        first_a: string; last_a: string;
        first_b: string; last_b: string;
      }>;

      for (const pair of signalUuidPairs) {
        // Require at least 1 shared group thread
        const sharedGroups = this.db.prepare(`
          SELECT COUNT(DISTINCT m1.thread_id) as cnt
          FROM messages m1
          INNER JOIN messages m2 ON m1.thread_id = m2.thread_id
          WHERE m1.sender_id = ? AND m2.sender_id = ?
            AND m1.thread_id IN (
              SELECT id FROM threads WHERE platform = 'signal' AND thread_type = 'group'
            )
        `).get(pair.uuid_a, pair.uuid_b) as { cnt: number };
        if (sharedGroups.cnt < 1) continue;

        // Negative filter: if they ever DM each other, they're different people
        const hasDM = this.db.prepare(`
          SELECT 1 FROM threads t
          WHERE t.platform = 'signal' AND t.thread_type = 'dm'
            AND EXISTS (SELECT 1 FROM messages WHERE thread_id = t.id AND sender_id = ?)
            AND EXISTS (SELECT 1 FROM messages WHERE thread_id = t.id AND sender_id = ?)
          LIMIT 1
        `).get(pair.uuid_a, pair.uuid_b);
        if (hasDM) continue;

        // Extract platform_id from sender_id format "signal:user:UUID"
        const platformIdA = pair.uuid_a.substring('signal:'.length);
        const platformIdB = pair.uuid_b.substring('signal:'.length);

        // Check if either is already linked
        const existingA = this.db.prepare(
          'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
        ).get('signal', platformIdA) as { identity_id: string } | undefined;
        const existingB = this.db.prepare(
          'SELECT identity_id FROM identity_links WHERE platform = ? AND platform_id = ?'
        ).get('signal', platformIdB) as { identity_id: string } | undefined;

        if (existingA && existingB) {
          if (existingA.identity_id !== existingB.identity_id) continue;
          report.skipped_already_linked++;
          continue;
        }

        // Get display names for identity creation
        const contactA = this.db.prepare('SELECT display_name FROM contacts WHERE id = ?').get(pair.uuid_a) as { display_name: string | null } | undefined;
        const contactB = this.db.prepare('SELECT display_name FROM contacts WHERE id = ?').get(pair.uuid_b) as { display_name: string | null } | undefined;

        let identityId: string;
        const linkedContacts: string[] = [];

        if (existingA) {
          identityId = existingA.identity_id;
        } else if (existingB) {
          identityId = existingB.identity_id;
        } else {
          const bestName = contactA?.display_name ?? contactB?.display_name ?? pair.uuid_a;
          const identity = this.createIdentity(bestName);
          identityId = identity.id;
          report.identities_created++;
        }

        if (!existingA) {
          this.linkContact(identityId, 'signal', platformIdA, 0.85, 'signal_uuid_dedup', contactA?.display_name ?? undefined);
          report.links_created++;
          linkedContacts.push(pair.uuid_a);
        }
        if (!existingB) {
          this.linkContact(identityId, 'signal', platformIdB, 0.85, 'signal_uuid_dedup', contactB?.display_name ?? undefined);
          report.links_created++;
          linkedContacts.push(pair.uuid_b);
        }

        if (linkedContacts.length > 0) {
          report.signal_uuid_dedup_matches++;
          report.details.push({ identity_id: identityId, action: 'signal_uuid_dedup', contacts_linked: linkedContacts });
        }
      }

      // Pass 6: Nickname + Fuzzy name matching
      // 6a: Nickname lookup — cross-platform only, requires full-name or unique canonical match
      // 6b: Jaro-Winkler fuzzy — full-name only, threshold 0.88+, cross-platform required below 0.93

      // Build identity candidate list with extracted first names
      const allIdentities = this.db.prepare('SELECT id, display_name FROM identities')
        .all() as { id: string; display_name: string }[];
      const identityCandidates: IdentityCandidate[] = allIdentities.map(i => ({
        id: i.id,
        display_name: i.display_name,
        first_name: extractFirstName(i.display_name),
        name_lower: i.display_name.toLowerCase(),
      }));

      // Get unlinked contacts (re-fetch — earlier passes may have linked some)
      const unlinkedForFuzzy = this.db.prepare(`
        SELECT c.id, c.display_name, c.platform
        FROM contacts c
        LEFT JOIN identity_links il
          ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
        WHERE il.id IS NULL AND c.display_name IS NOT NULL AND LENGTH(c.display_name) >= 3
      `).all() as Array<{ id: string; display_name: string; platform: string }>;

      // Pre-compute: count unlinked contacts per first-name canonical (for common name guard)
      const canonicalContactCounts = new Map<string, number>();
      for (const c of unlinkedForFuzzy) {
        const firstName = extractFirstName(c.display_name) ?? c.display_name.toLowerCase().trim();
        const canonicals = this.getNicknameCanonicals(firstName);
        for (const can of canonicals) {
          canonicalContactCounts.set(can, (canonicalContactCounts.get(can) ?? 0) + 1);
        }
      }

      for (const contact of unlinkedForFuzzy) {
        const contactNameLower = contact.display_name.toLowerCase().trim();
        const contactFirstName = extractFirstName(contact.display_name) ?? contactNameLower;
        const platformId = contact.id.substring(contact.platform.length + 1);

        // 6a: Nickname lookup — only if contact has a multi-word name (first+last)
        // Single names ("Samu") still go through; the canonical must match the identity's
        // canonical form (not an indirect chain like ian→john)
        const canonicals = this.getNicknameCanonicals(contactFirstName);
        let nicknameLinked = false;
        for (const canonical of canonicals) {
          if (canonical === contactFirstName) continue; // skip self-match (exact matching already handled)

          // Common name guard: skip if 5+ unlinked contacts share this canonical
          if ((canonicalContactCounts.get(canonical) ?? 0) >= 5) continue;

          // Find identities whose canonical name matches — the identity's first name
          // must be the CANONICAL itself (not just any variant)
          const matchingIdentities = identityCandidates.filter(ic => {
            const icFirst = ic.first_name ?? ic.name_lower;
            return icFirst === canonical;
          });
          if (matchingIdentities.length === 0 || matchingIdentities.length >= 3) continue;

          // Cross-platform requirement — nickname match must bridge platforms
          const target = matchingIdentities[0];
          const targetPlatforms = this.db.prepare(
            "SELECT DISTINCT platform FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
          ).all(target.id) as { platform: string }[];
          const samePlatform = targetPlatforms.some(p => p.platform === contact.platform);
          if (samePlatform) continue;

          this.linkContact(target.id, contact.platform, platformId, 0.75, 'nickname_match', contact.display_name);
          report.links_created++;
          report.nickname_matches++;
          report.details.push({ identity_id: target.id, action: 'nickname_match', contacts_linked: [contact.id] });
          nicknameLinked = true;
          break;
        }
        if (nicknameLinked) continue;

        // 6b: Jaro-Winkler fuzzy matching — full names only, higher threshold
        // Skip short names (< 5 chars) — JW inflates scores on short strings
        if (contactNameLower.length < 5) continue;

        // Only match full names against full names — filter candidates to similar word count
        const contactWords = contactNameLower.split(/\s+/).length;
        // Single-word names need to be longer to be reliable (skip "Darren"→"Warren" type matches)
        if (contactWords === 1 && contactNameLower.length < 7) continue;
        const filteredCandidates = identityCandidates.filter(ic => {
          const icWords = ic.name_lower.split(/\s+/).length;
          // Never match single-word identity against multi-word contact (prefix inflation)
          if (icWords === 1 && contactWords > 1) return false;
          if (contactWords === 1 && icWords > 1) return false;
          // Single-word identities need length too
          if (icWords === 1 && ic.name_lower.length < 7) return false;
          return true;
        });

        const match = findBestFuzzyMatch(contactNameLower, null, filteredCandidates, 0.88);
        if (!match) continue;

        // Cross-platform requirement for scores below 0.93
        if (match.score < 0.93) {
          const identityPlatforms = this.db.prepare(
            "SELECT DISTINCT platform FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
          ).all(match.identityId) as { platform: string }[];
          const samePlatform = identityPlatforms.some(p => p.platform === contact.platform);
          if (samePlatform) continue;
        }

        // Common name guard: skip if 3+ candidates score >= 0.88
        const matchCount = filteredCandidates.filter(ic =>
          jaroWinkler(contactNameLower, ic.name_lower) >= 0.88
        ).length;
        if (matchCount >= 3) continue;

        const confidence = match.score >= 0.93 ? 0.80 : 0.70;
        this.linkContact(match.identityId, contact.platform, platformId, confidence, 'fuzzy_match', contact.display_name);
        report.links_created++;
        report.fuzzy_matches++;
        report.details.push({ identity_id: match.identityId, action: 'fuzzy_match', contacts_linked: [contact.id] });
      }

      // Pass 7: Identity-to-identity merging
      // Compare identity display_names pairwise, merge clearly-same-person identities
      const pass7Identities = this.db.prepare('SELECT id, display_name, created_at FROM identities')
        .all() as { id: string; display_name: string; created_at: string }[];
      const mergedAway = new Set<string>();

      const getWeight = (id: string) => {
        const linkCount = (this.db.prepare(
          'SELECT COUNT(*) as c FROM identity_links WHERE identity_id = ?'
        ).get(id) as { c: number }).c;
        const msgCount = (this.db.prepare(`
          SELECT COALESCE(SUM(mc.cnt), 0) as c FROM identity_links il
          LEFT JOIN (SELECT sender_id, COUNT(*) as cnt FROM messages GROUP BY sender_id) mc
            ON mc.sender_id = il.platform || ':' || il.platform_id
          WHERE il.identity_id = ?
        `).get(id) as { c: number }).c;
        const platforms = (this.db.prepare(
          "SELECT DISTINCT platform FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
        ).all(id) as { platform: string }[]).map(p => p.platform);
        return { linkCount, msgCount, platforms };
      };

      type Ident7 = { id: string; display_name: string; created_at: string };
      const pickMergeDirection = (a: Ident7, wA: ReturnType<typeof getWeight>, b: Ident7, wB: ReturnType<typeof getWeight>): [Ident7, Ident7] => {
        // [source, target] — target (survivor) has more links/messages/older
        if (wA.linkCount > wB.linkCount) return [b, a];
        if (wB.linkCount > wA.linkCount) return [a, b];
        if (wA.msgCount > wB.msgCount) return [b, a];
        if (wB.msgCount > wA.msgCount) return [a, b];
        return a.created_at <= b.created_at ? [b, a] : [a, b];
      };

      const hasPlatformConflict = (pA: string[], pB: string[]) =>
        pA.some(p => pB.includes(p));

      // Tier 1: Exact name match
      const nameGroups7 = new Map<string, Ident7[]>();
      for (const ident of pass7Identities) {
        if (!ident.display_name) continue;
        const lower = ident.display_name.toLowerCase().trim();
        if (!lower) continue;
        if (!nameGroups7.has(lower)) nameGroups7.set(lower, []);
        nameGroups7.get(lower)!.push(ident);
      }

      for (const [name, group] of nameGroups7) {
        if (group.length !== 2) continue; // only pairs; 3+ = common name

        const [a, b] = group;
        if (mergedAway.has(a.id) || mergedAway.has(b.id)) continue;

        const wA = getWeight(a.id);
        const wB = getWeight(b.id);
        if (hasPlatformConflict(wA.platforms, wB.platforms)) continue;

        const [source, target] = pickMergeDirection(a, wA, b, wB);
        this._mergeIdentityRaw(source.id, target.id, 'auto_resolve_pass7');
        mergedAway.add(source.id);
        report.identity_merges++;
        report.details.push({
          identity_id: target.id,
          action: 'identity_merge',
          contacts_linked: [],
          merged_into: target.id,
          merge_evidence: `Exact name: "${a.display_name}" (${wA.platforms.join(',')}|${wA.linkCount} links) = "${b.display_name}" (${wB.platforms.join(',')}|${wB.linkCount} links)`,
        });
      }

      // Tier 2: High-confidence fuzzy (JW >= 0.95)
      const remaining7 = pass7Identities.filter(i => !mergedAway.has(i.id) && i.display_name && i.display_name.length >= 5);
      for (let i = 0; i < remaining7.length; i++) {
        if (mergedAway.has(remaining7[i].id)) continue;
        const aName = remaining7[i].display_name.toLowerCase().trim();
        const aWords = aName.split(/\s+/).length;
        if (aWords === 1 && aName.length < 7) continue;

        for (let j = i + 1; j < remaining7.length; j++) {
          if (mergedAway.has(remaining7[j].id)) continue;
          const bName = remaining7[j].display_name.toLowerCase().trim();
          const bWords = bName.split(/\s+/).length;
          if (bWords !== aWords) continue;
          if (bWords === 1 && bName.length < 7) continue;
          if (aName === bName) continue; // handled by Tier 1

          const score = jaroWinkler(aName, bName);
          if (score < 0.95) continue;

          const a = remaining7[i];
          const b = remaining7[j];
          const wA = getWeight(a.id);
          const wB = getWeight(b.id);
          if (hasPlatformConflict(wA.platforms, wB.platforms)) continue;

          // Common name guard: skip if 2+ others also score >= 0.95
          let highMatches = 0;
          for (const other of remaining7) {
            if (other.id === a.id || other.id === b.id || mergedAway.has(other.id)) continue;
            const otherName = other.display_name.toLowerCase().trim();
            if (jaroWinkler(aName, otherName) >= 0.95 || jaroWinkler(bName, otherName) >= 0.95) {
              highMatches++;
              if (highMatches >= 2) break;
            }
          }
          if (highMatches >= 2) continue;

          const [source, target] = pickMergeDirection(a, wA, b, wB);
          this._mergeIdentityRaw(source.id, target.id, 'auto_resolve_pass7');
          mergedAway.add(source.id);
          report.identity_merges++;
          report.details.push({
            identity_id: target.id,
            action: 'identity_merge',
            contacts_linked: [],
            merged_into: target.id,
            merge_evidence: `Fuzzy (JW=${score.toFixed(3)}): "${a.display_name}" ~ "${b.display_name}"`,
          });
          break; // a's partner found, move to next i
        }
      }

      // Tier 3: Nickname + last name match
      const remaining7b = pass7Identities.filter(i => !mergedAway.has(i.id) && i.display_name && i.display_name.includes(' '));
      const lastNameGroups = new Map<string, Ident7[]>();
      for (const ident of remaining7b) {
        const parts = ident.display_name.toLowerCase().trim().split(/\s+/);
        const lastName = parts[parts.length - 1];
        if (lastName.length < 2) continue;
        if (!lastNameGroups.has(lastName)) lastNameGroups.set(lastName, []);
        lastNameGroups.get(lastName)!.push(ident);
      }

      for (const [, group] of lastNameGroups) {
        if (group.length < 2) continue;
        for (let i = 0; i < group.length; i++) {
          if (mergedAway.has(group[i].id)) continue;
          const aFirst = extractFirstName(group[i].display_name);
          if (!aFirst) continue;
          const aCan = new Set(this.getNicknameCanonicals(aFirst));

          for (let j = i + 1; j < group.length; j++) {
            if (mergedAway.has(group[j].id)) continue;
            const bFirst = extractFirstName(group[j].display_name);
            if (!bFirst) continue;
            if (aFirst === bFirst) continue; // exact = handled by Tier 1

            const bCan = new Set(this.getNicknameCanonicals(bFirst));
            const overlap = [...aCan].filter(c => bCan.has(c));
            if (overlap.length === 0) continue;

            const a = group[i];
            const b = group[j];
            const wA = getWeight(a.id);
            const wB = getWeight(b.id);
            if (hasPlatformConflict(wA.platforms, wB.platforms)) continue;

            const [source, target] = pickMergeDirection(a, wA, b, wB);
            this._mergeIdentityRaw(source.id, target.id, 'auto_resolve_pass7');
            mergedAway.add(source.id);
            report.identity_merges++;
            report.details.push({
              identity_id: target.id,
              action: 'identity_merge',
              contacts_linked: [],
              merged_into: target.id,
              merge_evidence: `Nickname + last name: "${a.display_name}" ~ "${b.display_name}" (canonical: ${overlap.join(',')})`,
            });
            break; // a consumed
          }
        }
      }
    });

    txn();
    return report;
  }

  getRelationships(identityId: string, limit = 20): IdentityRelationship[] {
    // Get all sender_ids for this identity
    const linkRows = this.db.prepare(
      "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
    ).all(identityId) as { sender_id: string }[];

    const senderIds = linkRows.map(r => r.sender_id);
    if (senderIds.length === 0) return [];

    const placeholders = senderIds.map(() => '?').join(',');

    // Find all threads where this identity participates, then find other participants
    const rows = this.db.prepare(`
      WITH my_threads AS (
        SELECT DISTINCT thread_id FROM messages WHERE sender_id IN (${placeholders})
      ),
      other_senders AS (
        SELECT m.sender_id, m.thread_id, m.platform, m.platform_ts
        FROM messages m
        INNER JOIN my_threads mt ON mt.thread_id = m.thread_id
        WHERE m.sender_id NOT IN (${placeholders})
      ),
      agg AS (
        SELECT
          os.sender_id,
          COUNT(DISTINCT os.thread_id) as shared_threads,
          COUNT(*) as total_messages,
          MAX(os.platform_ts) as last_interaction
        FROM other_senders os
        GROUP BY os.sender_id
      )
      SELECT a.*, il.identity_id, i.display_name as identity_name
      FROM agg a
      LEFT JOIN identity_links il
        ON il.platform = SUBSTR(a.sender_id, 1, INSTR(a.sender_id, ':') - 1)
        AND il.platform_id = SUBSTR(a.sender_id, INSTR(a.sender_id, ':') + 1)
      LEFT JOIN identities i ON i.id = il.identity_id
      ORDER BY a.total_messages DESC
      LIMIT ?
    `).all(...senderIds, ...senderIds, limit) as Array<{
      sender_id: string; shared_threads: number; total_messages: number;
      last_interaction: string | null; identity_id: string | null; identity_name: string | null;
    }>;

    // Aggregate by identity (multiple sender_ids may map to the same identity)
    // Track unique thread_ids per identity to avoid double-counting shared_threads
    const identityMap = new Map<string, IdentityRelationship>();
    const identityThreads = new Map<string, Set<string>>();

    // First pass: collect per-sender thread_ids for accurate shared_threads counting
    const senderThreads = new Map<string, Set<string>>();
    for (const row of rows) {
      // Get this sender's thread_ids from the other_senders CTE data
      if (!senderThreads.has(row.sender_id)) {
        const threadRows = this.db.prepare(`
          SELECT DISTINCT thread_id FROM messages
          WHERE sender_id = ? AND thread_id IN (
            SELECT DISTINCT thread_id FROM messages WHERE sender_id IN (${placeholders})
          )
        `).all(row.sender_id, ...senderIds) as { thread_id: string }[];
        senderThreads.set(row.sender_id, new Set(threadRows.map(r => r.thread_id)));
      }
    }

    for (const row of rows) {
      const key = row.identity_id ?? row.sender_id;
      const existing = identityMap.get(key);
      const platform = row.sender_id.substring(0, row.sender_id.indexOf(':'));
      const threads = senderThreads.get(row.sender_id) ?? new Set<string>();

      if (existing) {
        existing.total_messages += row.total_messages;
        if (row.last_interaction && (!existing.last_interaction || row.last_interaction > existing.last_interaction)) {
          existing.last_interaction = row.last_interaction;
        }
        if (!existing.platforms.includes(platform)) existing.platforms.push(platform);
        // Merge thread sets for accurate shared_threads
        const existingThreads = identityThreads.get(key)!;
        for (const t of threads) existingThreads.add(t);
        existing.shared_threads = existingThreads.size;
      } else {
        // Get display name: prefer identity name, fall back to contact name
        let displayName = row.identity_name ?? row.sender_id;
        if (!row.identity_name) {
          const contact = this.db.prepare('SELECT display_name FROM contacts WHERE id = ?').get(row.sender_id) as { display_name: string | null } | undefined;
          if (contact?.display_name) displayName = contact.display_name;
        }

        identityThreads.set(key, new Set(threads));
        identityMap.set(key, {
          identity_id: row.identity_id ?? row.sender_id,
          display_name: displayName,
          shared_threads: threads.size,
          total_messages: row.total_messages,
          last_interaction: row.last_interaction,
          platforms: [platform],
        });
      }
    }

    return [...identityMap.values()]
      .sort((a, b) => b.total_messages - a.total_messages)
      .slice(0, limit);
  }

  getMergeSuggestions(limit = 30): MergeSuggestion[] {
    // Find unlinked contacts, group by name, surface ambiguous cases
    const unlinkedRows = this.db.prepare(`
      SELECT c.*, COALESCE(mc.cnt, 0) as message_count
      FROM contacts c
      LEFT JOIN identity_links il
        ON il.platform = c.platform AND il.platform_id = SUBSTR(c.id, LENGTH(c.platform) + 2)
      LEFT JOIN (SELECT sender_id, COUNT(*) as cnt FROM messages GROUP BY sender_id) mc
        ON mc.sender_id = c.id
      WHERE il.id IS NULL AND c.display_name IS NOT NULL AND LENGTH(c.display_name) >= 2
    `).all() as Array<Record<string, unknown> & { message_count: number }>;

    const nameGroups = new Map<string, Array<Contact & { message_count: number }>>();
    for (const row of unlinkedRows) {
      const contact = this.parseContact(row);
      const lower = contact.display_name!.toLowerCase().trim();
      if (!nameGroups.has(lower)) nameGroups.set(lower, []);
      nameGroups.get(lower)!.push({ ...contact, message_count: row.message_count as number });
    }

    const suggestions: MergeSuggestion[] = [];
    for (const [name, group] of nameGroups) {
      if (group.length < 2) continue;

      const platforms = new Set(group.map(c => c.platform));
      const platformList = [...platforms].join(' + ');

      // First-name-only across platforms (skipped by autoResolve Pass 4)
      if (!name.includes(' ') && platforms.size >= 2) {
        suggestions.push({
          contacts: group.map(c => ({ id: c.id, platform: c.platform, display_name: c.display_name, message_count: c.message_count })),
          confidence: 0.4,
          evidence: `Same first name '${group[0].display_name}' across ${platformList}`,
        });
        continue;
      }

      // Same-platform ambiguity (multiple contacts with same name on one platform)
      const platformCounts = new Map<string, number>();
      for (const c of group) platformCounts.set(c.platform, (platformCounts.get(c.platform) ?? 0) + 1);
      const hasAmbiguousPlatform = [...platformCounts.values()].some(count => count > 1);

      if (hasAmbiguousPlatform && platforms.size >= 2) {
        suggestions.push({
          contacts: group.map(c => ({ id: c.id, platform: c.platform, display_name: c.display_name, message_count: c.message_count })),
          confidence: 0.3,
          evidence: `Name '${group[0].display_name}' appears on ${platformList} but duplicated on one platform`,
        });
      }
    }

    // Phase B: Fuzzy name suggestions for unlinked contacts
    const unlinkedWithMessages = unlinkedRows.filter(r => (r.message_count as number) > 0);

    const fuzzyPool = unlinkedWithMessages.slice(0, 200); // cap O(n²) — max ~20K JW calls
    for (let i = 0; i < fuzzyPool.length && suggestions.length < limit * 2; i++) {
      for (let j = i + 1; j < fuzzyPool.length && suggestions.length < limit * 2; j++) {
        const a = this.parseContact(fuzzyPool[i]);
        const b = this.parseContact(fuzzyPool[j]);
        if (!a.display_name || !b.display_name) continue;
        if (a.display_name.toLowerCase() === b.display_name.toLowerCase()) continue;

        const score = jaroWinkler(a.display_name.toLowerCase(), b.display_name.toLowerCase());
        if (score >= 0.75 && score < 0.90) {
          suggestions.push({
            contacts: [
              { id: a.id, platform: a.platform, display_name: a.display_name, message_count: fuzzyPool[i].message_count as number },
              { id: b.id, platform: b.platform, display_name: b.display_name, message_count: fuzzyPool[j].message_count as number },
            ],
            confidence: Math.round((score - 0.3) * 100) / 100,
            evidence: `Fuzzy name match: '${a.display_name}' ~ '${b.display_name}' (JW=${score.toFixed(2)})`,
          });
        }
      }
    }

    // Phase C: Nickname variant suggestions
    const nicknameGroups = new Map<string, Array<Contact & { message_count: number }>>();
    for (const row of unlinkedWithMessages) {
      const contact = this.parseContact(row);
      if (!contact.display_name) continue;
      const firstName = extractFirstName(contact.display_name) ?? contact.display_name.toLowerCase();
      const canonicals = this.getNicknameCanonicals(firstName);
      for (const canonical of canonicals) {
        if (!nicknameGroups.has(canonical)) nicknameGroups.set(canonical, []);
        nicknameGroups.get(canonical)!.push({ ...contact, message_count: row.message_count as number });
      }
    }
    for (const [canonical, group] of nicknameGroups) {
      if (group.length < 2) continue;
      const uniqueNames = new Set(group.map(c => c.display_name?.toLowerCase()));
      if (uniqueNames.size < 2) continue;
      suggestions.push({
        contacts: group.map(c => ({ id: c.id, platform: c.platform, display_name: c.display_name, message_count: c.message_count })),
        confidence: 0.6,
        evidence: `Nickname variants of '${canonical}': ${[...uniqueNames].map(n => `'${n}'`).join(' + ')}`,
      });
    }

    return suggestions
      .sort((a, b) => {
        // Sort by total messages across contacts, descending
        const aMessages = a.contacts.reduce((sum, c) => sum + c.message_count, 0);
        const bMessages = b.contacts.reduce((sum, c) => sum + c.message_count, 0);
        return bMessages - aMessages;
      })
      .slice(0, limit);
  }

  exportIdentities(): Array<{ id: string; display_name: string; notes: string | null; platforms: string[]; stats: { total_messages: number; first_seen: string | null; last_seen: string | null } }> {
    const identities = this.db.prepare('SELECT * FROM identities ORDER BY display_name').all() as Record<string, unknown>[];

    return identities.map(row => {
      const parsed = this.parseIdentity(row);

      const links = this.db.prepare(
        "SELECT DISTINCT platform FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
      ).all(parsed.id) as { platform: string }[];

      const senderRows = this.db.prepare(
        "SELECT platform || ':' || platform_id as sender_id FROM identity_links WHERE identity_id = ? AND platform != 'phone'"
      ).all(parsed.id) as { sender_id: string }[];

      const senderIds = senderRows.map(r => r.sender_id);
      let total_messages = 0;
      let first_seen: string | null = null;
      let last_seen: string | null = null;

      if (senderIds.length > 0) {
        const placeholders = senderIds.map(() => '?').join(',');
        const statsRow = this.db.prepare(`
          SELECT COUNT(*) as total, MIN(platform_ts) as first_seen, MAX(platform_ts) as last_seen
          FROM messages WHERE sender_id IN (${placeholders})
        `).get(...senderIds) as { total: number; first_seen: string | null; last_seen: string | null };
        total_messages = statsRow.total;
        first_seen = statsRow.first_seen;
        last_seen = statsRow.last_seen;
      }

      return {
        id: parsed.id,
        display_name: parsed.display_name,
        notes: parsed.notes,
        platforms: links.map(l => l.platform),
        stats: { total_messages, first_seen, last_seen },
      };
    });
  }
}
