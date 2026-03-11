import Database from 'better-sqlite3';
import crypto from 'node:crypto';
import type { Contact, Thread, Message, Identity, IdentityLink, IdentityEvent, IdentityCard, AutoResolveReport } from './types.js';

export class MessageDB {
  private db: Database.Database;

  constructor(dbPath: string) {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
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
    `);

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

  mergeIdentities(sourceId: string, targetId: string): IdentityCard {
    const txn = this.db.transaction(() => {
      // Move all links from source to target
      const sourceLinks = this.db.prepare('SELECT * FROM identity_links WHERE identity_id = ?')
        .all(sourceId) as Record<string, unknown>[];

      for (const link of sourceLinks) {
        const l = this.parseIdentityLink(link);
        // Check if target already has a link for this platform+platform_id
        const conflict = this.db.prepare(
          'SELECT id FROM identity_links WHERE platform = ? AND platform_id = ? AND identity_id = ?'
        ).get(l.platform, l.platform_id, targetId);
        if (conflict) {
          // Target already has this link — drop source's duplicate
          this.db.prepare('DELETE FROM identity_links WHERE id = ?').run(l.id);
        } else {
          this.db.prepare('UPDATE identity_links SET identity_id = ? WHERE id = ?').run(targetId, l.id);
        }
      }

      // Log merge event on target
      this.logIdentityEvent(targetId, 'merged', { absorbed_identity: sourceId, links_moved: sourceLinks.length });

      // Delete source identity (CASCADE will clean up any remaining links)
      this.db.prepare('DELETE FROM identities WHERE id = ?').run(sourceId);
      this.touchIdentity(targetId);
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

  autoResolve(): AutoResolveReport {
    const report: AutoResolveReport = {
      identities_created: 0,
      links_created: 0,
      phone_matches: 0,
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
    });

    txn();
    return report;
  }
}
