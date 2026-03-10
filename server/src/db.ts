import Database from 'better-sqlite3';
import type { Contact, Thread, Message } from './types.js';

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
}
