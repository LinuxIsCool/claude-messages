/**
 * WhatsApp Chat Export Importer
 *
 * Reads WhatsApp "Export Chat" .txt files and inserts contacts, threads,
 * and messages into the unified messages.db.
 *
 * Format (Canadian Android locale):
 *   2023-12-18, 5:48 p.m. - Sender: Message content
 *   2023-12-18, 5:48 p.m. - System message (no colon)
 *
 * Usage: node build/import-whatsapp-export.mjs <dir-of-txt-files> --my-name <YourName>
 */

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import readline from 'node:readline';
import { MessageDB } from './db.js';
import type { Contact, Thread, Message } from './types.js';

// --- Configuration ---

const SELF_NAME = process.argv.includes('--my-name')
  ? process.argv[process.argv.indexOf('--my-name') + 1]
  : null;

const DB_PATH = path.join(
  process.env.HOME ?? '',
  '.claude/local/messages/messages.db'
);

// --- Parsing ---

// Canadian Android format: 2023-12-18, 5:48 p.m. - Sender: message
// Also handles: 2023-12-18, 5:48 a.m. - Sender: message
// And 24h variants: 2023-12-18, 17:48 - Sender: message
const LINE_PATTERN =
  /^(\d{4}-\d{2}-\d{2}),\s+(\d{1,2}:\d{2}(?:\s*[ap]\.m\.)?)\s+-\s+(.+)$/;

// System messages to skip (no sender:message split, or known patterns)
const SYSTEM_PATTERNS = [
  /messages and calls are end-to-end encrypted/i,
  /created group/i,
  /added you/i,
  /changed the subject/i,
  /changed this group/i,
  /\bleft$/i,
  /\bremoved$/i,
  /joined using this group/i,
  /changed the group description/i,
  /deleted this message/i,
  /message was deleted/i,
  /is a contact$/i,
  /changed their phone number/i,
  /security code changed/i,
  /you were added/i,
  /turned on disappearing messages/i,
  /turned off disappearing messages/i,
  /pinned a message/i,
];

interface ParsedMessage {
  timestamp: string; // ISO 8601
  sender: string;
  content: string;
}

function parseTimestamp(dateStr: string, timeStr: string): string {
  // dateStr: "2023-12-18"
  // timeStr: "5:48 p.m." or "17:48" or "5:48 a.m."
  const cleanTime = timeStr.trim();

  let hours: number;
  let minutes: number;

  const ampmMatch = cleanTime.match(/^(\d{1,2}):(\d{2})\s*([ap])\.m\.$/i);
  if (ampmMatch) {
    hours = parseInt(ampmMatch[1], 10);
    minutes = parseInt(ampmMatch[2], 10);
    const isPM = ampmMatch[3].toLowerCase() === 'p';
    if (isPM && hours !== 12) hours += 12;
    if (!isPM && hours === 12) hours = 0;
  } else {
    // 24-hour format
    const parts = cleanTime.split(':').map(p => parseInt(p, 10));
    hours = parts[0];
    minutes = parts[1];
  }

  // Build ISO 8601 (local time — no timezone offset since we don't know the export TZ)
  const hh = String(hours).padStart(2, '0');
  const mm = String(minutes).padStart(2, '0');
  // No Z suffix — export timestamps are local time with unknown timezone
  return `${dateStr}T${hh}:${mm}:00.000`;
}

function parseLine(line: string): { type: 'message'; data: ParsedMessage } | { type: 'system' } | null {
  const match = line.match(LINE_PATTERN);
  if (!match) return null;

  const [, dateStr, timeStr, rest] = match;

  // Split rest into sender:content — first colon separates them
  const colonIdx = rest.indexOf(':');
  if (colonIdx === -1) {
    // No colon → system message (e.g. "Alice is a contact")
    return { type: 'system' };
  }

  const sender = rest.slice(0, colonIdx).trim();
  const content = rest.slice(colonIdx + 1).trim();

  // Check for system message patterns
  const fullText = `${sender}: ${content}`;
  for (const pattern of SYSTEM_PATTERNS) {
    if (pattern.test(fullText) || pattern.test(content)) {
      return { type: 'system' };
    }
  }

  return {
    type: 'message',
    data: {
      timestamp: parseTimestamp(dateStr, timeStr),
      sender,
      content,
    },
  };
}

function extractChatName(filename: string): string {
  const base = path.basename(filename, '.txt');
  const match = base.match(/^WhatsApp Chat with (.+)$/i);
  return match ? match[1] : base;
}

function sanitizeId(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
}

// --- Import ---

interface ImportStats {
  contacts: number;
  threads: number;
  messages: number;
  skipped: number;
  dupes: number;
}

async function importFile(
  db: MessageDB,
  filePath: string,
  stats: ImportStats,
): Promise<number> {
  const chatName = extractChatName(filePath);
  const chatSlug = sanitizeId(chatName);
  // Thread ID — namespaced under export to avoid collision with live adapter JID-based IDs
  const threadId = `whatsapp:export:chat:${chatSlug}`;
  const now = new Date().toISOString();

  // Collect all messages first (for participant list, thread type detection, and timestamps)
  const messages: ParsedMessage[] = [];
  const senderSet = new Set<string>();

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  let pending: ParsedMessage | null = null;

  for await (const line of rl) {
    const parsed = parseLine(line);

    if (parsed) {
      // Flush pending
      if (pending) {
        messages.push(pending);
        senderSet.add(pending.sender);
      }

      if (parsed.type === 'message') {
        pending = parsed.data;
      } else {
        pending = null;
        stats.skipped++;
      }
    } else if (pending && line.trim()) {
      // Multi-line continuation
      pending.content += '\n' + line;
    }
  }

  // Flush final
  if (pending) {
    messages.push(pending);
    senderSet.add(pending.sender);
  }

  if (messages.length === 0) {
    console.log(`  [skip] ${chatName}: no messages`);
    return 0;
  }

  let fileInserted = 0;

  // DM if exactly 2 participants (self + one other), otherwise group
  const nonSelfSenders = [...senderSet].filter(s => s !== SELF_NAME);
  const threadType = nonSelfSenders.length <= 1 ? 'dm' : 'group';

  // Upsert contacts
  for (const senderName of senderSet) {
    const contactId = `whatsapp:export:user:${sanitizeId(senderName)}`;
    const contact: Contact = {
      id: contactId,
      platform: 'whatsapp',
      display_name: senderName,
      username: null,
      phone: null,
      metadata: { source: 'export', is_self: senderName === SELF_NAME },
      first_seen: messages[0].timestamp,
      last_seen: messages[messages.length - 1].timestamp,
    };
    db.upsertContact(contact);
    stats.contacts++;
  }

  // Upsert thread
  const participantIds = [...senderSet].map(s => `whatsapp:export:user:${sanitizeId(s)}`);
  const thread: Thread = {
    id: threadId,
    platform: 'whatsapp',
    title: chatName,
    thread_type: threadType,
    participants: participantIds,
    metadata: { source: 'export' },
    created_at: messages[0].timestamp,
    updated_at: messages[messages.length - 1].timestamp,
  };
  db.upsertThread(thread);
  stats.threads++;

  // Insert messages
  for (const msg of messages) {
    // Skip <Media omitted> and empty
    if (!msg.content.trim() || msg.content === '<Media omitted>') {
      stats.skipped++;
      continue;
    }

    const senderId = `whatsapp:export:user:${sanitizeId(msg.sender)}`;
    // Deterministic ID: SHA-256 hash of full content for collision resistance
    const hash = crypto.createHash('sha256')
      .update(`${msg.timestamp}:${msg.sender}:${msg.content}`)
      .digest('base64url')
      .slice(0, 22); // 132 bits — negligible collision probability
    const msgId = `whatsapp:export:msg:${chatSlug}:${hash}`;

    const message: Message = {
      id: msgId,
      platform: 'whatsapp',
      thread_id: threadId,
      sender_id: senderId,
      content: msg.content,
      content_type: 'text',
      reply_to: null,
      metadata: { source: 'export' },
      platform_ts: msg.timestamp,
      synced_at: now,
    };

    const inserted = db.insertMessage(message);
    if (inserted) {
      stats.messages++;
      fileInserted++;
    } else {
      stats.dupes++;
    }
  }

  return fileInserted;
}

// --- Main ---

async function main(): Promise<void> {
  const inputDir = process.argv[2];
  if (!inputDir) {
    console.error('Usage: node import-whatsapp-export.mjs <dir-of-txt-files> --my-name <YourName>');
    process.exit(1);
  }

  const resolvedDir = inputDir.startsWith('~')
    ? path.join(process.env.HOME ?? '', inputDir.slice(1))
    : path.resolve(inputDir);

  if (!fs.existsSync(resolvedDir)) {
    console.error(`Directory not found: ${resolvedDir}`);
    process.exit(1);
  }

  const txtFiles = fs.readdirSync(resolvedDir)
    .filter(f => f.endsWith('.txt'))
    .map(f => path.join(resolvedDir, f));

  if (txtFiles.length === 0) {
    console.error(`No .txt files found in: ${resolvedDir}`);
    process.exit(1);
  }

  console.log(`[whatsapp-export] Found ${txtFiles.length} chat files`);
  console.log(`[whatsapp-export] Self name: ${SELF_NAME}`);
  console.log(`[whatsapp-export] DB: ${DB_PATH}`);
  console.log('');

  const db = new MessageDB(DB_PATH);
  const stats: ImportStats = { contacts: 0, threads: 0, messages: 0, skipped: 0, dupes: 0 };

  for (const file of txtFiles) {
    const chatName = extractChatName(file);
    console.log(`[${chatName}]`);
    const fileInserted = await importFile(db, file, stats);
    const fileDupes = stats.dupes; // dupes are cumulative
    console.log(`  [done] ${chatName}: ${fileInserted} inserted`);
  }

  console.log('');
  console.log('=== Import Complete ===');
  console.log(`  Chats:    ${stats.threads}`);
  console.log(`  Contacts: ${stats.contacts}`);
  console.log(`  Messages: ${stats.messages}`);
  console.log(`  Skipped:  ${stats.skipped} (system/media/empty)`);
  console.log(`  Dupes:    ${stats.dupes} (already in DB)`);

  db.close();
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
