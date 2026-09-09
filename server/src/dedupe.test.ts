import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';
import type { Message } from './types.js';

const now = new Date().toISOString();

/**
 * The same email, seen twice in two folders.
 *
 * Gmail's All Mail holds a second copy of every message in INBOX and Sent under
 * a different UID, and the email adapter's message id is folder+UID. So the two
 * copies differ in `id` and agree in `dedupe_key`, which is exactly the case
 * this table has to collapse (task-885 N2).
 */
function emailIn(folder: string, uid: number, messageId: string, over: Partial<Message> = {}): Message {
  return {
    id: folder === 'INBOX' ? `email:msg:work:${uid}` : `email:msg:work:${folder}:${uid}`,
    platform: 'email',
    thread_id: 'email:thread:work:abc',
    sender_id: 'email:user:gary@example.com',
    content: 'hello from your grandfather',
    content_type: 'text',
    reply_to: null,
    direction: 'received',
    metadata: { account: 'work', folder, message_id: messageId },
    platform_ts: now,
    synced_at: now,
    dedupe_key: `email|work|${messageId}`,
    ...over,
  };
}

describe('message dedupe_key', () => {
  let db: MessageDB;
  beforeEach(() => { db = new MessageDB(':memory:'); });

  const count = () => (db as any).db.prepare("SELECT COUNT(*) c FROM messages").get().c;

  it('stores one row when the same message arrives from two folders', () => {
    expect(db.insertMessage(emailIn('INBOX', 100, '<abc@mail>'))).toBe(true);
    expect(db.insertMessage(emailIn('all-mail', 9000, '<abc@mail>'))).toBe(false);
    expect(count()).toBe(1);
  });

  it('keeps the first row rather than replacing it with the second', () => {
    db.insertMessage(emailIn('INBOX', 100, '<abc@mail>'));
    db.insertMessage(emailIn('all-mail', 9000, '<abc@mail>', { content: 'a different body' }));
    const row = (db as any).db.prepare("SELECT id, content FROM messages").get();
    expect(row.id).toBe('email:msg:work:100');
    expect(row.content).toBe('hello from your grandfather');
  });

  it('stores two rows for two genuinely different messages', () => {
    db.insertMessage(emailIn('all-mail', 9000, '<abc@mail>'));
    db.insertMessage(emailIn('all-mail', 9001, '<def@mail>'));
    expect(count()).toBe(2);
  });

  it('does not collapse rows that carry no key', () => {
    // A draft with no RFC Message-ID gets dedupe_key null, and null is not a
    // value: many such rows must coexist rather than collapsing into one.
    db.insertMessage(emailIn('drafts', 1, 'x', { id: 'a', dedupe_key: null }));
    db.insertMessage(emailIn('drafts', 2, 'y', { id: 'b', dedupe_key: null }));
    expect(count()).toBe(2);
  });

  it('leaves platforms with no dedupe_key alone', () => {
    const tg = (id: string): Message => ({
      id, platform: 'telegram', thread_id: 't', sender_id: 's', content: 'x',
      content_type: 'text', reply_to: null, metadata: {}, platform_ts: now, synced_at: now,
    });
    expect(db.insertMessage(tg('telegram:msg:1'))).toBe(true);
    expect(db.insertMessage(tg('telegram:msg:2'))).toBe(true);
    expect(db.insertMessage(tg('telegram:msg:1'))).toBe(false);
    expect(count()).toBe(2);
  });

  it('repairs the stored twin direction when the second copy knows better', () => {
    // The Sent-folder copy knows the message was sent; the All Mail copy of the
    // same message, arriving later under another id, must be able to say so.
    db.insertMessage(emailIn('INBOX', 100, '<abc@mail>', { direction: 'unknown' }));
    db.insertMessage(emailIn('sent-mail', 7, '<abc@mail>', { direction: 'sent' }));
    const row = (db as any).db.prepare("SELECT direction FROM messages").get();
    expect(row.direction).toBe('sent');
  });
});
