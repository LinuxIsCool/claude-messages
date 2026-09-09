import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';
import type { Contact, Message } from './types.js';

const now = '2026-09-09T00:00:00.000Z';

function contact(id: string, over: Partial<Contact> = {}): Contact {
  return {
    id, platform: id.split(':')[0], display_name: null, username: null, phone: null,
    metadata: {}, first_seen: now, last_seen: now, ...over,
  };
}

function message(id: string, senderId: string, threadId = 'th1'): Message {
  return {
    id, platform: senderId.split(':')[0], thread_id: threadId, sender_id: senderId,
    content: 'hi', content_type: 'text', reply_to: null, direction: 'received',
    metadata: {}, platform_ts: now, synced_at: now,
  };
}

describe('admitObserved — being counted stops requiring being known', () => {
  let db: MessageDB;
  let self: string;

  beforeEach(() => {
    db = new MessageDB(':memory:');
    self = db.createIdentity('Shawn').id;
    db.setConfig('self_identity_id', self);
    db.linkContact(self, 'telegram', 'user:1', 1.0, 'manual');
  });

  const identityCount = () => (db as any).db.prepare('SELECT COUNT(*) c FROM identities').get().c;
  const nameOf = (senderId: string) => (db as any).db.prepare(
    "SELECT i.display_name n FROM identities i JOIN identity_links il ON il.identity_id = i.id WHERE il.platform || ':' || il.platform_id = ?"
  ).get(senderId)?.n;

  it('admits a contact nothing points at', () => {
    db.upsertContact(contact('signal:user:abc', { display_name: 'Kaylian Jay' }));
    const r = db.admitObserved();
    expect(r.from_contacts).toBe(1);
    expect(nameOf('signal:user:abc')).toBe('Kaylian Jay');
  });

  it('admits a contact with no phone, which is what the old gate refused', () => {
    // autoResolve Pass 2 created an identity only for a contact carrying a
    // phone number. This is that contact without one.
    db.upsertContact(contact('signal:user:nophone', { display_name: 'Mike DeMelo' }));
    expect(db.admitObserved().from_contacts).toBe(1);
    expect(nameOf('signal:user:nophone')).toBe('Mike DeMelo');
  });

  it('admits a sender that has no contacts row at all', () => {
    // A Telegram group member: the adapter wrote a contact for the dialog and
    // never for the people speaking in it. 1,549 of these in the live corpus.
    db.insertMessage(message('m1', 'telegram:user:9999'));
    const r = db.admitObserved();
    expect(r.from_senders).toBe(1);
    expect(r.contacts_created).toBe(1);
    expect(nameOf('telegram:user:9999')).toBe('telegram:user:9999');
  });

  it('names an unknown sender by its handle rather than inventing one', () => {
    db.insertMessage(message('m1', 'telegram:user:9999'));
    db.admitObserved();
    const row = (db as any).db.prepare("SELECT display_name d, metadata m FROM identities WHERE display_name LIKE 'telegram:%'").get();
    expect(row.d).toBe('telegram:user:9999');
    expect(JSON.parse(row.m).admitted).toBe(true);
    expect(JSON.parse(row.m).admitted_from).toBe('sender');
  });

  it('gives an admitted sender the observed message range, not the clock', () => {
    const early = { ...message('m1', 'signal:user:z'), platform_ts: '2021-01-01T00:00:00.000Z' };
    const late = { ...message('m2', 'signal:user:z'), platform_ts: '2024-06-01T00:00:00.000Z' };
    db.insertMessage(early); db.insertMessage(late);
    db.admitObserved();
    const c = (db as any).db.prepare("SELECT first_seen f, last_seen l FROM contacts WHERE id = 'signal:user:z'").get();
    expect(c.f).toBe('2021-01-01T00:00:00.000Z');
    expect(c.l).toBe('2024-06-01T00:00:00.000Z');
  });

  it('leaves an already-linked contact alone', () => {
    const existing = db.createIdentity('Darren');
    db.upsertContact(contact('signal:user:darren', { display_name: 'Darren' }));
    db.linkContact(existing.id, 'signal', 'user:darren', 1.0, 'manual');
    const before = identityCount();
    expect(db.admitObserved().from_contacts).toBe(0);
    expect(identityCount()).toBe(before);
  });

  it('never admits Shawn as a second person', () => {
    // His link already exists, so the unlinked filter alone excludes him.
    db.insertMessage(message('m1', 'telegram:user:1'));
    const r = db.admitObserved();
    expect(r.from_contacts + r.from_senders).toBe(0);
    expect(identityCount()).toBe(1);
  });

  it('counts without writing under dryRun', () => {
    db.upsertContact(contact('signal:user:abc', { display_name: 'Kaylian Jay' }));
    db.insertMessage(message('m1', 'telegram:user:9999'));
    const before = identityCount();
    const r = db.admitObserved({ dryRun: true });
    expect(r.from_contacts).toBe(1);
    expect(r.from_senders).toBe(1);
    expect(identityCount()).toBe(before);
  });

  it('admits nothing twice', () => {
    db.upsertContact(contact('signal:user:abc', { display_name: 'Kaylian Jay' }));
    db.insertMessage(message('m1', 'telegram:user:9999'));
    db.admitObserved();
    const after = identityCount();
    const second = db.admitObserved();
    expect(second.from_contacts + second.from_senders).toBe(0);
    expect(identityCount()).toBe(after);
  });

  it('can be held to contacts only', () => {
    db.upsertContact(contact('signal:user:abc', { display_name: 'Kaylian Jay' }));
    db.insertMessage(message('m1', 'telegram:user:9999'));
    const r = db.admitObserved({ includeUnknownSenders: false });
    expect(r.from_contacts).toBe(1);
    expect(r.from_senders).toBe(0);
  });
});

describe('computeAllScores retraction', () => {
  it('removes a score the current run did not produce', () => {
    // Before this, `INSERT OR REPLACE` left a stale row standing forever. On
    // 2026-09-09 two scores from 2026-07-14 were being served beside 132 fresh
    // ones, one of them at rank 15 on a two-month-old number.
    const db = new MessageDB(':memory:');
    const self = db.createIdentity('Shawn').id;
    db.setConfig('self_identity_id', self);
    db.linkContact(self, 'telegram', 'user:1', 1.0, 'manual');
    const other = db.createIdentity('Andrea Vogel').id;
    db.linkContact(other, 'telegram', 'user:2', 1.0, 'manual');

    (db as any).db.prepare(`
      INSERT INTO contact_scores (identity_id, frequency, recency, reciprocity, channel_diversity,
        dm_ratio, structural, temporal_regularity, response_latency, composite, dunbar_layer,
        confidence, computed_at)
      VALUES (?, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9, 'sympathy_group', 0.9, '2026-07-14T00:00:00Z')
    `).run(other);

    const before = (db as any).db.prepare('SELECT COUNT(*) c FROM contact_scores').get().c;
    expect(before).toBe(1);

    const r = db.computeAllScores();   // no messages, so nobody scores
    expect(r.computed).toBe(0);
    expect(r.retracted).toBe(1);
    expect((db as any).db.prepare('SELECT COUNT(*) c FROM contact_scores').get().c).toBe(0);
  });
});

describe('identityMergeSuggestions — the unit after admission is the identity', () => {
  let db: MessageDB;
  beforeEach(() => {
    db = new MessageDB(':memory:');
    const self = db.createIdentity('Shawn').id;
    db.setConfig('self_identity_id', self);
    db.linkContact(self, 'telegram', 'user:me', 1.0, 'manual');
  });

  const two = (nameA: string, nameB: string) => {
    const a = db.createIdentity(nameA).id;
    const b = db.createIdentity(nameB).id;
    db.linkContact(a, 'signal', 'user:a', 1.0, 'manual');
    db.linkContact(b, 'telegram', 'user:b', 1.0, 'manual');
    return [a, b];
  };

  it('proposes two identities that share a full name on different platforms', () => {
    two('Darren Zal', 'DARREN ZAL');
    const s = db.identityMergeSuggestions();
    expect(s.map(x => x.kind)).toContain('name');
  });

  it('refuses a first name, which identifies nobody', () => {
    two('Darren', 'Darren');
    expect(db.identityMergeSuggestions()).toHaveLength(0);
  });

  it('refuses initials, which are a first name wearing a space', () => {
    two('M D', 'M D');
    expect(db.identityMergeSuggestions()).toHaveLength(0);
  });

  it('refuses a placeholder even when it has the shape of a name', () => {
    // `User 2028622406` has two tokens and clears every structural check, so
    // only namesNobody stands between it and a merge with a different stranger
    // whose platform wrote the same placeholder.
    two('User 2028622406', 'User 2028622406');
    expect(db.identityMergeSuggestions()).toHaveLength(0);
  });

  it('refuses a raw platform id', () => {
    two('telegram:user:8654320174', 'telegram:user:8654320174');
    expect(db.identityMergeSuggestions()).toHaveLength(0);
  });

  it('refuses a name that appears twice on one platform only', () => {
    const a = db.createIdentity('Chris Baker').id;
    const b = db.createIdentity('Chris Baker').id;
    db.linkContact(a, 'signal', 'user:a', 1.0, 'manual');
    db.linkContact(b, 'signal', 'user:b', 1.0, 'manual');
    expect(db.identityMergeSuggestions()).toHaveLength(0);
  });

  it('ranks a curated address-book match above a name match', () => {
    db.replaceAddressBook('google', [{
      full_name: 'Jeff Emmett', given_name: 'Jeff', family_name: 'Emmett',
      nickname: null, organization: null, labels: null,
      handles: [
        { kind: 'phone', value: '+12505551234', raw_value: '250 555 1234', label: null },
        { kind: 'email', value: 'jeff@block.science', raw_value: 'jeff@block.science', label: null },
      ],
    }]);
    const a = db.createIdentity('Jeff').id;
    const b = db.createIdentity('jeff emmett (work)').id;
    db.linkContact(a, 'phone', '+12505551234', 1.0, 'manual');
    db.linkContact(b, 'email', 'user:jeff@block.science', 1.0, 'manual');
    const s = db.identityMergeSuggestions();
    expect(s[0].kind).toBe('address_book');
    expect(s[0].confidence).toBe(1.0);
  });

  it('merges on evidence and keeps the identity the corpus already points at', () => {
    const [a, b] = two('Darren Zal', 'Darren Zal');
    // `a` carries the messages, so `a` must be the survivor whichever order the
    // suggestion happens to list them in.
    for (let i = 0; i < 5; i++) {
      db.insertMessage({
        id: `m${i}`, platform: 'signal', thread_id: 't', sender_id: 'signal:user:a', content: 'x',
        content_type: 'text', reply_to: null, metadata: {}, platform_ts: now, synced_at: now,
      });
    }
    const r = db.applyEvidencedMerges();
    expect(r.merged).toHaveLength(1);
    expect(r.merged[0].kind).toBe('name');
    const survivors = (db as any).db.prepare("SELECT id FROM identities WHERE display_name LIKE 'Darren%'").all();
    expect(survivors).toHaveLength(1);
    expect(survivors[0].id).toBe(a);
    expect(b).not.toBe(a);
  });

  it('holds anything below the confidence floor instead of acting on it', () => {
    two('Darren Zal', 'Darren Zal');
    const r = db.applyEvidencedMerges({ minConfidence: 0.9 });
    expect(r.merged).toHaveLength(0);
    expect(r.held.map(h => h.kind)).toEqual(['name']);
  });

  it('writes an event naming what was absorbed and on what evidence', () => {
    two('Darren Zal', 'Darren Zal');
    db.applyEvidencedMerges();
    const ev = (db as any).db.prepare("SELECT details FROM identity_events WHERE event_type='merged_on_evidence'").get();
    const d = JSON.parse(ev.details);
    expect(d.kind).toBe('name');
    expect(d.claim_class).toBe('observed');
    expect(d.absorbed_name).toBe('Darren Zal');
  });
});
