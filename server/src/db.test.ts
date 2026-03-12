import { describe, it, expect, beforeEach } from 'vitest';
import { MessageDB } from './db.js';
import type { Contact } from './types.js';

const now = new Date().toISOString();

function makeContact(overrides: Partial<Contact> & { id: string; platform: string }): Contact {
  return {
    display_name: null,
    username: null,
    phone: null,
    metadata: {},
    first_seen: now,
    last_seen: now,
    ...overrides,
  };
}

describe('tryAutoLink — email metadata matching (Fix 6)', () => {
  let db: MessageDB;

  beforeEach(() => {
    // In-memory SQLite — fresh DB for each test
    db = new MessageDB(':memory:');
  });

  it('links Slack contact to existing email identity via metadata.email', () => {
    // Set up: email contact already linked to an identity
    const emailContact = makeContact({
      id: 'email:user:shawn@example.com',
      platform: 'email',
      display_name: 'Shawn',
      username: 'shawn@example.com',
    });
    db.upsertContact(emailContact);

    // Create identity and link email contact
    const identity = (db as any).createIdentity('Shawn');
    (db as any).linkContact(identity.id, 'email', 'user:shawn@example.com', 1.0, 'manual');

    // Now upsert a Slack contact with matching metadata.email
    const slackContact = makeContact({
      id: 'slack:T123:user:U456',
      platform: 'slack',
      display_name: 'Shawn',
      username: 'shawn',
      metadata: { email: 'shawn@example.com' },
    });
    db.upsertContact(slackContact);

    // tryAutoLink should find the match via Path 3
    const result = db.tryAutoLink(slackContact);
    expect(result.linked).toBe(true);
    expect(result.identity_id).toBe(identity.id);
  });

  it('does not link when metadata.email has no matching email contact', () => {
    const slackContact = makeContact({
      id: 'slack:T123:user:U789',
      platform: 'slack',
      display_name: 'Nobody',
      metadata: { email: 'nobody@example.com' },
    });
    db.upsertContact(slackContact);

    const result = db.tryAutoLink(slackContact);
    expect(result.linked).toBe(false);
  });

  it('is case-insensitive on email matching', () => {
    const emailContact = makeContact({
      id: 'email:user:alice@example.com',
      platform: 'email',
      display_name: 'Alice',
    });
    db.upsertContact(emailContact);

    const identity = (db as any).createIdentity('Alice');
    (db as any).linkContact(identity.id, 'email', 'user:alice@example.com', 1.0, 'manual');

    const slackContact = makeContact({
      id: 'slack:T123:user:U111',
      platform: 'slack',
      display_name: 'Alice',
      metadata: { email: 'Alice@Example.COM' },
    });
    db.upsertContact(slackContact);

    const result = db.tryAutoLink(slackContact);
    expect(result.linked).toBe(true);
    expect(result.identity_id).toBe(identity.id);
  });

  it('skips already-linked contacts', () => {
    const slackContact = makeContact({
      id: 'slack:T123:user:U222',
      platform: 'slack',
      display_name: 'Bob',
      metadata: { email: 'bob@example.com' },
    });
    db.upsertContact(slackContact);

    // Link the Slack contact first
    const identity = (db as any).createIdentity('Bob');
    (db as any).linkContact(identity.id, 'slack', 'T123:user:U222', 1.0, 'manual');

    // tryAutoLink should return false (already linked)
    const result = db.tryAutoLink(slackContact);
    expect(result.linked).toBe(false);
  });
});

describe('autoResolve — email metadata pass (Fix 6)', () => {
  let db: MessageDB;

  beforeEach(() => {
    db = new MessageDB(':memory:');
  });

  it('includes email_metadata_matches in report', () => {
    // Create an email contact linked to an identity
    const emailContact = makeContact({
      id: 'email:user:carol@test.com',
      platform: 'email',
      display_name: 'Carol',
    });
    db.upsertContact(emailContact);

    const identity = (db as any).createIdentity('Carol');
    (db as any).linkContact(identity.id, 'email', 'user:carol@test.com', 1.0, 'manual');

    // Create an unlinked Slack contact with matching metadata.email
    const slackContact = makeContact({
      id: 'slack:T999:user:U333',
      platform: 'slack',
      display_name: 'Carol',
      metadata: { email: 'carol@test.com' },
    });
    db.upsertContact(slackContact);

    const report = db.autoResolve();
    expect(report.email_metadata_matches).toBeGreaterThanOrEqual(1);
    expect(report.links_created).toBeGreaterThanOrEqual(1);

    // Verify the link was actually created
    const recheck = db.tryAutoLink(slackContact);
    expect(recheck.linked).toBe(false); // false because already linked by autoResolve
  });
});
