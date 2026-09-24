import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const imapState = vi.hoisted(() => ({
  connectFailuresRemaining: 0,
  probeResults: [] as any[],
  messageResults: [] as any[],
  rawByUid: new Map<number, Buffer>(),
  instances: [] as Array<{
    connect: ReturnType<typeof vi.fn>;
    getMailboxLock: ReturnType<typeof vi.fn>;
    fetchOne: ReturnType<typeof vi.fn>;
  }>,
}));

vi.mock('imapflow', () => ({
  ImapFlow: class {
    connect = vi.fn(async () => {
      if (imapState.connectFailuresRemaining > 0) {
        imapState.connectFailuresRemaining--;
        throw new Error('temporary DNS failure');
      }
    });
    list = vi.fn(async () => [{ path: 'INBOX', specialUse: null }]);
    on = vi.fn();
    close = vi.fn(async () => {});
    logout = vi.fn(async () => {});
    getMailboxLock = vi.fn(async () => ({ release: vi.fn() }));
    fetchOne = vi.fn(async (uid: string) => {
      const source = imapState.rawByUid.get(Number(uid));
      return source ? { uid: Number(uid), source } : false;
    });
    async *fetch(_range: unknown, query: Record<string, unknown>) {
      const rows = query.bodyStructure ? imapState.probeResults : imapState.messageResults;
      for (const row of rows) yield row;
    }

    constructor() {
      imapState.instances.push(this);
    }
  },
}));

import { decodeEmailBody, EmailAdapter } from './email.js';

async function drain(gen: AsyncGenerator<unknown>): Promise<void> {
  for await (const _event of gen) {
    // The fake mailbox is empty.
  }
}

describe('EmailAdapter connection readiness', () => {
  let dataDir: string;

  beforeEach(() => {
    imapState.connectFailuresRemaining = 0;
    imapState.probeResults.length = 0;
    imapState.messageResults.length = 0;
    imapState.rawByUid.clear();
    imapState.instances.length = 0;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'email-adapter-test-'));
    fs.mkdirSync(path.join(dataDir, 'secrets'));
    fs.writeFileSync(path.join(dataDir, 'secrets', 'email.env'), '');
  });

  afterEach(() => {
    fs.rmSync(dataDir, { recursive: true, force: true });
  });

  it('keeps a failed account and reconnects on the next bounded sync attempt', async () => {
    imapState.connectFailuresRemaining = 1;
    const adapter = new EmailAdapter(() => {});
    const config = {
      enabled: true,
      data_dir: dataDir,
      accounts: [{
        id: 'primary',
        name: 'Primary',
        host: 'imap.example.test',
        user: 'person@example.test',
        password: 'test-password',
      }],
    };

    await expect(adapter.init(config)).rejects.toThrow('initialization failed');
    expect(imapState.instances).toHaveLength(1);

    await expect(drain(adapter.sync(null))).resolves.toBeUndefined();

    expect(imapState.instances).toHaveLength(2);
    expect(imapState.instances[1].connect).toHaveBeenCalledTimes(1);
    expect(imapState.instances[1].getMailboxLock).toHaveBeenCalledWith('INBOX');
    expect(adapter.getSourceObservation()).toMatchObject({
      evidence: 'IMAP successful folder scan',
    });
  });

  it('rejects an enabled adapter with zero accounts instead of disabling silently', async () => {
    const adapter = new EmailAdapter(() => {});

    await expect(adapter.init({ enabled: true, data_dir: dataDir, accounts: [] })).rejects.toThrow(
      'zero configured accounts',
    );
    await expect(drain(adapter.sync(null))).rejects.toThrow('zero configured accounts');
  });

  it('preserves calendar MIME and attaches parsed events before advancing the cursor', async () => {
    const calendar = [
      'BEGIN:VCALENDAR',
      'METHOD:REQUEST',
      'BEGIN:VEVENT',
      'UID:event-42',
      'SEQUENCE:1',
      'DTSTART:20260924T180000Z',
      'DTEND:20260924T183000Z',
      'SUMMARY:Team meeting',
      'END:VEVENT',
      'END:VCALENDAR',
    ].join('\r\n');
    const boundary = 'fixture-boundary';
    const source = Buffer.from([
      'Subject: Team meeting',
      `Content-Type: multipart/alternative; boundary="${boundary}"`,
      '',
      `--${boundary}`,
      'Content-Type: text/plain',
      '',
      'Join us.',
      `--${boundary}`,
      'Content-Type: text/calendar; method=REQUEST',
      '',
      calendar,
      `--${boundary}--`,
    ].join('\r\n'));
    imapState.probeResults.push({
      uid: 42,
      bodyStructure: { type: 'multipart/alternative', childNodes: [
        { part: '1', type: 'text/plain' },
        { part: '2', type: 'text/calendar' },
      ] },
    });
    imapState.messageResults.push({
      uid: 42,
      envelope: {
        from: [{ address: 'organizer@example.test', name: 'Organizer' }],
        to: [{ address: 'person@example.test', name: 'Person' }],
        subject: 'Team meeting',
        messageId: '<event-42@example.test>',
        date: new Date('2026-09-24T17:00:00.000Z'),
      },
      headers: Buffer.from(''),
      bodyParts: new Map([['1', Buffer.from('Join us.')]]),
    });
    imapState.rawByUid.set(42, source);

    const adapter = new EmailAdapter(() => {});
    await adapter.init({
      enabled: true,
      data_dir: dataDir,
      accounts: [{
        id: 'primary',
        name: 'Primary',
        host: 'imap.example.test',
        user: 'person@example.test',
        password: 'test-password',
      }],
    });
    const events: any[] = [];
    for await (const event of adapter.sync(null)) events.push(event);
    const message = events.find(event => event.type === 'message')?.data;

    expect(message.metadata.calendar_events).toMatchObject([{ uid: 'event-42', method: 'REQUEST' }]);
    expect(message.metadata.calendar_capture.raw_ref).toMatch(/^raw\/email-calendar\/primary\/inbox\/42-/);
    expect(fs.existsSync(path.join(dataDir, ...message.metadata.calendar_capture.raw_ref.split('/')))).toBe(true);
    expect(JSON.parse(adapter.getCursor()!)).toMatchObject({
      accounts: { primary: { folders: { INBOX: { lastUid: 42 } } } },
    });
  });
});

describe('decodeEmailBody', () => {
  it('decodes a base64 text/plain MIME part', async () => {
    const raw = [
      'Content-Type: text/plain; charset="UTF-8"',
      'Content-Transfer-Encoding: base64',
      '',
      Buffer.from('Hello Shawn, the agenda is ready.').toString('base64'),
    ].join('\r\n');
    expect((await decodeEmailBody(raw)).trim()).toBe('Hello Shawn, the agenda is ready.');
  });

  it('decodes quoted-printable', async () => {
    const raw = 'Content-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nCaf=C3=A9 meeting';
    expect((await decodeEmailBody(raw)).trim()).toBe('Café meeting');
  });

  it('passes clean text through unchanged', async () => {
    expect((await decodeEmailBody('just plain text')).trim()).toBe('just plain text');
  });
});
