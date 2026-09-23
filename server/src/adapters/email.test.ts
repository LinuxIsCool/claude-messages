import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const imapState = vi.hoisted(() => ({
  connectFailuresRemaining: 0,
  instances: [] as Array<{
    connect: ReturnType<typeof vi.fn>;
    getMailboxLock: ReturnType<typeof vi.fn>;
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
    async *fetch() {}

    constructor() {
      imapState.instances.push(this);
    }
  },
}));

import { EmailAdapter } from './email.js';

async function drain(gen: AsyncGenerator<unknown>): Promise<void> {
  for await (const _event of gen) {
    // The fake mailbox is empty.
  }
}

describe('EmailAdapter connection readiness', () => {
  let dataDir: string;

  beforeEach(() => {
    imapState.connectFailuresRemaining = 0;
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
  });

  it('rejects an enabled adapter with zero accounts instead of disabling silently', async () => {
    const adapter = new EmailAdapter(() => {});

    await expect(adapter.init({ data_dir: dataDir, accounts: [] })).rejects.toThrow(
      'zero configured accounts',
    );
    await expect(drain(adapter.sync(null))).rejects.toThrow('zero configured accounts');
  });
});
