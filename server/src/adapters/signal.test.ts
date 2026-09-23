import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const childProcess = vi.hoisted(() => ({
  execFileSync: vi.fn((_file: string, args: string[], options?: { input?: string }) => {
    if (args[0] === '--version') return 'sqlcipher test';
    return options?.input?.includes('COUNT(*)') ? '[{"count":1}]' : '[]';
  }),
}));

vi.mock('node:child_process', () => childProcess);

import { SignalAdapter } from './signal.js';

const keepalive =
  '[WebsocketResources] WebSocketResources.KeepAlive(WebSocketResource(authenticated)).send: Sending a keepalive message';

async function drain(gen: AsyncGenerator<unknown>): Promise<void> {
  for await (const _event of gen) {
    // Test SQLCipher responses contain no message rows.
  }
}

describe('SignalAdapter source health', () => {
  let root: string;
  let dataDir: string;
  let dbPath: string;
  let logPath: string;

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'signal-adapter-test-'));
    dataDir = path.join(root, 'messages');
    dbPath = path.join(root, 'db.sqlite');
    logPath = path.join(root, 'app.log');
    fs.mkdirSync(path.join(dataDir, 'secrets'), { recursive: true });
    fs.writeFileSync(dbPath, 'test');
    fs.writeFileSync(path.join(dataDir, 'secrets', 'signal.env'), 'SIGNAL_DB_KEY=test-key\n');
    childProcess.execFileSync.mockClear();
  });

  afterEach(() => {
    fs.rmSync(root, { recursive: true, force: true });
  });

  function config() {
    return {
      enabled: true,
      data_dir: dataDir,
      db_path: dbPath,
      source_log_path: logPath,
      source_max_age_seconds: 180,
    };
  }

  it('does not report a zero-yield poll as success when Signal Desktop is stale', async () => {
    fs.writeFileSync(logPath, `${JSON.stringify({
      time: new Date(Date.now() - 20 * 60_000).toISOString(),
      msg: keepalive,
    })}\n`);
    const adapter = new SignalAdapter(() => {});
    await adapter.init(config());

    await expect(drain(adapter.sync('{"lastSentAt":1}'))).rejects.toThrow(
      'authenticated websocket is stale',
    );
    expect(adapter.getSourceObservation()).toBeNull();
  });

  it('accepts zero new messages only with current authenticated source evidence', async () => {
    const observedAt = new Date(Date.now() - 10_000).toISOString();
    fs.writeFileSync(logPath, `${JSON.stringify({ time: observedAt, msg: keepalive })}\n`);
    const adapter = new SignalAdapter(() => {});
    await adapter.init(config());

    await expect(drain(adapter.sync('{"lastSentAt":1}'))).resolves.toBeUndefined();
    expect(adapter.getSourceObservation()).toEqual({
      observed_at: observedAt,
      evidence: 'signal-desktop authenticated websocket keepalive',
    });
  });

  it('throws on missing static dependencies instead of silently disabling itself', async () => {
    fs.rmSync(dbPath);
    const adapter = new SignalAdapter(() => {});

    await expect(adapter.init(config())).rejects.toThrow('Database not found');
    await expect(drain(adapter.sync(null))).rejects.toThrow('Adapter is not ready');
  });
});
