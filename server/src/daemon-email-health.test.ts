import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { describe, expect, it, vi } from 'vitest';
import { Daemon, isMainModule } from './daemon.js';
import type { Adapter } from './adapters/base.js';
import type { AdapterHealth } from './types.js';

function emailHealth(): AdapterHealth {
  return {
    platform: 'email',
    tier: 2,
    last_success: null,
    last_failure: null,
    last_error: null,
    last_duration_ms: 0,
    last_yield: { messages: 0, threads: 0, contacts: 0 },
    consecutive_failures: 0,
    timed_out: false,
    skipped: false,
    cooldown_until: null,
  };
}

function daemonHarness() {
  const daemon = Object.create(Daemon.prototype) as Daemon;
  const updateCursor = vi.fn();
  Object.assign(daemon as unknown as Record<string, unknown>, {
    running: true,
    config: {
      adapters: {
        email: { enabled: true, poll_interval: 30, cooldown_after_failures: 0 },
        telegram: { enabled: true, poll_interval: 300, cooldown_after_failures: 0 },
      },
    },
    db: { getCursor: vi.fn(() => null), updateCursor },
    adapterHealth: new Map([['email', emailHealth()]]),
    cycleCount: 0,
    awareness: { emit: vi.fn() },
    writeHealth: vi.fn(),
    log: vi.fn(),
    processEvent: vi.fn(),
  });
  return { daemon, updateCursor };
}

function failingEmailAdapter(): Adapter {
  return {
    platform: 'email',
    init: vi.fn(async () => {}),
    async *sync() {
      throw new Error('zero connected accounts');
    },
    getCursor: vi.fn(() => '{"accounts":{},"version":2}'),
    shutdown: vi.fn(async () => {}),
  };
}

describe('Daemon email health', () => {
  it('recognizes a bundled entrypoint reached through a plugin symlink', () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'daemon-main-test-'));
    const realPath = path.join(tempDir, 'daemon.mjs');
    const linkedPath = path.join(tempDir, 'plugin-daemon.mjs');
    fs.writeFileSync(realPath, '');
    fs.symlinkSync(realPath, linkedPath);

    try {
      expect(isMainModule(linkedPath, pathToFileURL(realPath).href)).toBe(true);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('advances email while Telegram is still hung', async () => {
    const { daemon, updateCursor } = daemonHarness();
    let releaseTelegram!: () => void;
    const telegramGate = new Promise<void>(resolve => { releaseTelegram = resolve; });
    let telegramFinished = false;

    const telegram: Adapter = {
      platform: 'telegram',
      init: vi.fn(async () => {}),
      async *sync() {
        await telegramGate;
        telegramFinished = true;
      },
      getCursor: vi.fn(() => 'telegram-cursor'),
      shutdown: vi.fn(async () => {}),
    };
    const email: Adapter = {
      platform: 'email',
      init: vi.fn(async () => {}),
      async *sync() {
        // Stop both loops after this first independent email pass.
        (daemon as unknown as { running: boolean }).running = false;
      },
      getCursor: vi.fn(() => 'email-cursor'),
      shutdown: vi.fn(async () => {}),
    };
    const health = (daemon as unknown as { adapterHealth: Map<string, AdapterHealth> }).adapterHealth;
    health.set('telegram', { ...emailHealth(), platform: 'telegram' });
    (daemon as unknown as { adapters: Adapter[] }).adapters = [telegram, email];
    const pollIntervalMs = (daemon as unknown as { pollIntervalMs(adapter: Adapter): number })
      .pollIntervalMs.bind(daemon);
    expect(pollIntervalMs(email)).toBe(30_000);
    expect(pollIntervalMs(telegram)).toBe(300_000);

    const loops = (daemon as unknown as { startAdapterLoops(): Promise<void>[] })
      .startAdapterLoops();

    await vi.waitFor(() => {
      expect(health.get('email')?.last_success).not.toBeNull();
    });
    expect(telegramFinished).toBe(false);
    expect(updateCursor).toHaveBeenCalledWith('email', 'email-cursor');

    releaseTelegram();
    await Promise.all(loops);
  });

  it('starts unhealthy when email initialization failed', () => {
    const { daemon } = daemonHarness();
    (daemon as unknown as { adapterInitErrors: Map<string, string> }).adapterInitErrors =
      new Map([['email', 'Error: temporary DNS failure']]);

    const health = (daemon as unknown as {
      initialHealth(adapter: Adapter): AdapterHealth;
    }).initialHealth(failingEmailAdapter());

    expect(health.last_success).toBeNull();
    expect(health.last_failure).not.toBeNull();
    expect(health.last_error).toContain('temporary DNS failure');
    expect(health.consecutive_failures).toBe(1);
  });

  it('does not turn a zero-yield email failure into success or cursor freshness', async () => {
    const { daemon, updateCursor } = daemonHarness();

    await expect(
      (daemon as unknown as { syncOneAdapter(adapter: Adapter): Promise<void> })
        .syncOneAdapter(failingEmailAdapter()),
    ).rejects.toThrow('zero connected accounts');

    const health = (daemon as unknown as { adapterHealth: Map<string, AdapterHealth> })
      .adapterHealth.get('email')!;
    expect(health.last_success).toBeNull();
    expect(health.last_failure).not.toBeNull();
    expect(health.last_error).toContain('zero connected accounts');
    expect(health.consecutive_failures).toBe(1);
    expect(updateCursor).not.toHaveBeenCalled();
  });
});
