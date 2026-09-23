import { describe, expect, it, vi } from 'vitest';
import { Daemon } from './daemon.js';
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
    config: { adapters: { email: { enabled: true, cooldown_after_failures: 0 } } },
    db: { getCursor: vi.fn(() => null), updateCursor },
    adapterHealth: new Map([['email', emailHealth()]]),
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
