import { describe, it, expect, vi } from 'vitest';
import { TelegramAdapter } from './telegram.js';

// The daemon keeps one TelegramClient for its whole life. When gramjs exhausts
// its reconnect retries the sender is dead and every request hangs forever,
// which surfaced as 27 consecutive "Timed out after 1800s" cycles (Sep 5 to 8,
// 2026). sync() must reconnect a disconnected client before using it.

function fakeClient(connected: boolean) {
  return {
    connected,
    connect: vi.fn(async function (this: { connected: boolean }) { this.connected = true; }),
    getDialogs: vi.fn(async () => []),
  };
}

async function drain(gen: AsyncGenerator<unknown>): Promise<unknown[]> {
  const out: unknown[] = [];
  for await (const ev of gen) out.push(ev);
  return out;
}

describe('TelegramAdapter.sync reconnects a dead client', () => {
  it('calls connect() when the client is disconnected', async () => {
    const adapter = new TelegramAdapter(() => {});
    const client = fakeClient(false);
    (adapter as unknown as { client: unknown }).client = client;

    await drain(adapter.sync(null));

    expect(client.connect).toHaveBeenCalledTimes(1);
    expect(client.getDialogs).toHaveBeenCalled();
  });

  it('does not reconnect when the client is already connected', async () => {
    const adapter = new TelegramAdapter(() => {});
    const client = fakeClient(true);
    (adapter as unknown as { client: unknown }).client = client;

    await drain(adapter.sync(null));

    expect(client.connect).not.toHaveBeenCalled();
    expect(client.getDialogs).toHaveBeenCalled();
  });
});
