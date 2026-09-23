import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { SignalCliAdapter, type SignalCliRunner } from './signal-cli.js';
import type { SyncEvent } from '../types.js';

class FakeRunner implements SignalCliRunner {
  output = '';
  failure: Error | null = null;

  async version(): Promise<string> { return 'signal-cli 0.test'; }
  async json(command: string): Promise<unknown> {
    if (command === 'listAccounts') return [{ number: '+15550000000' }];
    if (command === 'listGroups') return [{ id: 'gpu-group', name: 'GPU Indigenomics' }];
    return null;
  }
  async receiveToFile(outputPath: string): Promise<void> {
    if (this.failure) throw this.failure;
    fs.writeFileSync(outputPath, this.output, { mode: 0o600 });
  }
}

async function collect(gen: AsyncGenerator<SyncEvent>): Promise<SyncEvent[]> {
  const events: SyncEvent[] = [];
  for await (const event of gen) events.push(event);
  return events;
}

describe('SignalCliAdapter', () => {
  let root: string;
  let runner: FakeRunner;
  let adapter: SignalCliAdapter;

  beforeEach(async () => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'signal-cli-adapter-'));
    runner = new FakeRunner();
    adapter = new SignalCliAdapter(() => {}, runner);
    await adapter.init({ enabled: true, data_dir: root });
  });

  afterEach(() => fs.rmSync(root, { recursive: true, force: true }));

  it('reports fresh source evidence only after a successful receive request', async () => {
    await expect(collect(adapter.sync(null))).resolves.toEqual([
      expect.objectContaining({ type: 'thread' }),
    ]);
    expect(adapter.getSourceObservation()).toEqual({
      observed_at: expect.any(String),
      evidence: 'signal-cli successful receive request',
    });

    runner.failure = new Error('network unavailable');
    const failed = new SignalCliAdapter(() => {}, runner);
    await failed.init({ enabled: true, data_dir: root });
    await expect(collect(failed.sync(null))).rejects.toThrow('network unavailable');
    expect(failed.getSourceObservation()).toBeNull();
  });

  it('spools an incoming group message before yielding it', async () => {
    runner.output = `${JSON.stringify({ envelope: {
      sourceUuid: 'alice-aci',
      sourceName: 'Alice',
      sourceDevice: 1,
      timestamp: 1790179200000,
      dataMessage: {
        timestamp: 1790179200000,
        message: 'GPU update',
        groupInfo: { groupId: 'gpu-group' },
        attachments: [],
      },
    } })}\n`;

    const events = await collect(adapter.sync(null));
    const message = events.find(event => event.type === 'message');
    expect(message?.data).toMatchObject({
      platform: 'signal',
      sender_id: 'signal:user:alice-aci',
      content: 'GPU update',
      direction: 'received',
    });
    const rawFiles = fs.readdirSync(path.join(root, 'raw', 'signal-cli'));
    expect(rawFiles).toHaveLength(1);
    expect(fs.readFileSync(path.join(root, 'raw', 'signal-cli', rawFiles[0]), 'utf-8'))
      .toContain('GPU update');
  });

  it('recovers an interrupted raw capture after restart and maps sent sync messages', async () => {
    const spoolDir = path.join(root, 'raw', 'signal-cli');
    const fileName = 'capture-1-replay.jsonl';
    fs.writeFileSync(path.join(spoolDir, `.${fileName}.tmp`), `${JSON.stringify({ envelope: {
      sourceUuid: 'self-aci',
      sourceDevice: 1,
      timestamp: 1790179201000,
      syncMessage: { sentMessage: {
        destinationUuid: 'bob-aci',
        timestamp: 1790179201000,
        message: 'Sent update',
      } },
    } })}\n`);

    const restarted = new SignalCliAdapter(() => {}, runner);
    await restarted.init({ enabled: true, data_dir: root });
    const events = await collect(restarted.sync(null));
    const message = events.find(event => event.type === 'message');
    expect(message?.data).toMatchObject({
      sender_id: 'signal:user:self',
      content: 'Sent update',
      direction: 'sent',
    });
    expect(restarted.getCursor()).toContain(fileName);
    expect(fs.existsSync(path.join(spoolDir, `.${fileName}.tmp`))).toBe(false);
    expect(fs.existsSync(path.join(spoolDir, fileName))).toBe(true);
  });
});
