import { createHash, randomUUID } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { promisify } from 'node:util';
import type { Adapter, SourceObservation } from './base.js';
import type { AdapterConfig, Contact, Message, SyncEvent, Thread } from '../types.js';

const execFileAsync = promisify(execFile);

interface SignalCliCursor {
  lastFile?: string;
  lastPollAt?: string;
}

interface SignalCliGroup {
  id: string;
  name?: string;
}

interface SignalCliEnvelope {
  source?: string;
  sourceNumber?: string;
  sourceUuid?: string;
  sourceName?: string;
  sourceDevice?: number;
  timestamp?: number;
  dataMessage?: SignalCliDataMessage;
  syncMessage?: { sentMessage?: SignalCliDataMessage & {
    destination?: string;
    destinationNumber?: string;
    destinationUuid?: string;
  } };
}

interface SignalCliDataMessage {
  timestamp?: number;
  message?: string;
  groupInfo?: { groupId?: string };
  attachments?: unknown[];
  viewOnce?: boolean;
  expiresInSeconds?: number;
}

export interface SignalCliRunner {
  version(): Promise<string>;
  json(command: string, account?: string): Promise<unknown>;
  receiveToFile(outputPath: string, account: string): Promise<void>;
}

export class ProcessSignalCliRunner implements SignalCliRunner {
  constructor(
    private readonly binary: string,
    private readonly configDir: string,
  ) {}

  async version(): Promise<string> {
    const result = await execFileAsync(this.binary, ['--version'], {
      timeout: 10_000,
      encoding: 'utf-8',
    });
    return result.stdout.trim();
  }

  async json(command: string, account?: string): Promise<unknown> {
    const args = ['--config', this.configDir, '--output', 'json'];
    if (account) args.push('--account', account);
    args.push(command);
    const result = await execFileAsync(this.binary, args, {
      timeout: 30_000,
      maxBuffer: 20 * 1024 * 1024,
      encoding: 'utf-8',
    });
    const text = result.stdout.trim();
    return text ? JSON.parse(text) : null;
  }

  async receiveToFile(outputPath: string, account: string): Promise<void> {
    const fd = fs.openSync(outputPath, 'wx', 0o600);
    const args = [
      '--config', this.configDir,
      '--output', 'json',
      '--account', account,
      'receive',
      '--timeout', '5',
      '--ignore-attachments',
      '--ignore-avatars',
      '--ignore-stickers',
    ];

    try {
      await new Promise<void>((resolve, reject) => {
        const child = spawn(this.binary, args, {
          stdio: ['ignore', fd, 'pipe'],
        });
        let stderr = '';
        const timeout = setTimeout(() => child.kill('SIGTERM'), 30_000);
        child.stderr?.setEncoding('utf-8');
        child.stderr?.on('data', chunk => {
          stderr += String(chunk);
          if (stderr.length > 32_000) stderr = stderr.slice(-32_000);
        });
        child.on('error', reject);
        child.on('close', (code, signal) => {
          clearTimeout(timeout);
          if (code === 0) {
            resolve();
          } else {
            reject(new Error(
              `signal-cli receive failed (${signal ?? `exit ${code}`}): ${stderr.trim()}`,
            ));
          }
        });
      });
      fs.fsyncSync(fd);
    } catch (err) {
      fs.closeSync(fd);
      fs.rmSync(outputPath, { force: true });
      throw err;
    }
    fs.closeSync(fd);
  }
}

function resolveHome(value: string): string {
  return value.startsWith('~/')
    ? path.join(process.env.HOME ?? '', value.slice(2))
    : value;
}

function accountList(value: unknown): Array<{ number?: string; aci?: string }> {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object' && Array.isArray((value as { accounts?: unknown }).accounts)) {
    return (value as { accounts: Array<{ number?: string; aci?: string }> }).accounts;
  }
  return [];
}

function extractEnvelope(record: unknown): { envelope: SignalCliEnvelope; account?: string } | null {
  if (!record || typeof record !== 'object') return null;
  const root = record as Record<string, unknown>;
  const params = root.params && typeof root.params === 'object'
    ? root.params as Record<string, unknown>
    : null;
  const result = params?.result && typeof params.result === 'object'
    ? params.result as Record<string, unknown>
    : null;
  const envelope = root.envelope ?? params?.envelope ?? result?.envelope;
  if (!envelope || typeof envelope !== 'object') return null;
  const account = root.account ?? params?.account ?? result?.account;
  return {
    envelope: envelope as SignalCliEnvelope,
    account: typeof account === 'string' ? account : undefined,
  };
}

function safeId(value: string): string {
  return Buffer.from(value).toString('base64url');
}

export class SignalCliAdapter implements Adapter {
  platform = 'signal';
  private runner: SignalCliRunner | null;
  private dataDir = '';
  private spoolDir = '';
  private account = '';
  private groups = new Map<string, string>();
  private cursor: SignalCliCursor = {};
  private observation: SourceObservation | null = null;

  constructor(
    private readonly log: (message: string) => void = console.log,
    runner?: SignalCliRunner,
  ) {
    this.runner = runner ?? null;
  }

  async init(config: AdapterConfig): Promise<void> {
    this.dataDir = resolveHome(
      (config.data_dir as string | undefined) ?? '~/.claude/local/messages',
    );
    this.spoolDir = path.join(this.dataDir, 'raw', 'signal-cli');
    fs.mkdirSync(this.spoolDir, { recursive: true, mode: 0o700 });

    if (!this.runner) {
      const binary = resolveHome(
        (config.signal_cli_path as string | undefined) ?? '~/.local/bin/signal-cli',
      );
      const signalCliDataDir = resolveHome(
        (config.signal_cli_data_dir as string | undefined) ?? '~/.local/share/signal-cli',
      );
      this.runner = new ProcessSignalCliRunner(binary, signalCliDataDir);
    }

    const version = await this.runner.version();
    const accounts = accountList(await this.runner.json('listAccounts'));
    const configuredAccount = config.account as string | undefined;
    const available = accounts
      .map(item => item.number ?? item.aci)
      .filter((item): item is string => Boolean(item));

    if (configuredAccount) {
      if (!available.includes(configuredAccount)) {
        throw new Error(`[signal] signal-cli account is not linked: ${configuredAccount}`);
      }
      this.account = configuredAccount;
    } else if (available.length === 1) {
      this.account = available[0];
    } else if (available.length === 0) {
      throw new Error(
        '[signal] signal-cli has no linked account; run: signal-cli link --name "Legion Observation"',
      );
    } else {
      throw new Error('[signal] signal-cli has multiple accounts; configure signal.account');
    }

    const groups = await this.runner.json('listGroups', this.account);
    if (Array.isArray(groups)) {
      for (const group of groups as SignalCliGroup[]) {
        if (group.id) this.groups.set(group.id, group.name ?? 'Unnamed Group');
      }
    }
    this.log(`[signal] signal-cli ${version} ready for ${this.account}`);
  }

  async *sync(cursorString: string | null): AsyncGenerator<SyncEvent> {
    if (!this.runner || !this.account) {
      throw new Error('[signal] signal-cli adapter is not ready');
    }
    this.cursor = cursorString ? JSON.parse(cursorString) as SignalCliCursor : {};
    this.recoverInterruptedCaptures();

    const captureName = `capture-${Date.now()}-${randomUUID()}.jsonl`;
    const tempPath = path.join(this.spoolDir, `.${captureName}.tmp`);
    const capturePath = path.join(this.spoolDir, captureName);
    await this.runner.receiveToFile(tempPath, this.account);
    this.observation = {
      observed_at: new Date().toISOString(),
      evidence: 'signal-cli successful receive request',
    };

    if (fs.statSync(tempPath).size > 0) {
      fs.renameSync(tempPath, capturePath);
    } else {
      fs.rmSync(tempPath);
    }

    const pending = fs.readdirSync(this.spoolDir)
      .filter(name => name.endsWith('.jsonl') && (!this.cursor.lastFile || name > this.cursor.lastFile))
      .sort();

    if (!cursorString) {
      const now = new Date().toISOString();
      for (const [id, name] of this.groups) {
        yield {
          type: 'thread',
          data: {
            id: `signal:cli:group:${safeId(id)}`,
            platform: 'signal',
            title: name,
            thread_type: 'group',
            participants: [],
            metadata: { signalCliGroupId: id },
            created_at: now,
            updated_at: now,
          } satisfies Thread,
        };
      }
    }

    for (const fileName of pending) {
      const lines = fs.readFileSync(path.join(this.spoolDir, fileName), 'utf-8').split('\n');
      for (let index = 0; index < lines.length; index++) {
        const line = lines[index].trim();
        if (!line) continue;
        let record: unknown;
        try {
          record = JSON.parse(line);
        } catch (err) {
          throw new Error(`[signal] Invalid raw envelope ${fileName}:${index + 1}: ${String(err)}`);
        }
        for (const event of this.eventsForRecord(record)) yield event;
      }
      this.cursor.lastFile = fileName;
    }
    this.cursor.lastPollAt = this.observation.observed_at;
  }

  private recoverInterruptedCaptures(): void {
    for (const name of fs.readdirSync(this.spoolDir)) {
      if (!name.startsWith('.capture-') || !name.endsWith('.jsonl.tmp')) continue;
      const tempPath = path.join(this.spoolDir, name);
      if (fs.statSync(tempPath).size === 0) {
        fs.rmSync(tempPath);
        continue;
      }
      const captureName = name.slice(1, -'.tmp'.length);
      fs.renameSync(tempPath, path.join(this.spoolDir, captureName));
    }
  }

  private eventsForRecord(record: unknown): SyncEvent[] {
    const extracted = extractEnvelope(record);
    if (!extracted) return [];
    const envelope = extracted.envelope;
    const incoming = envelope.dataMessage;
    const outgoing = envelope.syncMessage?.sentMessage;
    const payload = incoming ?? outgoing;
    if (!payload) return [];

    const direction = outgoing ? 'sent' : 'received';
    const sourceId = envelope.sourceUuid ?? envelope.sourceNumber ?? envelope.source ?? 'unknown';
    const destinationId = outgoing?.destinationUuid ?? outgoing?.destinationNumber ?? outgoing?.destination;
    const peerId = direction === 'sent' ? destinationId ?? 'unknown' : sourceId;
    const groupId = payload.groupInfo?.groupId;
    const timestamp = payload.timestamp ?? envelope.timestamp;
    if (!timestamp) return [];

    const now = new Date().toISOString();
    const senderId = direction === 'sent' ? 'signal:user:self' : `signal:user:${sourceId}`;
    const threadId = groupId
      ? `signal:cli:group:${safeId(groupId)}`
      : `signal:cli:dm:${safeId(peerId)}`;
    const title = groupId
      ? this.groups.get(groupId) ?? 'Unnamed Group'
      : envelope.sourceName ?? envelope.sourceNumber ?? peerId;
    const contactId = `signal:user:${sourceId}`;
    const messageHash = createHash('sha256')
      .update(JSON.stringify({
        account: extracted.account ?? this.account,
        sourceId,
        sourceDevice: envelope.sourceDevice,
        timestamp,
        direction,
        groupId,
      }))
      .digest('hex')
      .slice(0, 24);

    const events: SyncEvent[] = [];
    if (direction === 'received') {
      events.push({
        type: 'contact',
        data: {
          id: contactId,
          platform: 'signal',
          display_name: envelope.sourceName ?? null,
          username: null,
          phone: envelope.sourceNumber ?? null,
          metadata: { serviceId: envelope.sourceUuid ?? null, source: 'signal-cli' },
          first_seen: now,
          last_seen: now,
        } satisfies Contact,
      });
    }
    events.push({
      type: 'thread',
      data: {
        id: threadId,
        platform: 'signal',
        title,
        thread_type: groupId ? 'group' : 'dm',
        participants: groupId ? [] : [direction === 'sent' ? `signal:user:${peerId}` : contactId],
        metadata: groupId ? { signalCliGroupId: groupId } : { signalCliPeerId: peerId },
        created_at: new Date(timestamp).toISOString(),
        updated_at: now,
      } satisfies Thread,
    });
    events.push({
      type: 'message',
      data: {
        id: `signal:cli:msg:${messageHash}`,
        platform: 'signal',
        thread_id: threadId,
        sender_id: senderId,
        content: payload.message ?? null,
        content_type: payload.attachments?.length ? 'document' : 'text',
        reply_to: null,
        direction,
        metadata: {
          source: 'signal-cli',
          viewOnce: payload.viewOnce ?? false,
          expiresInSeconds: payload.expiresInSeconds ?? 0,
          attachmentCount: payload.attachments?.length ?? 0,
        },
        platform_ts: new Date(timestamp).toISOString(),
        synced_at: now,
      } satisfies Message,
    });
    return events;
  }

  getCursor(): string {
    return JSON.stringify(this.cursor);
  }

  getSourceObservation(): SourceObservation | null {
    return this.observation;
  }

  async shutdown(): Promise<void> {
    this.log('[signal] signal-cli adapter shutdown');
  }
}
