import fs from 'node:fs';
import path from 'node:path';
import { parse as parseYaml } from 'yaml';
import { MessageDB } from './db.js';
import { EventLog } from './events.js';
import { TelegramAdapter } from './adapters/telegram.js';
import { SignalAdapter } from './adapters/signal.js';
import { EmailAdapter } from './adapters/email.js';
import { SlackAdapter } from './adapters/slack.js';
import { WhatsAppAdapter } from './adapters/whatsapp.js';
import type { Adapter } from './adapters/base.js';
import type { AppConfig, AdapterConfig, Contact, Thread, Message, SyncEvent } from './types.js';

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

class Daemon {
  private config: AppConfig;
  private db: MessageDB;
  private eventLog: EventLog;
  private adapters: Adapter[] = [];
  private running = false;
  private logFile: fs.WriteStream;

  constructor() {
    const configPath = resolveHome('~/.claude/local/messages/config.yml');
    const configContent = fs.readFileSync(configPath, 'utf-8');
    this.config = parseYaml(configContent) as AppConfig;

    const dataDir = resolveHome(this.config.data_dir);
    fs.mkdirSync(path.join(dataDir, 'logs'), { recursive: true });

    this.db = new MessageDB(path.join(dataDir, 'messages.db'));
    this.eventLog = new EventLog(path.join(dataDir, 'events'));
    this.logFile = fs.createWriteStream(path.join(dataDir, 'logs', 'daemon.log'), { flags: 'a' });
  }

  log(msg: string): void {
    const line = `[${new Date().toISOString()}] ${msg}`;
    this.logFile.write(line + '\n');
    console.log(line);
  }

  async start(): Promise<void> {
    this.log('Daemon starting');
    this.running = true;

    // Initialize adapters
    const adapterConfigs = this.config.adapters;
    const dataDir = resolveHome(this.config.data_dir);

    // Signal FIRST — reads from local SQLite, zero network dependency, instant sync.
    // Must run before network-dependent adapters to avoid head-of-line blocking.
    if (adapterConfigs.signal?.enabled) {
      try {
        const adapter = new SignalAdapter((msg) => this.log(msg));
        await adapter.init({ ...adapterConfigs.signal, data_dir: dataDir } as AdapterConfig);
        this.adapters.push(adapter);
        this.log('Signal adapter initialized');
      } catch (err) {
        this.log(`Signal adapter failed to initialize: ${err}`);
      }
    }

    // Telegram after Signal — network-dependent, can take 5-10 min for 881 dialogs
    if (adapterConfigs.telegram?.enabled) {
      try {
        const adapter = new TelegramAdapter((msg) => this.log(msg));
        await adapter.init({ ...adapterConfigs.telegram, data_dir: dataDir } as AdapterConfig);
        this.adapters.push(adapter);
        this.log('Telegram adapter initialized');
      } catch (err) {
        this.log(`Telegram adapter failed to initialize: ${err}`);
      }
    }

    if (adapterConfigs.email?.enabled) {
      try {
        const adapter = new EmailAdapter((msg) => this.log(msg));
        await adapter.init({ ...adapterConfigs.email, data_dir: dataDir } as AdapterConfig);
        this.adapters.push(adapter);
        this.log('Email adapter initialized');
      } catch (err) {
        this.log(`Email adapter failed to initialize: ${err}`);
      }
    }

    if (adapterConfigs.slack?.enabled) {
      const workspaces = (adapterConfigs.slack.workspaces as Array<{ id: string; name: string }>) ?? [];
      for (const ws of workspaces) {
        try {
          const adapter = new SlackAdapter((msg) => this.log(msg));
          await adapter.init({
            ...adapterConfigs.slack,
            workspace_id: ws.id,
            workspace_name: ws.name,
            data_dir: dataDir,
          } as AdapterConfig);
          this.adapters.push(adapter);
          this.log(`Slack adapter initialized: ${ws.name} (${ws.id})`);
        } catch (err) {
          this.log(`Slack adapter failed for ${ws.id}: ${err}`);
        }
      }
    }

    if (adapterConfigs.whatsapp?.enabled) {
      try {
        const adapter = new WhatsAppAdapter((msg) => this.log(msg));
        await adapter.init({ ...adapterConfigs.whatsapp, data_dir: dataDir } as AdapterConfig);
        this.adapters.push(adapter);
        this.log('WhatsApp adapter initialized');
      } catch (err) {
        this.log(`WhatsApp adapter failed to initialize: ${err}`);
      }
    }

    // Initial sync
    await this.syncAll();

    // Poll loop — use shortest enabled adapter interval
    const intervals = Object.values(adapterConfigs)
      .filter(c => c?.enabled)
      .map(c => (c?.poll_interval ?? 60) * 1000);
    const pollInterval = Math.min(...intervals, 60000);

    while (this.running) {
      await this.sleep(pollInterval);
      if (!this.running) break;
      await this.syncAll();
    }
  }

  // Per-adapter sync timeout (ms). Prevents one slow/hung adapter from blocking others.
  // Telegram with 881 dialogs typically takes 5-8 min; 10 min gives headroom.
  private static readonly ADAPTER_SYNC_TIMEOUT_MS = 10 * 60 * 1000;

  private async syncAll(): Promise<void> {
    for (const adapter of this.adapters) {
      try {
        await this.syncOneAdapter(adapter);
      } catch (err) {
        this.log(`Error syncing ${adapter.platform}: ${err}`);
      }
    }
  }

  private async syncOneAdapter(adapter: Adapter): Promise<void> {
    const cursor = this.db.getCursor(adapter.platform);
    this.log(`Syncing ${adapter.platform} (cursor: ${cursor ? 'exists' : 'none'})`);

    let msgCount = 0;
    let contactCount = 0;
    let threadCount = 0;
    let timedOut = false;

    // Capture the generator so we can cancel it on timeout
    const gen = adapter.sync(cursor);

    // Race the sync generator against a timeout
    let timeoutTimer: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutTimer = setTimeout(() => {
        timedOut = true;
        // Signal the generator to stop — async generators honor .return()
        gen.return(undefined as never);
        reject(new Error(`${adapter.platform} sync timed out after ${Daemon.ADAPTER_SYNC_TIMEOUT_MS / 1000}s`));
      }, Daemon.ADAPTER_SYNC_TIMEOUT_MS);
      // Don't keep the process alive just for this timer
      if (timeoutTimer.unref) timeoutTimer.unref();
    });

    const syncWork = async () => {
      for await (const event of gen) {
        // Respect shutdown requests mid-sync (break triggers gen.return() automatically)
        if (!this.running) break;
        this.processEvent(event);
        if (event.type === 'message') msgCount++;
        else if (event.type === 'contact') contactCount++;
        else if (event.type === 'thread') threadCount++;
      }
    };

    try {
      await Promise.race([syncWork(), timeoutPromise]);
    } catch (err) {
      // Re-throw non-timeout errors; timeout is handled below via finally
      if (!timedOut) throw err;
      this.log(`${adapter.platform} sync timed out after processing ${msgCount} msgs`);
    } finally {
      // Clear timeout if sync finished before it fired
      if (timeoutTimer) clearTimeout(timeoutTimer);

      // Always save cursor — even partial progress avoids re-processing on next cycle
      const newCursor = adapter.getCursor();
      if (newCursor) {
        this.db.updateCursor(adapter.platform, newCursor);
      }

      const suffix = timedOut ? ' (partial — timed out)' : '';
      this.log(`${adapter.platform} sync complete: ${msgCount} msgs, ${threadCount} threads, ${contactCount} contacts${suffix}`);
    }
  }

  private processEvent(event: SyncEvent): void {
    switch (event.type) {
      case 'contact':
        this.db.upsertContact(event.data as Contact);
        this.eventLog.append('contact.upsert', event.data);
        {
          const linkResult = this.db.tryAutoLink(event.data as Contact);
          if (linkResult.linked) {
            this.log(`[identity] Auto-linked ${(event.data as Contact).id} → ${linkResult.identity_id}`);
          }
        }
        break;
      case 'thread':
        this.db.upsertThread(event.data as Thread);
        this.eventLog.append('thread.upsert', event.data);
        break;
      case 'message': {
        const inserted = this.db.insertMessage(event.data as Message);
        if (inserted) {
          this.eventLog.append('message.insert', event.data);
        }
        break;
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => {
      let check: ReturnType<typeof setInterval>;
      const timer = setTimeout(() => {
        clearInterval(check);
        resolve();
      }, ms);
      // Allow shutdown to interrupt sleep
      check = setInterval(() => {
        if (!this.running) {
          clearTimeout(timer);
          clearInterval(check);
          resolve();
        }
      }, 1000);
    });
  }

  async shutdown(): Promise<void> {
    this.log('Daemon shutting down');
    this.running = false;
    for (const adapter of this.adapters) {
      await adapter.shutdown();
    }
    this.db.close();
    this.logFile.end();
  }
}

// Main
const daemon = new Daemon();

process.on('SIGTERM', async () => {
  await daemon.shutdown();
  process.exit(0);
});
process.on('SIGINT', async () => {
  await daemon.shutdown();
  process.exit(0);
});

daemon.start().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
