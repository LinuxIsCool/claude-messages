import fs from 'node:fs';
import path from 'node:path';
import { parse as parseYaml } from 'yaml';
import { MessageDB } from './db.js';
import { EventLog } from './events.js';
import { TelegramAdapter } from './adapters/telegram.js';
import { SignalAdapter } from './adapters/signal.js';
import { EmailAdapter } from './adapters/email.js';
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

  private async syncAll(): Promise<void> {
    for (const adapter of this.adapters) {
      try {
        const cursor = this.db.getCursor(adapter.platform);
        this.log(`Syncing ${adapter.platform} (cursor: ${cursor ? 'exists' : 'none'})`);

        let msgCount = 0;
        let contactCount = 0;
        let threadCount = 0;

        for await (const event of adapter.sync(cursor)) {
          this.processEvent(event);
          if (event.type === 'message') msgCount++;
          else if (event.type === 'contact') contactCount++;
          else if (event.type === 'thread') threadCount++;
        }

        // Save updated cursor
        const newCursor = adapter.getCursor();
        if (newCursor) {
          this.db.updateCursor(adapter.platform, newCursor);
        }

        this.log(`${adapter.platform} sync complete: ${msgCount} msgs, ${threadCount} threads, ${contactCount} contacts`);
      } catch (err) {
        this.log(`Error syncing ${adapter.platform}: ${err}`);
      }
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
      const timer = setTimeout(resolve, ms);
      // Allow shutdown to interrupt sleep
      const check = setInterval(() => {
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
