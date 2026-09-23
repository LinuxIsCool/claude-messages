import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';
import { MessageDB } from './db.js';
import { EventLog } from './events.js';
import { TelegramAdapter } from './adapters/telegram.js';
import { SignalAdapter } from './adapters/signal.js';
import { EmailAdapter } from './adapters/email.js';
import { SlackAdapter } from './adapters/slack.js';
import { WhatsAppAdapter } from './adapters/whatsapp.js';
import type { Adapter } from './adapters/base.js';
import type { AppConfig, AdapterConfig, Contact, Thread, Message, SyncEvent, AdapterHealth, AdapterTier, DaemonHealth } from './types.js';
import { AwarenessEmitter, DesktopSink, StatuslineSink } from './awareness.js';

function resolveHome(p: string): string {
  if (p.startsWith('~/')) return path.join(process.env.HOME ?? '', p.slice(2));
  return p;
}

export class Daemon {
  private config: AppConfig;
  private db: MessageDB;
  private eventLog: EventLog;
  private adapters: Adapter[] = [];
  private running = false;
  private logFile: fs.WriteStream;

  // Health tracking
  private healthPath: string = '';
  private startedAt: string = '';
  private cycleCount: number = 0;
  private adapterHealth: Map<string, AdapterHealth> = new Map();
  private adapterInitErrors: Map<string, string> = new Map();
  private awareness: AwarenessEmitter;

  constructor() {
    const configPath = resolveHome('~/.claude/local/messages/config.yml');
    const configContent = fs.readFileSync(configPath, 'utf-8');
    this.config = parseYaml(configContent) as AppConfig;

    const dataDir = resolveHome(this.config.data_dir);
    fs.mkdirSync(path.join(dataDir, 'logs'), { recursive: true });

    this.db = new MessageDB(path.join(dataDir, 'messages.db'));
    this.awareness = new AwarenessEmitter(this.db, {
      config: this.config.awareness,
      desktop: new DesktopSink(),
      statusline: new StatuslineSink(path.join(dataDir, 'awareness.json')),
    });
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
      const adapter = new SignalAdapter((msg) => this.log(msg));
      // Keep Signal visible in health even when a static dependency is absent.
      this.adapters.push(adapter);
      try {
        await adapter.init({ ...adapterConfigs.signal, data_dir: dataDir } as AdapterConfig);
        this.log('Signal adapter initialized');
      } catch (err) {
        this.adapterInitErrors.set(adapter.platform, String(err));
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
      const adapter = new EmailAdapter((msg) => this.log(msg));
      // Keep the adapter registered even when its first network connection
      // fails. Its next sync cycle retries the retained account definitions.
      this.adapters.push(adapter);
      try {
        await adapter.init({ ...adapterConfigs.email, data_dir: dataDir } as AdapterConfig);
        this.log('Email adapter initialized');
      } catch (err) {
        this.adapterInitErrors.set(adapter.platform, String(err));
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

    // Initialize health tracking
    this.healthPath = path.join(dataDir, 'health.json');
    this.startedAt = new Date().toISOString();
    this.cycleCount = 0;
    for (const adapter of this.adapters) {
      this.adapterHealth.set(adapter.platform, this.initialHealth(adapter));
    }
    this.seedHealthFromPreviousRun();

    // Each adapter owns its polling loop. A slow network adapter must never
    // delay a local source or another network source.
    await Promise.all(this.startAdapterLoops());
  }

  // Per-adapter sync timeout (ms). Prevents one slow/hung adapter from blocking others.
  // Telegram with 881 dialogs typically takes 5-8 min; 10 min gives headroom.
  private static readonly ADAPTER_SYNC_TIMEOUT_MS = 10 * 60 * 1000;
  private static readonly DEFAULT_COOLDOWN_AFTER_FAILURES = 3;
  private static readonly DEFAULT_FAILURE_COOLDOWN_MINUTES = 60;

  /** Adapter dependency tiers: 0=local (Signal, WhatsApp), 2=network (Telegram, Email, Slack) */
  private static readonly ADAPTER_TIERS: Record<string, AdapterTier> = {
    signal: 0,
    whatsapp: 0,
    telegram: 2,
    email: 2,
    slack: 2,
  };

  private initialHealth(adapter: Adapter): AdapterHealth {
    const initError = this.adapterInitErrors.get(adapter.platform) ?? null;
    return {
      platform: adapter.platform,
      tier: Daemon.ADAPTER_TIERS[adapter.platform] ?? 2,
      last_success: null,
      last_failure: initError ? new Date().toISOString() : null,
      last_error: initError,
      last_duration_ms: 0,
      last_yield: { messages: 0, threads: 0, contacts: 0 },
      consecutive_failures: initError ? 1 : 0,
      timed_out: false,
      skipped: false,
      cooldown_until: null,
      source_observed_at: null,
      source_evidence: null,
    };
  }

  private seedHealthFromPreviousRun(): void {
    try {
      if (!fs.existsSync(this.healthPath)) return;
      const previous = JSON.parse(fs.readFileSync(this.healthPath, 'utf-8')) as Partial<DaemonHealth>;
      for (const [platform, current] of this.adapterHealth.entries()) {
        const previousHealth = previous.adapters?.[platform];
        if (!previousHealth) continue;
        const currentInitFailed = this.adapterInitErrors.has(platform);
        this.adapterHealth.set(platform, {
          ...current,
          last_success: previousHealth.last_success,
          last_failure: currentInitFailed ? current.last_failure : previousHealth.last_failure,
          last_error: currentInitFailed ? current.last_error : previousHealth.last_error,
          last_duration_ms: previousHealth.last_duration_ms,
          last_yield: previousHealth.last_yield,
          consecutive_failures: currentInitFailed
            ? Math.max(1, previousHealth.consecutive_failures)
            : previousHealth.consecutive_failures,
          timed_out: previousHealth.timed_out,
          skipped: previousHealth.skipped ?? false,
          cooldown_until: previousHealth.cooldown_until ?? null,
          source_observed_at: previousHealth.source_observed_at ?? null,
          source_evidence: previousHealth.source_evidence ?? null,
        });
      }
    } catch (err) {
      this.log(`Failed to seed health from previous run: ${err}`);
    }
  }

  private adapterConfigFor(platform: string): AdapterConfig | undefined {
    return this.config.adapters[platform] ?? this.config.adapters[platform.split(':')[0]];
  }

  private cooldownAfterFailures(platform: string): number {
    const configured = this.adapterConfigFor(platform)?.cooldown_after_failures;
    // Signal Desktop reconnects without intervention. Probe every cycle so a
    // recovered source is not hidden behind the generic one-hour cooldown.
    if (platform === 'signal' && typeof configured !== 'number') return 0;
    return typeof configured === 'number'
      ? configured
      : Daemon.DEFAULT_COOLDOWN_AFTER_FAILURES;
  }

  private failureCooldownMs(platform: string): number {
    const configured = this.adapterConfigFor(platform)?.failure_cooldown_minutes;
    const minutes = typeof configured === 'number'
      ? configured
      : Daemon.DEFAULT_FAILURE_COOLDOWN_MINUTES;
    return Math.max(1, minutes) * 60 * 1000;
  }

  private startFailureCooldown(platform: string, health: AdapterHealth): void {
    const afterFailures = this.cooldownAfterFailures(platform);
    if (afterFailures <= 0 || health.consecutive_failures < afterFailures) return;
    health.cooldown_until = new Date(Date.now() + this.failureCooldownMs(platform)).toISOString();
  }

  private shouldSkipForCooldown(platform: string, health: AdapterHealth): boolean {
    const afterFailures = this.cooldownAfterFailures(platform);
    if (afterFailures <= 0 || health.consecutive_failures < afterFailures) return false;

    if (!health.cooldown_until) {
      this.startFailureCooldown(platform, health);
    }

    const cooldownUntilMs = health.cooldown_until ? Date.parse(health.cooldown_until) : NaN;
    if (!Number.isFinite(cooldownUntilMs) || cooldownUntilMs <= Date.now()) {
      health.cooldown_until = null;
      return false;
    }

    health.skipped = true;
    health.timed_out = false;
    health.last_duration_ms = 0;
    health.last_yield = { messages: 0, threads: 0, contacts: 0 };
    health.last_error = `Skipped until ${health.cooldown_until} after ${health.consecutive_failures} consecutive failures`;
    this.log(`${platform} sync skipped: ${health.last_error}`);
    return true;
  }

  private pollIntervalMs(adapter: Adapter): number {
    const seconds = this.adapterConfigFor(adapter.platform)?.poll_interval ?? 60;
    return Math.max(1, seconds) * 1000;
  }

  private startAdapterLoops(): Promise<void>[] {
    return this.adapters.map(adapter => this.runAdapterLoop(adapter));
  }

  private async runAdapterLoop(adapter: Adapter): Promise<void> {
    const pollInterval = this.pollIntervalMs(adapter);
    while (this.running) {
      const attemptStart = Date.now();
      try {
        await this.syncOneAdapter(adapter);
      } catch (err) {
        this.log(`Error syncing ${adapter.platform}: ${err}`);
      }

      // SQLite operations above are synchronous and execute atomically on the
      // single Node event loop. Health writes are synchronous and atomically
      // renamed, so independently completing adapters cannot overlap a write.
      this.cycleCount++;
      this.writeHealth(Date.now() - attemptStart);
      try {
        this.awareness.emit();
      } catch (err) {
        this.log(`awareness emit error: ${err}`);  // must never break a sync loop
      }

      if (!this.running) break;
      await this.sleep(pollInterval);
    }
  }

  private writeHealth(cycleDurationMs: number): void {
    const health: DaemonHealth = {
      daemon: 'legion-messages',
      version: '2.2.0',
      pid: process.pid,
      started_at: this.startedAt,
      last_cycle: new Date().toISOString(),
      cycle_count: this.cycleCount,
      cycle_duration_ms: cycleDurationMs,
      adapters: Object.fromEntries(this.adapterHealth),
    };

    // Atomic write: temp file → rename (prevents partial reads by health checker)
    const tmpPath = this.healthPath + '.tmp';
    try {
      fs.writeFileSync(tmpPath, JSON.stringify(health, null, 2) + '\n');
      fs.renameSync(tmpPath, this.healthPath);
    } catch (err) {
      this.log(`Failed to write health.json: ${err}`);
    }
  }

  private async syncOneAdapter(adapter: Adapter): Promise<void> {
    const existingHealth = this.adapterHealth.get(adapter.platform);
    if (existingHealth && this.shouldSkipForCooldown(adapter.platform, existingHealth)) {
      return;
    }

    const cursor = this.db.getCursor(adapter.platform);
    this.log(`Syncing ${adapter.platform} (cursor: ${cursor ? 'exists' : 'none'})`);

    const syncStartMs = Date.now();
    let msgCount = 0;
    let contactCount = 0;
    let threadCount = 0;
    let timedOut = false;
    let syncError: unknown = null;

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
      syncError = err;
      if (timedOut) this.log(`${adapter.platform} sync timed out after processing ${msgCount} msgs`);
    } finally {
      // Clear timeout if sync finished before it fired
      if (timeoutTimer) clearTimeout(timeoutTimer);

      // Save successful progress. A failed zero-yield pass must not advance the
      // cursor timestamp and masquerade as source freshness.
      const newCursor = adapter.getCursor();
      const processedEvents = msgCount + contactCount + threadCount;
      if (newCursor && (!syncError || processedEvents > 0)) {
        this.db.updateCursor(adapter.platform, newCursor);
      }

      // Update adapter health
      const health = this.adapterHealth.get(adapter.platform);
      if (health) {
        const sourceObservation = adapter.getSourceObservation?.() ?? null;
        health.source_observed_at = sourceObservation?.observed_at ?? null;
        health.source_evidence = sourceObservation?.evidence ?? null;
        health.last_duration_ms = Date.now() - syncStartMs;
        health.last_yield = { messages: msgCount, threads: threadCount, contacts: contactCount };
        health.timed_out = timedOut;
        health.skipped = false;
        if (syncError) {
          health.last_failure = new Date().toISOString();
          health.last_error = timedOut
            ? `Timed out after ${Daemon.ADAPTER_SYNC_TIMEOUT_MS / 1000}s`
            : String(syncError);
          health.consecutive_failures++;
          this.startFailureCooldown(adapter.platform, health);
        } else {
          health.last_success = new Date().toISOString();
          health.last_error = null;
          health.consecutive_failures = 0;
          health.cooldown_until = null;
        }
      }

      if (syncError) {
        this.log(`${adapter.platform} sync failed after ${msgCount} msgs, ${threadCount} threads, ${contactCount} contacts: ${String(syncError)}`);
      } else {
        this.log(`${adapter.platform} sync complete: ${msgCount} msgs, ${threadCount} threads, ${contactCount} contacts`);
      }
    }

    if (syncError) throw syncError;
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

export function isMainModule(argvPath: string | undefined, moduleUrl: string): boolean {
  if (!argvPath) return false;
  try {
    return fs.realpathSync(argvPath) === fs.realpathSync(fileURLToPath(moduleUrl));
  } catch {
    return false;
  }
}

// The realpath comparison supports systemd entrypoints reached through plugin symlinks.
if (isMainModule(process.argv[1], import.meta.url)) {
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
}
