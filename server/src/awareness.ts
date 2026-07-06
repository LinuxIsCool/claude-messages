import { execFile } from 'node:child_process';
import { writeFileSync, renameSync } from 'node:fs';
import type { InboxEntry, AwarenessCounts, AwarenessConfig } from './types.js';
import type { MessageDB } from './db.js';

export type QuietHours = { start: string; end: string };

function hhmm(d: Date): number {
  return d.getHours() * 60 + d.getMinutes();
}
function parseHM(s: string): number {
  const [h, m] = s.split(':').map(Number);
  return h * 60 + m;
}

export function isQuietHours(now: Date, quiet?: QuietHours): boolean {
  if (!quiet) return false;
  const t = hhmm(now);
  const start = parseHM(quiet.start);
  const end = parseHM(quiet.end);
  if (start === end) return false;
  if (start < end) return t >= start && t < end;      // same-day window
  return t >= start || t < end;                        // window crosses midnight
}

export function dedupeByThread(entries: InboxEntry[]): InboxEntry[] {
  const seen = new Set<string>();
  const out: InboxEntry[] = [];
  for (const e of entries) {
    const key = e.thread_id ?? e.message_id;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(e);
  }
  return out;
}

export function formatNotification(entry: InboxEntry, senderName: string): { title: string; body: string } {
  const body = (entry.content ?? '').trim().slice(0, 140);
  return { title: `⚡ ${senderName}`, body };
}

export type Spawner = (cmd: string, args: string[]) => void;

const defaultSpawner: Spawner = (cmd, args) => {
  // fire-and-forget; swallow errors so a missing notify-send can't crash the daemon
  execFile(cmd, args, () => {});
};

export class DesktopSink {
  private spawn: Spawner;
  constructor(spawn: Spawner = defaultSpawner) {
    this.spawn = spawn;
  }
  notify(title: string, body: string): void {
    this.spawn('notify-send', ['--urgency=critical', '--app-name=Messages', title, body]);
  }
}

export class StatuslineSink {
  constructor(private filePath: string) {}
  write(counts: AwarenessCounts): void {
    const payload = JSON.stringify({ ...counts, updated_at: new Date().toISOString() });
    // atomic write: temp file + rename so a statusline reader never sees a partial file
    const tmp = `${this.filePath}.tmp`;
    writeFileSync(tmp, payload);
    renameSync(tmp, this.filePath);
  }
}

export interface EmitterOpts {
  config?: AwarenessConfig;
  desktop: DesktopSink;
  statusline: StatuslineSink;
  now?: () => Date;
}

export class AwarenessEmitter {
  constructor(private db: MessageDB, private opts: EmitterOpts) {}

  emit(): void {
    const cfg = this.opts.config ?? {};
    const now = (this.opts.now ?? (() => new Date()))();

    // Statusline: always publish current counts (unless explicitly disabled).
    if (cfg.statusline?.enabled !== false) {
      this.opts.statusline.write(this.db.awarenessCounts());
    }

    // Desktop: gated by enabled flags + quiet hours.
    const awarenessOn = cfg.enabled !== false;
    const desktopOn = cfg.desktop?.enabled !== false;
    if (!awarenessOn || !desktopOn) return;
    if (isQuietHours(now, cfg.desktop?.quiet_hours)) return;

    const criticals = this.db.getUnnotifiedCritical();
    if (criticals.length === 0) return;

    const names = this.db.resolveContactNames(
      [...new Set(criticals.map(c => c.sender_id).filter(Boolean) as string[])]
    );
    for (const e of dedupeByThread(criticals)) {
      const name = names.get(e.sender_id ?? '') ?? e.sender_id ?? 'Someone';
      const { title, body } = formatNotification(e, name);
      this.opts.desktop.notify(title, body);
    }

    // Mark ALL fetched criticals notified (incl. same-thread ones we collapsed),
    // so a thread that already pinged doesn't re-ping next cycle for old messages.
    this.db.markNotified(criticals.map(c => c.message_id));
  }
}
