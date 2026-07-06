import { execFile } from 'node:child_process';
import { writeFileSync, renameSync } from 'node:fs';
import type { InboxEntry, AwarenessCounts } from './types.js';

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
