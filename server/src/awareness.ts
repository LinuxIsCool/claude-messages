import type { InboxEntry } from './types.js';

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
