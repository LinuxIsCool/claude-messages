import fs from 'node:fs';
import path from 'node:path';

export interface AuditEvent {
  type: string;
  data: unknown;
  ts: string;
}

export class EventLog {
  private eventsDir: string;

  constructor(eventsDir: string) {
    this.eventsDir = eventsDir;
    fs.mkdirSync(eventsDir, { recursive: true });
  }

  append(type: string, data: unknown): void {
    const now = new Date();
    const filename = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}.jsonl`;
    const filepath = path.join(this.eventsDir, filename);
    const event: AuditEvent = { type, data, ts: now.toISOString() };
    fs.appendFileSync(filepath, JSON.stringify(event) + '\n');
  }
}
