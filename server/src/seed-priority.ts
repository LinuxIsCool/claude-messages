import path from 'node:path';
import { MessageDB } from './db.js';

function resolveHome(p: string): string {
  return p.startsWith('~/') ? path.join(process.env.HOME ?? '', p.slice(2)) : p;
}

const dataDir = process.env.MESSAGES_DATA_DIR ?? resolveHome('~/.claude/local/messages');
const db = new MessageDB(path.join(dataDir, 'messages.db'));

// Idempotent: only create cohorts if absent.
const existing = new Set(db.listCohorts().map(c => c.name));
for (const name of ['Telus', 'Indigenomics Capstone']) {
  if (!existing.has(name)) { db.createCohort(name); console.log(`created cohort: ${name}`); }
}

const n = db.rescoreAllPriority();
console.log(`rescored ${n} messages`);
console.log('Next: resolve thread/identity ids and add rules via MCP tools (see docs/priority-setup.md).');
