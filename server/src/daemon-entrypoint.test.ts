import { execFileSync, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';

const serverDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe('bundled daemon entrypoint', () => {
  it('starts when invoked through a symlink', () => {
    execFileSync(process.execPath, ['build.mjs'], { cwd: serverDir, stdio: 'pipe' });

    const testHome = fs.mkdtempSync(path.join(os.tmpdir(), 'messages-entrypoint-'));
    tempDirs.push(testHome);
    const configDir = path.join(testHome, '.claude', 'local', 'messages');
    fs.mkdirSync(configDir, { recursive: true });
    fs.writeFileSync(
      path.join(configDir, 'config.yml'),
      'data_dir: ~/.claude/local/messages\nadapters: {}\n',
    );
    // This deployed revision creates an index on `direction` before its ALTER
    // migration. Legion1 already has the column, so mirror that deployed state.
    const db = new Database(path.join(configDir, 'messages.db'));
    db.exec(`
      CREATE TABLE messages (
        id TEXT PRIMARY KEY,
        platform TEXT NOT NULL,
        thread_id TEXT,
        sender_id TEXT,
        platform_ts TEXT NOT NULL,
        direction TEXT DEFAULT 'unknown'
      )
    `);
    db.close();

    const symlinkPath = path.join(testHome, 'legion-messages-daemon.mjs');
    fs.symlinkSync(path.join(serverDir, 'build', 'daemon.mjs'), symlinkPath);

    const result = spawnSync(process.execPath, [symlinkPath], {
      env: { ...process.env, HOME: testHome },
      encoding: 'utf-8',
      timeout: 5_000,
    });

    expect(result.status, result.stderr).toBe(0);
    expect(result.stdout).toContain('Daemon starting');
  });
});
