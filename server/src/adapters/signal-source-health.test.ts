import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  assertSignalSourceFresh,
  latestAuthenticatedKeepalive,
} from './signal-source-health.js';

const tempDirs: string[] = [];

function logLine(time: string, msg: string): string {
  return JSON.stringify({ level: 30, time, msg });
}

function writeLog(lines: string[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'signal-source-health-'));
  tempDirs.push(dir);
  const logPath = path.join(dir, 'app.log');
  fs.writeFileSync(logPath, `${lines.join('\n')}\n`);
  return logPath;
}

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

describe('Signal source freshness', () => {
  const keepalive =
    '[WebsocketResources] WebSocketResources.KeepAlive(WebSocketResource(authenticated)).send: Sending a keepalive message';

  it('accepts a recent authenticated websocket keepalive as source evidence', () => {
    const now = Date.parse('2026-09-23T16:00:00.000Z');
    const logPath = writeLog([logLine('2026-09-23T15:59:30.000Z', keepalive)]);

    expect(assertSignalSourceFresh(logPath, 180_000, now)).toEqual({
      observed_at: '2026-09-23T15:59:30.000Z',
      evidence: 'signal-desktop authenticated websocket keepalive',
    });
  });

  it('rejects a readable but disconnected source whose keepalive is stale', () => {
    const now = Date.parse('2026-09-23T16:00:00.000Z');
    const logPath = writeLog([logLine('2026-09-23T15:40:00.000Z', keepalive)]);

    expect(() => assertSignalSourceFresh(logPath, 180_000, now)).toThrow(
      'authenticated websocket is stale',
    );
  });

  it('does not treat unauthenticated keepalives as linked-device access', () => {
    const logPath = writeLog([
      logLine(
        '2026-09-23T15:59:30.000Z',
        '[WebsocketResources] WebSocketResources.KeepAlive(WebSocketResource(unauthenticated)).send: Sending a keepalive message',
      ),
    ]);

    expect(latestAuthenticatedKeepalive(fs.readFileSync(logPath, 'utf-8'))).toBeNull();
    expect(() => assertSignalSourceFresh(logPath, 180_000)).toThrow(
      'no authenticated websocket keepalive',
    );
  });

  it('rejects a missing log instead of reporting an empty poll as success', () => {
    expect(() => assertSignalSourceFresh('/missing/signal/app.log', 180_000)).toThrow(
      'Signal source log not found',
    );
  });
});
