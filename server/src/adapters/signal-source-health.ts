import fs from 'node:fs';

export interface SignalSourceObservation {
  observed_at: string;
  evidence: string;
}

const AUTHENTICATED_KEEPALIVE =
  /WebSocketResources\.KeepAlive\(WebSocketResource\(authenticated\)\)\.send: Sending a keepalive message/;

export function readLogTail(logPath: string, maxBytes = 2 * 1024 * 1024): string {
  const stat = fs.statSync(logPath);
  const bytes = Math.min(stat.size, maxBytes);
  if (bytes === 0) return '';

  const fd = fs.openSync(logPath, 'r');
  try {
    const buffer = Buffer.allocUnsafe(bytes);
    fs.readSync(fd, buffer, 0, bytes, stat.size - bytes);
    return buffer.toString('utf-8');
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Find the newest authenticated Signal websocket keepalive.
 *
 * A database query alone only proves that a frozen local cache is readable.
 * Signal Desktop writes this record every roughly 30 seconds while its
 * authenticated server connection is alive, including when no messages arrive.
 */
export function latestAuthenticatedKeepalive(logText: string): number | null {
  let latest: number | null = null;

  for (const line of logText.split('\n')) {
    if (!AUTHENTICATED_KEEPALIVE.test(line)) continue;
    try {
      const record = JSON.parse(line) as { time?: unknown };
      if (typeof record.time !== 'string') continue;
      const timestamp = Date.parse(record.time);
      if (Number.isFinite(timestamp) && (latest === null || timestamp > latest)) {
        latest = timestamp;
      }
    } catch {
      // Ignore incomplete boundary lines from a bounded tail read.
    }
  }

  return latest;
}

export function assertSignalSourceFresh(
  logPath: string,
  maxAgeMs: number,
  nowMs = Date.now(),
): SignalSourceObservation {
  if (!fs.existsSync(logPath)) {
    throw new Error(`Signal source log not found: ${logPath}`);
  }

  const observedMs = latestAuthenticatedKeepalive(readLogTail(logPath));
  if (observedMs === null) {
    throw new Error(`Signal source has no authenticated websocket keepalive: ${logPath}`);
  }

  const ageMs = nowMs - observedMs;
  if (ageMs < -60_000) {
    throw new Error(`Signal source observation is ${Math.abs(ageMs)}ms in the future`);
  }
  if (ageMs > maxAgeMs) {
    throw new Error(
      `Signal source authenticated websocket is stale: ${Math.round(ageMs / 1000)}s old ` +
      `(maximum ${Math.round(maxAgeMs / 1000)}s)`,
    );
  }

  return {
    observed_at: new Date(observedMs).toISOString(),
    evidence: 'signal-desktop authenticated websocket keepalive',
  };
}
