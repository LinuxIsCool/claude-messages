/**
 * WhatsApp pairing (task-885 N3).
 *
 * The adapter is written, imported and tiered; the only thing between Legion
 * and WhatsApp is one QR code that has to be scanned by a phone. The daemon can
 * print that QR, but it prints it into the journal, where every line carries a
 * timestamp prefix and the block characters no longer line up into something a
 * camera can read. So pairing gets its own terminal.
 *
 * Writes the same `whatsapp-auth` directory the daemon reads, so once this
 * succeeds the adapter connects on the daemon's next start with no QR at all.
 *
 * Usage:
 *   node build/pair-whatsapp.mjs            # show a QR, wait for the scan
 *   node build/pair-whatsapp.mjs --status   # paired, half-paired or not paired
 *
 * Safe to re-run. If a scan already happened but the link did not complete, it
 * resumes from the stored credentials without asking for another code.
 *
 * On the phone: WhatsApp > Settings > Linked devices > Link a device.
 *
 * Run this while the daemon's WhatsApp adapter is disabled. Two Baileys clients
 * sharing one auth directory fight over the same Signal session and both lose.
 */

import fs from 'node:fs';
import path from 'node:path';
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState,
  type ConnectionState,
} from '@whiskeysockets/baileys';
// @ts-ignore — qrcode-terminal has no types
import qrcode from 'qrcode-terminal';

const AUTH_DIR = path.join(process.env.HOME ?? '', '.claude/local/messages/whatsapp-auth');
const CREDS = path.join(AUTH_DIR, 'creds.json');

/**
 * Whether the phone is actually linked.
 *
 * Not the same question as whether `creds.json` exists. `useMultiFileAuthState`
 * writes that file the moment it loads, before any QR has been shown, so an
 * existence check calls an unpaired directory paired and then refuses every
 * retry. `registered` is the flag WhatsApp sets once the link completes; `me`
 * and `account` appear earlier, partway through the handshake.
 */
function credsState(): { exists: boolean; registered: boolean; broken: boolean; me: string | null } {
  if (!fs.existsSync(CREDS)) return { exists: false, registered: false, broken: false, me: null };
  const raw = fs.readFileSync(CREDS, 'utf-8');
  if (!raw.trim()) return { exists: true, registered: false, broken: true, me: null };
  try {
    const c = JSON.parse(raw);
    return { exists: true, registered: c?.registered === true, broken: false, me: c?.me?.id ?? null };
  } catch {
    return { exists: true, registered: false, broken: true, me: null };
  }
}

/**
 * Move a dead auth directory aside rather than reusing or deleting it.
 *
 * An empty or unparseable `creds.json` cannot be recovered: the pre-key files
 * beside it are worthless without the noise key and identity in that file. But
 * it is still the wreckage of a real pairing, so it is archived rather than
 * removed, and the next run starts clean instead of resuming from nothing.
 */
function archiveBrokenAuth(): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const dest = `${AUTH_DIR}.broken-${stamp}`;
  fs.renameSync(AUTH_DIR, dest);
  return dest;
}

/**
 * One connection attempt. Resolves 'paired', 'restart' or throws.
 *
 * `restart` is the normal path, not an error: WhatsApp closes the socket with
 * status 515 immediately after a successful scan and expects the client to
 * reconnect with the credentials it just wrote. Treating that close as the end
 * of the story leaves `registered: false` and a directory that looks paired to
 * anything checking for the file.
 */
async function attempt(showQr: boolean): Promise<'paired' | 'restart'> {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version } = await fetchLatestBaileysVersion();
  const silent = {
    level: 'silent' as const, child: () => silent,
    trace: () => {}, debug: () => {}, info: () => {}, warn: () => {}, error: () => {}, fatal: () => {},
  };

  const sock = makeWASocket({
    version,
    auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, silent as any) },
    logger: silent as any,
    markOnlineOnConnect: false,
    // No history here. This process exists to establish credentials; the daemon
    // does the reading, and asking for the archive twice would fetch it twice.
    syncFullHistory: false,
  });

  // Track writes in flight. `saveCreds` is async and fires around the same
  // moment the connection opens, so exiting on 'open' truncates the file it is
  // still writing. That is exactly how a completed pairing was destroyed on
  // 2026-09-09: 500 pre-keys on disk and a zero-byte creds.json.
  let inFlight = 0;
  sock.ev.on('creds.update', () => {
    inFlight++;
    Promise.resolve(saveCreds()).catch(() => { /* reported by the state check */ }).finally(() => { inFlight--; });
  });
  const settled = async () => {
    for (let i = 0; i < 100 && inFlight > 0; i++) await new Promise(r => setTimeout(r, 100));
    await new Promise(r => setTimeout(r, 500));   // one more beat for the last write to land
  };

  try {
    return await new Promise<'paired' | 'restart'>((resolve, reject) => {
      sock.ev.on('connection.update', (update: Partial<ConnectionState>) => {
        const { connection, lastDisconnect, qr } = update;
        if (qr && showQr) {
          console.log('\nWhatsApp > Settings > Linked devices > Link a device, then scan:\n');
          qrcode.generate(qr, { small: true });
          console.log('\n(the code refreshes every 20 seconds until you scan it)');
        }
        if (connection === 'open') { void settled().then(() => resolve('paired')); return; }
        if (connection === 'close') {
          const code = (lastDisconnect?.error as any)?.output?.statusCode;
          if (code === DisconnectReason.restartRequired) return resolve('restart');
          if (code === DisconnectReason.loggedOut) {
            return reject(new Error('WhatsApp rejected the link. Delete ' + AUTH_DIR + ' and scan again.'));
          }
          // A scan that got as far as writing an account is worth one reconnect
          // even when the close carries no status we recognise.
          if (credsState().me) return resolve('restart');
          return reject(new Error(`Connection closed before pairing (status ${code ?? 'unknown'})`));
        }
      });
    });
  } finally {
    await settled();
    try { sock.end(undefined); } catch { /* the socket is already going away */ }
    await settled();
  }
}

async function main(): Promise<void> {
  let before = credsState();

  if (before.broken && !process.argv.includes('--status')) {
    const dest = archiveBrokenAuth();
    console.log(`Previous credentials were unusable (empty or unparseable creds.json).`);
    console.log(`Moved to ${dest}. Starting a fresh pairing, so a new scan is needed.`);
    before = credsState();
  }

  if (process.argv.includes('--status')) {
    if (before.registered) {
      console.log(`paired as ${before.me ?? 'unknown'}`);
      console.log('Set adapters.whatsapp.enabled: true in config.yml and restart legion-messages.');
    } else if (before.broken) {
      console.log(`unusable: ${CREDS} is empty or unparseable`);
      console.log('Run this command with no arguments; it will archive the directory and ask for a new scan.');
    } else if (before.exists) {
      console.log(`half-paired: ${CREDS} exists but registered=false` + (before.me ? ` (scanned as ${before.me})` : ''));
      console.log('Run this command with no arguments to finish; no new scan is needed.');
    } else {
      console.log(`not paired: no ${CREDS}`);
      console.log('Run this command with no arguments and scan the QR.');
    }
    return;
  }

  if (before.registered) {
    console.log(`Already paired as ${before.me ?? 'unknown'}.`);
    console.log(`Delete ${AUTH_DIR} first if you want to pair a different phone.`);
    return;
  }

  if (before.me) {
    console.log(`Resuming a scan that did not finish (${before.me}). No new QR needed.`);
  }

  // Up to four attempts: the first shows a QR if one is needed, the rest are
  // the reconnects WhatsApp asks for after the link is accepted.
  for (let i = 0; i < 4; i++) {
    const outcome = await attempt(i === 0 && !credsState().me);
    const now = credsState();
    // Success is what the file says, never what the socket said. A connection
    // that opened and left `registered: false` behind is not a pairing.
    if (now.registered) {
      console.log(`\nPaired as ${now.me ?? 'unknown'}.`);
      console.log('Next: set adapters.whatsapp.enabled: true in ~/.claude/local/messages/config.yml');
      console.log('      systemctl --user restart legion-messages');
      return;
    }
    if (outcome === 'paired') {
      console.log('Connected, but the stored credentials still say registered=false; reconnecting...');
    }
    console.log('WhatsApp asked for a reconnect (this is normal right after a scan); reconnecting...');
    await new Promise(r => setTimeout(r, 1500));
  }

  const end = credsState();
  if (end.registered) { console.log(`\nPaired as ${end.me}.`); return; }
  throw new Error(`Did not complete after four attempts (registered=${end.registered}). Re-run to archive and start over.`);
}

main().catch(err => { console.error(String(err?.message ?? err)); process.exit(1); });
