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
 *   node build/pair-whatsapp.mjs --status   # report whether a session exists
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

function haveSession(): boolean {
  return fs.existsSync(path.join(AUTH_DIR, 'creds.json'));
}

async function main(): Promise<void> {
  if (process.argv.includes('--status')) {
    console.log(haveSession()
      ? `paired: ${AUTH_DIR}/creds.json exists\nSet adapters.whatsapp.enabled: true in config.yml and restart legion-messages.`
      : `not paired: no ${AUTH_DIR}/creds.json\nRun this command with no arguments and scan the QR.`);
    return;
  }

  if (haveSession()) {
    console.log(`Already paired (${AUTH_DIR}/creds.json).`);
    console.log('Delete that directory first if you want to pair a different phone.');
    return;
  }

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

  sock.ev.on('creds.update', saveCreds);

  await new Promise<void>((resolve, reject) => {
    sock.ev.on('connection.update', (update: Partial<ConnectionState>) => {
      const { connection, lastDisconnect, qr } = update;
      if (qr) {
        console.log('\nWhatsApp > Settings > Linked devices > Link a device, then scan:\n');
        qrcode.generate(qr, { small: true });
        console.log('\n(the code refreshes every 20 seconds until you scan it)');
      }
      if (connection === 'open') {
        console.log(`\nPaired as ${sock.user?.id ?? 'unknown'}.`);
        console.log('Next: set adapters.whatsapp.enabled: true in ~/.claude/local/messages/config.yml');
        console.log('      systemctl --user restart legion-messages');
        resolve();
      }
      if (connection === 'close') {
        const code = (lastDisconnect?.error as any)?.output?.statusCode;
        if (code === DisconnectReason.loggedOut) return reject(new Error('WhatsApp rejected the pairing (logged out)'));
        if (!haveSession()) return reject(new Error(`Connection closed before pairing (status ${code ?? 'unknown'})`));
        resolve();
      }
    });
  });

  try { sock.end(undefined); } catch { /* the socket is already going away */ }
  process.exit(0);
}

main().catch(err => { console.error(String(err?.message ?? err)); process.exit(1); });
