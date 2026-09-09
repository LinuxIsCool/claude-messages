import * as esbuild from 'esbuild';

const common = {
  bundle: true,
  platform: 'node',
  target: 'node20',
  format: 'esm',
  external: ['better-sqlite3', 'telegram', 'yaml', 'imapflow', 'mailparser', '@slack/web-api', '@slack/socket-mode', '@whiskeysockets/baileys', 'qrcode-terminal'],
  sourcemap: true,
  loader: { '.csv': 'text' },
};

await Promise.all([
  esbuild.build({
    ...common,
    entryPoints: ['src/daemon.ts'],
    outfile: 'build/daemon.mjs',
    banner: { js: "// claude-messages daemon — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/mcp.ts'],
    outfile: 'build/mcp.mjs',
    banner: { js: "// claude-messages MCP server — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/backfill.ts'],
    outfile: 'build/backfill.mjs',
    banner: { js: "// claude-messages Telegram backfill — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/backfill-email.ts'],
    outfile: 'build/backfill-email.mjs',
    banner: { js: "// claude-messages email Sent-folder backfill — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/import-whatsapp-export.ts'],
    outfile: 'build/import-whatsapp-export.mjs',
    banner: { js: "// WhatsApp chat export importer — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/import-google-contacts.ts'],
    outfile: 'build/import-google-contacts.mjs',
    banner: { js: "// Google Contacts address book importer — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/pair-whatsapp.ts'],
    outfile: 'build/pair-whatsapp.mjs',
    banner: { js: "// WhatsApp pairing — bundled with esbuild" },
  }),
  esbuild.build({
    ...common,
    entryPoints: ['src/seed-priority.ts'],
    outfile: 'build/seed-priority.mjs',
    banner: { js: "// claude-messages priority seed — bundled with esbuild" },
  }),
]);

console.log('Built daemon.mjs, mcp.mjs, backfill.mjs, backfill-email.mjs, import-whatsapp-export.mjs, import-google-contacts.mjs, pair-whatsapp.mjs, and seed-priority.mjs');
