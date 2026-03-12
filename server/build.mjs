import * as esbuild from 'esbuild';

const common = {
  bundle: true,
  platform: 'node',
  target: 'node20',
  format: 'esm',
  external: ['better-sqlite3', 'telegram', 'yaml', 'imapflow', 'mailparser', '@slack/web-api', '@slack/socket-mode'],
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
]);

console.log('Built daemon.mjs, mcp.mjs, and backfill.mjs');
