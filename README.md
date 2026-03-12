# claude-messages

A Claude Code plugin that syncs your messaging platforms into a unified SQLite database with full-text search, exposed via MCP tools.

**Supported platforms:**
- **Telegram** — via Telegram Client API (MTProto)
- **Signal** — reads Signal Desktop's encrypted database directly (zero extra processes)
- **Email** — IMAP sync with multi-account support and RFC 2822 threading
- **Slack** — via Slack Web API (user token), multi-workspace support

All messages are searchable through FTS5 and queryable through MCP tools, giving Claude full conversational context across your communication channels.

## How It Works

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ Telegram │  │  Signal  │  │  Email   │  │  Slack   │
│ MTProto  │  │ Desktop  │  │  IMAP    │  │ Web API  │
└────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     └──────┬──────┴──────┬──────┴──────┬───────┘
            │             │             │
     ┌──────▼──────┐   ┌──▼────────────▼──┐
     │   Daemon    │   │    MCP Server    │
     │ (sync loop) │   │    (queries)     │
     └──────┬──────┘   └───────┬──────────┘
            │                  │
       ┌────▼──────────────────▼────┐
       │  SQLite + FTS5             │
       │  messages.db               │
       └────────────────────────────┘
```

The **daemon** runs as a systemd user service, polling each adapter on a configurable interval. It yields a unified stream of `Contact`, `Thread`, and `Message` events into SQLite with cursor-based incremental sync.

The **MCP server** runs via stdio transport, giving Claude read access to the database through structured tools.

## Installation

### 1. Install the plugin

```bash
claude plugin add /path/to/claude-messages
```

Or clone and install from source:

```bash
git clone https://github.com/LinuxIsCool/claude-messages.git
cd claude-messages/server
npm install
npm run build
claude plugin add /path/to/claude-messages
```

### 2. Create the data directory

```bash
mkdir -p ~/.claude/local/messages/{secrets,logs,events}
```

### 3. Configure adapters

Create `~/.claude/local/messages/config.yml`:

```yaml
data_dir: ~/.claude/local/messages

adapters:
  telegram:
    enabled: false        # Set true + add credentials
    poll_interval: 60     # seconds
    initial_days: 30      # how far back on first sync

  signal:
    enabled: false        # Set true + install sqlcipher + extract key
    poll_interval: 60
    db_path: ~/.config/Signal/sql/db.sqlite

  email:
    enabled: false        # Set true + add IMAP credentials
    poll_interval: 300
    initial_days: 30
    folder: INBOX
    accounts:
      - id: personal
        name: "Personal"
      # - id: work
      #   name: "Work"

  slack:
    enabled: false        # Set true + add user token
    poll_interval: 300
    initial_days: 365
    workspaces:
      - id: myworkspace   # short identifier
        name: "My Workspace"
```

Enable only the adapters you want. Each has its own setup below.

### 4. Set up the daemon

Create `~/.config/systemd/user/claude-messages.service`:

```ini
[Unit]
Description=Claude Messages Daemon
After=network-online.target

[Service]
Type=simple
ExecStart=/path/to/node /path/to/claude-messages/server/build/daemon.mjs
Restart=on-failure
RestartSec=10
Environment=HOME=%h

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now claude-messages
```

## Platform Setup

### Telegram

**Requirements:** Telegram API credentials ([my.telegram.org/apps](https://my.telegram.org/apps))

Create `~/.claude/local/messages/secrets/telegram.env`:

```bash
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_STRING_SESSION=your_session_string_here
```

To generate a session string, use the [telegram](https://www.npmjs.com/package/telegram) library's `StringSession`:

```javascript
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import readline from 'readline';

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
const ask = (q) => new Promise(r => rl.question(q, r));

const client = new TelegramClient(new StringSession(''), API_ID, API_HASH, {});
await client.start({
  phoneNumber: () => ask('Phone: '),
  password: () => ask('2FA password: '),
  phoneCode: () => ask('Code: '),
  onError: console.error,
});

console.log('Session string:', client.session.save());
```

Set `telegram.enabled: true` in config.yml, then restart the daemon.

### Signal

**Requirements:**
- Signal Desktop installed and running (any OS with Signal Desktop)
- `sqlcipher` CLI tool

**How it works:** Signal Desktop maintains a SQLCipher-encrypted SQLite database with your full message history. This adapter reads it directly — no extra processes, no signal-cli, no Java runtime. Just a transient `sqlcipher` CLI call (~30ms) on each poll cycle.

**Step 1: Install sqlcipher**

```bash
# Arch/CachyOS
sudo pacman -S sqlcipher

# Ubuntu/Debian
sudo apt install sqlcipher

# macOS
brew install sqlcipher

# Verify
sqlcipher --version
```

**Step 2: Extract the database encryption key**

Signal Desktop encrypts its database with a key stored in your OS keychain via Electron's `safeStorage` API. You need to extract it once.

Save this as `extract-signal-key.js`:

```javascript
const { app, safeStorage } = require('electron');
const fs = require('fs');
const path = require('path');

app.whenReady().then(() => {
  try {
    const configPath = path.join(process.env.HOME, '.config', 'Signal', 'config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));

    if (!config.encryptedKey) {
      console.error('No encryptedKey in Signal config.json');
      app.exit(1);
      return;
    }

    if (!safeStorage.isEncryptionAvailable()) {
      console.error('Electron safeStorage not available (keychain locked?)');
      app.exit(1);
      return;
    }

    const decryptedKey = safeStorage.decryptString(
      Buffer.from(config.encryptedKey, 'hex')
    );

    const envPath = path.join(
      process.env.HOME, '.claude', 'local', 'messages', 'secrets', 'signal.env'
    );
    fs.writeFileSync(envPath, `SIGNAL_DB_KEY=x'${decryptedKey}'\n`, { mode: 0o600 });
    console.log(`Key written to ${envPath}`);
    app.exit(0);
  } catch (err) {
    console.error('Error:', err.message);
    app.exit(1);
  }
});
```

Run it:

```bash
electron --no-sandbox extract-signal-key.js
```

This writes the hex key to `~/.claude/local/messages/secrets/signal.env`. The key only changes if you reinstall Signal Desktop.

**Step 3:** Set `signal.enabled: true` in config.yml, restart the daemon.

**Verify:**

```bash
# Check daemon log
journalctl --user -u claude-messages --since "5 min ago" | grep signal
# Should show: [signal] DB access verified — XXXXX messages in Signal Desktop
```

**Notes:**
- Signal Desktop uses WAL mode — unlimited concurrent readers, so read-only access is always safe
- The `db_path` default is `~/.config/Signal/sql/db.sqlite` (Linux). On macOS: `~/Library/Application Support/Signal/sql/db.sqlite`
- On macOS, `safeStorageBackend` is Keychain. On KDE, it's KWallet. On GNOME, it's libsecret. The Electron script handles all backends.

### Email

**Requirements:** IMAP-enabled email accounts with app passwords

Create `~/.claude/local/messages/secrets/email.env`:

```bash
# Format: IMAP_{ACCOUNT_ID}_HOST, IMAP_{ACCOUNT_ID}_USER, IMAP_{ACCOUNT_ID}_PASSWORD
# Account IDs must match the `id` field in config.yml

IMAP_PERSONAL_HOST=imap.gmail.com
IMAP_PERSONAL_USER=you@gmail.com
IMAP_PERSONAL_PASSWORD=your_app_password

IMAP_WORK_HOST=imap.fastmail.com
IMAP_WORK_USER=you@company.com
IMAP_WORK_PASSWORD=your_app_password
```

For Gmail: generate an app password at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).

Set `email.enabled: true` and configure your `accounts` list in config.yml, then restart the daemon.

### Slack

**Requirements:** A Slack user token (`xoxp-...`) with appropriate scopes.

**Step 1: Create a Slack app** at [api.slack.com/apps](https://api.slack.com/apps)

Required OAuth scopes (User Token):
- `channels:history`, `channels:read` — public channels
- `groups:history`, `groups:read` — private channels
- `im:history`, `im:read` — direct messages
- `mpim:history`, `mpim:read` — multi-party DMs
- `users:read`, `users:read.email` — user profiles and email metadata

**Step 2: Install to workspace** and copy the User OAuth Token (`xoxp-...`).

**Step 3: Create the secret file**

Create `~/.claude/local/messages/secrets/slack_WORKSPACE_ID.env`:

```bash
SLACK_USER_TOKEN=xoxp-your-token-here
```

Where `WORKSPACE_ID` matches the `id` field in your config.yml (e.g., `slack_myworkspace.env`).

**Step 4:** Set `slack.enabled: true` in config.yml, then restart the daemon.

**Notes:**
- User tokens see all channels the user belongs to without needing `/invite`
- V1 uses poll-based sync with cursor tracking — incremental after first backfill
- Archived channels are synced on first run but skipped on incremental syncs
- Thread replies are fetched separately after channel messages
- Contacts with email in their Slack profile are auto-linked to email-platform identities
- **Known V1 limitation:** New replies to old thread parents (outside the incremental window) are missed until V2 Socket Mode adds real-time events

## MCP Tools

Once the daemon has synced, Claude has access to these tools:

### Messaging

| Tool | Description |
|------|-------------|
| `search_messages` | Full-text search with FTS5 syntax (`"exact phrase"`, `word1 OR word2`, `NOT excluded`) |
| `recent_messages` | Get the latest messages across all platforms |
| `get_thread` | Get messages from a specific conversation by thread ID |
| `list_threads` | List conversation threads, optionally filtered by platform |
| `message_stats` | Aggregate statistics: counts by platform, date range, totals |

### Identity Resolution

The same person often appears as separate contacts on each platform. Identity resolution groups them into unified person records with confidence-scored links.

| Tool | Description |
|------|-------------|
| `auto_resolve` | Run multi-pass cross-platform matching — phone, email, name, metadata email, Signal UUID dedup, nickname, fuzzy |
| `resolve_contact` | Look up the unified identity for a given platform contact |
| `who_is` | Fuzzy search across names, phones, usernames — returns identity cards for linked contacts, raw contacts for unlinked |
| `link_identities` | Manually link a contact to an identity (creates identity if no ID provided) |
| `unlink_identity` | Remove a platform link from an identity |
| `merge_identities` | Merge two identities (source absorbed into target) |
| `list_identities` | Browse and search unified identities |
| `get_identity` | Full identity card with linked platforms, message stats, and audit events |
| `unlinked_contacts` | Audit unlinked contacts sorted by message activity |
| `identity_health` | Diagnostic view of identity resolution coverage |
| `update_identity` | Update an identity's display name or notes |
| `cleanup_identities` | Remove orphaned identities with zero links |
| `identity_relationships` | Who talks to whom — shared thread participation |
| `merge_suggestions` | Surface ambiguous name matches for human review |
| `export_identities` | Bulk export all identities for plugin integration |

### ContactRank (Relationship Scoring)

| Tool | Description |
|------|-------------|
| `relationship_score` | Full ContactRank score breakdown — 8 factors, composite, Dunbar layer, confidence |
| `inner_circle` | Contacts ranked by relationship strength, grouped by Dunbar layer |
| `fading_relationships` | Detect unusually silent contacts — inner circle members first |
| `refresh_scores` | Recompute all ContactRank scores |
| `set_dunbar_override` | Manual Dunbar layer override — survives re-scoring |

**How auto-resolution works:** Multi-pass identity resolution with decreasing confidence: phone numbers (0.95), email username (0.9), metadata email (0.9), cross-platform name matching (0.75), Signal UUID dedup, nickname matching, and fuzzy matching. The algorithm is idempotent — running it twice produces zero changes. All operations are logged to an events table for auditability.

### Search examples

```
"quarterly report"          # Exact phrase
kubernetes OR k8s           # Either term
budget NOT draft            # Exclude term
deploy*                     # Prefix match
```

## Slash Commands

| Command | Description |
|---------|-------------|
| `/messages <query>` | Search messages with formatted output |
| `/messages-status` | Daemon health, sync status, database stats |

## Architecture

### Adapter Interface

All platform adapters implement the same interface:

```typescript
interface Adapter {
  platform: string;
  init(config: AdapterConfig): Promise<void>;
  sync(cursor: string | null): AsyncGenerator<SyncEvent>;
  getCursor(): string | null;
  shutdown(): Promise<void>;
}
```

The `sync()` method is an async generator that yields a unified stream of events:

```typescript
type SyncEvent = {
  type: 'contact' | 'thread' | 'message';
  data: Contact | Thread | Message;
};
```

### ID Namespacing

All entity IDs use `platform:type:identifier` format:

```
telegram:user:12345              # Telegram user
signal:user:<uuid>               # Signal user (by service ID)
email:user:alice@co.com          # Email address
slack:<team>:user:<uid>          # Slack user
slack:<team>:bot:<bid>           # Slack bot

telegram:chat:-100123456         # Telegram group
signal:conv:<uuid>               # Signal conversation
email:thread:acct:hash           # Email thread (RFC 2822)
slack:<team>:channel:<cid>       # Slack channel/DM/MPIM

telegram:msg:12345:678           # Telegram message
signal:msg:<uuid>                # Signal message
email:msg:acct:uid               # Email message
slack:<team>:<cid>:<ts>          # Slack message (channel:timestamp)
```

### Database

SQLite with WAL mode and FTS5. Tables: `contacts`, `threads`, `messages`, `sync_cursors`, `identities`, `identity_links`, `identity_events`. The FTS5 virtual table uses Porter stemming with Unicode61 tokenization.

### Two-Process Model

- **`daemon.mjs`** — Long-running sync service (systemd). Polls adapters, writes to SQLite.
- **`mcp.mjs`** — Stateless MCP server (stdio). Reads from SQLite, serves tools to Claude.

Both are compiled from TypeScript via esbuild.

## Adding a New Adapter

1. Create `server/src/adapters/yourplatform.ts` implementing the `Adapter` interface
2. Import and instantiate it in `server/src/daemon.ts` (follow the pattern of existing adapters)
3. Add configuration to your `config.yml`
4. Run `npm run build` in `server/`

The adapter just needs to yield `Contact`, `Thread`, and `Message` events from its `sync()` generator. The daemon handles storage, cursor management, and scheduling.

## Development

```bash
cd server

# Install dependencies
npm install

# Build (esbuild — fast)
npm run build

# Dev mode with watch
npm run dev

# Test MCP server (should output tool list then exit)
timeout 3 node build/mcp.mjs 2>&1 || true

# Run unit tests (vitest)
npm test
```

## File Structure

```
claude-messages/
├── .claude-plugin/
│   └── plugin.json           # Plugin metadata
├── .mcp.json                 # MCP server configuration
├── server/
│   ├── build.mjs             # esbuild bundler
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── daemon.ts          # Sync daemon
│       ├── mcp.ts             # MCP tool server
│       ├── db.ts              # SQLite + FTS5
│       ├── types.ts           # Shared types
│       ├── events.ts          # JSONL audit log
│       ├── fuzzy.ts           # Jaro-Winkler, nickname matching
│       └── adapters/
│           ├── base.ts        # Adapter interface
│           ├── telegram.ts    # Telegram (MTProto)
│           ├── signal.ts      # Signal Desktop (SQLCipher)
│           ├── email.ts       # Email (IMAP)
│           ├── slack.ts       # Slack (Web API)
│           └── slack.test.ts  # Adapter unit tests
├── skills/                    # Claude Code skills
├── commands/                  # Slash commands
├── hooks/                     # Session hooks
└── agents/                    # Planned: entity extraction
```

## License

MIT
