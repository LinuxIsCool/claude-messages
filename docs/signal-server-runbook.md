# Always-on Signal observation

## Decision

Run the already-linked Signal Desktop profile on the always-on server under a
private Xvfb display. Keep the existing SQLCipher importer. This is smaller and
safer than adding a second Signal client and a second message mapping.

`signal-cli` is a viable later replacement, but it requires a new linked-device
QR scan, stores separate credentials, and produces live envelopes that need a
new importer. It cannot reuse Signal Desktop's linked-device state. The current
server already has a linked Signal Desktop profile and its encrypted history.

## Install

From this repository on the server:

```bash
bash scripts/install-signal-headless-user.sh
```

The installer downloads Xvfb into `~/.local/opt` when it is not installed
system-wide. It installs user services. It does not read, replace, link, or
unlink Signal credentials, and it does not start Signal Desktop.

Add these settings to the existing Signal adapter block in
`~/.claude/local/messages/config.yml`:

```yaml
  signal:
    enabled: true
    poll_interval: 60
    db_path: ~/.config/Signal/sql/db.sqlite
    source_log_path: ~/.config/Signal/logs/app.log
    source_max_age_seconds: 180
    cooldown_after_failures: 0
```

Then start and enable the source:

```bash
systemctl --user enable --now claude-signal-desktop.service
systemctl --user restart legion-messages.service
```

## Verify the real source

Wait up to three minutes, then run:

```bash
systemctl --user is-active claude-signal-desktop.service legion-messages.service
jq '.adapters.signal | {
  last_success,
  last_failure,
  consecutive_failures,
  source_observed_at,
  source_evidence
}' ~/.claude/local/messages/health.json
bash scripts/check-messages-health.sh
```

The lane is healthy only when `source_observed_at` is recent and
`source_evidence` says `signal-desktop authenticated websocket keepalive`.
Reading a frozen `db.sqlite` does not count as source access.

## Failure behavior

The Signal Desktop service restarts after a crash. The message daemon probes
the authenticated websocket evidence every poll without a failure cooldown.
If Signal Desktop is disconnected, Signal sync turns red while email and
Telegram continue independently.

## Rollback

```bash
systemctl --user disable --now claude-signal-desktop.service
rm ~/.config/systemd/user/legion-messages.service.d/signal-source.conf
systemctl --user daemon-reload
systemctl --user restart legion-messages.service
```

This leaves the Signal Desktop profile and message database unchanged.

## Human action only if the existing link has expired

Open Signal Desktop in a graphical session on the server. On the phone, open
Signal Settings, select **Linked devices**, select **Link New Device**, and scan
the QR code. Do not remove the existing laptop device. This is not required
while the server's existing profile still authenticates.
