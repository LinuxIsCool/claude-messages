# Always-on Signal observation

## Decision

Use `signal-cli` as a dedicated linked device on the always-on server. The
message adapter invokes `receive` directly, stores every non-empty response as
an immutable JSONL capture before parsing, and advances its cursor only after
the database accepts the events.

Signal Desktop under Xvfb was rejected after live proof. The server profile's
database key is protected by KWallet6. KWallet needs an interactive unlock
after reboot, so that design cannot provide unattended recovery. `signal-cli`
is built for servers and keeps independent linked-device credentials without a
desktop keyring.

## Install

From this repository on the server:

```bash
bash scripts/install-signal-cli-user.sh
```

The installer downloads the current native release to `~/.local/opt`, links it
from `~/.local/bin`, and configures the message service to select the
`signal-cli` backend. It does not link Signal or restart the message daemon.

## One-time human action

Run this in an interactive terminal connected to the server:

```bash
~/.local/bin/signal-cli link --name "Legion Observation"
```

The command prints a QR code. On the phone, open Signal Settings, select
**Linked devices**, select **Link New Device**, and scan that QR code. Do not
remove the existing laptop or desktop devices. Keep the command running until
it reports that linking completed.

## Activate and verify

```bash
systemctl --user restart legion-messages.service
sleep 10
jq '.adapters.signal | {
  last_success,
  last_failure,
  consecutive_failures,
  source_observed_at,
  source_evidence
}' ~/.claude/local/messages/health.json
bash scripts/check-messages-health.sh
```

Healthy Signal observation requires:

```text
source_evidence = signal-cli successful receive request
consecutive_failures = 0
```

A successful empty `receive` is valid source access. A readable frozen Signal
Desktop database is not.

## Recovery and custody

Raw non-empty responses are retained in:

```text
~/.claude/local/messages/raw/signal-cli/
```

If the daemon stops after receiving but before storing an event, the next cycle
replays every capture newer than the committed cursor. Message IDs are stable,
so replay is idempotent.

`signal-cli` must be updated within three months of a release because Signal
server compatibility changes. Re-run the installer monthly and verify the
version plus one successful source observation.

## Rollback

Remove the `LEGION_SIGNAL_BACKEND` line from the user service drop-in, reload
the user manager, and restart `legion-messages.service`. Do not delete the
`signal-cli` account data. Keeping it preserves queued messages and makes
rollback reversible.
