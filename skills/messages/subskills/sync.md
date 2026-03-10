---
name: sync
description: Sync management and troubleshooting
---

# Message Sync Management

## Daemon Control
```bash
systemctl --user start legion-messages    # Start daemon
systemctl --user stop legion-messages     # Stop daemon
systemctl --user restart legion-messages  # Restart
systemctl --user status legion-messages   # Check status
journalctl --user -u legion-messages -f   # Follow logs
```

## Troubleshooting
1. **Daemon won't start**: Check `~/.claude/local/messages/logs/daemon.log`
2. **Telegram auth failed**: StringSession may have expired. Re-auth needed.
3. **No new messages**: Check sync cursor in database, verify poll interval
4. **Rate limited**: Telegram FloodWaitError — daemon handles this automatically

## Manual Sync
Stop daemon, run manually for debugging:
```bash
systemctl --user stop legion-messages
cd ~/.claude/plugins/cache/legion-plugins/plugins/claude-messages/server
node build/daemon.mjs
```
