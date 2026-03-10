---
name: messages-status
description: Show messaging daemon and sync health
---

Check the legion-messages daemon status:

1. Run: `systemctl --user status legion-messages`
2. Use the `message_stats` MCP tool for database statistics
3. Check the last 20 lines of the daemon log: `tail -20 ~/.claude/local/messages/logs/daemon.log`
4. Show sync cursor status from the database

Present a clear health dashboard showing:
- Daemon: running/stopped, uptime, memory usage
- Database: message count, thread count, contact count, by platform
- Last sync: timestamp and result from log
- Disk: size of messages.db and events/ directory
