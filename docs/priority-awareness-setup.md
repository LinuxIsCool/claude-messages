# Priority awareness setup (A1)

The daemon writes unseen critical/exceptional counts to
`~/.claude/local/messages/awareness.json` every sync cycle, e.g.:
`{ "critical": 2, "exceptional": 5, "updated_at": "2026-07-06T…Z" }`
and fires a desktop notification (via `notify-send`) once per critical thread.

## Config (`~/.claude/local/messages/config.yml`)
```yaml
awareness:
  enabled: true            # master switch (default true)
  desktop:
    enabled: true          # desktop notifications for CRITICAL messages (default true)
    quiet_hours:           # optional; skip desktop in this window (local time)
      start: "22:00"
      end: "07:00"
  statusline:
    enabled: true          # write awareness.json (default true)
```
Omit the whole block to accept defaults (all on, no quiet hours). Restart the
daemon after editing: `systemctl --user restart legion-messages`.

## Statusline wiring (claude-statusline is a separate plugin)
Add a segment that reads the JSON and prints a glanceable count. Example shell
snippet (prints `⚡2 !5` when there are unseen items, nothing when clear):
```bash
f=~/.claude/local/messages/awareness.json
[ -f "$f" ] && python3 -c "import json,sys; d=json.load(open('$f')); c,e=d.get('critical',0),d.get('exceptional',0); print((f'⚡{c} ' if c else '')+(f'!{e}' if e else ''), end='')"
```
Marking messages seen (`priority_mark_seen`, or viewing the priority inbox)
clears the counts on the next cycle.
