// Phase 1: Human-readable text formatters for MCP tool output

import type { Message, Thread } from './types.js';

export type OutputFormat = 'text' | 'json' | 'compact';

interface FormatOpts {
  format: OutputFormat;
  header?: string;
}

interface ThreadInfo {
  title: string | null;
  thread_type: string;
  message_count: number;
}

const PLATFORM_ABBREV: Record<string, string> = {
  telegram: 'TG',
  signal: 'SIG',
  email: 'EMAIL',
  slack: 'SLACK',
  whatsapp: 'WA',
};

function abbrevPlatform(platform: string): string {
  return PLATFORM_ABBREV[platform] ?? platform.toUpperCase();
}

function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const hh = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd} ${hh}:${min}`;
  } catch {
    return iso;
  }
}

function truncate(text: string | null, maxLen: number): string {
  if (!text) return '';
  const clean = text.replace(/\n/g, ' ').trim();
  if (clean.length <= maxLen) return clean;
  return clean.slice(0, maxLen) + '...';
}

function threadTypeLabel(type: string | null | undefined): string {
  if (!type) return '';
  if (type === 'dm') return 'DM';
  if (type === 'group') return 'Group';
  if (type === 'channel') return 'Channel';
  if (type === 'supergroup') return 'Group';
  return type;
}

/**
 * Format a list of messages for search/recent results.
 */
export function formatMessages(
  messages: Message[],
  names: Map<string, string>,
  threadInfoMap: Map<string, ThreadInfo>,
  opts: FormatOpts,
): string {
  if (opts.format === 'json') {
    return JSON.stringify(messages.map(m => ({
      id: m.id,
      thread: m.thread_id,
      sender: m.sender_id,
      sender_name: names.get(m.sender_id) ?? null,
      content: m.content,
      type: m.content_type,
      direction: m.direction,
      time: m.platform_ts,
    })), null, 2);
  }

  if (opts.format === 'compact') {
    const lines: string[] = [];
    if (opts.header) lines.push(`--- ${opts.header} ---`);
    for (const m of messages) {
      const name = names.get(m.sender_id) ?? m.sender_id;
      const plat = abbrevPlatform(m.platform);
      const tInfo = threadInfoMap.get(m.thread_id);
      const tType = tInfo ? `/${threadTypeLabel(tInfo.thread_type)}` : '';
      const ts = formatTimestamp(m.platform_ts);
      const body = truncate(m.content, 100);
      lines.push(`${ts} ${name} [${plat}${tType}] "${body}"`);
    }
    return lines.join('\n');
  }

  // text format (default)
  const lines: string[] = [];
  if (opts.header) lines.push(`=== ${opts.header} ===`, '');

  for (let i = 0; i < messages.length; i++) {
    const m = messages[i];
    const name = names.get(m.sender_id) ?? m.sender_id;
    const plat = abbrevPlatform(m.platform);
    const tInfo = threadInfoMap.get(m.thread_id);
    const tType = tInfo ? ` ${threadTypeLabel(tInfo.thread_type)}` : '';
    const dir = m.direction ? ` (${m.direction})` : '';
    const ts = formatTimestamp(m.platform_ts);
    const body = truncate(m.content, 200);

    lines.push(`${i + 1}. [${name} | ${plat}${tType} | ${ts}]${dir}`);
    lines.push(`   "${body}"`);

    if (tInfo) {
      const tTitle = tInfo.title ?? m.thread_id;
      const extra = tInfo.message_count > 1 ? ` | ${tInfo.message_count} messages in thread` : '';
      lines.push(`   -> Thread: ${tTitle}${extra}`);
    }
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}

/**
 * Format a list of threads.
 */
export function formatThreadList(
  threads: Array<{ id: string; title: string | null; thread_type: string; platform: string; updated_at: string }>,
  opts: FormatOpts,
): string {
  if (opts.format === 'json') {
    return JSON.stringify(threads.map(t => ({
      id: t.id,
      title: t.title,
      type: t.thread_type,
      platform: t.platform,
      updated: t.updated_at,
    })), null, 2);
  }

  if (opts.format === 'compact') {
    const lines: string[] = [];
    if (opts.header) lines.push(`--- ${opts.header} ---`);
    for (const t of threads) {
      const ts = formatTimestamp(t.updated_at);
      lines.push(`${t.title ?? t.id} [${abbrevPlatform(t.platform)}/${threadTypeLabel(t.thread_type)}] updated ${ts}`);
    }
    return lines.join('\n');
  }

  // text
  const lines: string[] = [];
  if (opts.header) lines.push(`=== ${opts.header} ===`, '');
  for (let i = 0; i < threads.length; i++) {
    const t = threads[i];
    const ts = formatTimestamp(t.updated_at);
    lines.push(`${i + 1}. ${t.title ?? t.id} [${abbrevPlatform(t.platform)}/${threadTypeLabel(t.thread_type)}] -- updated ${ts}`);
  }
  return lines.join('\n').trimEnd();
}

/**
 * Format a single thread with its messages (for get_thread).
 */
export function formatThread(
  thread: { id: string; title: string | null; thread_type: string; platform: string; participants: string[] } | null,
  messages: Message[],
  names: Map<string, string>,
  participantNames: Map<string, string>,
  opts: FormatOpts,
): string {
  if (opts.format === 'json') {
    return JSON.stringify({
      thread: thread ? {
        id: thread.id,
        title: thread.title,
        type: thread.thread_type,
        platform: thread.platform,
        participants: thread.participants.map(p => ({ id: p, name: participantNames.get(p) ?? null })),
      } : null,
      messages: messages.map(m => ({
        id: m.id,
        sender: m.sender_id,
        sender_name: names.get(m.sender_id) ?? null,
        content: m.content,
        type: m.content_type,
        direction: m.direction,
        time: m.platform_ts,
      })),
    }, null, 2);
  }

  const lines: string[] = [];

  if (thread) {
    const tType = threadTypeLabel(thread.thread_type);
    const header = `${thread.title ?? thread.id} [${abbrevPlatform(thread.platform)} ${tType}] -- ${messages.length} messages`;
    lines.push(`=== ${header} ===`);

    const pNames = thread.participants
      .map(p => participantNames.get(p) ?? p)
      .slice(0, 20);
    if (pNames.length > 0) {
      lines.push(`Participants: ${pNames.join(', ')}`);
    }
    lines.push('');
  }

  if (opts.format === 'compact') {
    for (const m of messages) {
      const name = names.get(m.sender_id) ?? m.sender_id;
      const ts = formatTimestamp(m.platform_ts);
      const body = truncate(m.content, 100);
      lines.push(`[${ts}] ${name}: "${body}"`);
    }
    return lines.join('\n');
  }

  // text
  for (const m of messages) {
    const name = names.get(m.sender_id) ?? m.sender_id;
    const ts = formatTimestamp(m.platform_ts);
    const dir = m.direction ? ` (${m.direction})` : '';
    lines.push(`[${ts}] ${name}${dir}:`);
    if (m.content) {
      lines.push(`  ${m.content.replace(/\n/g, '\n  ')}`);
    }
    lines.push('');
  }

  return lines.join('\n').trimEnd();
}

/**
 * Format context around a specific message (for get_message_context).
 * The target message is highlighted with >>> markers.
 */
export function formatContext(
  target: Message,
  messages: Message[],
  thread: { id: string; title: string | null; thread_type: string; platform: string } | null,
  names: Map<string, string>,
  opts: FormatOpts,
): string {
  if (opts.format === 'json') {
    return JSON.stringify({
      thread: thread ? {
        id: thread.id,
        title: thread.title,
        type: thread.thread_type,
        platform: thread.platform,
      } : null,
      target_id: target.id,
      messages: messages.map(m => ({
        id: m.id,
        sender: m.sender_id,
        sender_name: names.get(m.sender_id) ?? null,
        content: m.content,
        type: m.content_type,
        direction: m.direction,
        time: m.platform_ts,
        is_target: m.id === target.id,
      })),
    }, null, 2);
  }

  const lines: string[] = [];
  const tTitle = thread?.title ?? target.thread_id;
  lines.push(`=== Context: ${tTitle} ===`, '');

  for (const m of messages) {
    const name = names.get(m.sender_id) ?? m.sender_id;
    const ts = formatTimestamp(m.platform_ts);
    const isTarget = m.id === target.id;
    const marker = isTarget ? '>>> ' : '    ';
    const dir = m.direction ? ` (${m.direction})` : '';

    if (opts.format === 'compact') {
      lines.push(`${marker}[${ts}] ${name}: "${truncate(m.content, 100)}"`);
    } else {
      lines.push(`${marker}[${ts}] ${name}${dir}:`);
      if (m.content) {
        lines.push(`${marker}  ${m.content.replace(/\n/g, `\n${marker}  `)}`);
      }
      if (isTarget) lines.push(`${marker}  ^ TARGET MESSAGE`);
      lines.push('');
    }
  }

  return lines.join('\n').trimEnd();
}
