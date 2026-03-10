// Message platform types for the unified messaging system

export interface Contact {
  id: string;               // 'telegram:user:12345'
  platform: string;
  display_name: string | null;
  username: string | null;
  phone: string | null;
  metadata: Record<string, unknown>;
  first_seen: string;
  last_seen: string;
}

export interface Thread {
  id: string;               // 'telegram:chat:-100123456'
  platform: string;
  title: string | null;
  thread_type: 'dm' | 'group' | 'channel' | 'supergroup';
  participants: string[];   // contact IDs
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface Message {
  id: string;               // 'telegram:msg:12345'
  platform: string;
  thread_id: string;
  sender_id: string;
  content: string | null;
  content_type: 'text' | 'photo' | 'document' | 'sticker' | 'voice' | 'video' | 'other';
  reply_to: string | null;
  metadata: Record<string, unknown>;
  platform_ts: string;      // ISO 8601
  synced_at: string;
}

export type SyncEventType = 'contact' | 'thread' | 'message';

export interface SyncEvent {
  type: SyncEventType;
  data: Contact | Thread | Message;
}

export interface AdapterConfig {
  enabled: boolean;
  poll_interval?: number;
  initial_days?: number;
  [key: string]: unknown;
}

export interface AppConfig {
  data_dir: string;
  adapters: Record<string, AdapterConfig>;
}

export interface SyncCursor {
  adapter: string;
  cursor_value: string;
  updated_at: string;
}
