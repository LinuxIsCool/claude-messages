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

// --- Identity Resolution ---

export interface Identity {
  id: string;
  display_name: string;
  notes: string | null;
  metadata: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface IdentityLink {
  id: number;
  identity_id: string;
  platform: string;
  platform_id: string;
  display_name: string | null;
  username: string | null;
  confidence: number;
  source: string;
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface IdentityEvent {
  id: number;
  event_type: string;
  identity_id: string;
  details: Record<string, unknown>;
  created_at: string;
}

export interface IdentityCard {
  id: string;
  display_name: string;
  notes: string | null;
  platforms: Array<{
    platform: string;
    platform_id: string;
    display_name: string | null;
    username: string | null;
    confidence: number;
    source: string;
    contact_id: string;
  }>;
  stats: {
    total_messages: number;
    platforms_active: number;
    first_seen: string | null;
    last_seen: string | null;
  };
  events: IdentityEvent[];
  created_at: string;
  updated_at: string;
}

export interface AutoResolveReport {
  identities_created: number;
  links_created: number;
  phone_matches: number;
  name_matches: number;
  single_platform_created: number;
  cross_platform_name_matches: number;
  skipped_ambiguous_names: number;
  signal_uuid_dedup_matches: number;
  nickname_matches: number;
  fuzzy_matches: number;
  skipped_already_linked: number;
  details: Array<{
    phone?: string;
    identity_id: string;
    action: 'created' | 'extended' | 'name_matched' | 'single_platform' | 'cross_platform_name' | 'signal_uuid_dedup' | 'nickname_match' | 'fuzzy_match';
    contacts_linked: string[];
  }>;
}

export interface IdentityHealth {
  total_contacts: number;
  total_identities: number;
  total_links: number;
  contacts_linked: number;
  contacts_unlinked: number;
  coverage_pct: number;
  links_by_source: Record<string, number>;
  unlinked_with_messages: number;
  unlinked_with_phone: number;
  top_unlinked: Array<{ id: string; display_name: string | null; platform: string; message_count: number }>;
  orphaned_identities: number;
}

export interface IdentityRelationship {
  identity_id: string;
  display_name: string;
  shared_threads: number;
  total_messages: number;
  last_interaction: string | null;
  platforms: string[];
}

export interface MergeSuggestion {
  contacts: Array<{ id: string; platform: string; display_name: string | null; message_count: number }>;
  confidence: number;
  evidence: string;
}
