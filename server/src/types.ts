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
  direction?: 'sent' | 'received' | 'unknown';
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
  identity_merges: number;
  email_metadata_matches: number;
  skipped_already_linked: number;
  details: Array<{
    phone?: string;
    identity_id: string;
    action: 'created' | 'extended' | 'name_matched' | 'single_platform' | 'cross_platform_name' | 'signal_uuid_dedup' | 'nickname_match' | 'fuzzy_match' | 'identity_merge' | 'email_metadata';
    contacts_linked: string[];
    merged_into?: string;
    merge_evidence?: string;
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

// --- Relationship Intelligence (Phase 2) ---

export interface RawMetrics {
  identity_id: string;
  total_messages: number;
  sent: number;
  received: number;
  dm_messages: number;
  group_messages: number;
  platforms: string[];
  shared_groups: number;
  last_message_ts: string | null;
  message_timestamps: string[];       // ISO 8601 timestamps for temporal analysis
  response_latencies_ms: number[];    // response times in ms for latency scoring
}

export interface ScoringContext {
  maxMessages: number;
  maxSharedGroups: number;
  maxMedianLatency: number;
}

export interface ScoringFactor {
  name: string;
  compute: (metrics: RawMetrics, context: ScoringContext) => number;  // returns 0-1
  weight: number;
}

export interface ScoringConfig {
  id: string;
  factors: ScoringFactor[];
  aggregate: (factorScores: Record<string, number>, factors: ScoringFactor[]) => number;
}

export type DunbarLayer = 'support_clique' | 'sympathy_group' | 'affinity_group' | 'active_network' | 'acquaintance';

export interface ContactScore {
  identity_id: string;
  display_name: string;
  frequency: number;
  recency: number;
  reciprocity: number;
  channel_diversity: number;
  dm_ratio: number;
  structural: number;
  temporal_regularity: number;
  response_latency: number;
  composite: number;
  dunbar_layer: DunbarLayer;
  confidence: number;
  computed_at: string;
}

export interface FadingRelationship {
  identity_id: string;
  display_name: string;
  dunbar_layer: DunbarLayer;
  median_interval_days: number;
  days_since_last: number;
  silence_ratio: number;            // days_since_last / median_interval_days
  last_message_ts: string;
  total_messages: number;
}

// --- Daemon Health Reporting ---

/** Dependency tier for scheduling and staleness thresholds */
export type AdapterTier = 0 | 1 | 2;

/** Per-adapter health snapshot, written after each sync cycle */
export interface AdapterHealth {
  platform: string;
  tier: AdapterTier;
  last_success: string | null;      // ISO 8601 of last successful sync
  last_failure: string | null;       // ISO 8601 of last failed sync (null if never failed)
  last_error: string | null;         // error message from last failure
  last_duration_ms: number;          // duration of last sync attempt
  last_yield: {
    messages: number;
    threads: number;
    contacts: number;
  };
  consecutive_failures: number;      // reset to 0 on success
  timed_out: boolean;                // true if last sync was a timeout
  skipped?: boolean;                 // true if this cycle intentionally skipped the adapter
  cooldown_until?: string | null;     // ISO 8601 time before retrying a failing adapter
}

/** Top-level daemon health file — shared contract for all Legion daemons */
export interface DaemonHealth {
  daemon: string;                    // 'legion-messages'
  version: string;                   // package version or commit hash
  pid: number;
  started_at: string;                // ISO 8601 daemon start time
  last_cycle: string;                // ISO 8601 of last completed syncAll()
  cycle_count: number;               // total sync cycles since start
  cycle_duration_ms: number;         // duration of last full syncAll()
  adapters: Record<string, AdapterHealth>;
}

export interface PriorityRule {
  id: number;
  rule_type: string;
  match_value: string;
  importance_floor: number;
  tier_floor: string;
  note: string | null;
  enabled: number;
  created_at: string;
}

export interface Cohort {
  id: number;
  name: string;
  description: string | null;
  created_at: string;
}

export interface MessagePriority {
  message_id: string;
  importance: number;
  urgency: number;
  attention: number;
  tier: string;
  source: string;
  model_version: string | null;
  rationale: string | null;
  needs_llm: number;
  seen: number;
  scored_at: string;
}

export interface AwarenessCounts {
  critical: number;
  exceptional: number;
}

export interface InboxEntry {
  message_id: string;
  importance: number;
  urgency: number;
  attention: number;
  tier: string;
  source: string;
  model_version: string | null;
  rationale: string | null;
  needs_llm: number;
  seen: number;
  scored_at: string;
  content: string | null;
  sender_id: string | null;
  thread_id: string | null;
}
