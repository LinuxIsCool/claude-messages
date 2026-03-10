import type { SyncEvent, AdapterConfig } from '../types.js';

export interface Adapter {
  platform: string;
  init(config: AdapterConfig): Promise<void>;
  sync(cursor: string | null): AsyncGenerator<SyncEvent>;
  /** Returns updated cursor after sync completes */
  getCursor(): string | null;
  shutdown(): Promise<void>;
}
