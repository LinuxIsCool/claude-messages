import type { SyncEvent, AdapterConfig } from '../types.js';

export interface SourceObservation {
  observed_at: string;
  evidence: string;
}

export interface Adapter {
  platform: string;
  init(config: AdapterConfig): Promise<void>;
  sync(cursor: string | null): AsyncGenerator<SyncEvent>;
  /** Returns updated cursor after sync completes */
  getCursor(): string | null;
  /** Returns direct evidence that the upstream source was reachable. */
  getSourceObservation?(): SourceObservation | null;
  shutdown(): Promise<void>;
}
