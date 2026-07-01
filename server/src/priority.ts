export type PriorityTier = 'critical' | 'exceptional' | 'somewhat' | 'irrelevant';
export type RuleType = 'thread' | 'identity' | 'cohort' | 'platform_folder' | 'keyword';

// Floor value assigned when a rule declares a tier.
const TIER_FLOOR: Record<PriorityTier, number> = {
  critical: 0.95,
  exceptional: 0.75,
  somewhat: 0.40,
  irrelevant: 0.10,
};

export function tierToImportance(tier: PriorityTier): number {
  return TIER_FLOOR[tier];
}

// A0 uses fixed thresholds. Phase A2 replaces this with quantile calibration.
export function importanceToTier(importance: number): PriorityTier {
  if (importance >= 0.90) return 'critical';
  if (importance >= 0.60) return 'exceptional';
  if (importance >= 0.25) return 'somewhat';
  return 'irrelevant';
}

export function blendAttention(importance: number, urgency: number): number {
  const a = 0.6 * importance + 0.4 * urgency;
  return Math.max(0, Math.min(1, a));
}

const DEADLINE_WORDS = /\b(asap|urgent|today|tonight|tomorrow|deadline|eod|end of day|by (mon|tues|tue|wednes|wed|thurs|thur|thu|fri|satur|sat|sun)(day)?)\b/i;

export function detectUrgencySignals(content: string | null): number {
  if (!content) return 0;
  let score = 0;
  if (content.includes('?')) score += 0.5;
  if (DEADLINE_WORDS.test(content)) score += 0.5;
  return Math.min(1, score);
}
