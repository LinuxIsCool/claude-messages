import { parseFullName } from 'parse-full-name';

/**
 * Jaro-Winkler similarity between two strings (0..1).
 * Winkler prefix bonus with p=0.1, max 4 chars.
 */
export function jaroWinkler(s1: string, s2: string): number {
  if (s1 === s2) return 1.0;
  if (s1.length === 0 || s2.length === 0) return 0.0;

  const matchWindow = Math.max(Math.floor(Math.max(s1.length, s2.length) / 2) - 1, 0);

  const s1Matches = new Array(s1.length).fill(false);
  const s2Matches = new Array(s2.length).fill(false);
  let matches = 0;
  let transpositions = 0;

  for (let i = 0; i < s1.length; i++) {
    const start = Math.max(0, i - matchWindow);
    const end = Math.min(i + matchWindow + 1, s2.length);
    for (let j = start; j < end; j++) {
      if (s2Matches[j] || s1[i] !== s2[j]) continue;
      s1Matches[i] = true;
      s2Matches[j] = true;
      matches++;
      break;
    }
  }

  if (matches === 0) return 0.0;

  let k = 0;
  for (let i = 0; i < s1.length; i++) {
    if (!s1Matches[i]) continue;
    while (!s2Matches[k]) k++;
    if (s1[i] !== s2[k]) transpositions++;
    k++;
  }

  const jaro = (matches / s1.length + matches / s2.length + (matches - transpositions / 2) / matches) / 3;

  // Winkler prefix bonus (up to 4 chars)
  let prefix = 0;
  for (let i = 0; i < Math.min(4, s1.length, s2.length); i++) {
    if (s1[i] === s2[i]) prefix++;
    else break;
  }

  return jaro + prefix * 0.1 * (1 - jaro);
}

/**
 * Extract first name from a full display name.
 * Returns null if the name has no space (already a single name).
 */
export function extractFirstName(displayName: string): string | null {
  if (!displayName || !displayName.includes(' ')) return null;
  const parsed = parseFullName(displayName);
  return parsed.first ? parsed.first.toLowerCase() : null;
}

export interface FuzzyMatch {
  identityId: string;
  identityName: string;
  score: number;
  matchType: 'full_name' | 'first_name';
}

export interface IdentityCandidate {
  id: string;
  display_name: string;
  first_name: string | null;
  name_lower: string;
}

/**
 * Find the best fuzzy match for a contact name against a list of identity candidates.
 * Returns the highest-scoring match above threshold, or null.
 */
export function findBestFuzzyMatch(
  contactName: string,
  contactFirstName: string | null,
  identities: IdentityCandidate[],
  threshold: number,
): FuzzyMatch | null {
  let best: FuzzyMatch | null = null;

  for (const identity of identities) {
    // Compare full names
    const fullScore = jaroWinkler(contactName, identity.name_lower);
    if (fullScore >= threshold && (!best || fullScore > best.score)) {
      best = { identityId: identity.id, identityName: identity.display_name, score: fullScore, matchType: 'full_name' };
    }
    // Compare first names if both exist
    if (contactFirstName && identity.first_name) {
      const firstScore = jaroWinkler(contactFirstName, identity.first_name);
      if (firstScore >= threshold && (!best || firstScore > best.score)) {
        best = { identityId: identity.id, identityName: identity.display_name, score: firstScore, matchType: 'first_name' };
      }
    }
  }

  return best;
}
