/**
 * Decoding a MIME body part (task-885 N2).
 *
 * The email adapter fetches one body part and calls `.toString()` on it, which
 * is correct only when the part is 7bit. A base64 part becomes a wall of
 * `SGVsbG8gYWxs…` and a quoted-printable one keeps its `=3D` and soft breaks.
 * 1,038 of the first 19,376 email rows are unreadable this way, and the FTS
 * index has them indexed as gibberish, so they cannot be searched either.
 *
 * Pure: no IMAP, no database.
 */

/** `iso-8859-1` and friends are `latin1` to Node; everything else tries utf-8. */
function charsetToNode(charset: string | null): BufferEncoding {
  const c = (charset ?? '').toLowerCase().replace(/["']/g, '').trim();
  if (c === 'iso-8859-1' || c === 'latin1' || c === 'windows-1252' || c === 'cp1252') return 'latin1';
  if (c === 'us-ascii' || c === 'ascii') return 'ascii';
  return 'utf8';
}

export function parseMimeHeaders(raw: Buffer | string | undefined | null): { encoding: string | null; charset: string | null } {
  if (!raw) return { encoding: null, charset: null };
  const text = typeof raw === 'string' ? raw : raw.toString('latin1');
  // Unfold: a header continues on the next line when it starts with whitespace.
  const unfolded = text.replace(/\r?\n[ \t]+/g, ' ');
  const enc = /^content-transfer-encoding:\s*([^\s;]+)/im.exec(unfolded);
  const cs = /charset\s*=\s*("[^"]+"|'[^']+'|[^\s;]+)/im.exec(unfolded);
  return {
    encoding: enc ? enc[1].toLowerCase() : null,
    charset: cs ? cs[1] : null,
  };
}

export function decodeQuotedPrintable(text: string): string {
  return text
    .replace(/=\r?\n/g, '')                                     // soft line break
    .replace(/=([0-9A-Fa-f]{2})/g, (_, h) => String.fromCharCode(parseInt(h, 16)));
}

/** Decode one part given its own MIME headers. The headers are the truth. */
export function decodeBodyPart(body: Buffer | string, mimeHeaders?: Buffer | string | null): string {
  const { encoding, charset } = parseMimeHeaders(mimeHeaders);
  const enc = charsetToNode(charset);
  const raw = Buffer.isBuffer(body) ? body : Buffer.from(body, 'latin1');
  if (encoding === 'base64') return Buffer.from(raw.toString('ascii'), 'base64').toString(enc);
  if (encoding === 'quoted-printable') {
    // Decode the escapes as bytes first, then apply the charset, or a
    // multi-byte utf-8 sequence written as =C3=A9 comes out as two characters.
    return Buffer.from(decodeQuotedPrintable(raw.toString('latin1')), 'latin1').toString(enc);
  }
  return raw.toString(enc);
}

/** How much of a string is ordinary readable text. */
function printableRatio(s: string): number {
  if (!s.length) return 0;
  let ok = 0;
  for (const ch of s) {
    const c = ch.codePointAt(0)!;
    if (c === 9 || c === 10 || c === 13 || (c >= 32 && c !== 0xfffd)) ok++;
  }
  return ok / [...s].length;
}

/**
 * Text a decode is allowed to produce.
 *
 * Two conditions, and the second is what makes the sniffing safe. Mostly
 * printable rules out binary. No U+FFFD at all rules out a false positive:
 * a real base64 or quoted-printable body decodes to valid UTF-8, and random
 * bytes that happened to look encoded do not. A single replacement character
 * is enough to refuse, because the alternative is replacing readable prose
 * with noise.
 */
function isCleanText(s: string): boolean {
  return !s.includes('\ufffd') && printableRatio(s) > 0.95;
}

/**
 * Recover a body that was already stored undecoded.
 *
 * The MIME headers are gone by the time a row is in the database, so this reads
 * the text itself and only rewrites when the result is unambiguously better:
 * the candidate decodes cleanly, it is mostly printable, and it contains actual
 * whitespace, which base64 never does and English always does. Returns null
 * when it is not sure, because leaving a row unreadable is a smaller harm than
 * replacing a readable one with noise.
 */
export function repairStoredBody(content: string): string | null {
  const trimmed = (content ?? '').trim();
  if (trimmed.length < 24) return null;

  const compact = trimmed.replace(/\s+/g, '');
  const looksBase64 = /^[A-Za-z0-9+/]+={0,2}$/.test(compact) && compact.length % 4 === 0;
  if (looksBase64) {
    const decoded = Buffer.from(compact, 'base64').toString('utf8');
    if (decoded.length >= 8 && isCleanText(decoded) && /\s/.test(decoded)) return decoded;
    return null;
  }

  // Quoted-printable announces itself: `=` followed by two hex digits, or a
  // line ending in a bare `=`. Plain text almost never contains either.
  const qpMarkers = (trimmed.match(/=[0-9A-Fa-f]{2}/g) ?? []).length + (trimmed.match(/=\r?\n/g) ?? []).length;
  if (qpMarkers >= 2) {
    const decoded = Buffer.from(decodeQuotedPrintable(trimmed), 'latin1').toString('utf8');
    if (isCleanText(decoded) && decoded !== trimmed) return decoded;
  }
  return null;
}
