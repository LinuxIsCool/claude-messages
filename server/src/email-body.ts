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

/**
 * Pull the readable text out of a raw MIME multipart block.
 *
 * `bodyParts: ['1']` asks IMAP for the first part of the message, and when that
 * part is itself `multipart/alternative` the server hands back the whole
 * container: a boundary line, sub-part headers, and both the plain-text and
 * HTML alternatives concatenated. 4,325 of 33,796 email rows (12.8%) look like
 * this, and they include the ones that matter most here, the threads with
 * Shawn's grandfather.
 *
 * Prefers `text/plain`; falls back to `text/html` so a message with no plain
 * alternative is still readable rather than empty. Recurses, because
 * `multipart/mixed` wrapping `multipart/alternative` is ordinary, with a depth
 * limit so a malformed boundary cannot loop.
 *
 * Returns null when the text is not a multipart block, which is the common case.
 */
export function extractTextFromMultipart(text: string, depth = 0): string | null {
  if (depth > 4) return null;
  // The boundary is whatever follows the first `--`, dashes included:
  // `------=_Part_477901_1432188472` is an ordinary JavaMail boundary, and a
  // rule excluding a leading dash left 1,155 rows unreadable to avoid a
  // signature marker it never needed to guard against. What actually protects
  // ordinary prose is the content-type test below, which no prose passes.
  const opening = /^--([^\r\n]*[^\r\n\s-])\s*\r?\n/.exec(text.replace(/^\s+/, ''));
  if (!opening) return null;
  const boundary = '--' + opening[1].replace(/--+$/, '');
  const sections = text.split(boundary).slice(1);
  if (!sections.length) return null;

  // Split each section once into its own headers and body, then choose by
  // content type rather than by position. A container's headers name
  // `multipart/...` and its body is another block to walk into.
  const parts: Array<{ headers: string; body: string }> = [];
  for (const section of sections) {
    const sep = /\r?\n\r?\n/.exec(section);
    if (!sep) continue;
    const body = section.slice(sep.index + sep[0].length).replace(/\r?\n\s*$/, '');
    if (!body.trim()) continue;
    parts.push({ headers: section.slice(0, sep.index), body });
  }

  for (const wanted of [/text\/plain/i, /text\/html/i]) {
    for (const { headers, body } of parts) {
      if (/multipart\//i.test(headers)) {
        const nested = extractTextFromMultipart(body, depth + 1);
        if (nested) return nested;
        continue;
      }
      if (!wanted.test(headers)) continue;
      const decoded = decodeBodyPart(body, headers);
      return extractTextFromMultipart(decoded, depth + 1) ?? decoded;
    }
  }
  return null;
}

/**
 * Decode a quoted-printable run inside text that is already decoded.
 *
 * A Buffer from IMAP is bytes and a string from the database is characters,
 * and the difference matters. Pushing a string through latin1 to get bytes
 * truncates every code point above 255, so `Keyser Smöze` becomes an invalid
 * UTF-8 byte and then U+FFFD. Here each contiguous run of `=XX` escapes is
 * decoded as bytes on its own and everything around it is left alone, so
 * `=C3=A9` becomes one character and a literal `ö` survives untouched.
 */
function decodeQuotedPrintableInText(text: string, enc: BufferEncoding): string {
  return text
    .replace(/=\r?\n/g, '')
    .replace(/(?:=[0-9A-Fa-f]{2})+/g, run => {
      const bytes = run.split('=').filter(Boolean).map(h => parseInt(h, 16));
      return Buffer.from(bytes).toString(enc);
    });
}

/** Decode one part given its own MIME headers. The headers are the truth. */
export function decodeBodyPart(body: Buffer | string, mimeHeaders?: Buffer | string | null): string {
  const { encoding, charset } = parseMimeHeaders(mimeHeaders);
  const enc = charsetToNode(charset);

  if (typeof body === 'string') {
    if (encoding === 'base64') return Buffer.from(body.replace(/\s+/g, ''), 'base64').toString(enc);
    if (encoding === 'quoted-printable') return decodeQuotedPrintableInText(body, enc);
    return body;
  }

  if (encoding === 'base64') return Buffer.from(body.toString('ascii'), 'base64').toString(enc);
  if (encoding === 'quoted-printable') {
    // Bytes here, so the escapes and the surrounding octets are the same kind
    // of thing and the charset applies to the whole result at once.
    return Buffer.from(decodeQuotedPrintable(body.toString('latin1')), 'latin1').toString(enc);
  }
  return body.toString(enc);
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

  // A multipart container first: its sub-parts carry their own encodings, so
  // testing the container for base64 or quoted-printable would answer about
  // the wrong thing.
  const inner = extractTextFromMultipart(trimmed);
  if (inner !== null && inner.trim() && isCleanText(inner)) return inner;

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
    const decoded = decodeQuotedPrintableInText(trimmed, 'utf8');
    if (isCleanText(decoded) && decoded !== trimmed) return decoded;
  }
  return null;
}
