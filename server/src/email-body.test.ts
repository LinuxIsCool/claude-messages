import { describe, it, expect } from 'vitest';
import { decodeBodyPart, parseMimeHeaders, decodeQuotedPrintable, repairStoredBody } from './email-body.js';

const b64 = (s: string) => Buffer.from(s, 'utf8').toString('base64');

describe('parseMimeHeaders', () => {
  it('reads the encoding and charset, case-insensitively', () => {
    const h = 'Content-Type: text/plain; charset="UTF-8"\r\nContent-Transfer-Encoding: BASE64\r\n\r\n';
    expect(parseMimeHeaders(h)).toEqual({ encoding: 'base64', charset: '"UTF-8"' });
  });

  it('unfolds a header split across lines', () => {
    const h = 'Content-Type: text/plain;\r\n\tcharset=iso-8859-1\r\nContent-Transfer-Encoding: quoted-printable\r\n';
    expect(parseMimeHeaders(h).charset).toBe('iso-8859-1');
  });

  it('says nothing when there are no headers', () => {
    expect(parseMimeHeaders(undefined)).toEqual({ encoding: null, charset: null });
  });
});

describe('decodeBodyPart', () => {
  it('decodes base64, which is what made 1,038 rows unreadable', () => {
    const body = Buffer.from(b64('Hello all! I will set for 1130 am pst on Friday'), 'ascii');
    const out = decodeBodyPart(body, 'Content-Transfer-Encoding: base64\r\ncharset=utf-8\r\n');
    expect(out).toBe('Hello all! I will set for 1130 am pst on Friday');
  });

  it('decodes quoted-printable escapes and soft line breaks', () => {
    const body = 'upn=3D3Du001 and a very long line that was wrapped=\r\nhere';
    const out = decodeBodyPart(Buffer.from(body, 'latin1'), 'Content-Transfer-Encoding: quoted-printable\r\n');
    expect(out).toBe('upn=3Du001 and a very long line that was wrappedhere');
  });

  it('decodes a multi-byte character written as two quoted-printable bytes', () => {
    const out = decodeBodyPart(Buffer.from('caf=C3=A9', 'latin1'), 'Content-Transfer-Encoding: quoted-printable\r\ncharset=utf-8');
    expect(out).toBe('café');
  });

  it('leaves 7bit text alone', () => {
    const out = decodeBodyPart(Buffer.from('Hello Indigenomics team,', 'utf8'), 'Content-Transfer-Encoding: 7bit\r\n');
    expect(out).toBe('Hello Indigenomics team,');
  });

  it('reads latin-1 as latin-1 rather than mangling it into replacement characters', () => {
    const out = decodeBodyPart(Buffer.from([0x63, 0x61, 0x66, 0xe9]), 'Content-Type: text/plain; charset=iso-8859-1\r\n');
    expect(out).toBe('café');
  });

  it('falls back to utf-8 when there are no headers at all', () => {
    expect(decodeBodyPart(Buffer.from('plain text', 'utf8'), null)).toBe('plain text');
  });
});

describe('repairStoredBody — the headers are gone, so read the text', () => {
  it('recovers a base64 body already in the database', () => {
    expect(repairStoredBody(b64('I could do Thursday, 9-11.'))).toBe('I could do Thursday, 9-11.');
  });

  it('recovers a base64 body stored with the line wrapping intact', () => {
    const wrapped = b64('Ohhh fun! For sure, Thursday at 9 am works. Uniting business and land').replace(/(.{20})/g, '$1\n');
    expect(repairStoredBody(wrapped)).toContain('Thursday at 9 am works');
  });

  it('recovers a quoted-printable body', () => {
    expect(repairStoredBody('meet me at caf=C3=A9 at 9=\r\nam sharp')).toBe('meet me at café at 9am sharp');
  });

  it('leaves ordinary prose alone', () => {
    const prose = 'Hello Indigenomics team, It has just come to my attention that the FIFA match will occur.';
    expect(repairStoredBody(prose)).toBeNull();
  });

  it('leaves a single base64-looking word alone rather than turning it into noise', () => {
    // A tracking id or a hash is base64-shaped and decodes to bytes, not text.
    expect(repairStoredBody('K6vdzmYALMxFXsbJMvT1OzS9wjK1TLJDw')).toBeNull();
  });

  it('refuses a decode that comes out as bytes rather than text', () => {
    // An inline image or a PGP block is base64-shaped and decodes to binary.
    const binary = Buffer.from(Array.from({ length: 300 }, (_, i) => (i * 37) % 256));
    expect(repairStoredBody(binary.toString('base64'))).toBeNull();
  });

  it('refuses prose that merely contains two hex-looking escapes', () => {
    // `=20` and `=3D` here are literal text in a code sample, not an encoding.
    // Decoding it would silently damage a readable message, which is a worse
    // outcome than leaving an unreadable one alone.
    const prose = 'The regex matched =ZZ and the encoder wrote =C3 alone, which is invalid on its own line.';
    expect(repairStoredBody(prose)).toBeNull();
  });

  it('leaves a URL containing = alone', () => {
    const url = 'http://link.empwr.ai/ls/click?upn=abcdef and please read it';
    expect(repairStoredBody(url)).toBeNull();
  });
});
