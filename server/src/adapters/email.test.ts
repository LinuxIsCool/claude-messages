import { describe, it, expect } from 'vitest';
import { decodeEmailBody } from './email.js';

describe('decodeEmailBody', () => {
  it('decodes a base64 text/plain MIME part', async () => {
    const raw = [
      'Content-Type: text/plain; charset="UTF-8"',
      'Content-Transfer-Encoding: base64',
      '',
      Buffer.from('Hello Shawn, the agenda is ready.').toString('base64'),
    ].join('\r\n');
    expect((await decodeEmailBody(raw)).trim()).toBe('Hello Shawn, the agenda is ready.');
  });
  it('decodes quoted-printable', async () => {
    const raw = 'Content-Type: text/plain\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\nCaf=C3=A9 meeting';
    expect((await decodeEmailBody(raw)).trim()).toBe('Café meeting');
  });
  it('passes clean text through unchanged', async () => {
    expect((await decodeEmailBody('just plain text')).trim()).toBe('just plain text');
  });
});
