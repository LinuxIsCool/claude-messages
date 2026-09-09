import { describe, it, expect } from 'vitest';
import { normalizePhone, normalizeEmail, parseCsv, entriesFromGoogleRows, namesNobody } from './address-book.js';

describe('normalizePhone', () => {
  it('makes the four spellings of one NANP number compare equal', () => {
    const forms = ['+1 250-288-3940', '(250) 288-3940', '2502883940', '+12502883940', '1 (250) 288-3940'];
    const normalised = new Set(forms.map(normalizePhone));
    expect([...normalised]).toEqual(['+12502883940']);
  });

  it('assumes +1 for a bare ten-digit number and nothing else', () => {
    // Ten digits is unambiguously a NANP national number. Nine is not a country
    // we can name, so inventing one would invent the person's number.
    expect(normalizePhone('6041234567')).toBe('+16041234567');
    expect(normalizePhone('604123456')).toBe('+604123456');
  });

  it('keeps a foreign number foreign', () => {
    expect(normalizePhone('+919103806171')).toBe('+919103806171');
    expect(normalizePhone('919103806171')).toBe('+919103806171');
  });

  it('treats the 00 international prefix as +', () => {
    expect(normalizePhone('0090 555 1234567')).toBe('+905551234567');
  });

  it('passes a Telegram shortcode through rather than rejecting it', () => {
    // 42777 is Telegram's own sender. It is not a phone number, but it is a
    // stable key, and dropping it would lose the contact row that uses it.
    expect(normalizePhone('42777')).toBe('+42777');
  });

  it('returns null for a value with no digits', () => {
    expect(normalizePhone('')).toBeNull();
    expect(normalizePhone('   ')).toBeNull();
    expect(normalizePhone('n/a')).toBeNull();
  });
});

describe('normalizeEmail', () => {
  it('lowercases and trims, and refuses a non-address', () => {
    expect(normalizeEmail('  Jessica.Zartler@Gmail.com ')).toBe('jessica.zartler@gmail.com');
    expect(normalizeEmail('Omananda')).toBeNull();
  });
});

describe('parseCsv', () => {
  it('reads quoted fields, doubled quotes and embedded newlines', () => {
    const text = 'First Name,Notes\r\n"Ada","said ""hi""\nthen left"\r\n';
    expect(parseCsv(text)).toEqual([{ 'First Name': 'Ada', Notes: 'said "hi"\nthen left' }]);
  });

  it('strips a UTF-8 BOM from the first header', () => {
    const rows = parseCsv('﻿First Name,Last Name\nAda,Lovelace\n');
    expect(rows[0]['First Name']).toBe('Ada');
  });

  it('drops blank lines rather than emitting empty records', () => {
    expect(parseCsv('A,B\n1,2\n\n3,4\n')).toHaveLength(2);
  });
});

describe('entriesFromGoogleRows', () => {
  const row = (over: Record<string, string> = {}) => ({
    'First Name': '', 'Middle Name': '', 'Last Name': '', Nickname: '', 'File As': '',
    'Organization Name': '', Labels: '* myContacts',
    'E-mail 1 - Label': '', 'E-mail 1 - Value': '',
    'Phone 1 - Label': '', 'Phone 1 - Value': '',
    ...over,
  });

  it('composes a full name from the name parts', () => {
    const [e] = entriesFromGoogleRows([row({ 'First Name': 'Adam', 'Middle Name': '(CTV)', 'Last Name': 'Sawatsky' })]);
    expect(e.full_name).toBe('Adam (CTV) Sawatsky');
    expect(e.given_name).toBe('Adam');
    expect(e.family_name).toBe('Sawatsky');
  });

  it('splits the ::: multi-value cell Google writes for a second number', () => {
    const [e] = entriesFromGoogleRows([row({ 'First Name': 'Kim', 'Phone 1 - Value': '(250)5551234 ::: +90 555 1234567' })]);
    expect(e.handles.map(h => h.value)).toEqual(['+12505551234', '+905551234567']);
    expect(e.handles[0].raw_value).toBe('(250)5551234');
  });

  it('reads every numbered phone and email column, not just the first', () => {
    const [e] = entriesFromGoogleRows([{
      ...row({ 'First Name': 'Jeff', 'E-mail 1 - Value': 'jeff@block.science' }),
      'E-mail 2 - Value': 'JEFF@example.org',
      'Phone 2 - Value': '2505551234',
    }]);
    expect(e.handles.map(h => `${h.kind}:${h.value}`).sort())
      .toEqual(['email:jeff@block.science', 'email:jeff@example.org', 'phone:+12505551234']);
  });

  it('keeps one handle when a person lists the same number twice', () => {
    const [e] = entriesFromGoogleRows([{
      ...row({ 'First Name': 'Dup', 'Phone 1 - Value': '(250) 555-1234' }),
      'Phone 2 - Value': '+12505551234',
    }]);
    expect(e.handles).toHaveLength(1);
  });

  it('falls back to File As, then Nickname, then the organisation', () => {
    expect(entriesFromGoogleRows([row({ 'File As': 'Brandy OUR Ecovillage' })])[0].full_name).toBe('Brandy OUR Ecovillage');
    expect(entriesFromGoogleRows([row({ Nickname: 'Hash' })])[0].full_name).toBe('Hash');
    expect(entriesFromGoogleRows([row({ 'Organization Name': 'Colin Island Pest Control' })])[0].full_name)
      .toBe('Colin Island Pest Control');
  });

  it('skips a row it cannot name rather than inventing a placeholder', () => {
    // The whole value of this table is that its names are trustworthy, so a row
    // with a number and no name belongs nowhere in it.
    expect(entriesFromGoogleRows([row({ 'Phone 1 - Value': '2505551234' })])).toHaveLength(0);
  });
});

describe('namesNobody', () => {
  it('recognises the placeholders a platform writes when it has no name', () => {
    for (const n of ['+12502883940', '250 288-3940', 'fe5d105a-befb-4831-833c-773a792058f4',
                     'user:1565818053', 'User 2028622406', 'amanda_g99', 'user_884', '']) {
      expect(namesNobody(n), n).toBe(true);
    }
  });

  it('leaves a name a source actually asserted alone, however odd', () => {
    // Every one of these is a real display name in the live corpus. Rewriting
    // one would destroy the disagreement merge review needs to see.
    for (const n of ['Cathy', 'Mike Dad', 'Jen 🙏', 'QWIN AMARATI 🕉 💎 (Daniella Marie)',
                     'molly henderson', '.:Ĵεɖḭ.ŦɭųẌ:.', 'Mayari', 'Omananda']) {
      expect(namesNobody(n), n).toBe(false);
    }
  });

  it('leaves a bare lowercase handle alone, because no rule tells it from a name', () => {
    // `ashdotxyz` is a Telegram username and `omananda` is what a person is
    // actually called, and nothing in the two strings distinguishes them. The
    // conservative reading wins: a false positive rewrites a real name, a false
    // negative only sends the row to review, which is where it was going.
    expect(namesNobody('ashdotxyz')).toBe(false);
    expect(namesNobody('omananda')).toBe(false);
  });
});
