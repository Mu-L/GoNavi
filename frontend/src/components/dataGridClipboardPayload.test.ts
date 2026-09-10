import { describe, expect, it, vi } from 'vitest';

import {
  buildTabularClipboardPayload,
  buildTabularClipboardPayloadFromTsv,
  writeClipboardPayload,
  writeClipboardPayloadToEvent,
} from './dataGridClipboardPayload';

describe('dataGridClipboardPayload', () => {
  it('builds plain text, HTML and CSV from one table payload', () => {
    const payload = buildTabularClipboardPayload({
      columns: ['id', 'name'],
      rows: [
        ['1', 'Alice & Bob'],
        ['2', '<Admin>'],
      ],
      jsonRows: [
        { id: 1, name: 'Alice & Bob' },
        { id: 2, name: '<Admin>' },
      ],
    });

    expect(payload.plainText).toBe('id\tname\n1\tAlice & Bob\n2\t<Admin>');
    expect(payload.csv).toBe('"id","name"\n"1","Alice & Bob"\n"2","<Admin>"');
    expect(payload.html).toContain('<th>name</th>');
    expect(payload.html).toContain('<td>Alice &amp; Bob</td>');
    expect(payload.html).toContain('<td>&lt;Admin&gt;</td>');
    expect(payload.markdown).toBe('| id | name |\n| --- | --- |\n| 1 | Alice & Bob |\n| 2 | <Admin> |');
    expect(payload.json).toBe('[\n  {\n    "id": 1,\n    "name": "Alice & Bob"\n  },\n  {\n    "id": 2,\n    "name": "<Admin>"\n  }\n]');
  });

  it('keeps the original TSV plain text when deriving rich formats', () => {
    const payload = buildTabularClipboardPayloadFromTsv('id\tname\n1\talpha', { firstRowIsHeader: true });

    expect(payload.plainText).toBe('id\tname\n1\talpha');
    expect(payload.html).toContain('<thead><tr><th>id</th><th>name</th></tr></thead>');
    expect(payload.csv).toBe('"id","name"\n"1","alpha"');
  });

  it('keeps rich formats lossless while plain text remains a safe TSV fallback', () => {
    const payload = buildTabularClipboardPayload({
      rows: [['alpha\tbeta', 'line1\nline2']],
    });

    expect(payload.plainText).toBe('alpha beta\tline1 line2');
    expect(payload.csv).toBe('"alpha\tbeta","line1\nline2"');
    expect(payload.html).toContain('<td>alpha\tbeta</td>');
    expect(payload.html).toContain('<td>line1\nline2</td>');
  });

  it('sets multiple clipboard MIME types without overwriting different formats', () => {
    const values: Record<string, string> = {};
    const event = {
      clipboardData: {
        clearData: vi.fn(() => {
          Object.keys(values).forEach((key) => delete values[key]);
        }),
        setData: vi.fn((type: string, value: string) => {
          values[type] = value;
        }),
      },
      preventDefault: vi.fn(),
    };

    const payload = buildTabularClipboardPayload({
      columns: ['id'],
      rows: [['1']],
      jsonRows: [{ id: 1 }],
    });

    expect(writeClipboardPayloadToEvent(event, payload)).toBe(true);
    expect(values['text/plain']).toBe('id\n1');
    expect(values['text/html']).toContain('<table>');
    expect(values['text/csv']).toBe('"id"\n"1"');
    expect(values['text/markdown']).toBe('| id |\n| --- |\n| 1 |');
    expect(values['application/json']).toBe('[\n  {\n    "id": 1\n  }\n]');
    expect(event.preventDefault).toHaveBeenCalledTimes(1);
  });

  it('preserves typed nulls in in-app HTML, plain text and JSON payloads', () => {
    const payload = buildTabularClipboardPayload({
      columns: ['literal', 'empty'],
      rows: [['NULL', null], ['alpha', '']],
      preserveCellTypes: true,
    });

    expect(payload.plainText).toBe('literal\tempty\n"NULL"\tNULL\nalpha\t');
    expect(payload.csv).toBe('"literal","empty"\n"NULL","NULL"\n"alpha",""');
    expect(payload.markdown).toBe('| literal | empty |\n| --- | --- |\n| NULL | NULL |\n| alpha |  |');
    expect(payload.html).toContain('data-gonavi-clipboard="true"');
    expect(payload.html).toContain('<th>literal</th>');
    expect(payload.html).toContain('<td>NULL</td>');
    expect(payload.html).toContain('<td data-gonavi-null="true">NULL</td>');
    expect(payload.json).toBe(JSON.stringify({
      gonaviGrid: 1,
      values: [
        ['NULL', null],
        ['alpha', ''],
      ],
    }));
  });

  it('keeps untyped null cells as empty spreadsheet values', () => {
    const payload = buildTabularClipboardPayload({
      rows: [[null, 'NULL']],
    });

    expect(payload.plainText).toBe('\tNULL');
    expect(payload.csv).toBe('"","NULL"');
    expect(payload.html).toContain('<td></td>');
    expect(payload.html).toContain('<td>NULL</td>');
    expect(payload.html).not.toContain('data-gonavi-clipboard');
    expect(payload.json).toBeUndefined();
  });

  it('writes the in-app JSON envelope when copying typed cells', () => {
    const values: Record<string, string> = {};
    const event = {
      clipboardData: {
        clearData: vi.fn(),
        setData: vi.fn((type: string, value: string) => {
          values[type] = value;
        }),
      },
      preventDefault: vi.fn(),
    };

    const payload = buildTabularClipboardPayload({
      rows: [['NULL', null]],
      preserveCellTypes: true,
    });

    expect(writeClipboardPayloadToEvent(event, payload)).toBe(true);
    expect(values['text/plain']).toBe('"NULL"\tNULL');
    expect(values['text/html']).toContain('data-gonavi-null="true"');
    expect(values['application/json']).toBe(payload.json);
  });

  it('encodes undefined cells with the same null token as real nulls', () => {
    const payload = buildTabularClipboardPayload({
      rows: [[undefined as unknown as null]],
      preserveCellTypes: true,
    });

    expect(payload.plainText).toBe('NULL');
    expect(payload.csv).toBe('"NULL"');
    expect(payload.html).toContain('<td data-gonavi-null="true">NULL</td>');
  });

  it('does not write clipboard data when the event or plain text is missing', () => {
    expect(writeClipboardPayloadToEvent({}, { plainText: 'alpha' })).toBe(false);
    expect(writeClipboardPayloadToEvent({
      clipboardData: { clearData: vi.fn(), setData: vi.fn() },
    }, { plainText: '' })).toBe(false);
  });

  it('keeps an empty header row when deriving formats from a leading TSV newline', () => {
    const payload = buildTabularClipboardPayloadFromTsv('\n1', { firstRowIsHeader: true });
    expect(payload.plainText).toBe('\n1');
    expect(payload.html).toContain('<thead>');
  });

  it('treats TSV as data rows when no header option is set', () => {
    const payload = buildTabularClipboardPayloadFromTsv('a\tb');
    expect(payload.plainText).toBe('a\tb');
    expect(payload.html).not.toContain('<thead>');
    expect(payload.html).toContain('<td>a</td>');
  });

  it('writes typed clipboard parts through ClipboardItem and falls back to plain text', async () => {
    const originalClipboardItem = globalThis.ClipboardItem;
    class MockClipboardItem {
      parts: Record<string, Blob>;
      constructor(parts: Record<string, Blob>) {
        this.parts = parts;
      }
      static supports(type: string) {
        return type === 'text/plain' || type === 'text/html' || type === 'application/json';
      }
    }
    Object.defineProperty(globalThis, 'ClipboardItem', { configurable: true, value: MockClipboardItem });

    try {
      const payload = buildTabularClipboardPayload({
        rows: [['NULL', null]],
        preserveCellTypes: true,
      });

      const write = vi.fn(async () => undefined);
      await writeClipboardPayload(payload, { write, writeText: vi.fn() });
      expect(write).toHaveBeenCalledTimes(1);
      const item = write.mock.calls[0][0][0] as { parts: Record<string, Blob> };
      expect(Object.keys(item.parts).sort()).toEqual(['application/json', 'text/html', 'text/plain']);

      const writeText = vi.fn(async () => undefined);
      await writeClipboardPayload(payload, {
        write: vi.fn(async () => {
          throw new Error('partial clipboard');
        }),
        writeText,
      });
      expect(writeText).toHaveBeenCalledWith('"NULL"\tNULL');

      class HtmlOnlyClipboardItem {
        parts: Record<string, Blob>;
        constructor(parts: Record<string, Blob>) {
          this.parts = parts;
        }
      }
      Object.defineProperty(globalThis, 'ClipboardItem', { configurable: true, value: HtmlOnlyClipboardItem });
      const htmlOnlyWrite = vi.fn(async () => undefined);
      const typedForHtmlOnly = buildTabularClipboardPayload({
        columns: ['name'],
        rows: [['alpha']],
      });
      await writeClipboardPayload(typedForHtmlOnly, { write: htmlOnlyWrite, writeText: vi.fn() });
      const htmlOnlyItem = htmlOnlyWrite.mock.calls[0][0][0] as { parts: Record<string, Blob> };
      expect(Object.keys(htmlOnlyItem.parts).sort()).toEqual(['text/html', 'text/plain']);

      await writeClipboardPayload({ plainText: '' }, { writeText: vi.fn() });

      const fallbackWriteText = vi.fn(async () => undefined);
      Object.defineProperty(globalThis, 'ClipboardItem', { configurable: true, value: undefined });
      await writeClipboardPayload({ plainText: 'beta' }, { writeText: fallbackWriteText });
      expect(fallbackWriteText).toHaveBeenCalledWith('beta');

      const navigatorWriteText = vi.fn(async () => undefined);
      vi.stubGlobal('navigator', { clipboard: { writeText: navigatorWriteText } });
      await writeClipboardPayload({ plainText: 'nav' });
      expect(navigatorWriteText).toHaveBeenCalledWith('nav');
      vi.unstubAllGlobals();

      await expect(writeClipboardPayload({ plainText: 'gamma' }, {} as any)).rejects.toThrow('Clipboard write is not supported');
    } finally {
      Object.defineProperty(globalThis, 'ClipboardItem', { configurable: true, value: originalClipboardItem });
    }
  });
});
