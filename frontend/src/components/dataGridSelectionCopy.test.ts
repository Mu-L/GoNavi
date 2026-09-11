import { describe, expect, it } from 'vitest';

import {
  buildSelectedCellClipboardPayload,
  buildSelectedCellClipboardText,
  canSelectGridCellForClipboard,
} from './dataGridSelectionCopy';

describe('dataGridSelectionCopy helpers', () => {
  it('allows displayed read-only cells while keeping editable expressions out of batch selection', () => {
    expect(canSelectGridCellForClipboard({
      canModifyData: false,
      isDisplayedColumn: true,
      isWritableColumn: false,
    })).toBe(true);
    expect(canSelectGridCellForClipboard({
      canModifyData: true,
      isDisplayedColumn: true,
      isWritableColumn: false,
    })).toBe(false);
    expect(canSelectGridCellForClipboard({
      canModifyData: false,
      isDisplayedColumn: false,
      isWritableColumn: false,
    })).toBe(false);
  });

  it('builds clipboard text in visible row and column order', () => {
    const text = buildSelectedCellClipboardText({
      selectedCells: [
        { rowKey: 'row-2', colName: 'name' },
        { rowKey: 'row-1', colName: 'id' },
        { rowKey: 'row-1', colName: 'name' },
        { rowKey: 'row-2', colName: 'id' },
      ],
      rows: [
        { __rowKey: 'row-1', id: 1, name: 'Alice' },
        { __rowKey: 'row-2', id: 2, name: 'Bob' },
      ],
      columnOrder: ['id', 'name', 'email'],
      rowKeyField: '__rowKey',
    });

    expect(text).toBe('1\tAlice\n2\tBob');
  });

  it('normalizes null, objects and multiline text for clipboard safety', () => {
    const text = buildSelectedCellClipboardText({
      selectedCells: [
        { rowKey: 'row-1', colName: 'notes' },
        { rowKey: 'row-1', colName: 'meta' },
        { rowKey: 'row-2', colName: 'notes' },
        { rowKey: 'row-2', colName: 'meta' },
      ],
      rows: [
        { __rowKey: 'row-1', notes: null, meta: { a: 1 } },
        { __rowKey: 'row-2', notes: 'line1\nline2\tvalue', meta: [1, 2] },
      ],
      columnOrder: ['notes', 'meta'],
      rowKeyField: '__rowKey',
    });

    expect(text).toBe('NULL\t{"a":1}\nline1 line2 value\t[1,2]');
  });

  it('builds a multi-format payload for selected cells', () => {
    const payload = buildSelectedCellClipboardPayload({
      selectedCells: [
        { rowKey: 'row-1', colName: 'name' },
        { rowKey: 'row-1', colName: 'note' },
      ],
      rows: [
        { __rowKey: 'row-1', name: 'A&B', note: '<owner>' },
      ],
      columnOrder: ['name', 'note'],
      rowKeyField: '__rowKey',
    });

    expect(payload.plainText).toBe('A&B\t<owner>');
    expect(payload.csv).toBe('"A&B","<owner>"');
    expect(payload.html).toContain('<td>A&amp;B</td>');
    expect(payload.html).toContain('<td>&lt;owner&gt;</td>');
  });

  it('preserves tabs and newlines in rich selected-cell formats', () => {
    const payload = buildSelectedCellClipboardPayload({
      selectedCells: [
        { rowKey: 'row-1', colName: 'note' },
      ],
      rows: [
        { __rowKey: 'row-1', note: 'left\tright\nnext' },
      ],
      columnOrder: ['note'],
      rowKeyField: '__rowKey',
    });

    expect(payload.plainText).toBe('left right next');
    expect(payload.csv).toBe('"left\tright\nnext"');
    expect(payload.html).toContain('<td>left\tright\nnext</td>');
  });

  it('keeps NULL, quoted NULL text and real null distinct in typed payloads', () => {
    const payload = buildSelectedCellClipboardPayload({
      selectedCells: [
        { rowKey: 'row-1', colName: 'literal' },
        { rowKey: 'row-1', colName: 'quoted' },
        { rowKey: 'row-1', colName: 'empty' },
        { rowKey: 'row-2', colName: 'literal' },
        { rowKey: 'row-2', colName: 'quoted' },
        { rowKey: 'row-2', colName: 'empty' },
      ],
      rows: [
        { __rowKey: 'row-1', literal: 'NULL', quoted: '"NULL"', empty: null },
        { __rowKey: 'row-2', literal: 'NULL', quoted: '"NULL"', empty: undefined },
      ],
      columnOrder: ['literal', 'quoted', 'empty'],
      rowKeyField: '__rowKey',
    });

    expect(payload.plainText).toBe('"NULL"\t"""NULL"""\tNULL\n"NULL"\t"""NULL"""\tNULL');
    expect(payload.html).toContain('data-gonavi-clipboard="true"');
    expect(payload.html).toContain('<td>NULL</td>');
    expect(payload.html).toContain('<td data-gonavi-null="true">NULL</td>');
    expect(payload.json).toBe(JSON.stringify({
      gonaviGrid: 1,
      values: [
        ['NULL', '"NULL"', null],
        ['NULL', '"NULL"', null],
      ],
    }));
  });

  it('uses the reversible typed encoding for the plain-text-only helper', () => {
    expect(buildSelectedCellClipboardText({
      selectedCells: [
        { rowKey: 'row-1', colName: 'literal' },
        { rowKey: 'row-1', colName: 'quoted' },
        { rowKey: 'row-1', colName: 'empty' },
      ],
      rows: [{ __rowKey: 'row-1', literal: 'NULL', quoted: '"NULL"', empty: null }],
      columnOrder: ['literal', 'quoted', 'empty'],
      rowKeyField: '__rowKey',
    })).toBe('"NULL"\t"""NULL"""\tNULL');
  });

  it('stringifies remaining primitive and fallback clipboard values', () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;

    const text = buildSelectedCellClipboardText({
      selectedCells: [
        { rowKey: 'row-1', colName: 'flag' },
        { rowKey: 'row-1', colName: 'big' },
        { rowKey: 'row-1', colName: 'loop' },
        { rowKey: 'row-1', colName: 'sym' },
      ],
      rows: [{
        __rowKey: 'row-1',
        flag: true,
        big: 1n,
        loop: circular,
        sym: Symbol('cell'),
      }],
      columnOrder: ['flag', 'big', 'loop', 'sym'],
      rowKeyField: '__rowKey',
    });

    expect(text.startsWith('true\t1\t[object Object]\tSymbol(')).toBe(true);
  });

  it('returns empty clipboard text when the selection cannot form a matrix', () => {
    const base = {
      rows: [{ __rowKey: 'row-1', name: 'A' }],
      columnOrder: ['name'],
      rowKeyField: '__rowKey',
    };

    expect(buildSelectedCellClipboardText({ ...base, selectedCells: [] })).toBe('');
    expect(buildSelectedCellClipboardText({
      selectedCells: [{ rowKey: 'row-1', colName: 'name' }],
      rows: [],
      columnOrder: ['name'],
      rowKeyField: '__rowKey',
    })).toBe('');
    expect(buildSelectedCellClipboardText({
      selectedCells: [{ rowKey: 'row-1', colName: 'name' }],
      rows: base.rows,
      columnOrder: [],
      rowKeyField: '__rowKey',
    })).toBe('');
    expect(buildSelectedCellClipboardText({
      selectedCells: [{ rowKey: 'row-1', colName: 'name' }],
      rows: base.rows,
      columnOrder: ['name'],
      rowKeyField: '',
    })).toBe('');
    expect(buildSelectedCellClipboardText({
      ...base,
      selectedCells: [{ rowKey: 'missing', colName: 'name' }],
    })).toBe('');
    expect(buildSelectedCellClipboardText({
      ...base,
      selectedCells: [{ rowKey: 'row-1', colName: 'hidden' }],
    })).toBe('');
    expect(buildSelectedCellClipboardText({
      selectedCells: [{ rowKey: '', colName: 'name' }],
      rows: [{ name: 'A' }],
      columnOrder: ['name'],
      rowKeyField: '__rowKey',
    })).toBe('A');
  });

  it('fills unselected cells in a sparse rectangle with empty strings', () => {
    expect(buildSelectedCellClipboardText({
      selectedCells: [
        { rowKey: 'row-1', colName: 'a' },
        { rowKey: 'row-2', colName: 'b' },
      ],
      rows: [
        { __rowKey: 'row-1', a: 'A1', b: 'B1' },
        { __rowKey: 'row-2', a: 'A2', b: 'B2' },
      ],
      columnOrder: ['a', 'b'],
      rowKeyField: '__rowKey',
    })).toBe('A1\t\n\tB2');
  });

  it('preserves object JSON and fallback text in rich selected-cell payloads', () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const payload = buildSelectedCellClipboardPayload({
      selectedCells: [
        { rowKey: 'row-1', colName: 'meta' },
        { rowKey: 'row-1', colName: 'loop' },
      ],
      rows: [{
        __rowKey: 'row-1',
        meta: { a: 1 },
        loop: circular,
      }],
      columnOrder: ['meta', 'loop'],
      rowKeyField: '__rowKey',
    });

    expect(payload.html).toContain('<td>{&quot;a&quot;:1}</td>');
    expect(payload.html).toContain('<td>[object Object]</td>');
  });
});
