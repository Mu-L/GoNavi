import { describe, expect, it } from 'vitest';

import { calculateTableBodyBottomPadding, calculateVirtualTableScrollX } from './dataGridLayout';

describe('dataGridLayout helpers', () => {
  it('returns zero bottom padding without horizontal overflow', () => {
    expect(calculateTableBodyBottomPadding({
      hasHorizontalOverflow: false,
      floatingScrollbarHeight: 10,
      floatingScrollbarGap: 6,
    })).toBe(0);
  });

  it('adds safe area when horizontal overflow exists', () => {
    expect(calculateTableBodyBottomPadding({
      hasHorizontalOverflow: true,
      floatingScrollbarHeight: 10,
      floatingScrollbarGap: 6,
    })).toBe(28);
    expect(calculateTableBodyBottomPadding({
      hasHorizontalOverflow: true,
      floatingScrollbarHeight: 14,
      floatingScrollbarGap: 4,
    })).toBe(30);
  });

  it('keeps scroll width aligned with viewport or content width', () => {
    expect(calculateVirtualTableScrollX({ totalWidth: 646, tableViewportWidth: 1200, isMacLike: false })).toBe(1200);
    expect(calculateVirtualTableScrollX({ totalWidth: 646, tableViewportWidth: 0, isMacLike: false })).toBe(646);
    expect(calculateVirtualTableScrollX({ totalWidth: 1200, tableViewportWidth: 800, isMacLike: true })).toBe(1202);
  });
});
