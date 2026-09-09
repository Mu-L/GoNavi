import { describe, expect, it } from 'vitest';

import { resolveSidebarTreeVirtualHeight } from './sidebarV2Utils';

describe('resolveSidebarTreeVirtualHeight', () => {
  it('matches the V2 tree virtual viewport to the visible holder height', () => {
    expect(resolveSidebarTreeVirtualHeight(500)).toBe(464);
  });

  it('never returns a negative height and preserves subpixel measurements', () => {
    expect(resolveSidebarTreeVirtualHeight(20)).toBe(0);
    expect(resolveSidebarTreeVirtualHeight(Number.NaN)).toBe(0);
    expect(resolveSidebarTreeVirtualHeight(500.9)).toBeCloseTo(464.9);
  });
});
