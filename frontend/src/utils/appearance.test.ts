import { describe, expect, it } from 'vitest';

import { blurToFilter, normalizeBlurForPlatform, normalizeOpacityForPlatform, resolveAppearanceValues } from './appearance';

describe('appearance helpers', () => {
  it('falls back to opaque non-blurred appearance when disabled', () => {
    expect(resolveAppearanceValues({ enabled: false, opacity: 0.3, blur: 12 })).toEqual({ opacity: 1, blur: 0 });
  });

  it('preserves configured values when appearance is enabled', () => {
    expect(resolveAppearanceValues({ enabled: true, opacity: 0.72, blur: 9 })).toEqual({ opacity: 0.72, blur: 9 });
  });

  it('caps opacity at full opacity upper bound', () => {
    expect(normalizeOpacityForPlatform(2)).toBe(1);
  });

  it('never returns negative blur and formats blur filter correctly', () => {
    expect(normalizeBlurForPlatform(-4)).toBe(0);
    expect(blurToFilter(0)).toBeUndefined();
    expect(blurToFilter(8)).toBe('blur(8px)');
  });
});
