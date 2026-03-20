import { describe, expect, it } from 'vitest';

import { buildOverlayWorkbenchTheme } from './overlayWorkbenchTheme';

describe('buildOverlayWorkbenchTheme', () => {
  it('builds dark theme tokens', () => {
    const darkTheme = buildOverlayWorkbenchTheme(true);
    expect(darkTheme.isDark).toBe(true);
    expect(darkTheme.shellBg).toMatch(/rgba\(15, 15, 17,/);
    expect(darkTheme.sectionBg).toMatch(/rgba\(255,?\s*255,?\s*255,?\s*0\.03\)/);
    expect(darkTheme.iconColor).toBe('#ffd666');
  });

  it('builds light theme tokens', () => {
    const lightTheme = buildOverlayWorkbenchTheme(false);
    expect(lightTheme.isDark).toBe(false);
    expect(lightTheme.shellBg).toMatch(/rgba\(255,255,255,0\.98\)/);
    expect(lightTheme.sectionBg).toMatch(/rgba\(255,?\s*255,?\s*255,?\s*0\.84\)/);
    expect(lightTheme.iconColor).toBe('#1677ff');
  });
});
