import { describe, expect, it } from 'vitest';

import { buildRedisWorkbenchTheme } from './redisViewerWorkbenchTheme';

describe('buildRedisWorkbenchTheme', () => {
  it('builds dark redis workbench theme', () => {
    const darkTheme = buildRedisWorkbenchTheme({ darkMode: true, opacity: 0.72, blur: 14 });
    expect(darkTheme.isDark).toBe(true);
    expect(darkTheme.panelBg).toMatch(/^rgba\(/);
    expect(darkTheme.toolbarPrimaryBg).toMatch(/^linear-gradient\(/);
    expect(darkTheme.actionDangerBg).not.toBe(darkTheme.actionSecondaryBg);
    expect(darkTheme.treeSelectedBg).not.toBe(darkTheme.treeHoverBg);
    expect(darkTheme.appBg).toMatch(/rgba\(15, 15, 17,/);
    expect(darkTheme.panelBg).toMatch(/rgba\(24, 24, 28,/);
    expect(darkTheme.panelBgStrong).toMatch(/rgba\(31, 31, 36,/);
    expect(darkTheme.backdropFilter).toBe('blur(14px)');
  });

  it('builds light redis workbench theme', () => {
    const lightTheme = buildRedisWorkbenchTheme({ darkMode: false, opacity: 1, blur: 0 });
    expect(lightTheme.isDark).toBe(false);
    expect(lightTheme.panelBg).toMatch(/^rgba\(/);
    expect(lightTheme.contentEmptyBg).toMatch(/^linear-gradient\(/);
    expect(lightTheme.textPrimary).not.toBe(lightTheme.textSecondary);
    expect(lightTheme.statusTagBg).not.toBe(lightTheme.statusTagMutedBg);
    expect(lightTheme.backdropFilter).toBe('none');
  });
});
