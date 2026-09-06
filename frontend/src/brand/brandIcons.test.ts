import { describe, expect, it } from 'vitest';
import {
  BRAND_ICONS,
  resolveBrandAboutSrc,
  resolveBrandDockSrc,
  resolveBrandFullSrc,
  resolveBrandIconSrc,
  resolveBrandTitlebarSrc,
} from './brandIcons';

describe('brand icon asset resolution', () => {
  it('keeps tile assets for app surfaces and uses a transparent lockup on the about page', () => {
    for (const icon of BRAND_ICONS) {
      const selectedAsset = resolveBrandIconSrc(icon.id);
      expect(selectedAsset).toMatch(/^\/brand-icons\/\d{2}-.+\.svg$/);
      expect(resolveBrandFullSrc(icon.id)).toBe(selectedAsset);
      expect(resolveBrandDockSrc(icon.id)).toBe(selectedAsset);

      const aboutAsset = resolveBrandAboutSrc(icon.id);
      expect(aboutAsset).toMatch(/^\/brand-icons\/\d{2}-.+-about\.png$/);
      expect(aboutAsset).not.toBe(selectedAsset);
    }
  });

  it('uses the transparent compact mark for the default titlebar icon', () => {
    const defaultTitlebarAsset = '/brand-icons/03-ribbon-graphite-glow.svg';
    expect(resolveBrandTitlebarSrc('03')).toBe(defaultTitlebarAsset);
    expect(resolveBrandTitlebarSrc()).toBe(defaultTitlebarAsset);
    expect(resolveBrandTitlebarSrc('unknown')).toBe(defaultTitlebarAsset);
    expect(resolveBrandTitlebarSrc('01')).toBe(resolveBrandIconSrc('01'));
  });

  it('falls back to the default about lockup for invalid selections', () => {
    const defaultAboutAsset = resolveBrandAboutSrc('03');
    expect(resolveBrandAboutSrc()).toBe(defaultAboutAsset);
    expect(resolveBrandAboutSrc('unknown')).toBe(defaultAboutAsset);
  });
});
