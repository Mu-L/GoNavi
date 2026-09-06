export type BrandIconId =
  | '01'
  | '02'
  | '03'
  | '04'
  | '05'
  | '06';

export type BrandIconDefinition = {
  id: BrandIconId;
  slug: string;
  titleZh: string;
  titleEn: string;
  iconPath: string;
  aboutPath: string;
  titlebarPath?: string;
};

export const DEFAULT_BRAND_ICON_ID: BrandIconId = '03';

export const BRAND_ICONS: BrandIconDefinition[] = [
  {
    id: '01',
    slug: 'ribbon-graphite-air',
    titleZh: '石墨碳白',
    titleEn: 'Graphite air',
    iconPath: '/brand-icons/01-ribbon-graphite-air.svg',
    aboutPath: '/brand-icons/01-ribbon-graphite-air-about.png',
  },
  {
    id: '02',
    slug: 'ribbon-graphite',
    titleZh: '石墨商务',
    titleEn: 'Graphite business',
    iconPath: '/brand-icons/02-ribbon-graphite.svg',
    aboutPath: '/brand-icons/02-ribbon-graphite-about.png',
  },
  {
    id: '03',
    slug: 'ribbon-graphite-glow',
    titleZh: '石墨冷蓝',
    titleEn: 'Graphite glow',
    iconPath: '/brand-icons/03-ribbon-graphite-glow.svg',
    aboutPath: '/brand-icons/03-ribbon-graphite-glow-about.png',
    titlebarPath: '/brand-icons/03-ribbon-graphite-glow.svg',
  },
  {
    id: '04',
    slug: 'ribbon-indigo-light',
    titleZh: '浅底深紫',
    titleEn: 'Indigo light',
    iconPath: '/brand-icons/04-ribbon-indigo-light.svg',
    aboutPath: '/brand-icons/04-ribbon-indigo-light-about.png',
  },
  {
    id: '05',
    slug: 'ribbon-graphite-light',
    titleZh: '浅底石墨',
    titleEn: 'Graphite on light',
    iconPath: '/brand-icons/05-ribbon-graphite-light.svg',
    aboutPath: '/brand-icons/05-ribbon-graphite-light-about.png',
  },
  {
    id: '06',
    slug: 'ribbon-lilac-dark',
    titleZh: '石墨浅紫',
    titleEn: 'Lilac on graphite',
    iconPath: '/brand-icons/06-ribbon-lilac-dark.svg',
    aboutPath: '/brand-icons/06-ribbon-lilac-dark-about.png',
  },
];

const BRAND_ICON_BY_ID = new Map(BRAND_ICONS.map((item) => [item.id, item]));

export function sanitizeBrandIconId(value: unknown): BrandIconId {
  const raw = String(value || '').trim();
  if (BRAND_ICON_BY_ID.has(raw as BrandIconId)) {
    return raw as BrandIconId;
  }
  return DEFAULT_BRAND_ICON_ID;
}

export function resolveBrandIcon(id?: unknown): BrandIconDefinition {
  return BRAND_ICON_BY_ID.get(sanitizeBrandIconId(id)) || BRAND_ICONS[2];
}

export function resolveBrandIconSrc(id?: unknown): string {
  return resolveBrandIcon(id).iconPath;
}

export function resolveBrandFullSrc(id?: unknown): string {
  return resolveBrandIconSrc(id);
}

export function resolveBrandAboutSrc(id?: unknown): string {
  return resolveBrandIcon(id).aboutPath;
}

export function resolveBrandTitlebarSrc(id?: unknown): string {
  const icon = resolveBrandIcon(id);
  return icon.titlebarPath || icon.iconPath;
}

/** Dock / runtime surfaces use the same SVG lockup as BrandIconPicker. */
export function resolveBrandDockSrc(id?: unknown): string {
  return resolveBrandIconSrc(id);
}
