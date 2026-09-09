import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { BUILTIN_CUSTOM_THEME_PRESETS } from './utils/customThemePresets';

const appSource = readFileSync(new URL('./App.tsx', import.meta.url), 'utf8');
const v2ThemeCss = readFileSync(new URL('./v2-theme.css', import.meta.url), 'utf8');
const v2ThemeCssWithoutComments = v2ThemeCss.replace(/\/\*[\s\S]*?\*\//g, '');
const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const findRuleBodies = (selector: string): string[] => Array.from(
  v2ThemeCssWithoutComments.matchAll(/(?<selectors>[^{}]+)\{(?<body>[^{}]*)\}/g),
).filter((match) => (match.groups?.selectors ?? '')
  .split(',')
  .some((candidate) => candidate.trim() === selector))
  .map((match) => match.groups?.body ?? '');

describe('V2 window transparency contract', () => {
  it('publishes the effective opacity for V2 theme surfaces', () => {
    expect(appSource).toContain(
      "document.documentElement.style.setProperty('--gn-window-opacity', `${effectiveOpacity}`);",
    );
    expect(appSource).toContain(
      "document.documentElement.style.setProperty('--gn-window-opacity-percent', `${effectiveOpacity * 100}%`);",
    );
    expect(appSource).not.toContain('--gn-titlebar-opacity');
    expect(appSource).toContain(
      'SetWindowTranslucency(resolvedAppearance.opacity, resolvedAppearance.blur, darkMode)',
    );
  });

  it('keeps the main light and dark surfaces connected to the live opacity', () => {
    const surfaceProperties = [
      '--gn-bg-app',
      '--gn-bg-chrome',
      '--gn-bg-chrome-strong',
      '--gn-bg-panel',
      '--gn-bg-panel-2',
      '--gn-bg-input',
    ];

    for (const property of surfaceProperties) {
      const declarations = v2ThemeCss.match(
        new RegExp(`${escapeRegExp(property)}\\s*:\\s*[^;]+`, 'g'),
      ) ?? [];
      expect(declarations, property).toHaveLength(2);
      expect(declarations.every((declaration) => declaration.includes('var(--gn-window-opacity, 1)'))).toBe(true);
    }
  });

  it('uses the adjacent panel-2 theme color with the shared window opacity', () => {
    const titlebarDeclarations = v2ThemeCss.match(/--gn-bg-titlebar\s*:\s*[^;]+/g) ?? [];
    expect(titlebarDeclarations).toHaveLength(2);
    expect(titlebarDeclarations.every((declaration) => declaration.includes('var(--gn-window-opacity, 1)'))).toBe(true);
    expect(titlebarDeclarations[0]).toContain('rgb(250 250 248');
    expect(titlebarDeclarations[1]).toContain('rgb(27 31 39');
    expect(v2ThemeCss).toMatch(
      /body\[data-ui-version="v2"\] \.gn-v2-titlebar\s*\{[^}]*background:\s*var\(--gn-bg-titlebar\) !important;/s,
    );

    const lilacDusk = BUILTIN_CUSTOM_THEME_PRESETS.find((preset) => preset.id === 'builtin-lilac-dusk');
    expect(lilacDusk?.css).toContain(
      '--gn-bg-titlebar: color-mix(in srgb, #2d2939 var(--gn-window-opacity-percent, 100%), transparent);',
    );
  });

  it('paints one shared theme backdrop before the regional translucent surfaces', () => {
    const transparentSelectors = [
      '.ant-layout',
      '.gn-v2-sidebar-shell',
      '.gn-v2-sidebar-redesign',
      '.gn-v2-object-explorer',
      '.gn-v2-explorer-actions',
      '.gn-v2-app-shell',
      '.gn-v2-workspace-shell',
      '.gn-v2-workspace-body',
      '.gn-v2-tab-workbench',
      '.gn-v2-empty-workbench',
      '.gn-v2-empty-hero',
      '.gn-v2-empty-recent-card',
      '.gn-v2-empty-resource-card',
    ];

    for (const selector of transparentSelectors) {
      const declarations = findRuleBodies(`body[data-ui-version="v2"] ${selector}`);
      expect(declarations.length, selector).toBeGreaterThan(0);
      expect(
        declarations.every((body) => {
          const backgrounds = Array.from(
            body.matchAll(/background\s*:\s*(?<value>[^;]+);/g),
          ).map((match) => match.groups?.value.trim().replace(/\s*!important$/, ''));
          return backgrounds.every((value) => value === 'transparent');
        }),
        selector,
      ).toBe(true);
    }

    expect(appSource).toContain('className="gn-v2-app-root"');
    expect(v2ThemeCss).toMatch(
      /body\[data-ui-version="v2"\] \.gn-v2-app-root\s*\{[^}]*background:\s*var\(--gn-bg-app\) !important;/s,
    );
    expect(v2ThemeCss).toMatch(
      /body\[data-ui-version="v2"\] \.ant-layout-content\s*\{[^}]*background:\s*var\(--gn-bg-panel-2\) !important;/s,
    );
    expect(v2ThemeCss).toMatch(
      /body\[data-ui-version="v2"\] \.ant-layout-sider\s*\{[^}]*background:\s*var\(--gn-bg-panel-2\) !important;/s,
    );
    expect(appSource).toContain(
      "background: 'transparent', marginBottom:",
    );
    expect(appSource).not.toContain(
      "background: 'var(--gn-bg-panel-2)', marginBottom:",
    );
  });

  it('keeps built-in theme surfaces connected to the live opacity', () => {
    const surfaceProperties = [
      '--gn-bg-app',
      '--gn-bg-chrome',
      '--gn-bg-panel',
      '--gn-bg-panel-2',
      '--gn-bg-titlebar',
      '--gn-bg-input',
      '--gn-bg-subtle',
    ];

    for (const preset of BUILTIN_CUSTOM_THEME_PRESETS) {
      for (const property of surfaceProperties) {
        const declaration = preset.css.match(
          new RegExp(`${escapeRegExp(property)}\\s*:\\s*[^;]+`),
        )?.[0];
        expect(declaration, `${preset.id} ${property}`).toContain('var(--gn-window-opacity-percent, 100%)');
      }
    }
  });

  it('uses a white foreground hierarchy for every dark translucent theme', () => {
    expect(v2ThemeCss).toMatch(
      /body\[data-ui-version="v2"\]\[data-theme="dark"\]\s*\{[^}]*--gn-fg-1:\s*#ffffff;[^}]*--gn-fg-2:\s*#f5f5f5;[^}]*--gn-fg-3:\s*#e6e6e6;[^}]*--gn-fg-4:\s*#d9d9d9;[^}]*--gn-fg-5:\s*#cccccc;/s,
    );

    const darkPresets = BUILTIN_CUSTOM_THEME_PRESETS.filter((preset) => (
      preset.css.includes('color-scheme: dark;')
    ));
    expect(darkPresets.length).toBeGreaterThan(0);
    for (const preset of darkPresets) {
      expect(preset.css, preset.id).toContain('--gn-fg-1: #ffffff;');
      expect(preset.css, preset.id).toContain('--gn-fg-2: #f5f5f5;');
      expect(preset.css, preset.id).toContain('--gn-fg-3: #e6e6e6;');
      expect(preset.css, preset.id).toContain('--gn-fg-4: #d9d9d9;');
      expect(preset.css, preset.id).toContain('--gn-fg-5: #cccccc;');
    }
  });
});
