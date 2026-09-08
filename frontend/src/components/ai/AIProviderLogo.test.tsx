import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { AIProviderLogo, PRESET_ICON_SLUG } from './AIProviderLogo';

describe('AIProviderLogo', () => {
  it('maps known presets to brand SVG paths', () => {
    expect(PRESET_ICON_SLUG.openai).toBe('openai');
    expect(PRESET_ICON_SLUG['claude-subscription']).toBe('claudecode');
    expect(PRESET_ICON_SLUG['qwen-bailian']).toBe('alibabacloud');
    expect(PRESET_ICON_SLUG.atlascloud).toBe('atlascloud');
    expect(PRESET_ICON_SLUG.orcarouter).toBe('orcarouter');
    expect(PRESET_ICON_SLUG.zhipu).toBe('zhipu');
    expect(PRESET_ICON_SLUG.moonshot).toBe('moonshot');
    expect(PRESET_ICON_SLUG['volcengine-ark']).toBe('volcengine');
    expect(PRESET_ICON_SLUG['volcengine-coding']).toBe('volcengine');
    const markup = renderToStaticMarkup(<AIProviderLogo presetKey="openai" label="OpenAI" />);
    expect(markup).toContain('/icons/ai/openai.svg');
  });

  it('falls back to the first letter when there is no asset', () => {
    const markup = renderToStaticMarkup(<AIProviderLogo presetKey="unknown-provider" label="Kimi" />);
    expect(markup).toContain('is-fallback');
    expect(markup).toContain('K');
    expect(markup).not.toContain('/icons/ai/');
  });
});
