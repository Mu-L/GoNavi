import { describe, expect, it } from 'vitest';

import {
  getMacNativeTitlebarPaddingLeft,
  getMacNativeTitlebarPaddingRight,
  shouldHandleMacNativeFullscreenShortcut,
  shouldSuppressMacNativeEscapeExit,
} from './macWindow';

describe('macWindow helpers', () => {
  it('uses compact padding when native controls are disabled', () => {
    expect(getMacNativeTitlebarPaddingLeft(1, false)).toBe(16);
    expect(getMacNativeTitlebarPaddingRight(1, false)).toBe(0);
  });

  it('reserves traffic-light safe area when native controls are enabled', () => {
    expect(getMacNativeTitlebarPaddingLeft(1, true)).toBe(96);
    expect(getMacNativeTitlebarPaddingRight(1, true)).toBe(16);
  });

  it('keeps minimum safe area under small ui scales', () => {
    expect(getMacNativeTitlebarPaddingLeft(0.5, true)).toBe(88);
    expect(getMacNativeTitlebarPaddingRight(0.5, true)).toBe(12);
  });

  it('matches Control+Command+F only for mac native mode', () => {
    expect(shouldHandleMacNativeFullscreenShortcut(true, true, { ctrlKey: true, metaKey: true, altKey: false, key: 'f' })).toBe(true);
    expect(shouldHandleMacNativeFullscreenShortcut(true, true, { ctrlKey: true, metaKey: true, altKey: false, key: 'F' })).toBe(true);
  });

  it('rejects conflicting modifiers and non-target keys', () => {
    expect(shouldHandleMacNativeFullscreenShortcut(true, true, { ctrlKey: true, metaKey: true, altKey: true, key: 'f' })).toBe(false);
    expect(shouldHandleMacNativeFullscreenShortcut(true, true, { ctrlKey: true, metaKey: false, altKey: false, key: 'f' })).toBe(false);
    expect(shouldHandleMacNativeFullscreenShortcut(false, true, { ctrlKey: true, metaKey: true, altKey: false, key: 'f' })).toBe(false);
    expect(shouldHandleMacNativeFullscreenShortcut(true, false, { ctrlKey: true, metaKey: true, altKey: false, key: 'f' })).toBe(false);
    expect(shouldHandleMacNativeFullscreenShortcut(true, true, { ctrlKey: true, metaKey: true, altKey: false, key: 'g' })).toBe(false);
  });

  it('suppresses Escape only in mac native fullscreen mode', () => {
    expect(shouldSuppressMacNativeEscapeExit(true, true, true, { key: 'Escape', defaultPrevented: false })).toBe(true);
    expect(shouldSuppressMacNativeEscapeExit(true, true, false, { key: 'Escape', defaultPrevented: false })).toBe(false);
    expect(shouldSuppressMacNativeEscapeExit(true, false, true, { key: 'Escape', defaultPrevented: false })).toBe(false);
    expect(shouldSuppressMacNativeEscapeExit(false, true, true, { key: 'Escape', defaultPrevented: false })).toBe(false);
    expect(shouldSuppressMacNativeEscapeExit(true, true, true, { key: 'Enter', defaultPrevented: false })).toBe(false);
    expect(shouldSuppressMacNativeEscapeExit(true, true, true, { key: 'Escape', defaultPrevented: true })).toBe(false);
  });
});
