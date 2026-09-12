import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  calculateMacOSDockCornerRadius,
  calculateMacOSDockImageRect,
  calculateWindowsNativeIconSourceCrop,
  composeMacOSDockIconBase64,
  composeWindowsNativeIconBase64,
  shouldSyncApplicationBrandIcon,
} from './macDockIcon';

describe('shouldSyncApplicationBrandIcon', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('allows native macOS and Windows runtimes', () => {
    expect(shouldSyncApplicationBrandIcon({ platform: 'darwin', buildType: 'production' })).toBe(true);
    expect(shouldSyncApplicationBrandIcon({ platform: 'DARWIN', buildType: 'debug' })).toBe(true);
    expect(shouldSyncApplicationBrandIcon({ platform: 'windows', buildType: 'production' })).toBe(true);
    expect(shouldSyncApplicationBrandIcon({ platform: 'WINDOWS', buildType: 'debug' })).toBe(true);
  });

  it('skips browser and unsupported desktop runtimes before image composition', () => {
    expect(shouldSyncApplicationBrandIcon({ platform: 'darwin', buildType: 'web' })).toBe(false);
    expect(shouldSyncApplicationBrandIcon({ platform: 'windows', buildType: 'web' })).toBe(false);
    expect(shouldSyncApplicationBrandIcon({ platform: 'linux', buildType: 'production' })).toBe(false);
    expect(shouldSyncApplicationBrandIcon()).toBe(false);
  });

  it('fills the Dock canvas so GoNavi matches neighboring macOS app icons', () => {
    const rect = calculateMacOSDockImageRect(512, 512);

    expect(rect).toEqual({
      x: 0,
      y: 0,
      width: 1024,
      height: 1024,
    });
    expect(calculateMacOSDockCornerRadius(rect)).toBe(229);
  });

  it('keeps the restored 0.9.7 mascot inside the Dock safe area', () => {
    expect(calculateMacOSDockImageRect(512, 512, 100)).toEqual({
      x: 100,
      y: 100,
      width: 824,
      height: 824,
    });
  });

  it('centres portrait brand lockups without stretching them into a square', () => {
    expect(calculateMacOSDockImageRect(272, 449)).toEqual({
      x: 202,
      y: 0,
      width: 620,
      height: 1024,
    });
  });

  it('clips the complete brand image to the standard macOS rounded tile before drawing', async () => {
    const calls: string[] = [];
    const arcTo = vi.fn((...args: number[]) => calls.push(`arcTo:${args[4]}`));
    const context = {
      beginPath: vi.fn(() => calls.push('beginPath')),
      moveTo: vi.fn(() => calls.push('moveTo')),
      lineTo: vi.fn(() => calls.push('lineTo')),
      arcTo,
      closePath: vi.fn(() => calls.push('closePath')),
      clip: vi.fn(() => calls.push('clip')),
      drawImage: vi.fn(() => calls.push('drawImage')),
      imageSmoothingEnabled: false,
      imageSmoothingQuality: 'low',
    } as unknown as CanvasRenderingContext2D;
    const canvas = {
      width: 0,
      height: 0,
      getContext: vi.fn(() => context),
      toDataURL: vi.fn(() => 'data:image/png;base64,encoded'),
    } as unknown as HTMLCanvasElement;

    class FakeImage {
      onload: (() => void) | null = null;
      onerror: (() => void) | null = null;
      naturalWidth = 512;
      naturalHeight = 512;
      width = 512;
      height = 512;

      set src(_value: string) {
        queueMicrotask(() => this.onload?.());
      }
    }

    vi.stubGlobal('Image', FakeImage);
    vi.stubGlobal('document', { createElement: vi.fn(() => canvas) });

    await expect(composeMacOSDockIconBase64('/brand-icons/03-ribbon-graphite-glow.webp')).resolves.toBe('encoded');
    expect(arcTo.mock.calls.map((args) => args[4])).toEqual([229, 229, 229, 229]);
    expect(calls.indexOf('clip')).toBeGreaterThan(calls.indexOf('beginPath'));
    expect(calls.indexOf('drawImage')).toBeGreaterThan(calls.indexOf('clip'));
  });
});

describe('calculateWindowsNativeIconSourceCrop', () => {
  it('keeps the full source when no zoom is requested', () => {
    expect(calculateWindowsNativeIconSourceCrop(512)).toEqual({ offset: 0, size: 512 });
    expect(calculateWindowsNativeIconSourceCrop(512, 1)).toEqual({ offset: 0, size: 512 });
  });

  it('crops the 1.13 mascot zoom symmetrically without touching the artwork', () => {
    expect(calculateWindowsNativeIconSourceCrop(512, 1.13)).toEqual({ offset: 29, size: 454 });
  });

  it('degrades degenerate sources and clamps runaway zoom values', () => {
    expect(calculateWindowsNativeIconSourceCrop(0, 1.13)).toEqual({ offset: 0, size: 1 });
    expect(calculateWindowsNativeIconSourceCrop(512, Number.NaN)).toEqual({ offset: 0, size: 512 });
    expect(calculateWindowsNativeIconSourceCrop(512, 9)).toEqual({ offset: 128, size: 256 });
  });
});

describe('composeWindowsNativeIconBase64', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  function stubCanvasContext() {
    const drawImage = vi.fn();
    const arcTo = vi.fn();
    const context = {
      beginPath: vi.fn(),
      moveTo: vi.fn(),
      lineTo: vi.fn(),
      arcTo,
      closePath: vi.fn(),
      clip: vi.fn(),
      drawImage,
      imageSmoothingEnabled: false,
      imageSmoothingQuality: 'low',
    } as unknown as CanvasRenderingContext2D;
    const canvas = {
      width: 0,
      height: 0,
      getContext: vi.fn(() => context),
      toDataURL: vi.fn(() => 'data:image/png;base64,encoded'),
    } as unknown as HTMLCanvasElement;
    vi.stubGlobal('document', { createElement: vi.fn(() => canvas) });
    return { drawImage, arcTo };
  }

  function stubSquareImage(): void {
    class FakeImage {
      onload: (() => void) | null = null;
      onerror: (() => void) | null = null;
      naturalWidth = 512;
      naturalHeight = 512;
      width = 512;
      height = 512;

      set src(_value: string) {
        queueMicrotask(() => this.onload?.());
      }
    }
    vi.stubGlobal('Image', FakeImage);
  }

  it('fills the whole Windows tile without the macOS Dock safe-area inset', async () => {
    stubSquareImage();
    const { drawImage, arcTo } = stubCanvasContext();

    await expect(composeWindowsNativeIconBase64('/brand-icons/03-ribbon-graphite-glow.svg')).resolves.toBe('encoded');
    expect(drawImage.mock.calls[0]).toEqual([
      expect.anything(),
      0,
      0,
      1024,
      1024,
    ]);
    expect(arcTo.mock.calls.map((args) => args[4])).toEqual([229, 229, 229, 229]);
  });

  it('centre-crops the mascot zoom across the full rounded tile', async () => {
    stubSquareImage();
    const { drawImage } = stubCanvasContext();

    await expect(composeWindowsNativeIconBase64('/brand-icons/07-database-hug.webp', { zoom: 1.13 }))
      .resolves.toBe('encoded');
    expect(drawImage.mock.calls[0]).toEqual([
      expect.anything(),
      29,
      29,
      454,
      454,
      0,
      0,
      1024,
      1024,
    ]);
  });
});
