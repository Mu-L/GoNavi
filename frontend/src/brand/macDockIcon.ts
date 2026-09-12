const DOCK_ICON_SIZE = 1024;
// The PNG handed to NSApp is already a complete Dock tile.  Leaving a
// 100px transparent border here makes GoNavi render visibly smaller than
// neighbouring macOS apps, so use the full 1024px canvas.
const DOCK_ICON_INSET = 0;
// The 0.9.7 mascot artwork is a full rounded white tile. Keep its original
// safe area so it does not appear larger than neighbouring Dock icons.
export const LEGACY_MASCOT_DOCK_ICON_INSET = 100;
// Keep the same rounded-tile proportion used by the source artwork.
const DOCK_ICON_CORNER_RADIUS_RATIO = 184 / 824;

export type DockIconRuntimeEnvironment = {
  platform?: unknown;
  buildType?: unknown;
};

export type MacOSDockImageRect = {
  x: number;
  y: number;
  width: number;
  height: number;
};

/**
 * macOS and Windows both support changing the native runtime icon. The
 * generated Wails bridge also exists in the browser build, so checking method
 * presence alone would still serialize and post a large image from the web.
 */
export function shouldSyncApplicationBrandIcon(environment?: DockIconRuntimeEnvironment | null): boolean {
  const platform = String(environment?.platform || '').trim().toLowerCase();
  return (platform === 'darwin' || platform === 'windows')
    && String(environment?.buildType || '').trim().toLowerCase() !== 'web';
}

function loadImage(src: string): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new Error(`failed to load dock icon: ${src}`));
    img.src = src;
  });
}

function canvasToBase64Png(canvas: HTMLCanvasElement): string {
  const dataUrl = canvas.toDataURL('image/png');
  const comma = dataUrl.indexOf(',');
  return comma >= 0 ? dataUrl.slice(comma + 1) : dataUrl;
}

export function calculateMacOSDockImageRect(
  imageWidth: number,
  imageHeight: number,
  inset = DOCK_ICON_INSET,
): MacOSDockImageRect {
  const safeInset = Math.max(0, Math.min((DOCK_ICON_SIZE / 2) - 1, Number(inset) || 0));
  const tileSize = DOCK_ICON_SIZE - (safeInset * 2);
  const sourceWidth = Math.max(1, Number(imageWidth) || tileSize);
  const sourceHeight = Math.max(1, Number(imageHeight) || tileSize);
  const scale = Math.min(tileSize / sourceWidth, tileSize / sourceHeight);
  const width = Math.round(sourceWidth * scale);
  const height = Math.round(sourceHeight * scale);

  return {
    x: Math.round((DOCK_ICON_SIZE - width) / 2),
    y: Math.round((DOCK_ICON_SIZE - height) / 2),
    width,
    height,
  };
}

export function calculateMacOSDockCornerRadius(rect: MacOSDockImageRect): number {
  return Math.round(Math.min(rect.width, rect.height) * DOCK_ICON_CORNER_RADIUS_RATIO);
}

function clipMacOSDockImage(ctx: CanvasRenderingContext2D, rect: MacOSDockImageRect): void {
  const radius = calculateMacOSDockCornerRadius(rect);
  const right = rect.x + rect.width;
  const bottom = rect.y + rect.height;

  ctx.beginPath();
  ctx.moveTo(rect.x + radius, rect.y);
  ctx.lineTo(right - radius, rect.y);
  ctx.arcTo(right, rect.y, right, rect.y + radius, radius);
  ctx.lineTo(right, bottom - radius);
  ctx.arcTo(right, bottom, right - radius, bottom, radius);
  ctx.lineTo(rect.x + radius, bottom);
  ctx.arcTo(rect.x, bottom, rect.x, bottom - radius, radius);
  ctx.lineTo(rect.x, rect.y + radius);
  ctx.arcTo(rect.x, rect.y, rect.x + radius, rect.y, radius);
  ctx.closePath();
  ctx.clip();
}

/**
 * Place the selected complete brand icon on a transparent macOS canvas.
 * Keep the source artwork intact while normalising its outer tile to the
 * standard macOS corner geometry.
 */
export async function composeMacOSDockIconBase64(
  src: string,
  options: { inset?: number } = {},
): Promise<string> {
  const img = await loadImage(src);
  const size = DOCK_ICON_SIZE;
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext('2d');
  if (!ctx) {
    throw new Error('2d context unavailable');
  }
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  const rect = calculateMacOSDockImageRect(
    img.naturalWidth || img.width,
    img.naturalHeight || img.height,
    options.inset,
  );
  clipMacOSDockImage(ctx, rect);
  ctx.drawImage(img, rect.x, rect.y, rect.width, rect.height);
  return canvasToBase64Png(canvas);
}

/**
 * Largest uniform centre crop that never clips the mascot artwork's content.
 */
export function calculateWindowsNativeIconSourceCrop(
  sourceSize: number,
  zoom = 1,
): { offset: number; size: number } {
  const safeSize = Math.max(1, Math.floor(Number(sourceSize) || 0));
  const safeZoom = Math.max(1, Math.min(2, Number(zoom) || 1));
  const crop = Math.floor((safeSize * (1 - 1 / safeZoom)) / 2);
  return { offset: crop, size: safeSize - crop * 2 };
}

// How much of the Windows tile the cut-out mascot mark should span. The mark
// is the whole icon (no tile), so it fills as much of the cell as possible.
const WINDOWS_NATIVE_MARK_TARGET_FRACTION = 0.94;
// Pixels within this distance of pure white count as tile background when the
// flood fill walks in from the canvas borders. The mascot artworks are drawn
// on a solid #fff tile, so only the connected tile region can ever match.
const MARK_BACKGROUND_TOLERANCE = 12;

export type MarkBoundingBox = { x: number; y: number; width: number; height: number };

/**
 * Clear the background connected to the canvas borders when it is (nearly)
 * white, and return the bounding box of what remains. A flood fill from the
 * borders is what keeps the white fur inside the mascot intact: interior white
 * pixels are never reachable from outside without crossing the artwork's
 * outlines. Fully transparent pixels propagate the fill so already-cut-out
 * sources keep working. Returns null when everything was cleared.
 */
export function removeConnectedNearWhiteBackground(
  data: Uint8ClampedArray,
  width: number,
  height: number,
  tolerance = MARK_BACKGROUND_TOLERANCE,
): MarkBoundingBox | null {
  const threshold = 255 - Math.max(0, Math.floor(tolerance));
  const visited = new Uint8Array(width * height);
  const stack: number[] = [];
  for (let x = 0; x < width; x++) {
    stack.push(x, (height - 1) * width + x);
  }
  for (let y = 0; y < height; y++) {
    stack.push(y * width, y * width + width - 1);
  }
  while (stack.length > 0) {
    const index = stack.pop() as number;
    if (index < 0 || index >= width * height || visited[index]) continue;
    visited[index] = 1;
    const alpha = data[index * 4 + 3];
    const isBackground = alpha === 0 || (
      alpha > 0 &&
      data[index * 4] >= threshold &&
      data[index * 4 + 1] >= threshold &&
      data[index * 4 + 2] >= threshold
    );
    if (!isBackground) continue;
    data[index * 4 + 3] = 0;
    const x = index % width;
    const y = (index - x) / width;
    if (x > 0) stack.push(index - 1);
    if (x < width - 1) stack.push(index + 1);
    if (y > 0) stack.push(index - width);
    if (y < height - 1) stack.push(index + width);
  }
  let minX = width;
  let minY = height;
  let maxX = -1;
  let maxY = -1;
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      if (data[(y * width + x) * 4 + 3] > 16) {
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (maxX < 0) return null;
  return { x: minX, y: minY, width: maxX - minX + 1, height: maxY - minY + 1 };
}

/**
 * Bounding box of all remaining opaque pixels.
 */
export function opaqueBoundingBox(
  data: Uint8ClampedArray,
  width: number,
  height: number,
): MarkBoundingBox | null {
  let minX = width;
  let minY = height;
  let maxX = -1;
  let maxY = -1;
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      if (data[(y * width + x) * 4 + 3] > 16) {
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
      }
    }
  }
  if (maxX < 0) return null;
  return { x: minX, y: minY, width: maxX - minX + 1, height: maxY - minY + 1 };
}

/**
 * Full cut-out pipeline for the mascot artwork: drop the white tile and
 * detached add-ons (the GoNavi word mark), and return the final mark's
 * bounding box. Returns null when nothing remains.
 */
export function cutOutMarkFromTile(
  data: Uint8ClampedArray,
  width: number,
  height: number,
): MarkBoundingBox | null {
  removeConnectedNearWhiteBackground(data, width, height);
  keepLargestOpaqueComponent(data, width, height);
  return opaqueBoundingBox(data, width, height);
}
export function keepLargestOpaqueComponent(
  data: Uint8ClampedArray,
  width: number,
  height: number,
): MarkBoundingBox | null {
  const componentLabel = new Int32Array(width * height).fill(-1);
  const stack: number[] = [];
  let largestLabel = -1;
  let largestSize = 0;
  let label = 0;
  for (let start = 0; start < width * height; start++) {
    if (componentLabel[start] >= 0 || data[start * 4 + 3] <= 16) continue;
    let size = 0;
    stack.push(start);
    componentLabel[start] = label;
    while (stack.length > 0) {
      const index = stack.pop() as number;
      size++;
      const x = index % width;
      const y = (index - x) / width;
      if (x > 0 && componentLabel[index - 1] < 0 && data[(index - 1) * 4 + 3] > 16) {
        componentLabel[index - 1] = label;
        stack.push(index - 1);
      }
      if (x < width - 1 && componentLabel[index + 1] < 0 && data[(index + 1) * 4 + 3] > 16) {
        componentLabel[index + 1] = label;
        stack.push(index + 1);
      }
      if (y > 0 && componentLabel[index - width] < 0 && data[(index - width) * 4 + 3] > 16) {
        componentLabel[index - width] = label;
        stack.push(index - width);
      }
      if (y < height - 1 && componentLabel[index + width] < 0 && data[(index + width) * 4 + 3] > 16) {
        componentLabel[index + width] = label;
        stack.push(index + width);
      }
    }
    if (size > largestSize) {
      largestSize = size;
      largestLabel = label;
    }
    label++;
  }
  if (largestLabel < 0) return null;
  let minX = width;
  let minY = height;
  let maxX = -1;
  let maxY = -1;
  for (let index = 0; index < width * height; index++) {
    if (componentLabel[index] !== largestLabel) {
      data[index * 4 + 3] = 0;
      continue;
    }
    const x = index % width;
    const y = (index - x) / width;
    if (x < minX) minX = x;
    if (x > maxX) maxX = x;
    if (y < minY) minY = y;
    if (y > maxY) maxY = y;
  }
  return { x: minX, y: minY, width: maxX - minX + 1, height: maxY - minY + 1 };
}

/**
 * Centre and scale the cut-out mark so its largest dimension spans the target
 * fraction of the tile.
 */
export function calculateFittedMarkDrawRect(
  markBox: MarkBoundingBox,
  canvasSize: number,
  targetFraction = WINDOWS_NATIVE_MARK_TARGET_FRACTION,
): { x: number; y: number; width: number; height: number } {
  const safeFraction = Math.max(0.1, Math.min(1, targetFraction));
  const scale = Math.min(
    (canvasSize * safeFraction) / Math.max(1, markBox.width),
    (canvasSize * safeFraction) / Math.max(1, markBox.height),
  );
  const width = Math.max(1, Math.round(markBox.width * scale));
  const height = Math.max(1, Math.round(markBox.height * scale));
  return {
    x: Math.round((canvasSize - width) / 2),
    y: Math.round((canvasSize - height) / 2),
    width,
    height,
  };
}

/**
 * Compose the Windows native tile. The artwork fills the whole canvas (no
 * macOS Dock safe-area inset). With `transparentMark`, the mascot's white tile
 * and the detached GoNavi word mark are cut away and the dog itself becomes
 * the whole icon, centred at the target fraction — no background colour of
 * any kind. Windows scales the ICO down to 16-32px.
 */
export async function composeWindowsNativeIconBase64(
  src: string,
  options: { zoom?: number; transparentMark?: boolean } = {},
): Promise<string> {
  const img = await loadImage(src);
  const size = DOCK_ICON_SIZE;
  const canvas = document.createElement('canvas');
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext('2d');
  if (!ctx) {
    throw new Error('2d context unavailable');
  }
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  const rect = calculateMacOSDockImageRect(
    img.naturalWidth || img.width,
    img.naturalHeight || img.height,
    0,
  );
  const zoom = Math.max(1, Number(options.zoom) || 1);
  if (options.transparentMark === true) {
    const sourceWidth = img.naturalWidth || img.width;
    const sourceHeight = img.naturalHeight || img.height;
    const work = document.createElement('canvas');
    work.width = sourceWidth;
    work.height = sourceHeight;
    const workCtx = work.getContext('2d', { willReadFrequently: true });
    if (!workCtx) {
      throw new Error('2d context unavailable');
    }
    workCtx.imageSmoothingEnabled = true;
    workCtx.imageSmoothingQuality = 'high';
    workCtx.drawImage(img, 0, 0);
    const imageData = workCtx.getImageData(0, 0, sourceWidth, sourceHeight);
    const markBox = cutOutMarkFromTile(imageData.data, sourceWidth, sourceHeight);
    if (markBox) {
      workCtx.putImageData(imageData, 0, 0);
      const draw = calculateFittedMarkDrawRect(markBox, size);
      ctx.drawImage(
        work,
        markBox.x,
        markBox.y,
        markBox.width,
        markBox.height,
        draw.x,
        draw.y,
        draw.width,
        draw.height,
      );
      return canvasToBase64Png(canvas);
    }
    // The cut-out cleared everything (degenerate source); keep the plain tile
    // so the icon never regresses to an empty image.
  }
  clipMacOSDockImage(ctx, rect);
  if (zoom <= 1) {
    ctx.drawImage(img, rect.x, rect.y, rect.width, rect.height);
    return canvasToBase64Png(canvas);
  }
  const sourceWidth = img.naturalWidth || img.width;
  const sourceHeight = img.naturalHeight || img.height;
  const widthCrop = calculateWindowsNativeIconSourceCrop(sourceWidth, zoom);
  const heightCrop = calculateWindowsNativeIconSourceCrop(sourceHeight, zoom);
  ctx.drawImage(
    img,
    widthCrop.offset,
    heightCrop.offset,
    widthCrop.size,
    heightCrop.size,
    rect.x,
    rect.y,
    rect.width,
    rect.height,
  );
  return canvasToBase64Png(canvas);
}
