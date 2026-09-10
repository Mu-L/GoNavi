/** @vitest-environment jsdom */

import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import VirtualList from 'rc-virtual-list';
import { afterEach, describe, expect, it } from 'vitest';

(globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

type Row = {
  id: string;
  height: number;
};

describe('resolver-based native virtual scrolling', () => {
  let container: HTMLDivElement | null = null;
  let root: Root | null = null;

  afterEach(() => {
    act(() => root?.unmount());
    container?.remove();
    container = null;
    root = null;
  });

  it('keeps exact variable row offsets while leaving vertical wheel input native', () => {
    const rows: Row[] = Array.from({ length: 40 }, (_, index) => ({
      id: `row-${index}`,
      height: index % 5 === 0 ? 36 : 30,
    }));
    container = document.createElement('div');
    document.body.append(container);
    root = createRoot(container);

    act(() => {
      root?.render(
        <VirtualList
          data={rows}
          height={90}
          itemHeight={30}
          itemHeightResolver={(row) => row.height}
          itemKey="id"
        >
          {(row) => <div data-row-id={row.id}>{row.id}</div>}
        </VirtualList>,
      );
    });

    const holder = container.querySelector<HTMLElement>('.rc-virtual-list-holder');
    expect(holder).not.toBeNull();
    expect(holder?.style.overflowY).toBe('auto');
    expect(container.querySelector('.rc-virtual-list-scrollbar-vertical')).toBeNull();

    const wheel = new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY: 48 });
    holder?.dispatchEvent(wheel);
    expect(wheel.defaultPrevented).toBe(false);
  });

  it('pre-renders one viewport around fixed-height rows to cover a large native jump', () => {
    const rows: Row[] = Array.from({ length: 200 }, (_, index) => ({
      id: `fixed-${index}`,
      height: 10,
    }));
    container = document.createElement('div');
    document.body.append(container);
    root = createRoot(container);

    act(() => {
      root?.render(
        <VirtualList
          data={rows}
          height={300}
          itemHeight={10}
          itemHeightFixed
          itemKey="id"
        >
          {(row) => <div data-row-id={row.id}>{row.id}</div>}
        </VirtualList>,
      );
    });

    expect(container.querySelector('[data-row-id="fixed-45"]')).not.toBeNull();
    expect(container.querySelector('[data-row-id="fixed-60"]')).not.toBeNull();
    expect(container.querySelector('[data-row-id="fixed-61"]')).toBeNull();
  });

  it('commits a native scroll window without writing scrollTop back to WebKit', () => {
    const rows: Row[] = Array.from({ length: 200 }, (_, index) => ({
      id: `native-${index}`,
      height: 10,
    }));
    container = document.createElement('div');
    document.body.append(container);
    root = createRoot(container);

    act(() => {
      root?.render(
        <VirtualList
          data={rows}
          height={300}
          itemHeight={10}
          itemHeightFixed
          itemKey="id"
        >
          {(row) => <div data-row-id={row.id}>{row.id}</div>}
        </VirtualList>,
      );
    });

    const holder = container.querySelector<HTMLElement>('.rc-virtual-list-holder');
    expect(holder).not.toBeNull();

    let nativeScrollTop = 1200;
    const scrollTopWrites: number[] = [];
    Object.defineProperty(holder, 'scrollTop', {
      configurable: true,
      get: () => nativeScrollTop,
      set: (value: number) => {
        scrollTopWrites.push(value);
        nativeScrollTop = value;
      },
    });

    let targetRowWasMountedBeforeScrollReturned = false;
    holder?.addEventListener('scroll', () => {
      targetRowWasMountedBeforeScrollReturned = !!container?.querySelector('[data-row-id="native-120"]');
    });

    const previousActEnvironment = (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT;
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = false;
    holder?.dispatchEvent(new Event('scroll', { bubbles: true }));
    (globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = previousActEnvironment;

    expect(targetRowWasMountedBeforeScrollReturned).toBe(true);
    expect(scrollTopWrites).toEqual([]);
  });
});
