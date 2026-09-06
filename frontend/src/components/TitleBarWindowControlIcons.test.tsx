import React from 'react';
import { create } from 'react-test-renderer';
import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

import {
  TitleBarCloseIcon,
  TitleBarMaximizeIcon,
  TitleBarMinimizeIcon,
  TitleBarRestoreIcon,
  resolveTitleBarWindowToggleIcon,
} from './TitleBarWindowControlIcons';

const appSource = readFileSync(new URL('../App.tsx', import.meta.url), 'utf8');
const appCss = readFileSync(new URL('../App.css', import.meta.url), 'utf8');

describe('TitleBarWindowControlIcons', () => {
  it('renders thin-line minimize / maximize / restore / close glyphs', () => {
    const min = create(<TitleBarMinimizeIcon />).toJSON();
    const max = create(<TitleBarMaximizeIcon />).toJSON();
    const restore = create(<TitleBarRestoreIcon />).toJSON();
    const close = create(<TitleBarCloseIcon />).toJSON();

    expect(min?.type).toBe('svg');
    expect(max?.props?.viewBox).toBe('0 0 12 12');
    expect(JSON.stringify(max)).toContain('rx');
    expect(JSON.stringify(restore)).toContain('titlebar-window-control-restore-front');
    expect(JSON.stringify(close)).toContain('M3.1 3.1l5.8 5.8');
  });

  it('switches maximize and restore icons by kind', () => {
    const maximizeTree = create(resolveTitleBarWindowToggleIcon('maximize')).toJSON();
    const restoreTree = create(resolveTitleBarWindowToggleIcon('restore')).toJSON();
    expect(JSON.stringify(maximizeTree)).toContain('7.3');
    expect(JSON.stringify(restoreTree)).toContain('titlebar-window-control-restore-front');
  });

  it('wires the custom icons into the Windows/Linux titlebar controls', () => {
    expect(appSource).toContain("from './components/TitleBarWindowControlIcons'");
    expect(appSource).toContain('<TitleBarMinimizeIcon />');
    expect(appSource).toContain('<TitleBarCloseIcon />');
    expect(appSource).toContain("titleBarToggleIconKey === 'restore' ? <TitleBarRestoreIcon /> : <TitleBarMaximizeIcon />");
    expect(appSource).not.toMatch(/icon=\{<MinusOutlined\s*\/>\}/);
    expect(appSource).not.toMatch(/titleBarToggleIconKey === 'restore' \? <SwitcherOutlined/);
    expect(appCss).toContain('.titlebar-window-controls .titlebar-window-control-restore-front');
  });
});
