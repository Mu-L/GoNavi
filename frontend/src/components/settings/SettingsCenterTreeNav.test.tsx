import React from 'react';
import { act, create } from 'react-test-renderer';
import { describe, expect, it, vi } from 'vitest';

import { buildOverlayWorkbenchTheme } from '../../utils/overlayWorkbenchTheme';

vi.mock('@ant-design/icons', () => ({
  CaretRightOutlined: () => React.createElement('span', { 'data-tree-caret': 'true' }),
}));

import SettingsCenterTreeNav, {
  flattenVisibleSettingsCenterTree,
  settingsCenterTreeNodeId,
} from './SettingsCenterTreeNav';

const overlayTheme = buildOverlayWorkbenchTheme(false);

const createGroups = (onLanguageClick = vi.fn(), onProxyClick = vi.fn()) => ([
  {
    key: 'preferences',
    icon: <span>P</span>,
    title: '偏好设置',
    description: '语言与外观',
    items: [
      {
        key: 'language',
        icon: <span>L</span>,
        title: '语言',
        description: '界面语言',
        onClick: onLanguageClick,
      },
      {
        key: 'theme',
        icon: <span>T</span>,
        title: '主题',
        description: '外观主题',
        onClick: vi.fn(),
      },
    ],
  },
  {
    key: 'services',
    icon: <span>S</span>,
    title: '服务配置',
    description: '代理与下载',
    items: [
      {
        key: 'proxy',
        icon: <span>X</span>,
        title: '代理',
        description: '网络代理',
        onClick: onProxyClick,
      },
    ],
  },
]);

describe('SettingsCenterTreeNav', () => {
  it('flattens expanded groups and hides collapsed children', () => {
    const groups = createGroups();
    const expanded = flattenVisibleSettingsCenterTree(groups, new Set());
    expect(expanded.map((node) => node.id)).toEqual([
      'group:preferences',
      'item:preferences:language',
      'item:preferences:theme',
      'group:services',
      'item:services:proxy',
    ]);

    const collapsed = flattenVisibleSettingsCenterTree(groups, new Set(['group:services']));
    expect(collapsed.map((node) => node.id)).toEqual([
      'group:preferences',
      'item:preferences:language',
      'item:preferences:theme',
      'group:services',
    ]);
    expect(settingsCenterTreeNodeId({ type: 'item', groupKey: 'preferences', itemKey: 'language' }))
      .toBe('item:preferences:language');
  });

  it('renders a persistent tree so leaves are reachable without a back-to-list hop', () => {
    const onLanguageClick = vi.fn();
    const onSelectGroup = vi.fn();
    const renderer = create(
      <SettingsCenterTreeNav
        groups={createGroups(onLanguageClick)}
        activeGroupKey="preferences"
        activeItemKey="language"
        darkMode={false}
        overlayTheme={overlayTheme}
        ariaLabel="设置中心"
        onSelectGroup={onSelectGroup}
      />,
    );

    const tree = renderer.root.findByProps({ role: 'tree' });
    expect(tree.props['aria-label']).toBe('设置中心');
    expect(renderer.root.findAllByProps({ role: 'treeitem' })).toHaveLength(5);

    const languageNode = renderer.root.findByProps({ 'data-settings-pane-key': 'language' });
    expect(languageNode.props['aria-selected']).toBe(true);
    expect(languageNode.props.className).toContain('is-active');
    expect(renderer.root.findAllByProps({ className: 'gonavi-settings-center-tree-icon' })).toHaveLength(0);

    act(() => {
      languageNode.props.onClick();
    });
    expect(onLanguageClick).toHaveBeenCalledTimes(1);
    expect(onSelectGroup).not.toHaveBeenCalled();
  });

  it('collapses a group from the caret without changing the selected leaf', () => {
    const onSelectGroup = vi.fn();
    const renderer = create(
      <SettingsCenterTreeNav
        groups={createGroups()}
        activeGroupKey="preferences"
        activeItemKey="language"
        darkMode={false}
        overlayTheme={overlayTheme}
        ariaLabel="设置中心"
        onSelectGroup={onSelectGroup}
      />,
    );

    const preferencesGroup = renderer.root.findByProps({ 'data-settings-tree-node': 'group:preferences' });
    expect(preferencesGroup.props['aria-expanded']).toBe(true);
    const caret = renderer.root.findByProps({ 'data-settings-tree-toggle': 'group:preferences' });

    act(() => {
      caret.props.onClick({ stopPropagation() { /* caret should not bubble to the group */ } });
    });

    const collapsedGroup = renderer.root.findByProps({ 'data-settings-tree-node': 'group:preferences' });
    expect(collapsedGroup.props['aria-expanded']).toBe(false);
    expect(renderer.root.findAllByProps({ 'data-settings-pane-key': 'language' })).toHaveLength(0);
    expect(onSelectGroup).not.toHaveBeenCalled();
  });

  it('activates another group from the tree instead of requiring a settings list', () => {
    const onSelectGroup = vi.fn();
    const renderer = create(
      <SettingsCenterTreeNav
        groups={createGroups()}
        activeGroupKey="preferences"
        activeItemKey="language"
        darkMode={false}
        overlayTheme={overlayTheme}
        ariaLabel="设置中心"
        onSelectGroup={onSelectGroup}
      />,
    );

    const servicesGroup = renderer.root.findByProps({ 'data-settings-tree-node': 'group:services' });
    act(() => {
      servicesGroup.props.onClick();
    });
    expect(onSelectGroup).toHaveBeenCalledWith('services');
  });

  it('exposes nested section leaves under a settings node', () => {
    const onThemeClick = vi.fn();
    const onAppearanceClick = vi.fn();
    const groups = [{
      key: 'preferences',
      icon: <span>P</span>,
      title: '偏好设置',
      description: '语言与外观',
      items: [{
        key: 'theme',
        icon: <span>T</span>,
        title: '主题与外观',
        description: '主题设置',
        onClick: onThemeClick,
        children: [
          {
            key: 'theme-theme',
            icon: <span>I</span>,
            title: '主题与界面',
            description: '亮暗模式',
            onClick: vi.fn(),
          },
          {
            key: 'theme-appearance',
            icon: <span>F</span>,
            title: '显示与字体',
            description: '缩放字体',
            onClick: onAppearanceClick,
          },
          {
            key: 'theme-workspace',
            icon: <span>W</span>,
            title: '工作区',
            description: '工作区',
            onClick: vi.fn(),
          },
        ],
      }],
    }];

    expect(flattenVisibleSettingsCenterTree(groups, new Set()).map((node) => node.id)).toEqual([
      'group:preferences',
      'item:preferences:theme',
      'item:preferences:theme-theme',
      'item:preferences:theme-appearance',
      'item:preferences:theme-workspace',
    ]);
    expect(flattenVisibleSettingsCenterTree(groups, new Set(['item:preferences:theme'])).map((node) => node.id)).toEqual([
      'group:preferences',
      'item:preferences:theme',
    ]);

    const renderer = create(
      <SettingsCenterTreeNav
        groups={groups}
        activeGroupKey="preferences"
        activeItemKey="theme-appearance"
        darkMode={false}
        overlayTheme={overlayTheme}
        ariaLabel="设置中心"
        onSelectGroup={() => undefined}
      />,
    );

    expect(renderer.root.findAllByProps({ role: 'treeitem' })).toHaveLength(5);
    expect(renderer.root.findAllByProps({ className: 'gonavi-settings-center-tree-icon' })).toHaveLength(0);
    expect(renderer.root.findAllByProps({ className: 'gonavi-settings-center-tree-branch' })).toHaveLength(1);
    const appearanceNode = renderer.root.findByProps({ 'data-settings-pane-key': 'theme-appearance' });
    expect(appearanceNode.props.className).toContain('is-grandchild');
    expect(appearanceNode.props['aria-selected']).toBe(true);
    act(() => {
      appearanceNode.props.onClick();
    });
    expect(onAppearanceClick).toHaveBeenCalledTimes(1);

    const themeNode = renderer.root.findByProps({ 'data-settings-pane-key': 'theme' });
    act(() => {
      themeNode.props.onClick();
    });
    expect(onThemeClick).toHaveBeenCalledTimes(1);
  });

  it('renders a leaf group as a first-level node without a nested child', () => {
    const onSelectGroup = vi.fn();
    const groups = [{
      key: 'about',
      title: '关于 GoNavi',
      description: '版本与更新',
      items: [],
    }];

    expect(flattenVisibleSettingsCenterTree(groups, new Set()).map((node) => node.id)).toEqual([
      'group:about',
    ]);

    const renderer = create(
      <SettingsCenterTreeNav
        groups={groups}
        activeGroupKey="about"
        activeItemKey="about-go-navi"
        darkMode={false}
        overlayTheme={overlayTheme}
        ariaLabel="设置中心"
        onSelectGroup={onSelectGroup}
      />,
    );

    expect(renderer.root.findAllByProps({ role: 'treeitem' })).toHaveLength(1);
    const aboutNode = renderer.root.findByProps({ 'data-settings-tree-node': 'group:about' });
    expect(aboutNode.props['aria-selected']).toBe(true);
    expect(aboutNode.props['aria-expanded']).toBeUndefined();
    expect(aboutNode.props['aria-label']).toBe('关于 GoNavi');
    expect(renderer.root.findAllByProps({ 'data-settings-pane-key': 'about-go-navi' })).toHaveLength(0);

    act(() => {
      aboutNode.props.onClick();
    });
    expect(onSelectGroup).not.toHaveBeenCalled();
  });
});
