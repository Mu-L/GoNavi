import React, { useEffect, useMemo, useRef, useState } from 'react';
import { CaretRightOutlined } from '@ant-design/icons';

import type { OverlayWorkbenchTheme } from '../../utils/overlayWorkbenchTheme';
import './SettingsCenterTreeNav.css';

export type SettingsCenterTreeItem = {
  key: string;
  icon?: React.ReactNode;
  title: string;
  description: string;
  onClick: () => void;
  children?: ReadonlyArray<SettingsCenterTreeItem>;
};

export type SettingsCenterTreeGroup = {
  key: string;
  icon?: React.ReactNode;
  title: string;
  description: string;
  items: ReadonlyArray<SettingsCenterTreeItem>;
};

export type SettingsCenterTreeNodeId =
  | { type: 'group'; groupKey: string }
  | { type: 'item'; groupKey: string; itemKey: string; parentItemKey?: string };

type VisibleTreeNode = SettingsCenterTreeNodeId & {
  id: string;
  expandable: boolean;
  depth: number;
};

export const settingsCenterTreeNodeId = (node: SettingsCenterTreeNodeId): string => (
  node.type === 'group' ? `group:${node.groupKey}` : `item:${node.groupKey}:${node.itemKey}`
);

export const findSettingsCenterTreeItem = (
  groups: ReadonlyArray<SettingsCenterTreeGroup>,
  groupKey: string,
  itemKey: string | null,
): SettingsCenterTreeItem | null => {
  if (!itemKey) {
    return null;
  }
  const group = groups.find((entry) => entry.key === groupKey);
  if (!group) {
    return null;
  }
  for (const item of group.items) {
    if (item.key === itemKey) {
      return item;
    }
    const child = item.children?.find((entry) => entry.key === itemKey);
    if (child) {
      return child;
    }
  }
  return null;
};

export const flattenVisibleSettingsCenterTree = (
  groups: ReadonlyArray<SettingsCenterTreeGroup>,
  collapsedKeys: ReadonlySet<string>,
): VisibleTreeNode[] => {
  const nodes: VisibleTreeNode[] = [];
  groups.forEach((group) => {
    const groupId = settingsCenterTreeNodeId({ type: 'group', groupKey: group.key });
    const expandable = group.items.length > 0;
    nodes.push({
      type: 'group',
      groupKey: group.key,
      id: groupId,
      expandable,
      depth: 0,
    });
    if (!expandable || collapsedKeys.has(groupId) || collapsedKeys.has(group.key)) {
      return;
    }
    group.items.forEach((item) => {
      const itemId = settingsCenterTreeNodeId({ type: 'item', groupKey: group.key, itemKey: item.key });
      const hasChildren = Boolean(item.children && item.children.length > 0);
      nodes.push({
        type: 'item',
        groupKey: group.key,
        itemKey: item.key,
        id: itemId,
        expandable: hasChildren,
        depth: 1,
      });
      if (!hasChildren || collapsedKeys.has(itemId)) {
        return;
      }
      item.children?.forEach((child) => {
        nodes.push({
          type: 'item',
          groupKey: group.key,
          itemKey: child.key,
          parentItemKey: item.key,
          id: settingsCenterTreeNodeId({ type: 'item', groupKey: group.key, itemKey: child.key }),
          expandable: false,
          depth: 2,
        });
      });
    });
  });
  return nodes;
};

interface SettingsCenterTreeNavProps {
  groups: ReadonlyArray<SettingsCenterTreeGroup>;
  activeGroupKey: string;
  activeItemKey: string | null;
  darkMode: boolean;
  overlayTheme: OverlayWorkbenchTheme;
  ariaLabel: string;
  onSelectGroup: (groupKey: string) => void;
}

const SettingsCenterTreeNav: React.FC<SettingsCenterTreeNavProps> = ({
  groups,
  activeGroupKey,
  activeItemKey,
  darkMode,
  overlayTheme,
  ariaLabel,
  onSelectGroup,
}) => {
  const [collapsedKeys, setCollapsedKeys] = useState<Set<string>>(() => new Set());
  const suppressAutoExpandRef = useRef(false);
  const selectedNodeId = activeItemKey
    ? settingsCenterTreeNodeId({ type: 'item', groupKey: activeGroupKey, itemKey: activeItemKey })
    : settingsCenterTreeNodeId({ type: 'group', groupKey: activeGroupKey });
  const [focusedNodeId, setFocusedNodeId] = useState(selectedNodeId);

  const parentItemKey = useMemo(() => {
    const group = groups.find((entry) => entry.key === activeGroupKey);
    if (!group || !activeItemKey) {
      return null;
    }
    const parent = group.items.find((item) => item.children?.some((child) => child.key === activeItemKey));
    return parent?.key ?? null;
  }, [activeGroupKey, activeItemKey, groups]);

  useEffect(() => {
    if (suppressAutoExpandRef.current) {
      suppressAutoExpandRef.current = false;
      return;
    }
    setCollapsedKeys((current) => {
      const required = [settingsCenterTreeNodeId({ type: 'group', groupKey: activeGroupKey })];
      if (parentItemKey) {
        required.push(settingsCenterTreeNodeId({
          type: 'item',
          groupKey: activeGroupKey,
          itemKey: parentItemKey,
        }));
      }
      if (required.every((id) => !current.has(id))) {
        return current;
      }
      const next = new Set(current);
      required.forEach((id) => next.delete(id));
      return next;
    });
  }, [activeGroupKey, parentItemKey]);

  useEffect(() => {
    setFocusedNodeId(selectedNodeId);
  }, [selectedNodeId]);

  const visibleNodes = useMemo(
    () => flattenVisibleSettingsCenterTree(groups, collapsedKeys),
    [collapsedKeys, groups],
  );

  const toggleCollapsed = (nodeId: string) => {
    setCollapsedKeys((current) => {
      const next = new Set(current);
      if (next.has(nodeId)) {
        next.delete(nodeId);
      } else {
        next.add(nodeId);
      }
      return next;
    });
  };

  const toggleCollapsedFromUser = (nodeId: string) => {
    suppressAutoExpandRef.current = true;
    toggleCollapsed(nodeId);
  };

  const expandNode = (nodeId: string) => {
    setCollapsedKeys((current) => {
      if (!current.has(nodeId)) {
        return current;
      }
      const next = new Set(current);
      next.delete(nodeId);
      return next;
    });
  };

  const findItem = (groupKey: string, itemKey: string): SettingsCenterTreeItem | null => (
    findSettingsCenterTreeItem(groups, groupKey, itemKey)
  );

  const activateNode = (node: VisibleTreeNode) => {
    setFocusedNodeId(node.id);
    if (node.type === 'group') {
      expandNode(node.id);
      if (node.groupKey !== activeGroupKey) {
        onSelectGroup(node.groupKey);
      }
      return;
    }
    if (node.expandable) {
      expandNode(node.id);
    }
    findItem(node.groupKey, node.itemKey)?.onClick();
  };

  const revealNode = (node: VisibleTreeNode) => {
    setFocusedNodeId(node.id);
    if (node.type === 'item' && !node.expandable) {
      activateNode(node);
    }
  };

  const moveFocus = (currentId: string, delta: number) => {
    const currentIndex = visibleNodes.findIndex((visible) => visible.id === currentId);
    if (visibleNodes.length === 0) {
      return;
    }
    const nextIndex = currentIndex < 0
      ? 0
      : (currentIndex + delta + visibleNodes.length) % visibleNodes.length;
    revealNode(visibleNodes[nextIndex]);
  };

  const handleTreeKeyDown = (event: React.KeyboardEvent<HTMLElement>, node: VisibleTreeNode) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      activateNode(node);
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      moveFocus(node.id, 1);
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      moveFocus(node.id, -1);
      return;
    }
    if (event.key === 'Home') {
      event.preventDefault();
      if (visibleNodes[0]) {
        revealNode(visibleNodes[0]);
      }
      return;
    }
    if (event.key === 'End') {
      event.preventDefault();
      const lastNode = visibleNodes[visibleNodes.length - 1];
      if (lastNode) {
        revealNode(lastNode);
      }
      return;
    }
    if (event.key === 'ArrowRight' && node.expandable) {
      event.preventDefault();
      if (collapsedKeys.has(node.id)) {
        toggleCollapsedFromUser(node.id);
        return;
      }
      moveFocus(node.id, 1);
      return;
    }
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      if (node.expandable && !collapsedKeys.has(node.id)) {
        toggleCollapsedFromUser(node.id);
        return;
      }
      if (node.type === 'item' && node.parentItemKey) {
        setFocusedNodeId(settingsCenterTreeNodeId({
          type: 'item',
          groupKey: node.groupKey,
          itemKey: node.parentItemKey,
        }));
        return;
      }
      if (node.type === 'item') {
        setFocusedNodeId(settingsCenterTreeNodeId({ type: 'group', groupKey: node.groupKey }));
      }
    }
  };

  const nodeColor = (active: boolean) => (
    active
      ? (darkMode ? '#f5f7ff' : '#162033')
      : (darkMode ? 'rgba(255,255,255,0.82)' : '#3f4b5e')
  );

  const renderCaret = (expanded: boolean, active: boolean, nodeId: string) => (
    <button
      className="gonavi-settings-center-tree-caret"
      type="button"
      tabIndex={-1}
      aria-hidden="true"
      data-settings-tree-toggle={nodeId}
      onClick={(event) => {
        event.stopPropagation();
        toggleCollapsedFromUser(nodeId);
      }}
    >
      <CaretRightOutlined
        style={{
          fontSize: 10,
          transform: expanded ? 'rotate(90deg)' : 'rotate(0deg)',
          color: active ? overlayTheme.selectedText : overlayTheme.mutedText,
        }}
      />
    </button>
  );

  return (
    <div
      className="gonavi-settings-center-tree"
      role="tree"
      aria-label={ariaLabel}
      aria-orientation="vertical"
    >
      {groups.map((group) => {
        const groupId = settingsCenterTreeNodeId({ type: 'group', groupKey: group.key });
        const groupExpandable = group.items.length > 0;
        const groupExpanded = groupExpandable && !collapsedKeys.has(groupId) && !collapsedKeys.has(group.key);
        const groupActive = group.key === activeGroupKey && (!groupExpandable || !activeItemKey);
        const groupFocused = focusedNodeId === groupId;
        return (
          <div key={group.key} role="none">
            <div
              className={`gonavi-settings-center-tree-node${groupActive ? ' is-active' : ''}`}
              role="treeitem"
              aria-expanded={groupExpandable ? groupExpanded : undefined}
              aria-selected={groupActive}
              aria-label={group.title}
              title={`${group.title} - ${group.description}`}
              tabIndex={groupFocused ? 0 : -1}
              data-settings-tree-node={groupId}
              onClick={() => activateNode({
                type: 'group',
                groupKey: group.key,
                id: groupId,
                expandable: groupExpandable,
                depth: 0,
              })}
              onKeyDown={(event) => handleTreeKeyDown(event, {
                type: 'group',
                groupKey: group.key,
                id: groupId,
                expandable: groupExpandable,
                depth: 0,
              })}
              style={{
                background: groupActive ? overlayTheme.selectedBg : 'transparent',
                color: nodeColor(groupActive),
              }}
            >
              {groupExpandable ? renderCaret(groupExpanded, groupActive, groupId) : (
                <span className="gonavi-settings-center-tree-caret is-placeholder" aria-hidden="true" />
              )}
              <span className="gonavi-settings-center-tree-label">{group.title}</span>
            </div>
            {groupExpanded ? (
              <div role="group" aria-label={group.title}>
                {group.items.map((item) => {
                  const itemId = settingsCenterTreeNodeId({ type: 'item', groupKey: group.key, itemKey: item.key });
                  const hasChildren = Boolean(item.children && item.children.length > 0);
                  const itemExpanded = hasChildren && !collapsedKeys.has(itemId);
                  const itemActive = group.key === activeGroupKey && item.key === activeItemKey;
                  const itemFocused = focusedNodeId === itemId;
                  return (
                    <div key={item.key} role="none">
                      <div
                        className={`gonavi-settings-center-tree-node is-child${hasChildren ? ' has-children' : ''}${itemActive ? ' is-active' : ''}`}
                        role="treeitem"
                        aria-expanded={hasChildren ? itemExpanded : undefined}
                        aria-selected={itemActive}
                        aria-label={item.title}
                        title={`${item.title} - ${item.description}`}
                        tabIndex={itemFocused ? 0 : -1}
                        data-settings-pane-key={item.key}
                        data-settings-tree-node={itemId}
                        onClick={() => activateNode({
                          type: 'item',
                          groupKey: group.key,
                          itemKey: item.key,
                          id: itemId,
                          expandable: hasChildren,
                          depth: 1,
                        })}
                        onKeyDown={(event) => handleTreeKeyDown(event, {
                          type: 'item',
                          groupKey: group.key,
                          itemKey: item.key,
                          id: itemId,
                          expandable: hasChildren,
                          depth: 1,
                        })}
                        style={{
                          background: itemActive ? overlayTheme.selectedBg : 'transparent',
                          color: nodeColor(itemActive),
                        }}
                      >
                        {hasChildren ? renderCaret(itemExpanded, itemActive, itemId) : (
                          <span className="gonavi-settings-center-tree-caret is-placeholder" aria-hidden="true" />
                        )}
                        <span className="gonavi-settings-center-tree-label">{item.title}</span>
                      </div>
                      {itemExpanded ? (
                        <div className="gonavi-settings-center-tree-branch" role="group" aria-label={item.title}>
                          {item.children?.map((child) => {
                            const childId = settingsCenterTreeNodeId({
                              type: 'item',
                              groupKey: group.key,
                              itemKey: child.key,
                            });
                            const childActive = group.key === activeGroupKey && child.key === activeItemKey;
                            const childFocused = focusedNodeId === childId;
                            return (
                              <div
                                key={child.key}
                                className={`gonavi-settings-center-tree-node is-grandchild${childActive ? ' is-active' : ''}`}
                                role="treeitem"
                                aria-selected={childActive}
                                aria-label={child.title}
                                title={`${child.title} - ${child.description}`}
                                tabIndex={childFocused ? 0 : -1}
                                data-settings-pane-key={child.key}
                                data-settings-tree-node={childId}
                                onClick={() => activateNode({
                                  type: 'item',
                                  groupKey: group.key,
                                  itemKey: child.key,
                                  parentItemKey: item.key,
                                  id: childId,
                                  expandable: false,
                                  depth: 2,
                                })}
                                onKeyDown={(event) => handleTreeKeyDown(event, {
                                  type: 'item',
                                  groupKey: group.key,
                                  itemKey: child.key,
                                  parentItemKey: item.key,
                                  id: childId,
                                  expandable: false,
                                  depth: 2,
                                })}
                                style={{
                                  background: childActive ? overlayTheme.selectedBg : 'transparent',
                                  color: childActive ? nodeColor(true) : overlayTheme.mutedText,
                                }}
                              >
                                <span className="gonavi-settings-center-tree-label">{child.title}</span>
                              </div>
                            );
                          })}
                        </div>
                      ) : null}
                    </div>
                  );
                })}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
};

export default SettingsCenterTreeNav;
