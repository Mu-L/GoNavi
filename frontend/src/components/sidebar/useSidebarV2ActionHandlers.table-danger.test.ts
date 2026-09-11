import { describe, expect, it, vi } from 'vitest';

import { useSidebarV2ActionHandlers } from './useSidebarV2ActionHandlers';

const buildHandlers = () => {
  const dangerAction = vi.fn();
  const handlers = useSidebarV2ActionHandlers({
    connections: [],
    connectionTags: [],
    pinnedSidebarTables: [],
    pinnedSidebarDatabases: [],
    loadingNodesRef: { current: new Set() },
    treeDataRef: { current: [] },
    findTreeNodeByKeyRef: { current: () => null },
    refreshV2TableContextMenuStatsRef: { current: vi.fn() },
    handleTableDataDangerAction: dangerAction,
  } as any);
  return { handlers, dangerAction };
};

describe('useSidebarV2ActionHandlers table danger actions', () => {
  it('routes the table clear action through the existing danger handler', () => {
    const { handlers, dangerAction } = buildHandlers();
    const node = { dataRef: { tableName: 'orders', config: { type: 'mysql' } } };

    handlers.handleV2TableContextMenuAction(node, 'clear-table');

    expect(dangerAction).toHaveBeenCalledWith(node, 'clear');
  });

  it('rejects a forged clear action for an unsupported backend', () => {
    const { handlers, dangerAction } = buildHandlers();
    const node = { dataRef: { tableName: 'orders', config: { type: 'elasticsearch' } } };

    handlers.handleV2TableContextMenuAction(node, 'clear-table');

    expect(dangerAction).not.toHaveBeenCalled();
  });

  it.each([
    { readOnly: true },
    { protection: { restrictDataEdit: true } },
  ])('rejects a forged clear action for a protected SQL connection', (guard) => {
    const { handlers, dangerAction } = buildHandlers();
    const node = {
      dataRef: {
        tableName: 'orders',
        config: { type: 'mysql', ...guard },
      },
    };

    handlers.handleV2TableContextMenuAction(node, 'clear-table');

    expect(dangerAction).not.toHaveBeenCalled();
  });
});
