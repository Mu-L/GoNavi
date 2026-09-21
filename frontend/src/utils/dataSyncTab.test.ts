import { afterEach, describe, expect, it } from 'vitest';

import { setCurrentLanguage, t } from '../i18n';
import { getTabDisplayKindLabel } from './tabDisplay';
import {
  buildDataSyncWorkbenchTab,
  resolveDataSyncWorkbenchTabId,
  resolveExistingDataSyncWorkbenchTabId,
} from './dataSyncTab';

describe('dataSyncTab', () => {
  afterEach(() => {
    setCurrentLanguage('zh-CN');
  });

  it('builds one stable workbench tab for each data workflow entry', () => {
    expect(resolveDataSyncWorkbenchTabId('sync')).toBe('data-sync-workbench-sync');
    expect(resolveDataSyncWorkbenchTabId('compare')).toBe('data-sync-workbench-compare');
    expect(resolveDataSyncWorkbenchTabId('schemaCompare')).toBe('data-sync-workbench-compare');
    expect(resolveDataSyncWorkbenchTabId('dataCompare')).toBe('data-sync-workbench-compare');

    expect(buildDataSyncWorkbenchTab({ entryMode: 'schemaCompare' })).toMatchObject({
      id: 'data-sync-workbench-compare',
      title: t('data_sync.entry_mode.compare.title'),
      type: 'data-sync',
      connectionId: '',
      dataSyncEntryMode: 'compare',
    });
    expect(getTabDisplayKindLabel(buildDataSyncWorkbenchTab({ entryMode: 'sync' }))).toBe('SYNC');
    expect(getTabDisplayKindLabel(buildDataSyncWorkbenchTab({ entryMode: 'compare' }))).toBe('COMPARE');
  });

  it('localizes default titles without changing stable tab ids', () => {
    setCurrentLanguage('en-US');

    const tab = buildDataSyncWorkbenchTab({ entryMode: 'dataCompare' });

    expect(tab.id).toBe('data-sync-workbench-compare');
    expect(tab.title).toBe(t('data_sync.entry_mode.compare.title'));
  });

  it('reuses a legacy schema or data compare tab as the unified compare workbench', () => {
    expect(
      resolveExistingDataSyncWorkbenchTabId('compare', [
        {
          id: 'data-sync-workbench-schema-compare',
          type: 'data-sync',
          dataSyncEntryMode: 'schemaCompare',
        },
      ]),
    ).toBe('data-sync-workbench-schema-compare');
  });
});
