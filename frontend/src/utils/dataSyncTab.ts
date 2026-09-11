import type { TabData } from '../types';
import { t } from '../i18n';
import {
  normalizeDataSyncEntryMode,
  resolveDataSyncEntryModePresentation,
  type DataSyncEntryMode,
  type DataSyncEntryModeAlias,
} from '../components/dataSyncEntryMode';

type BuildDataSyncWorkbenchTabInput = {
  entryMode: DataSyncEntryModeAlias;
  title?: string;
};

const DATA_SYNC_ENTRY_MODE_SLUGS: Record<DataSyncEntryMode, string> = {
  sync: 'sync',
  compare: 'compare',
};

const LEGACY_COMPARE_TAB_IDS = [
  'data-sync-workbench-schema-compare',
  'data-sync-workbench-data-compare',
] as const;

export const resolveDataSyncWorkbenchTabId = (
  entryMode: DataSyncEntryModeAlias,
): string => `data-sync-workbench-${DATA_SYNC_ENTRY_MODE_SLUGS[normalizeDataSyncEntryMode(entryMode)]}`;

export const resolveExistingDataSyncWorkbenchTabId = (
  entryMode: DataSyncEntryModeAlias,
  tabs: Array<Pick<TabData, 'id' | 'type' | 'dataSyncEntryMode'>>,
): string | undefined => {
  const normalized = normalizeDataSyncEntryMode(entryMode);
  const preferredId = resolveDataSyncWorkbenchTabId(normalized);
  const match = tabs.find((tab) => {
    if (tab.type !== 'data-sync') return false;
    if (tab.id === preferredId) return true;
    if (normalized === 'compare' && (LEGACY_COMPARE_TAB_IDS as readonly string[]).includes(tab.id)) {
      return true;
    }
    return normalizeDataSyncEntryMode(tab.dataSyncEntryMode) === normalized;
  });
  return match?.id;
};

export const buildDataSyncWorkbenchTab = (
  input: BuildDataSyncWorkbenchTabInput,
): TabData => {
  const entryMode = normalizeDataSyncEntryMode(input.entryMode);
  const presentation = resolveDataSyncEntryModePresentation(entryMode, t);
  const title = String(input.title || presentation.title).trim() || presentation.title;

  return {
    id: resolveDataSyncWorkbenchTabId(entryMode),
    title,
    type: 'data-sync',
    connectionId: '',
    dataSyncEntryMode: entryMode,
  };
};
