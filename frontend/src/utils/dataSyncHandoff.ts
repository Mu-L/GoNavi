import type { DataSyncTaskDefinition, DataSyncTaskStage } from '../components/data-sync/model';

export type DataSyncHandoff = {
  task: DataSyncTaskDefinition;
  stage: DataSyncTaskStage;
  requestId: string;
};

let pendingHandoff: DataSyncHandoff | null = null;

export const setDataSyncHandoff = (handoff: DataSyncHandoff): void => {
  pendingHandoff = handoff;
};

export const takeDataSyncHandoff = (): DataSyncHandoff | null => {
  const next = pendingHandoff;
  pendingHandoff = null;
  return next;
};

export const peekDataSyncHandoff = (): DataSyncHandoff | null => pendingHandoff;
