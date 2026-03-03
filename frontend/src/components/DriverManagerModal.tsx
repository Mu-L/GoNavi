import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Alert, Button, Collapse, Input, Modal, Progress, Select, Space, Switch, Table, Tag, Typography, message } from 'antd';
import { DeleteOutlined, DownloadOutlined, FileSearchOutlined, FolderOpenOutlined, InfoCircleFilled, ReloadOutlined } from '@ant-design/icons';
import { EventsOn } from '../../wailsjs/runtime/runtime';
import { useStore } from '../store';
import { normalizeOpacityForPlatform } from '../utils/appearance';
import {
  CheckDriverNetworkStatus,
  DownloadDriverPackage,
  GetDriverVersionList,
  GetDriverVersionPackageSize,
  GetDriverStatusList,
  InstallLocalDriverPackage,
  RemoveDriverPackage,
  SelectDriverPackageDirectory,
  SelectDriverPackageFile,
} from '../../wailsjs/go/app/App';

const { Paragraph, Text } = Typography;

type DriverStatusRow = {
  type: string;
  name: string;
  builtIn: boolean;
  pinnedVersion?: string;
  installedVersion?: string;
  packageSizeText?: string;
  runtimeAvailable: boolean;
  packageInstalled: boolean;
  connectable: boolean;
  defaultDownloadUrl?: string;
  installDir?: string;
  packagePath?: string;
  executablePath?: string;
  downloadedAt?: string;
  message?: string;
};

type DriverProgressEvent = {
  driverType?: string;
  status?: 'start' | 'downloading' | 'done' | 'error';
  message?: string;
  percent?: number;
};

type ProgressState = {
  status: 'start' | 'downloading' | 'done' | 'error';
  message: string;
  percent: number;
};

type DriverActionKind = '' | 'install' | 'remove' | 'local';

type DriverLogEntry = {
  time: string;
  text: string;
  signature: string;
};

type DriverNetworkProbe = {
  name: string;
  url: string;
  reachable: boolean;
  httpStatus?: number;
  latencyMs?: number;
  tcpLatencyMs?: number;
  httpLatencyMs?: number;
  method?: string;
  error?: string;
};

type DriverNetworkStatus = {
  reachable: boolean;
  summary: string;
  recommendedProxy: boolean;
  proxyConfigured: boolean;
  downloadChainReachable?: boolean;
  downloadRequiredHosts?: string[];
  proxyEnv?: Record<string, string>;
  checks: DriverNetworkProbe[];
  checkedAt?: string;
  logPath?: string;
};

const parseOptionalLatency = (value: unknown): number | undefined => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return undefined;
  return parsed;
};

const sharedInfoAlertIcon = <InfoCircleFilled style={{ fontSize: 24 }} />;

type DriverVersionOption = {
  version: string;
  downloadUrl: string;
  packageSizeText?: string;
  recommended?: boolean;
  source?: string;
  year?: string;
  displayLabel?: string;
};

const buildVersionOptionKey = (option: DriverVersionOption) => `${option.version}@@${option.downloadUrl}`;
const buildVersionSizeLoadingKey = (driverType: string, optionKey: string) => `${driverType}@@${optionKey}`;
const DRIVER_TABLE_SCROLL_X = 1450;
const DRIVER_STATUS_CACHE_TTL_MS = 60 * 1000;
const DRIVER_NETWORK_CACHE_TTL_MS = 5 * 60 * 1000;
const normalizeDriverSearchText = (value: string) => String(value || '').trim().toLowerCase();

let driverStatusSnapshotCache: { rows: DriverStatusRow[]; downloadDir: string; cachedAt: number } | null = null;
let driverNetworkSnapshotCache: { status: DriverNetworkStatus; cachedAt: number } | null = null;

const isFreshCache = (cachedAt: number, ttlMs: number): boolean => Date.now() - cachedAt <= ttlMs;

const buildVersionSelectOptions = (options: DriverVersionOption[]) => {
  type SelectOption = { value: string; label: string };
  type SelectGroup = { label: string; options: SelectOption[] };

  if (options.length === 0) {
    return [] as Array<SelectOption | SelectGroup>;
  }

  const yearGroups = new Map<string, SelectOption[]>();
  const others: SelectOption[] = [];
  options.forEach((option) => {
    const selectOption: SelectOption = {
      value: buildVersionOptionKey(option),
      label: option.displayLabel || option.version || '默认版本',
    };
    const year = String(option.year || '').trim();
    if (!year) {
      others.push(selectOption);
      return;
    }
    const group = yearGroups.get(year) || [];
    group.push(selectOption);
    yearGroups.set(year, group);
  });

  const sortedYears = Array.from(yearGroups.keys()).sort((a, b) => {
    const left = Number.parseInt(a, 10);
    const right = Number.parseInt(b, 10);
    const leftValid = Number.isFinite(left);
    const rightValid = Number.isFinite(right);
    if (leftValid && rightValid) {
      return right - left;
    }
    return b.localeCompare(a);
  });

  const grouped: SelectGroup[] = sortedYears.map((year) => ({
    label: `${year} 年`,
    options: yearGroups.get(year) || [],
  }));
  if (others.length > 0) {
    grouped.push({ label: '其他', options: others });
  }
  return grouped;
};

const DriverManagerModal: React.FC<{ open: boolean; onClose: () => void; onOpenGlobalProxySettings?: () => void }> = ({
  open,
  onClose,
  onOpenGlobalProxySettings,
}) => {
  const theme = useStore((state) => state.theme);
  const appearance = useStore((state) => state.appearance);
  const darkMode = theme === 'dark';
  const opacity = normalizeOpacityForPlatform(appearance.opacity);
  const modalContentRef = useRef<HTMLDivElement | null>(null);
  const tableContainerRef = useRef<HTMLDivElement | null>(null);
  const tableScrollTargetsRef = useRef<HTMLElement[]>([]);
  const externalHScrollRef = useRef<HTMLDivElement | null>(null);
  const horizontalSyncSourceRef = useRef<'table' | 'external' | ''>('');
  const [loading, setLoading] = useState(false);
  const [downloadDir, setDownloadDir] = useState('');
  const [networkChecking, setNetworkChecking] = useState(false);
  const [networkStatus, setNetworkStatus] = useState<DriverNetworkStatus | null>(null);
  const [searchKeyword, setSearchKeyword] = useState('');
  const [rows, setRows] = useState<DriverStatusRow[]>([]);
  const [actionState, setActionState] = useState<{ driverType: string; kind: DriverActionKind }>({ driverType: '', kind: '' });
  const [progressMap, setProgressMap] = useState<Record<string, ProgressState>>({});
  const [operationLogMap, setOperationLogMap] = useState<Record<string, DriverLogEntry[]>>({});
  const [logDriverType, setLogDriverType] = useState('');
  const [logModalOpen, setLogModalOpen] = useState(false);
  const [batchDirectoryImporting, setBatchDirectoryImporting] = useState(false);
  const [forceOverwriteInstalled, setForceOverwriteInstalled] = useState(false);
  const [versionMap, setVersionMap] = useState<Record<string, DriverVersionOption[]>>({});
  const [selectedVersionMap, setSelectedVersionMap] = useState<Record<string, string>>({});
  const [versionLoadingMap, setVersionLoadingMap] = useState<Record<string, boolean>>({});
  const [versionSizeLoadingMap, setVersionSizeLoadingMap] = useState<Record<string, boolean>>({});
  const [horizontalScrollWidth, setHorizontalScrollWidth] = useState(DRIVER_TABLE_SCROLL_X);
  const downloadDirRef = useRef(downloadDir);

  useEffect(() => {
    downloadDirRef.current = downloadDir;
  }, [downloadDir]);

  const appendOperationLog = useCallback((
    driverType: string,
    text: string,
    signature?: string,
    mode: 'append' | 'update-last' = 'append',
  ) => {
    const normalized = String(driverType || '').trim().toLowerCase();
    const content = String(text || '').trim();
    if (!normalized || !content) {
      return;
    }
    const sign = String(signature || content).trim() || content;
    const now = new Date().toLocaleTimeString();
    setOperationLogMap((prev) => {
      const history = prev[normalized] || [];
      if (history.length > 0) {
        const last = history[history.length - 1];
        if (last.signature === sign) {
          if (mode === 'update-last') {
            if (last.text === content) {
              return prev;
            }
            const nextHistory = [...history];
            nextHistory[nextHistory.length - 1] = {
              ...last,
              text: content,
              time: now,
            };
            return { ...prev, [normalized]: nextHistory };
          }
          return prev;
        }
      }
      const nextHistory = [
        ...history,
        {
          time: now,
          text: content,
          signature: sign,
        },
      ];
      const sliced = nextHistory.length > 200 ? nextHistory.slice(nextHistory.length - 200) : nextHistory;
      return { ...prev, [normalized]: sliced };
    });
  }, []);

  const refreshHorizontalScrollState = useCallback(() => {
    const tableContainer = tableContainerRef.current;
    const targets = tableContainer
      ? [
          ...new Set(
            [
              ...Array.from(tableContainer.querySelectorAll('.ant-table-content')),
              ...Array.from(tableContainer.querySelectorAll('.ant-table-body')),
            ].filter((node): node is HTMLElement => node instanceof HTMLElement),
          ),
        ]
      : tableScrollTargetsRef.current;
    if (!targets || targets.length === 0) {
      setHorizontalScrollWidth(DRIVER_TABLE_SCROLL_X);
      return;
    }

    const nextWidth = Math.max(
      DRIVER_TABLE_SCROLL_X,
      ...targets.map((target) => Math.max(0, target.scrollWidth)),
    );
    setHorizontalScrollWidth((prev) => (prev === nextWidth ? prev : nextWidth));

    const externalScroll = externalHScrollRef.current;
    if (!externalScroll || horizontalSyncSourceRef.current === 'external') {
      return;
    }
    const preferredTarget =
      targets.find((target) => target.scrollWidth > target.clientWidth + 1) ||
      targets[0];
    const targetScrollLeft = preferredTarget?.scrollLeft || 0;
    if (Math.abs(externalScroll.scrollLeft - targetScrollLeft) > 1) {
      externalScroll.scrollLeft = targetScrollLeft;
    }
  }, []);

  const applyExternalScrollToTableTargets = useCallback(() => {
    const tableContainer = tableContainerRef.current;
    const externalScroll = externalHScrollRef.current;
    if (!(tableContainer instanceof HTMLElement) || !(externalScroll instanceof HTMLDivElement)) {
      return;
    }
    if (horizontalSyncSourceRef.current === 'table') {
      return;
    }

    const liveTargets = [
      ...new Set(
        [
          ...Array.from(tableContainer.querySelectorAll('.ant-table-content')),
          ...Array.from(tableContainer.querySelectorAll('.ant-table-body')),
        ].filter((node): node is HTMLElement => node instanceof HTMLElement),
      ),
    ];
    if (liveTargets.length === 0) {
      return;
    }

    horizontalSyncSourceRef.current = 'external';
    liveTargets.forEach((target) => {
      if (target.scrollWidth <= target.clientWidth + 1) {
        return;
      }
      if (Math.abs(target.scrollLeft - externalScroll.scrollLeft) > 1) {
        target.scrollLeft = externalScroll.scrollLeft;
      }
    });
    horizontalSyncSourceRef.current = '';
  }, []);

  const refreshStatus = useCallback(async (
    toastOnError = true,
    options?: { showLoading?: boolean },
  ) => {
    const showLoading = options?.showLoading ?? true;
    if (showLoading) {
      setLoading(true);
    }
    try {
      const res = await GetDriverStatusList(downloadDirRef.current, '');
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || '拉取驱动状态失败');
        }
        return;
      }

      const data = (res?.data || {}) as any;
      const resolvedDir = String(data.downloadDir || '').trim();
      const drivers = Array.isArray(data.drivers) ? data.drivers : [];

      const effectiveDownloadDir = resolvedDir || downloadDirRef.current;
      if (resolvedDir) {
        setDownloadDir(resolvedDir);
      }

      const nextRows: DriverStatusRow[] = drivers.map((item: any) => ({
        type: String(item.type || '').trim(),
        name: String(item.name || item.type || '').trim(),
        builtIn: !!item.builtIn,
        pinnedVersion: String(item.pinnedVersion || '').trim() || undefined,
        installedVersion: String(item.installedVersion || '').trim() || undefined,
        packageSizeText: String(item.packageSizeText || '').trim() || undefined,
        runtimeAvailable: !!item.runtimeAvailable,
        packageInstalled: !!item.packageInstalled,
        connectable: !!item.connectable,
        defaultDownloadUrl: String(item.defaultDownloadUrl || '').trim() || undefined,
        installDir: String(item.installDir || '').trim() || undefined,
        packagePath: String(item.packagePath || '').trim() || undefined,
        executablePath: String(item.executablePath || '').trim() || undefined,
        downloadedAt: String(item.downloadedAt || '').trim() || undefined,
        message: String(item.message || '').trim() || undefined,
      }));
      setRows(nextRows);
      driverStatusSnapshotCache = {
        rows: nextRows,
        downloadDir: effectiveDownloadDir,
        cachedAt: Date.now(),
      };
    } catch (err: any) {
      if (toastOnError) {
        message.error(`拉取驱动状态失败：${err?.message || String(err)}`);
      }
    } finally {
      if (showLoading) {
        setLoading(false);
      }
    }
  }, []);

  const checkNetworkStatus = useCallback(async (
    toastOnError = false,
    options?: { showLoading?: boolean },
  ) => {
    const showLoading = options?.showLoading ?? true;
    if (showLoading) {
      setNetworkChecking(true);
    }
    try {
      const res = await CheckDriverNetworkStatus();
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || '驱动网络检测失败');
        }
        return;
      }
      const data = (res?.data || {}) as any;
      const checks = Array.isArray(data.checks) ? data.checks : [];
      const normalizedChecks: DriverNetworkProbe[] = checks.map((item: any) => ({
        name: String(item.name || '').trim(),
        url: String(item.url || '').trim(),
        reachable: !!item.reachable,
        httpStatus: parseOptionalLatency(item.httpStatus),
        latencyMs: parseOptionalLatency(item.latencyMs),
        tcpLatencyMs: parseOptionalLatency(item.tcpLatencyMs),
        httpLatencyMs: parseOptionalLatency(item.httpLatencyMs),
        method: String(item.method || '').trim().toUpperCase() || undefined,
        error: String(item.error || '').trim() || undefined,
      }));
      const nextStatus: DriverNetworkStatus = {
        reachable: !!data.reachable,
        summary: String(data.summary || '').trim() || '驱动网络检测已完成',
        recommendedProxy: !!data.recommendedProxy,
        proxyConfigured: !!data.proxyConfigured,
        downloadChainReachable: typeof data.downloadChainReachable === 'boolean' ? data.downloadChainReachable : undefined,
        downloadRequiredHosts: Array.isArray(data.downloadRequiredHosts)
          ? data.downloadRequiredHosts.map((item: unknown) => String(item || '').trim()).filter(Boolean)
          : undefined,
        proxyEnv: (data.proxyEnv || {}) as Record<string, string>,
        checkedAt: String(data.checkedAt || '').trim() || undefined,
        checks: normalizedChecks,
        logPath: String(data.logPath || '').trim() || undefined,
      };
      setNetworkStatus(nextStatus);
      driverNetworkSnapshotCache = {
        status: nextStatus,
        cachedAt: Date.now(),
      };
    } catch (err: any) {
      if (toastOnError) {
        message.error(`驱动网络检测失败：${err?.message || String(err)}`);
      }
    } finally {
      if (showLoading) {
        setNetworkChecking(false);
      }
    }
  }, []);

  const loadVersionOptions = useCallback(async (row: DriverStatusRow, toastOnError = false) => {
    if (row.builtIn) {
      return [] as DriverVersionOption[];
    }
    const driverType = String(row.type || '').trim();
    if (!driverType) {
      return [] as DriverVersionOption[];
    }
    setVersionLoadingMap((prev) => ({ ...prev, [driverType]: true }));
    try {
      const res = await GetDriverVersionList(driverType, '');
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || `${row.name} 版本列表加载失败`);
        }
        return [] as DriverVersionOption[];
      }
      const data = (res?.data || {}) as any;
      const rawVersions = Array.isArray(data.versions) ? data.versions : [];
      const options: DriverVersionOption[] = rawVersions
        .map((item: any) => {
          const version = String(item.version || '').trim();
          const downloadUrl = String(item.downloadUrl || '').trim();
          if (!version && !downloadUrl) {
            return null;
          }
          return {
            version,
            downloadUrl,
            packageSizeText: String(item.packageSizeText || '').trim() || undefined,
            recommended: !!item.recommended,
            source: String(item.source || '').trim() || undefined,
            year: String(item.year || '').trim() || undefined,
            displayLabel: String(item.displayLabel || '').trim() || undefined,
          } as DriverVersionOption;
        })
        .filter((item: DriverVersionOption | null): item is DriverVersionOption => !!item);

      if (options.length === 0) {
        const fallbackVersion = String(row.pinnedVersion || '').trim();
        const fallbackURL = String(row.defaultDownloadUrl || '').trim();
        if (fallbackVersion || fallbackURL) {
          options.push({
            version: fallbackVersion,
            downloadUrl: fallbackURL,
            recommended: true,
            source: 'fallback',
            displayLabel: fallbackVersion || '默认版本',
          });
        }
      }

      setVersionMap((prev) => ({ ...prev, [driverType]: options }));
      setSelectedVersionMap((prev) => {
        const currentKey = prev[driverType];
        if (currentKey && options.some((option) => buildVersionOptionKey(option) === currentKey)) {
          return prev;
        }
        const preferred =
          options.find((option) => option.version === row.installedVersion) ||
          options.find((option) => option.version === row.pinnedVersion) ||
          options.find((option) => option.recommended) ||
          options[0];
        if (!preferred) {
          return prev;
        }
        return { ...prev, [driverType]: buildVersionOptionKey(preferred) };
      });
      return options;
    } catch (err: any) {
      if (toastOnError) {
        message.error(`加载 ${row.name} 版本列表失败：${err?.message || String(err)}`);
      }
      return [] as DriverVersionOption[];
    } finally {
      setVersionLoadingMap((prev) => ({ ...prev, [driverType]: false }));
    }
  }, []);

  const loadVersionPackageSize = useCallback(async (row: DriverStatusRow, optionKey: string) => {
    if (row.builtIn) {
      return;
    }
    const driverType = String(row.type || '').trim();
    if (!driverType || !optionKey) {
      return;
    }

    const options = versionMap[driverType] || [];
    const selectedOption = options.find((item) => buildVersionOptionKey(item) === optionKey);
    if (!selectedOption) {
      return;
    }
    if (String(selectedOption.packageSizeText || '').trim()) {
      return;
    }

    const versionText = String(selectedOption.version || '').trim();
    if (!versionText) {
      return;
    }

    const loadingKey = buildVersionSizeLoadingKey(driverType, optionKey);
    if (versionSizeLoadingMap[loadingKey]) {
      return;
    }

    setVersionSizeLoadingMap((prev) => ({ ...prev, [loadingKey]: true }));
    try {
      const res = await GetDriverVersionPackageSize(driverType, versionText);
      if (!res?.success) {
        return;
      }
      const data = (res?.data || {}) as any;
      const sizeText = String(data.packageSizeText || '').trim();
      if (!sizeText) {
        return;
      }

      setVersionMap((prev) => {
        const current = prev[driverType] || [];
        let changed = false;
        const next = current.map((item) => {
          if (buildVersionOptionKey(item) !== optionKey) {
            return item;
          }
          if (String(item.packageSizeText || '').trim() === sizeText) {
            return item;
          }
          changed = true;
          return { ...item, packageSizeText: sizeText };
        });
        if (!changed) {
          return prev;
        }
        return { ...prev, [driverType]: next };
      });
    } finally {
      setVersionSizeLoadingMap((prev) => {
        if (!prev[loadingKey]) {
          return prev;
        }
        const next = { ...prev };
        delete next[loadingKey];
        return next;
      });
    }
  }, [versionMap, versionSizeLoadingMap]);

  useEffect(() => {
    if (!open) {
      setHorizontalScrollWidth(DRIVER_TABLE_SCROLL_X);
      tableScrollTargetsRef.current = [];
      return;
    }

    const cachedStatus = driverStatusSnapshotCache;
    const hasCachedStatus = !!cachedStatus;
    if (cachedStatus) {
      setRows(cachedStatus.rows);
      if (cachedStatus.downloadDir) {
        setDownloadDir(cachedStatus.downloadDir);
      }
    }
    const shouldRefreshStatus = !cachedStatus || !isFreshCache(cachedStatus.cachedAt, DRIVER_STATUS_CACHE_TTL_MS);
    if (shouldRefreshStatus) {
      void refreshStatus(false, { showLoading: !hasCachedStatus });
    }

    const cachedNetwork = driverNetworkSnapshotCache;
    const hasCachedNetwork = !!cachedNetwork;
    if (cachedNetwork) {
      setNetworkStatus(cachedNetwork.status);
    }
    const shouldRefreshNetwork = !cachedNetwork || !isFreshCache(cachedNetwork.cachedAt, DRIVER_NETWORK_CACHE_TTL_MS);
    if (shouldRefreshNetwork) {
      void checkNetworkStatus(false, { showLoading: !hasCachedNetwork });
    }
  }, [checkNetworkStatus, open, refreshStatus]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const tableContainer = tableContainerRef.current;
    const externalScroll = externalHScrollRef.current;
    if (!(tableContainer instanceof HTMLElement) || !(externalScroll instanceof HTMLDivElement)) {
      return;
    }

    let currentTargets: HTMLElement[] = [];
    let rafId: number | null = null;
    let bodyResizeObserver: ResizeObserver | null = null;
    let containerResizeObserver: ResizeObserver | null = null;

    const pickSyncTarget = () => {
      if (currentTargets.length === 0) {
        return null;
      }
      return currentTargets.find((target) => target.scrollWidth > target.clientWidth + 1) || currentTargets[0];
    };

    const syncFromTableTarget = (event?: Event) => {
      const source = event?.currentTarget instanceof HTMLElement ? event.currentTarget : null;
      const activeTarget = source || pickSyncTarget();
      if (!activeTarget) {
        return;
      }
      if (horizontalSyncSourceRef.current === 'external') {
        return;
      }
      horizontalSyncSourceRef.current = 'table';
      if (Math.abs(externalScroll.scrollLeft - activeTarget.scrollLeft) > 1) {
        externalScroll.scrollLeft = activeTarget.scrollLeft;
      }
      horizontalSyncSourceRef.current = '';
    };

    const bindCurrentTableTargets = () => {
      const nextTargets = [
        ...new Set(
          [
            ...Array.from(tableContainer.querySelectorAll('.ant-table-content')),
            ...Array.from(tableContainer.querySelectorAll('.ant-table-body')),
          ].filter((node): node is HTMLElement => node instanceof HTMLElement),
        ),
      ];

      const sameTargets =
        nextTargets.length === currentTargets.length &&
        nextTargets.every((target, index) => target === currentTargets[index]);
      if (sameTargets) {
        return;
      }

      currentTargets.forEach((target) => {
        target.removeEventListener('scroll', syncFromTableTarget);
        bodyResizeObserver?.unobserve(target);
      });

      currentTargets = nextTargets;
      tableScrollTargetsRef.current = nextTargets;
      currentTargets.forEach((target) => {
        target.addEventListener('scroll', syncFromTableTarget, { passive: true });
        bodyResizeObserver?.observe(target);
      });

      refreshHorizontalScrollState();
      syncFromTableTarget();
    };

    const scheduleRefresh = () => {
      if (rafId !== null) {
        cancelAnimationFrame(rafId);
      }
      rafId = requestAnimationFrame(() => {
        bindCurrentTableTargets();
        refreshHorizontalScrollState();
      });
    };

    const mutationObserver = new MutationObserver(scheduleRefresh);
    mutationObserver.observe(tableContainer, { childList: true, subtree: true });

    bodyResizeObserver = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(scheduleRefresh) : null;
    containerResizeObserver = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(scheduleRefresh) : null;
    containerResizeObserver?.observe(tableContainer);
    if (typeof ResizeObserver !== 'undefined') {
      modalContentRef.current && containerResizeObserver?.observe(modalContentRef.current);
    }
    window.addEventListener('resize', scheduleRefresh);

    scheduleRefresh();
    return () => {
      mutationObserver.disconnect();
      window.removeEventListener('resize', scheduleRefresh);
      currentTargets.forEach((target) => {
        target.removeEventListener('scroll', syncFromTableTarget);
      });
      if (bodyResizeObserver) {
        bodyResizeObserver.disconnect();
      }
      if (containerResizeObserver) {
        containerResizeObserver.disconnect();
      }
      if (rafId !== null) {
        cancelAnimationFrame(rafId);
      }
    };
  }, [open, refreshHorizontalScrollState]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const off = EventsOn('driver:download-progress', (event: DriverProgressEvent) => {
      if (!event) {
        return;
      }
      const driverType = String(event.driverType || '').trim().toLowerCase();
      const status = event.status;
      if (!driverType || !status) {
        return;
      }
      const messageText = String(event.message || '').trim();
      const percent = Math.max(0, Math.min(100, Number(event.percent || 0)));
      setProgressMap((prev) => ({
        ...prev,
        [driverType]: {
          status,
          message: messageText,
          percent,
        },
      }));
      const progressText = `${Math.round(percent)}%`;
      const statusText = String(status || '').toUpperCase();
      const lineText = `[${statusText}] ${messageText || '-'} (${progressText})`;
      const lineSignature = `${statusText}|${messageText || '-'}`;
      appendOperationLog(driverType, lineText, lineSignature, 'update-last');
    });
    return () => {
      off();
    };
  }, [appendOperationLog, open]);

  const installDriver = useCallback(async (row: DriverStatusRow) => {
    setActionState({ driverType: row.type, kind: 'install' });
    setProgressMap((prev) => ({
      ...prev,
      [row.type]: {
        status: 'start',
        message: '开始安装',
        percent: 0,
      },
    }));
    appendOperationLog(row.type, '[START] 开始自动安装');
    try {
      let options = versionMap[row.type] || [];
      if (options.length === 0) {
        options = await loadVersionOptions(row, true);
      }
      const selectedKey = selectedVersionMap[row.type];
      const selectedOption =
        options.find((item) => buildVersionOptionKey(item) === selectedKey) ||
        options.find((item) => item.recommended) ||
        options[0];
      const selectedVersion = selectedOption?.version || row.pinnedVersion || '';
      const selectedDownloadURL = selectedOption?.downloadUrl || row.defaultDownloadUrl || '';

      const result = await DownloadDriverPackage(row.type, selectedVersion, selectedDownloadURL, downloadDir);
      if (!result?.success) {
        const errText = result?.message || `安装 ${row.name} 失败`;
        appendOperationLog(row.type, `[ERROR] ${errText}`);
        message.error(errText);
        return;
      }
      const versionTip = selectedVersion ? `（${selectedVersion}）` : '';
      appendOperationLog(row.type, `[DONE] 自动安装完成 ${versionTip}`);
      message.success(`${row.name}${versionTip} 已安装启用`);
      refreshStatus(false);
    } finally {
      setActionState({ driverType: '', kind: '' });
    }
  }, [appendOperationLog, downloadDir, loadVersionOptions, refreshStatus, selectedVersionMap, versionMap]);

  const installDriverFromLocalPath = useCallback(async (
    row: DriverStatusRow,
    sourcePath: string,
    sourceLabel: '文件' | '目录',
    options?: { silentToast?: boolean; skipRefresh?: boolean },
  ) => {
    const pathText = String(sourcePath || '').trim();
    if (!pathText) {
      if (!options?.silentToast) {
        message.error(`未选择有效的本地导入${sourceLabel}`);
      }
      return false;
    }

    setActionState({ driverType: row.type, kind: 'local' });
    setProgressMap((prev) => ({
      ...prev,
      [row.type]: {
        status: 'start',
        message: '开始导入本地驱动包',
        percent: 0,
      },
    }));
    appendOperationLog(row.type, `[START] 开始本地导入（${sourceLabel}）：${pathText}`);
    try {
      const result = await InstallLocalDriverPackage(row.type, pathText, downloadDir);
      if (!result?.success) {
        const errText = result?.message || `导入 ${row.name} 本地驱动包失败`;
        appendOperationLog(row.type, `[ERROR] ${errText}`);
        if (!options?.silentToast) {
          message.error(errText);
        }
        return false;
      }
      appendOperationLog(row.type, '[DONE] 本地导入安装完成');
      if (!options?.silentToast) {
        message.success(`${row.name} 本地驱动包已安装启用`);
      }
      if (!options?.skipRefresh) {
        await refreshStatus(false);
      }
      return true;
    } finally {
      setActionState({ driverType: '', kind: '' });
    }
  }, [appendOperationLog, downloadDir, refreshStatus]);

  const installDriverFromLocalFile = useCallback(async (row: DriverStatusRow) => {
    const fileRes = await SelectDriverPackageFile(downloadDir);
    if (!fileRes?.success) {
      if (String(fileRes?.message || '') !== 'Cancelled') {
        message.error(fileRes?.message || '选择本地驱动包文件失败');
      }
      return;
    }
    const filePath = String((fileRes?.data as any)?.path || '').trim();
    if (!filePath) {
      message.error('未选择有效的驱动包文件');
      return;
    }
    await installDriverFromLocalPath(row, filePath, '文件');
  }, [downloadDir, installDriverFromLocalPath]);

  const installDriversFromDirectory = useCallback(async () => {
    const directoryRes = await SelectDriverPackageDirectory(downloadDir);
    if (!directoryRes?.success) {
      if (String(directoryRes?.message || '') !== 'Cancelled') {
        message.error(directoryRes?.message || '选择本地驱动包目录失败');
      }
      return;
    }

    const directoryPath = String((directoryRes?.data as any)?.path || '').trim();
    if (!directoryPath) {
      message.error('未选择有效的驱动包目录');
      return;
    }
    const optionalRows = rows.filter((item) => !item.builtIn);
    if (optionalRows.length === 0) {
      message.info('当前没有可导入的外置驱动');
      return;
    }

    let successCount = 0;
    let failCount = 0;
    let dedupeSkipCount = 0;
    let slimSkipCount = 0;

    setBatchDirectoryImporting(true);
    try {
      for (const row of optionalRows) {
        const alreadyInstalled = row.packageInstalled || row.connectable;
        if (alreadyInstalled && !forceOverwriteInstalled) {
          dedupeSkipCount += 1;
          appendOperationLog(row.type, '[SKIP] 已检测到驱动已安装，目录导入去重跳过');
          continue;
        }
        if (alreadyInstalled && forceOverwriteInstalled) {
          appendOperationLog(row.type, '[INFO] 已启用覆盖已安装模式，执行重装导入');
        }
        const isSlimBuildUnavailable = (row.message || '').includes('精简构建') && !row.packageInstalled;
        if (isSlimBuildUnavailable) {
          slimSkipCount += 1;
          appendOperationLog(row.type, '[WARN] 当前发行包为精简构建，已跳过目录导入');
          continue;
        }
        const ok = await installDriverFromLocalPath(row, directoryPath, '目录', { silentToast: true, skipRefresh: true });
        if (ok) {
          successCount += 1;
        } else {
          failCount += 1;
        }
      }
      await refreshStatus(false);
    } finally {
      setBatchDirectoryImporting(false);
    }

    const skipParts: string[] = [];
    if (dedupeSkipCount > 0) {
      skipParts.push(`去重跳过 ${dedupeSkipCount}`);
    }
    if (slimSkipCount > 0) {
      skipParts.push(`精简版跳过 ${slimSkipCount}`);
    }
    const skipTip = skipParts.length > 0 ? `，${skipParts.join('，')}` : '';

    const forceTip = forceOverwriteInstalled ? '（覆盖已安装）' : '';
    if (failCount === 0) {
      message.success(`目录导入完成${forceTip}：成功 ${successCount}${skipTip}`);
      return;
    }
    if (successCount > 0) {
      message.warning(`目录导入完成${forceTip}：成功 ${successCount}，失败 ${failCount}${skipTip}`);
      return;
    }
    message.error(`目录导入失败${forceTip}：失败 ${failCount}${skipTip}`);
  }, [appendOperationLog, downloadDir, forceOverwriteInstalled, installDriverFromLocalPath, refreshStatus, rows]);

  const openDriverLog = useCallback((driverType: string) => {
    const normalized = String(driverType || '').trim().toLowerCase();
    if (!normalized) {
      return;
    }
    setLogDriverType(normalized);
    setLogModalOpen(true);
  }, []);

  const removeDriver = useCallback(async (row: DriverStatusRow) => {
    setActionState({ driverType: row.type, kind: 'remove' });
    appendOperationLog(row.type, '[START] 开始移除驱动');
    try {
      const result = await RemoveDriverPackage(row.type, downloadDir);
      if (!result?.success) {
        const errText = result?.message || `移除 ${row.name} 失败`;
        appendOperationLog(row.type, `[ERROR] ${errText}`);
        message.error(errText);
        return;
      }
      appendOperationLog(row.type, '[DONE] 驱动移除完成');
      message.success(`${row.name} 已移除`);
      setProgressMap((prev) => {
        const next = { ...prev };
        delete next[row.type];
        return next;
      });
      refreshStatus(false);
    } finally {
      setActionState({ driverType: '', kind: '' });
    }
  }, [appendOperationLog, downloadDir, refreshStatus]);

  const columns = useMemo(() => {
    return [
      {
        title: '数据源',
        dataIndex: 'name',
        key: 'name',
        width: 150,
      },
      {
        title: '安装包大小',
        dataIndex: 'packageSizeText',
        key: 'packageSizeText',
        width: 120,
        render: (_: string | undefined, row: DriverStatusRow) => {
          if (row.builtIn) {
            return row.packageSizeText || '-';
          }
          const options = versionMap[row.type] || [];
          const selectedKey = selectedVersionMap[row.type];
          const loadingKey = buildVersionSizeLoadingKey(row.type, selectedKey || '');
          const selectedOption =
            options.find((item) => buildVersionOptionKey(item) === selectedKey) ||
            options.find((item) => item.recommended) ||
            options[0];
          const anyKnownSize = options.find((item) => String(item.packageSizeText || '').trim())?.packageSizeText;
          if (selectedKey && versionSizeLoadingMap[loadingKey]) {
            return '计算中...';
          }
          return selectedOption?.packageSizeText || anyKnownSize || row.packageSizeText || '-';
        },
      },
      {
        title: '状态',
        key: 'status',
        width: 140,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Tag color="success">内置可用</Tag>;
          }
          const progress = progressMap[row.type];
          if (progress && (progress.status === 'start' || progress.status === 'downloading')) {
            return <Tag color="processing">安装中 {Math.round(progress.percent)}%</Tag>;
          }
          if (row.connectable) {
            return <Tag color="success">已启用</Tag>;
          }
          if (row.packageInstalled) {
            return <Tag color="warning">已安装</Tag>;
          }
          return <Tag color="default">未启用</Tag>;
        },
      },
      {
        title: '安装进度',
        key: 'progress',
        width: 170,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }

          const progress = progressMap[row.type];
          let percent = 0;
          let status: 'normal' | 'exception' | 'active' | 'success' = 'normal';

          if (progress?.status === 'error') {
            percent = Math.max(0, Math.min(100, Math.round(progress.percent || 0)));
            status = 'exception';
          } else if (progress && (progress.status === 'start' || progress.status === 'downloading')) {
            percent = Math.max(1, Math.min(99, Math.round(progress.percent || 0)));
            status = 'active';
          } else if (row.connectable || row.packageInstalled) {
            percent = 100;
            status = 'success';
          }

          return <Progress percent={percent} status={status} size="small" />;
        },
      },
      {
        title: '驱动版本',
        key: 'driverVersion',
        width: 230,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }
          const versionLocked = row.packageInstalled || row.connectable;
          if (versionLocked) {
            const installedVersion = String(row.installedVersion || '').trim();
            if (installedVersion) {
              return <Text type="secondary">{installedVersion}（已安装，移除后可更换）</Text>;
            }
            return <Text type="secondary">已安装（移除后可更换）</Text>;
          }
          const options = versionMap[row.type] || [];
          const selectedKey = selectedVersionMap[row.type];
          const selectOptions = buildVersionSelectOptions(options);
          return (
            <Select
              size="small"
              style={{ width: '100%' }}
              loading={!!versionLoadingMap[row.type]}
              disabled={actionState.driverType === row.type}
              placeholder={options.length > 0 ? '选择驱动版本' : '点击展开加载版本'}
              value={selectedKey}
              options={selectOptions as any}
              onOpenChange={(open) => {
                if (open && options.length === 0 && !versionLoadingMap[row.type]) {
                  void loadVersionOptions(row, true);
                  return;
                }
                if (open && selectedKey) {
                  void loadVersionPackageSize(row, selectedKey);
                }
              }}
              onChange={(value) => {
                setSelectedVersionMap((prev) => ({ ...prev, [row.type]: value }));
                void loadVersionPackageSize(row, value);
              }}
            />
          );
        },
      },
      {
        title: '操作',
        key: 'actions',
        width: 320,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">-</Text>;
          }
          const isSlimBuildUnavailable = (row.message || '').includes('精简构建');
          const loadingInstallOrRemove =
            actionState.driverType === row.type && (actionState.kind === 'install' || actionState.kind === 'remove');
          const loadingLocal = actionState.driverType === row.type && actionState.kind === 'local';
          if (isSlimBuildUnavailable && !row.packageInstalled) {
            return <Text type="secondary">需 Full 版</Text>;
          }

          const logs = operationLogMap[row.type] || [];
          const hasLogs = logs.length > 0;

          const mainAction = row.connectable ? (
            <Button
              danger
              icon={<DeleteOutlined />}
              loading={loadingInstallOrRemove}
              onClick={() => removeDriver(row)}
            >
              移除
            </Button>
          ) : (
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={loadingInstallOrRemove}
              onClick={() => installDriver(row)}
            >
              安装启用
            </Button>
          );

          return (
            <Space size={8} wrap>
              {mainAction}
              <Button
                icon={<FileSearchOutlined />}
                loading={loadingLocal}
                onClick={() => installDriverFromLocalFile(row)}
              >
                本地导入
              </Button>
              <Button
                type={hasLogs ? 'default' : 'text'}
                disabled={!hasLogs}
                onClick={() => openDriverLog(row.type)}
              >
                日志
              </Button>
            </Space>
          );
        },
      },
    ];
  }, [actionState, installDriver, installDriverFromLocalFile, loadVersionOptions, loadVersionPackageSize, openDriverLog, operationLogMap, progressMap, removeDriver, selectedVersionMap, versionLoadingMap, versionMap, versionSizeLoadingMap]);

  const activeLogRow = useMemo(() => {
    if (!logDriverType) {
      return undefined;
    }
    return rows.find((item) => item.type === logDriverType);
  }, [logDriverType, rows]);
  const normalizedSearchKeyword = useMemo(() => normalizeDriverSearchText(searchKeyword), [searchKeyword]);
  const filteredRows = useMemo(() => {
    if (!normalizedSearchKeyword) {
      return rows;
    }
    return rows.filter((row) => {
      const searchableParts = [
        row.name,
        row.type,
        row.pinnedVersion,
        row.installedVersion,
        row.message,
        row.builtIn ? '内置' : '外置',
        row.connectable ? '已启用' : row.packageInstalled ? '已安装' : '未启用',
      ];
      const searchableText = normalizeDriverSearchText(searchableParts.filter(Boolean).join(' '));
      return searchableText.includes(normalizedSearchKeyword);
    });
  }, [normalizedSearchKeyword, rows]);
  const filterSummaryText = useMemo(() => {
    if (normalizedSearchKeyword) {
      return `匹配 ${filteredRows.length} / ${rows.length}`;
    }
    return `共 ${rows.length} 个驱动`;
  }, [filteredRows.length, normalizedSearchKeyword, rows.length]);

  const activeDriverLogs = operationLogMap[logDriverType] || [];
  const activeDriverLogLines = activeDriverLogs.map((item) => `[${item.time}] ${item.text}`);
  const proxyEnvEntries = Object.entries(networkStatus?.proxyEnv || {});
  const downloadRequiredHosts = (networkStatus?.downloadRequiredHosts || []).filter(Boolean);
  const showDownloadChainAlert = networkStatus?.downloadChainReachable === false;
  const networkUnreachable = networkStatus?.reachable === false;
  const downloadRequiredHostText = (downloadRequiredHosts.length > 0
    ? downloadRequiredHosts
    : ['github.com', 'api.github.com', 'release-assets.githubusercontent.com', 'objects.githubusercontent.com', 'raw.githubusercontent.com']).join('、');
  const githubConnectivityProbe = networkStatus?.checks.find((item) => item.name === 'GitHub API')
    || networkStatus?.checks.find((item) => item.name === 'GitHub 驱动发布')
    || null;
  const githubConnectivityLatencyMs = githubConnectivityProbe
    ? (githubConnectivityProbe.httpLatencyMs ?? githubConnectivityProbe.latencyMs ?? githubConnectivityProbe.tcpLatencyMs)
    : undefined;
  const logBlockBackground = darkMode
    ? `rgba(28, 28, 28, ${Math.max(opacity, 0.82)})`
    : `rgba(255, 255, 255, ${Math.max(opacity, 0.92)})`;
  const logBlockBorderColor = darkMode ? 'rgba(255, 255, 255, 0.16)' : 'rgba(0, 0, 0, 0.12)';
  const logBlockTextColor = darkMode ? 'rgba(255, 255, 255, 0.88)' : 'rgba(0, 0, 0, 0.88)';

  return (
    <Modal
      title="驱动管理"
      open={open}
      onCancel={onClose}
      width={980}
      style={{ top: 24 }}
      styles={{
        body: {
          maxHeight: 'calc(100vh - 220px)',
          overflowY: 'auto',
          overflowX: 'hidden',
          paddingRight: 18,
        },
      }}
      destroyOnClose
      footer={(
        <div className="driver-manager-footer">
          <div
            ref={externalHScrollRef}
            className="driver-manager-hscroll"
            aria-hidden={false}
            onScroll={applyExternalScrollToTableTargets}
          >
            <div className="driver-manager-hscroll-inner" style={{ width: `${Math.max(horizontalScrollWidth, 1)}px` }} />
          </div>
          <Space className="driver-manager-footer-actions" size={8}>
            <Button key="refresh" icon={<ReloadOutlined />} onClick={() => refreshStatus(true)} loading={loading}>
              刷新
            </Button>
            <Button key="network" onClick={() => checkNetworkStatus(true)} loading={networkChecking}>
              网络检测
            </Button>
            <Button key="close" type="primary" onClick={onClose}>
              关闭
            </Button>
          </Space>
        </div>
      )}
    >
      <div ref={modalContentRef}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Text type="secondary">除 MySQL / Redis / Oracle / PostgreSQL 外，其他数据源需先安装启用后再连接。</Text>
        {networkStatus ? (
          networkUnreachable ? (
            <Alert
              type="error"
              showIcon
              message={showDownloadChainAlert ? '重要提醒：驱动下载链路域名不可达' : '重要提醒：驱动下载网络不可达'}
              description={(
                <Space direction="vertical" size={8} style={{ width: '100%' }}>
                  {showDownloadChainAlert ? (
                    <>
                      <Text>
                        当前可能能访问 GitHub 页面，但驱动包下载会跳转到资产域名。
                        请优先在 GoNavi 顶部“代理”中启用全局代理（填写代理应用本地地址和端口）。
                      </Text>
                      {onOpenGlobalProxySettings ? (
                        <Button size="small" onClick={onOpenGlobalProxySettings}>打开全局代理设置</Button>
                      ) : null}
                      <Text>
                        若仍失败，请在代理规则放行：{downloadRequiredHostText}；仍无法调整规则时，再考虑开启 TUN 模式。
                      </Text>
                    </>
                  ) : (
                    <Text>{networkStatus.summary}</Text>
                  )}
                  {proxyEnvEntries.length > 0 ? (
                    <Text type="secondary">
                      检测到代理环境变量：{proxyEnvEntries.map(([key]) => key).join('、')}
                    </Text>
                  ) : null}
                </Space>
              )}
            />
          ) : (
            <Alert
              type="success"
              showIcon
              message={networkStatus.summary}
              description={(
                <Collapse
                  size="small"
                  items={[
                    {
                      key: 'checks',
                      label: '查看网络检测明细',
                      children: (
                        <Space direction="vertical" size={4} style={{ width: '100%' }}>
                          <Text type="secondary">
                            代理链路到 GitHub 连通性延迟：{githubConnectivityProbe ? (githubConnectivityProbe.reachable ? '可达' : '不可达') : '暂无结果'}
                            {githubConnectivityLatencyMs !== undefined ? `，${githubConnectivityLatencyMs}ms` : ''}
                            {githubConnectivityProbe?.error ? `，${githubConnectivityProbe.error}` : ''}
                          </Text>
                          {proxyEnvEntries.length > 0 ? (
                            <Text type="secondary">
                              检测到代理环境变量：{proxyEnvEntries.map(([key]) => key).join('、')}
                            </Text>
                          ) : (
                            <Text type="secondary">未检测到系统代理环境变量。</Text>
                          )}
                        </Space>
                      ),
                    },
                  ]}
                />
              )}
            />
          )
        ) : (
          <Alert
            type="info"
            showIcon
            icon={sharedInfoAlertIcon}
            message={networkChecking ? '正在检测驱动下载网络...' : '尚未完成网络检测'}
          />
        )}

        <Alert
          type="info"
          showIcon
          icon={sharedInfoAlertIcon}
          message="驱动目录与复用说明"
          description={(
            <Collapse
              size="small"
              items={[
                {
                  key: 'driver-directory',
                  label: '查看驱动目录与复用说明',
                  children: (
                    <Space direction="vertical" size={6} style={{ width: '100%' }}>
                      <Text type="secondary">自动下载和手动导入的驱动都会落盘到以下目录；后续版本升级可重复复用已下载驱动。</Text>
                      <Text type="secondary">行内“本地导入”仅用于单个驱动文件/总包（如 `mariadb-driver-agent`、`mariadb-driver-agent.exe`、`GoNavi-DriverAgents.zip`）；批量导入请使用上方“导入驱动目录”。</Text>
                      <Paragraph copyable={{ text: downloadDir || '-' }} style={{ marginBottom: 0 }}>
                        驱动根目录：{downloadDir || '-'}
                      </Paragraph>
                      {networkStatus?.logPath ? (
                        <Paragraph copyable={{ text: networkStatus.logPath }} style={{ marginBottom: 0 }}>
                          运行日志文件：{networkStatus.logPath}
                        </Paragraph>
                      ) : null}
                    </Space>
                  ),
                },
              ]}
            />
          )}
        />

        <div style={{ width: '100%', display: 'flex', gap: 12, alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap' }}>
          <Input.Search
            allowClear
            placeholder="搜索驱动名称/类型（如 DuckDB、clickhouse）"
            value={searchKeyword}
            onChange={(event) => setSearchKeyword(event.target.value)}
            style={{ minWidth: 300, flex: '1 1 360px' }}
          />
          <Space size={8}>
            <Text type="secondary">覆盖已安装</Text>
            <Switch
              checked={forceOverwriteInstalled}
              onChange={(checked) => setForceOverwriteInstalled(checked)}
              disabled={batchDirectoryImporting}
            />
            <Button
              icon={<FolderOpenOutlined />}
              loading={batchDirectoryImporting}
              onClick={() => void installDriversFromDirectory()}
            >
              导入驱动目录
            </Button>
          </Space>
        </div>
        <Text type="secondary">{filterSummaryText}</Text>

        <div
          ref={tableContainerRef}
          className="driver-manager-table-wrap driver-manager-table-wrap-external-active"
        >
          <Table
            className="driver-manager-table"
            rowKey="type"
            loading={loading}
            columns={columns as any}
            dataSource={filteredRows}
            pagination={false}
            size="middle"
            sticky={false}
            scroll={{ x: DRIVER_TABLE_SCROLL_X }}
            locale={{
              emptyText: normalizedSearchKeyword
                ? `未找到匹配“${String(searchKeyword || '').trim()}”的驱动`
                : '暂无驱动数据',
            }}
          />
        </div>
      </Space>
      </div>
      <Modal
        title={`驱动日志 - ${activeLogRow?.name || logDriverType}`}
        open={logModalOpen}
        onCancel={() => setLogModalOpen(false)}
        footer={[
          <Button key="close-log" type="primary" onClick={() => setLogModalOpen(false)}>
            关闭
          </Button>,
        ]}
        width={780}
      >
        <Space direction="vertical" size={8} style={{ width: '100%' }}>
          {activeLogRow?.installDir ? (
            <Paragraph copyable={{ text: activeLogRow.installDir }} style={{ marginBottom: 0 }}>
              安装目录：{activeLogRow.installDir}
            </Paragraph>
          ) : null}
          {activeLogRow?.executablePath ? (
            <Paragraph copyable={{ text: activeLogRow.executablePath }} style={{ marginBottom: 0 }}>
              驱动可执行文件：{activeLogRow.executablePath}
            </Paragraph>
          ) : null}
          {activeDriverLogLines.length > 0 ? (
            <pre style={{ margin: 0, maxHeight: 360, overflow: 'auto', padding: 12, background: logBlockBackground, color: logBlockTextColor, borderRadius: 8, border: `1px solid ${logBlockBorderColor}`, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
              {activeDriverLogLines.join('\n')}
            </pre>
          ) : (
            <Text type="secondary">当前驱动暂无操作日志。</Text>
          )}
        </Space>
      </Modal>
    </Modal>
  );
};

export default DriverManagerModal;
