import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Alert, Button, Collapse, Modal, Progress, Select, Space, Table, Tag, Typography, message } from 'antd';
import { DeleteOutlined, DownloadOutlined, FileSearchOutlined, ReloadOutlined } from '@ant-design/icons';
import { EventsOn } from '../../wailsjs/runtime/runtime';
import {
  CheckDriverNetworkStatus,
  DownloadDriverPackage,
  GetDriverVersionList,
  GetDriverVersionPackageSize,
  GetDriverStatusList,
  InstallLocalDriverPackage,
  RemoveDriverPackage,
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
  error?: string;
};

type DriverNetworkStatus = {
  reachable: boolean;
  summary: string;
  recommendedProxy: boolean;
  proxyConfigured: boolean;
  proxyEnv?: Record<string, string>;
  checks: DriverNetworkProbe[];
  checkedAt?: string;
  logPath?: string;
};

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

const DriverManagerModal: React.FC<{ open: boolean; onClose: () => void }> = ({ open, onClose }) => {
  const [loading, setLoading] = useState(false);
  const [downloadDir, setDownloadDir] = useState('');
  const [networkChecking, setNetworkChecking] = useState(false);
  const [networkStatus, setNetworkStatus] = useState<DriverNetworkStatus | null>(null);
  const [rows, setRows] = useState<DriverStatusRow[]>([]);
  const [actionDriver, setActionDriver] = useState('');
  const [progressMap, setProgressMap] = useState<Record<string, ProgressState>>({});
  const [operationLogMap, setOperationLogMap] = useState<Record<string, DriverLogEntry[]>>({});
  const [logDriverType, setLogDriverType] = useState('');
  const [logModalOpen, setLogModalOpen] = useState(false);
  const [versionMap, setVersionMap] = useState<Record<string, DriverVersionOption[]>>({});
  const [selectedVersionMap, setSelectedVersionMap] = useState<Record<string, string>>({});
  const [versionLoadingMap, setVersionLoadingMap] = useState<Record<string, boolean>>({});
  const [versionSizeLoadingMap, setVersionSizeLoadingMap] = useState<Record<string, boolean>>({});

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

  const refreshStatus = useCallback(async (toastOnError = true) => {
    setLoading(true);
    try {
      const res = await GetDriverStatusList(downloadDir, '');
      if (!res?.success) {
        if (toastOnError) {
          message.error(res?.message || '拉取驱动状态失败');
        }
        return;
      }

      const data = (res?.data || {}) as any;
      const resolvedDir = String(data.downloadDir || '').trim();
      const drivers = Array.isArray(data.drivers) ? data.drivers : [];

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
    } catch (err: any) {
      if (toastOnError) {
        message.error(`拉取驱动状态失败：${err?.message || String(err)}`);
      }
    } finally {
      setLoading(false);
    }
  }, [downloadDir]);

  const checkNetworkStatus = useCallback(async (toastOnError = false) => {
    setNetworkChecking(true);
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
        httpStatus: Number(item.httpStatus || 0) || undefined,
        latencyMs: Number(item.latencyMs || 0) || undefined,
        error: String(item.error || '').trim() || undefined,
      }));
      setNetworkStatus({
        reachable: !!data.reachable,
        summary: String(data.summary || '').trim() || '驱动网络检测已完成',
        recommendedProxy: !!data.recommendedProxy,
        proxyConfigured: !!data.proxyConfigured,
        proxyEnv: (data.proxyEnv || {}) as Record<string, string>,
        checkedAt: String(data.checkedAt || '').trim() || undefined,
        checks: normalizedChecks,
        logPath: String(data.logPath || '').trim() || undefined,
      });
    } catch (err: any) {
      if (toastOnError) {
        message.error(`驱动网络检测失败：${err?.message || String(err)}`);
      }
    } finally {
      setNetworkChecking(false);
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
      return;
    }
    refreshStatus(false);
    checkNetworkStatus(false);
  }, [checkNetworkStatus, open, refreshStatus]);

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
    setActionDriver(row.type);
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
      setActionDriver('');
    }
  }, [appendOperationLog, downloadDir, loadVersionOptions, refreshStatus, selectedVersionMap, versionMap]);

  const installDriverFromLocalFile = useCallback(async (row: DriverStatusRow) => {
    const fileRes = await SelectDriverPackageFile(downloadDir);
    if (!fileRes?.success) {
      if (String(fileRes?.message || '') !== 'Cancelled') {
        message.error(fileRes?.message || '选择本地驱动包失败');
      }
      return;
    }
    const filePath = String((fileRes?.data as any)?.path || '').trim();
    if (!filePath) {
      message.error('未选择有效的驱动包文件');
      return;
    }

    setActionDriver(row.type);
    setProgressMap((prev) => ({
      ...prev,
      [row.type]: {
        status: 'start',
        message: '开始导入本地驱动包',
        percent: 0,
      },
    }));
    appendOperationLog(row.type, `[START] 开始本地导入：${filePath}`);
    try {
      const result = await InstallLocalDriverPackage(row.type, filePath, downloadDir);
      if (!result?.success) {
        const errText = result?.message || `导入 ${row.name} 本地驱动包失败`;
        appendOperationLog(row.type, `[ERROR] ${errText}`);
        message.error(errText);
        return;
      }
      appendOperationLog(row.type, '[DONE] 本地导入安装完成');
      message.success(`${row.name} 本地驱动包已安装启用`);
      refreshStatus(false);
    } finally {
      setActionDriver('');
    }
  }, [appendOperationLog, downloadDir, refreshStatus]);

  const openDriverLog = useCallback((driverType: string) => {
    const normalized = String(driverType || '').trim().toLowerCase();
    if (!normalized) {
      return;
    }
    setLogDriverType(normalized);
    setLogModalOpen(true);
  }, []);

  const removeDriver = useCallback(async (row: DriverStatusRow) => {
    setActionDriver(row.type);
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
      setActionDriver('');
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
        title: '安装位置',
        key: 'installPath',
        width: 260,
        render: (_: string, row: DriverStatusRow) => {
          if (row.builtIn) {
            return <Text type="secondary">内置</Text>;
          }
          const installPath = row.executablePath || row.installDir || '-';
          if (installPath === '-') {
            return <Text type="secondary">-</Text>;
          }
          return (
            <Text copyable={{ text: installPath }} style={{ fontSize: 12 }}>
              {installPath}
            </Text>
          );
        },
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
          const options = versionMap[row.type] || [];
          const selectedKey = selectedVersionMap[row.type];
          const selectOptions = buildVersionSelectOptions(options);
          return (
            <Select
              size="small"
              style={{ width: '100%' }}
              loading={!!versionLoadingMap[row.type]}
              disabled={actionDriver === row.type}
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
          const loadingAction = actionDriver === row.type;
          if (isSlimBuildUnavailable && !row.packageInstalled) {
            return <Text type="secondary">需 Full 版</Text>;
          }

          const logs = operationLogMap[row.type] || [];
          const hasLogs = logs.length > 0;

          const mainAction = row.connectable ? (
            <Button
              danger
              icon={<DeleteOutlined />}
              loading={loadingAction}
              onClick={() => removeDriver(row)}
            >
              移除
            </Button>
          ) : (
            <Button
              type="primary"
              icon={<DownloadOutlined />}
              loading={loadingAction}
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
                loading={loadingAction}
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
  }, [actionDriver, installDriver, installDriverFromLocalFile, loadVersionOptions, loadVersionPackageSize, openDriverLog, operationLogMap, progressMap, removeDriver, selectedVersionMap, versionLoadingMap, versionMap, versionSizeLoadingMap]);

  const activeLogRow = useMemo(() => {
    if (!logDriverType) {
      return undefined;
    }
    return rows.find((item) => item.type === logDriverType);
  }, [logDriverType, rows]);

  const activeDriverLogs = operationLogMap[logDriverType] || [];
  const activeDriverLogLines = activeDriverLogs.map((item) => `[${item.time}] ${item.text}`);
  const proxyEnvEntries = Object.entries(networkStatus?.proxyEnv || {});

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
      footer={[
        <Button key="refresh" icon={<ReloadOutlined />} onClick={() => refreshStatus(true)} loading={loading}>
          刷新
        </Button>,
        <Button key="network" onClick={() => checkNetworkStatus(true)} loading={networkChecking}>
          网络检测
        </Button>,
        <Button key="close" type="primary" onClick={onClose}>
          关闭
        </Button>,
      ]}
    >
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Text type="secondary">除 MySQL / Redis / Oracle / PostgreSQL 外，其他数据源需先安装启用后再连接。</Text>
        {networkStatus ? (
          <Alert
            type={networkStatus.reachable ? 'success' : 'warning'}
            showIcon
            message={networkStatus.summary}
            description={(
              <Space direction="vertical" size={6} style={{ width: '100%' }}>
                <Text type="secondary">
                  驱动下载依赖 GitHub 与 Go 模块代理网络。若检测失败，建议先启用 HTTP/HTTPS/SOCKS5 代理后重试。
                </Text>
                <Collapse
                  size="small"
                  items={[
                    {
                      key: 'checks',
                      label: '查看网络检测明细',
                      children: (
                        <Space direction="vertical" size={4} style={{ width: '100%' }}>
                          {networkStatus.checks.map((item) => (
                            <Text key={`${item.name}-${item.url}`} type={item.reachable ? 'secondary' : 'danger'}>
                              {item.name}：{item.reachable ? '可达' : '不可达'}{item.httpStatus ? `，HTTP ${item.httpStatus}` : ''}{item.latencyMs ? `，${item.latencyMs}ms` : ''}{item.error ? `，${item.error}` : ''}
                            </Text>
                          ))}
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
              </Space>
            )}
          />
        ) : (
          <Alert type="info" showIcon message={networkChecking ? '正在检测驱动下载网络...' : '尚未完成网络检测'} />
        )}

        <Alert
          type="info"
          showIcon
          message="驱动目录与复用说明"
          description={(
            <Space direction="vertical" size={6} style={{ width: '100%' }}>
              <Text type="secondary">自动下载和手动导入的驱动都会落盘到以下目录；后续版本升级可重复复用已下载驱动。</Text>
              <Text type="secondary">手动导入支持单个驱动代理文件（如 `mariadb-driver-agent` / `mariadb-driver-agent.exe`）或驱动总包 `GoNavi-DriverAgents.zip`。</Text>
              <Paragraph copyable={{ text: downloadDir || '-' }} style={{ marginBottom: 0 }}>
                驱动根目录：{downloadDir || '-'}
              </Paragraph>
              {networkStatus?.logPath ? (
                <Paragraph copyable={{ text: networkStatus.logPath }} style={{ marginBottom: 0 }}>
                  运行日志文件：{networkStatus.logPath}
                </Paragraph>
              ) : null}
            </Space>
          )}
        />

        <Table
          rowKey="type"
          loading={loading}
          columns={columns as any}
          dataSource={rows}
          pagination={false}
          size="middle"
          scroll={{ x: 1450 }}
        />
      </Space>
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
            <pre style={{ margin: 0, maxHeight: 360, overflow: 'auto', padding: 12, background: '#fafafa', borderRadius: 8, border: '1px solid #f0f0f0', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
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
