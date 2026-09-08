import React from 'react';
import { Button, Dropdown, Form, Input, Popconfirm, Select, Tooltip } from 'antd';
import { CheckOutlined, DeleteOutlined, DownOutlined, InfoCircleOutlined, LeftOutlined, RightOutlined } from '@ant-design/icons';
import type { FormInstance } from 'antd/es/form';

import type { AIProviderConfig } from '../../types';
import { buildProviderModelOptions, parseCLIModelCatalog, type CLIModelCatalog, type ProviderCheckResult } from '../../utils/aiProviderManagement';
import AIProviderModelSelect from './AIProviderModelSelect';
import { readCachedCLIModelCatalog, writeCachedCLIModelCatalog } from './cliModelCatalogCache';
import { passThroughHintTooltip } from '../common/tooltipTiming';
import './AISettingsProvidersSection.css';
import { getProviderEndpointType, getProviderEndpointTypes, type ProviderEndpointType } from '../../utils/aiProviderEndpoints';
import { t as catalogTranslate } from '../../i18n/catalog';
import { useOptionalI18n } from '../../i18n/provider';
import {
  getSingletonCLIIdentity,
  type ProviderPresetCandidate,
  type ProviderPresetEndpoint,
} from '../../utils/aiProviderPresets';
import { isProviderSecretRequirementSatisfied } from '../../utils/providerSecretDraft';
import { recordFromRows } from '../../utils/aiProviderKeyValue';
import { AIGetCLICapabilities, AIGetCLIModelCatalog } from '../../../wailsjs/go/aiservice/Service';
import type { ai } from '../../../wailsjs/go/models';
import type { OverlayWorkbenchTheme } from '../../utils/overlayWorkbenchTheme';
import AIProviderConfigList from './AIProviderConfigList';
import AIProviderPresetSelect from './AIProviderPresetSelect';
import AIProviderKeyValueRows from './AIProviderKeyValueRows';

export const REVEAL_ERROR_SELECTOR = '.ant-form-item-has-error, [role="alert"]';

interface RevealTarget { getBoundingClientRect(): { top: number; height: number } }
interface RevealContainer {
  scrollTop: number;
  clientHeight: number;
  querySelector(selector: string): RevealTarget | null;
  getBoundingClientRect(): { top: number };
  scrollTo?: (options: ScrollToOptions) => void;
}

export const revealFirstErrorIn = (container?: RevealContainer | null): boolean => {
  const target = container?.querySelector(REVEAL_ERROR_SELECTOR);
  if (!container || !target) return false;
  const { top: targetTop, height: targetHeight } = target.getBoundingClientRect();
  const centred = (container.clientHeight - targetHeight) / 2;
  const next = Math.max(0, container.scrollTop + targetTop - container.getBoundingClientRect().top - Math.max(0, centred));
  if (container.scrollTo) container.scrollTo({ top: next, behavior: 'smooth' });
  else container.scrollTop = next;
  return true;
};

export interface AISettingsProviderPresetOption {
  key: string;
  label: string;
  icon: React.ReactNode;
  desc: string;
  defaultBaseUrl: string;
  endpoints?: ProviderPresetEndpoint[];
  defaultModel?: string;
  models?: string[];
  authMode?: AIProviderConfig['authMode'];
  backendType?: AIProviderConfig['type'];
  fixedApiFormat?: string;
  defaultApiFormat?: string;
}

interface MatchedProviderPreset {
  key: string;
  label: string;
  icon: React.ReactNode;
}

const ProviderDisclosureSummary: React.FC<{
  open: boolean;
  label: React.ReactNode;
  hint?: React.ReactNode;
  onToggle: () => void;
}> = ({ open, label, hint, onToggle }) => (
  <summary aria-expanded={open} onClick={(event) => { event.preventDefault(); onToggle(); }}>
    <span className="gonavi-ai-provider-disclosure">
      <span className="gonavi-ai-provider-disclosure-lead">
        <span className="gonavi-ai-provider-disclosure-label">{label}</span>
        <span className="gonavi-ai-provider-caret" aria-hidden="true">{open ? <DownOutlined /> : <RightOutlined />}</span>
      </span>
      {hint}
    </span>
  </summary>
);

interface AISettingsProvidersSectionProps {
  providers: AIProviderConfig[];
  activeProviderId: string;
  pendingProviderId?: string;
  providersLoading?: boolean;
  loadError?: string;
  onReloadProviders?: () => void;
  editingProvider: AIProviderConfig | null;
  editorSessionKey?: number;
  isEditing: boolean;
  form: FormInstance;
  providerPresets: AISettingsProviderPresetOption[];
  watchedPresetKey?: string;
  watchedApiFormat?: string;
  loading: boolean;
  testing?: boolean;
  testStatus: 'idle' | 'success' | 'error';
  testResult?: ProviderCheckResult | null;
  onValuesChange?: (changed: Record<string, unknown>) => void;
  onCLIDefaults?: (capability: ai.CLICapabilityView) => void;
  primaryPasswordVisible: boolean;
  darkMode: boolean;
  overlayTheme: OverlayWorkbenchTheme;
  cardBg: string;
  cardBorder: string;
  inputBg: string;
  onPrimaryPasswordVisibleChange: (visible: boolean) => void;
  resolveProviderPreset: (provider: ProviderPresetCandidate) => MatchedProviderPreset;
  resolvePresetByKey: (presetKey: string) => AISettingsProviderPresetOption;
  onAddProvider: (presetKey?: string, endpointType?: ProviderEndpointType) => void;
  onEditProvider: (provider: AIProviderConfig) => void;
  onDeleteProvider: (id: string) => void;
  onSetActiveProvider: (id: string) => void;
  onCancelEdit: () => void;
  onPresetChange: (presetKey: string, endpointType?: ProviderEndpointType) => void;
  onTestProvider: () => void;
  onSaveProvider: () => void;
  onSaveProviderAsCopy?: () => void;
  saveMode?: 'save' | 'copy';
  dirty?: boolean;
  treeHostedView?: 'workspace' | 'connected';
  onOpenWorkspaceView?: () => void;
  onCloseHost?: () => void;
}

const AISettingsProvidersSection: React.FC<AISettingsProvidersSectionProps> = ({
  providers,
  activeProviderId,
  pendingProviderId,
  providersLoading = false,
  loadError,
  onReloadProviders,
  editingProvider,
  editorSessionKey = 0,
  isEditing,
  form,
  providerPresets,
  watchedPresetKey,
  watchedApiFormat,
  loading,
  testing = false,
  testStatus,
  testResult,
  onValuesChange,
  onCLIDefaults,
  primaryPasswordVisible,
  darkMode,
  overlayTheme,
  cardBorder,
  inputBg,
  onPrimaryPasswordVisibleChange,
  resolveProviderPreset,
  onAddProvider,
  onEditProvider,
  onDeleteProvider,
  onSetActiveProvider,
  onCancelEdit,
  onPresetChange,
  onTestProvider,
  onSaveProvider,
  onSaveProviderAsCopy,
  treeHostedView,
  onOpenWorkspaceView,
  saveMode = 'save',
  dirty = false,
}) => {
  const i18n = useOptionalI18n();
  const copy = (key: string, params?: Record<string, string | number>) => i18n ? i18n.t(key, params) : catalogTranslate('en-US', key, params);
  const presetKeyFromForm = watchedPresetKey || (editingProvider as (AIProviderConfig & { presetKey?: string }) | null)?.presetKey || 'openai';
  const presetFromForm = providerPresets.find((preset) => preset.key === presetKeyFromForm);
  const watchedType = Form.useWatch('type', form);
  const currentEndpointType = getProviderEndpointType({
    type: watchedType || presetFromForm?.backendType,
    apiFormat: watchedApiFormat || presetFromForm?.fixedApiFormat || presetFromForm?.defaultApiFormat,
  });
  const editorScope = `${editorSessionKey}:${editingProvider?.id || 'new'}:${presetKeyFromForm}:${isEditing}`;
  const selectedEndpointType = currentEndpointType;
  const editorReady = Boolean(isEditing && presetFromForm);
  const endpointOptions = (presetFromForm?.endpoints || []).filter((endpoint) => getProviderEndpointType({ type: endpoint.backendType }) === selectedEndpointType);
  const usesLocalCLI = presetFromForm?.authMode === 'local-cli';
  const providersByPreset = new Map<string, AIProviderConfig[]>();
  providers.forEach((provider) => {
    const key = resolveProviderPreset(provider).key;
    providersByPreset.set(key, [...(providersByPreset.get(key) || []), provider]);
  });
  const presetCLIIdentity = (preset: AISettingsProviderPresetOption) => getSingletonCLIIdentity({
    type: preset.backendType || 'custom', apiFormat: preset.fixedApiFormat, authMode: preset.authMode,
  });
  const canSelectPreset = (preset: AISettingsProviderPresetOption, editingId?: string) => {
    const identity = presetCLIIdentity(preset);
    if (!identity) return true;
    if (editingId && providers.some((provider) => provider.id === editingId && getSingletonCLIIdentity(provider) === identity)) return true;
    return !providers.some((provider) => getSingletonCLIIdentity(provider) === identity);
  };
  const duplicateCLI = Boolean(presetFromForm && !canSelectPreset(presetFromForm, editingProvider?.id));
  const singletonCLIPreset = Boolean(presetFromForm && presetCLIIdentity(presetFromForm));
  const canSaveAsCopy = Boolean(editingProvider?.id) && !singletonCLIPreset && Boolean(onSaveProviderAsCopy);
  const [modelManagementRequest, setModelManagementRequest] = React.useState({ scope: '', request: 0 });
  const editorScrollRef = React.useRef<HTMLDivElement>(null);
  React.useEffect(() => {
    if (editorScrollRef.current) editorScrollRef.current.scrollTop = 0;
  }, [editorScope]);
  const revealFirstError = React.useCallback(() => revealFirstErrorIn(editorScrollRef.current), []);
  const revealFirstErrorSoon = React.useCallback(() => {
    if (typeof window === 'undefined') return;
    window.requestAnimationFrame?.(revealFirstError);
    window.setTimeout?.(revealFirstError, 200);
  }, [revealFirstError]);
  React.useEffect(() => {
    if (duplicateCLI || testStatus === 'error') revealFirstError();
  }, [duplicateCLI, testStatus, revealFirstError]);
  const [moreOpen, setMoreOpen] = React.useState(true);
  React.useEffect(() => {
    setMoreOpen(true);
  }, [isEditing, editingProvider?.id, presetKeyFromForm]);
  const cliScope = `${editorScope}:${editorReady}:${usesLocalCLI}:${duplicateCLI}`;
  const [cliCapabilityResponse, setCLICapabilityResponse] = React.useState<{ scope: string; views: ai.CLICapabilityView[] }>({ scope: '', views: [] });
  const cliCapabilities = cliCapabilityResponse.scope === cliScope ? cliCapabilityResponse.views : [];
  const [capabilityError, setCapabilityError] = React.useState(false);
  React.useEffect(() => {
    if (!editorReady || !usesLocalCLI || duplicateCLI) {
      setCLICapabilityResponse({ scope: '', views: [] });
      setCapabilityError(false);
      return;
    }
    let cancelled = false;
    setCapabilityError(false);
    Promise.resolve().then(() => AIGetCLICapabilities())
      .then((views) => { if (!cancelled) { setCLICapabilityResponse({ scope: cliScope, views: views || [] }); setCapabilityError(!views?.length); } })
      .catch(() => { if (!cancelled) { setCLICapabilityResponse({ scope: cliScope, views: [] }); setCapabilityError(true); } });
    return () => { cancelled = true; };
  }, [cliScope, editorReady, usesLocalCLI, duplicateCLI]);
  const activeCLICapability = usesLocalCLI
    ? cliCapabilities.find((item) => item.apiFormat === String(watchedApiFormat || '').trim().toLowerCase())
    : undefined;
  React.useEffect(() => {
    if (editorReady && !duplicateCLI && activeCLICapability) onCLIDefaults?.(activeCLICapability);
  }, [editorReady, duplicateCLI, activeCLICapability, onCLIDefaults]);
  const watchedCLIPath = String(Form.useWatch('cliPath', form) || '').trim();
  const watchedCLIEnvRows = Form.useWatch('cliEnvRows', { form, preserve: true });
  const watchedCLIEnv = recordFromRows(Array.isArray(watchedCLIEnvRows) ? watchedCLIEnvRows : []);
  const cliExecutionScope = JSON.stringify({ path: watchedCLIPath, env: Object.entries(watchedCLIEnv || {}).sort(([a], [b]) => a.localeCompare(b)) });
  const hasCustomCLIExecution = watchedCLIPath !== '' || Boolean(watchedCLIEnv && Object.keys(watchedCLIEnv).length);
  const catalogScope = `${cliScope}:${watchedApiFormat || ''}:${cliExecutionScope}`;
  const [catalogRefresh, setCatalogRefresh] = React.useState(0);
  const catalogRefreshForced = React.useRef(false);
  const [catalogResponse, setCatalogResponse] = React.useState<{ scope: string; cliScope: string; catalog: CLIModelCatalog } | null>(null);
  const modelCatalog = catalogResponse?.scope === catalogScope ? catalogResponse.catalog : null;
  const [modelsLoading, setModelsLoading] = React.useState(false);
  const [modelDiscoveryError, setModelDiscoveryError] = React.useState(false);
  React.useEffect(() => {
    setModelDiscoveryError(false);
    setModelsLoading(false);
    if (!editorReady || !usesLocalCLI || duplicateCLI || !watchedApiFormat) return;
    const forced = catalogRefreshForced.current;
    catalogRefreshForced.current = false;
    const cached = forced || hasCustomCLIExecution ? null : readCachedCLIModelCatalog(watchedApiFormat);
    if (cached) { setCatalogResponse({ scope: catalogScope, cliScope, catalog: cached }); return; }
    let cancelled = false;
    setModelsLoading(true);
    Promise.resolve().then(() => AIGetCLIModelCatalog({
      id: editingProvider?.id || '', type: 'custom', name: '', apiKey: '', baseUrl: '', model: '', maxTokens: 0, temperature: 0,
      apiFormat: watchedApiFormat, authMode: 'local-cli', cliPath: watchedCLIPath, cliEnv: watchedCLIEnv,
    } as ai.ProviderConfig))
      .then((value) => {
        if (cancelled) return;
        const catalog = parseCLIModelCatalog(value);
        if (!catalog) throw new Error('Invalid model catalog');
        if (!hasCustomCLIExecution) writeCachedCLIModelCatalog(watchedApiFormat, catalog);
        setCatalogResponse({ scope: catalogScope, cliScope, catalog });
        setModelDiscoveryError(catalog.stale || (catalog.source !== 'none' && !catalog.models.length));
      })
      .catch(() => {
        if (cancelled) return;
        setModelDiscoveryError(true);
        setCatalogResponse((previous) => (previous?.scope === catalogScope ? previous : null));
      })
      .finally(() => { if (!cancelled) setModelsLoading(false); });
    return () => { cancelled = true; };
  }, [catalogScope, cliScope, catalogRefresh, editorReady, usesLocalCLI, duplicateCLI, watchedApiFormat, watchedCLIPath, hasCustomCLIExecution, editingProvider?.id]);
  const supportsAdvancedEndpoint = presetKeyFromForm === 'custom' || presetKeyFromForm === 'ollama' || presetKeyFromForm === 'codebuddy' || presetKeyFromForm === 'cursor';
  const supportsModelList = supportsAdvancedEndpoint || usesLocalCLI;
  const codeBuddyUsesOptionalSecret = presetKeyFromForm === 'codebuddy';
  const watchedModel = Form.useWatch('model', form);
  const watchedModels = Form.useWatch('models', form);
  const watchedInlineCompletionModel = Form.useWatch('inlineCompletionModel', form);
  const watchedDisabledModels = Form.useWatch('disabledModels', { form, preserve: true }) || [];
  const watchedCustomModels = Form.useWatch('customModels', { form, preserve: true }) || [];
  const modelOptions = buildProviderModelOptions(
    modelCatalog?.models,
    Array.isArray(watchedModels) ? watchedModels : [],
    [watchedModel, watchedInlineCompletionModel, activeCLICapability?.defaultModel, presetFromForm?.defaultModel],
    presetFromForm?.models,
    watchedCustomModels,
    watchedDisabledModels,
  );
  const disabledModels = new Set<string>(watchedDisabledModels);
  const enabledModelOptions = modelOptions.filter((option) => !disabledModels.has(option.value));
  const patchModels = (patch: Record<string, string[]>) => { form.setFieldsValue(patch); onValuesChange?.(patch); };
  const modelSourceKey = modelCatalog?.stale ? 'ai_settings.form.model_catalog.stale'
    : modelDiscoveryError ? 'ai_settings.form.models_manual_fallback'
      : !usesLocalCLI ? 'ai_settings.form.model_catalog.saved'
        : `ai_settings.form.model_catalog.${modelCatalog?.source || 'none'}`;
  const fieldLabel = (key: string) => <span style={{ fontWeight: 500, color: overlayTheme.titleText }}>{copy(key)}</span>;
  const endpointLabel = (endpoint: ProviderEndpointType) => copy(`ai_settings.endpoint.${endpoint}.label`);
  const saveActionLabel = copy('ai_settings.action.apply');
  const handleSaveProvider = () => { onSaveProvider(); revealFirstErrorSoon(); };
  const handleTestProvider = () => { onTestProvider(); revealFirstErrorSoon(); };
  const rootStyle = {
    '--provider-muted': overlayTheme.mutedText, '--provider-text': overlayTheme.titleText,
    '--provider-line': cardBorder, '--provider-active': overlayTheme.selectedText,
    '--provider-active-bg': overlayTheme.selectedBg, '--provider-bg': overlayTheme.shellBg,
  } as React.CSSProperties;
  const requiredModelRule = { validator: (_: unknown, value: string) => disabledModels.has(value)
    ? Promise.reject(new Error(copy('ai_settings.models.default_required'))) : Promise.resolve() };
  const hintIcon = (lines: React.ReactNode[]) => {
    const shown = lines.filter(Boolean);
    if (!shown.length) return null;
    return <Tooltip {...passThroughHintTooltip} title={<div className="gonavi-ai-provider-hint-body">
      {shown.map((line, index) => <div key={index}>{line}</div>)}
    </div>}>
      <button type="button" className="gonavi-ai-provider-hint"
        onClick={(event) => { event.preventDefault(); event.stopPropagation(); }}>
        <InfoCircleOutlined aria-hidden="true" />
        <span className="gonavi-ai-provider-hint-text">{copy('ai_settings.form.hint_label')}: {shown.map((line, index) => <span key={index}>{line} </span>)}</span>
      </button>
    </Tooltip>;
  };
  const showAuthMethod = !usesLocalCLI && (
    presetKeyFromForm === 'anthropic'
    || selectedEndpointType === 'anthropic'
    || (presetKeyFromForm === 'custom' && String(watchedApiFormat || '').toLowerCase() === 'anthropic')
  );
  const openWorkspaceThen = (action: () => void) => {
    if (treeHostedView === 'connected') onOpenWorkspaceView?.();
    action();
  };
  const addNewConfig = () => {
    const first = providerPresets.find((preset) => canSelectPreset(preset));
    openWorkspaceThen(() => onAddProvider(first?.key));
  };
  const listItems = providers.map((provider) => {
    const matched = resolveProviderPreset(provider);
    return {
      provider,
      presetKey: matched.key,
      presetLabel: matched.label,
      name: provider.name || matched.label,
      isDefault: provider.id === activeProviderId,
      isPending: provider.id === pendingProviderId,
    };
  });

  if (!isEditing) {
    return <div className="gonavi-ai-provider-management" style={rootStyle}>
      <AIProviderConfigList
        title={copy('ai_settings.provider.config_list')}
        addLabel={copy('ai_settings.provider.add_config')}
        emptyLabel={copy('ai_settings.provider.empty_configs')}
        defaultLabel={copy('ai_settings.provider.default')}
        setDefaultLabel={copy('ai_settings.provider.set_default')}
        editLabel={copy('common.edit')}
        deleteLabel={copy('ai_settings.provider.action.delete')}
        confirmDelete={copy('ai_settings.provider.confirm_delete')}
        cancelLabel={copy('common.cancel')}
        items={listItems}
        loadError={loadError}
        loading={providersLoading}
        loadingLabel={copy('ai_settings.provider.loading')}
        retryLabel={copy('ai_settings.provider.retry')}
        disabled={providersLoading || Boolean(loadError) || loading}
        dark={darkMode}
        onAdd={addNewConfig}
        onReload={onReloadProviders}
        onEdit={(provider) => openWorkspaceThen(() => onEditProvider(provider))}
        onDelete={onDeleteProvider}
        onSetDefault={onSetActiveProvider}
      />
    </div>;
  }

  return <div className="gonavi-ai-provider-management" style={rootStyle}>
    <div className="gonavi-ai-provider-editor is-list-edit">
      {!editorReady ? <div className="gonavi-ai-provider-editor-empty">{copy('ai_settings.provider.choose_configuration')}</div>
        : <Form form={form} layout="horizontal" labelAlign="right" size="small" colon={false}
          labelCol={{ flex: '0 0 12em' }} wrapperCol={{ flex: '1 1 0%' }}
          onValuesChange={onValuesChange} className="gonavi-ai-provider-form">
          <div className="gonavi-ai-provider-editor-heading is-edit-nav">
            <Button type="text" size="small" icon={<LeftOutlined />} onClick={onCancelEdit}>{copy('ai_settings.action.back_list')}</Button>
            <h3>{copy(editingProvider?.id ? 'ai_settings.provider.edit_config' : 'ai_settings.provider.add_config')}</h3>
            {editingProvider?.id && <Popconfirm title={copy('ai_settings.provider.confirm_delete')} onConfirm={() => onDeleteProvider(editingProvider.id)}
              disabled={Boolean(pendingProviderId) || loading} okButtonProps={{ danger: true }} okText={copy('ai_settings.provider.action.delete')} cancelText={copy('common.cancel')}>
              <Button type="text" size="small" icon={<DeleteOutlined />} aria-label={`${copy('ai_settings.provider.action.delete')}: ${editingProvider.name}`} danger disabled={Boolean(pendingProviderId) || loading} />
            </Popconfirm>}
          </div>
          <div ref={editorScrollRef} className="gonavi-ai-provider-editor-scroll">
            <Form.Item name="presetKey" hidden><Input /></Form.Item><Form.Item name="type" hidden><Input /></Form.Item>
            {!showAuthMethod && <Form.Item name="authMode" hidden><Input /></Form.Item>}
            <Form.Item name="apiFormat" hidden><Input /></Form.Item>
            {!usesLocalCLI && <Form.Item name="effort" hidden><Input /></Form.Item>}
            {duplicateCLI && <div role="alert">{copy('ai_settings.provider.duplicate_cli')}</div>}
            <Form.Item label={fieldLabel('ai_settings.form.config_name')} name="name">
              <Input placeholder={copy('ai_settings.form.provider_name_placeholder')} size="middle" />
            </Form.Item>
            <Form.Item label={fieldLabel('ai_settings.form.provider')}>
              <AIProviderPresetSelect
                value={presetKeyFromForm}
                presets={providerPresets}
                canSelect={(preset) => {
                  const full = providerPresets.find((item) => item.key === preset.key);
                  return full ? canSelectPreset(full, editingProvider?.id) : false;
                }}
                disabled={loading}
                dark={darkMode}
                builtinLabel={copy('ai_settings.provider.builtin')}
                partnerLabel={copy('ai_settings.provider.partners')}
                partnerEmpty={copy('ai_settings.provider.partners_empty')}
                ariaLabel={copy('ai_settings.form.provider')}
                onChange={(key) => onPresetChange(key)}
              />
            </Form.Item>
            {usesLocalCLI && <Form.Item label={fieldLabel('ai_settings.form.cli_path')} name="cliPath"
              extra={copy('ai_settings.form.cli_path_hint')}>
              <Input size="middle" placeholder={activeCLICapability?.command || ''} />
            </Form.Item>}
            {usesLocalCLI && <Form.Item label={fieldLabel('ai_settings.form.cli_env')} name="cliEnvRows">
              <AIProviderKeyValueRows
                namePlaceholder={copy('ai_settings.form.custom_headers_name')}
                valuePlaceholder={copy('ai_settings.form.custom_headers_value')}
                addLabel={copy('ai_settings.form.cli_env_add')}
                removeLabel={copy('common.cancel')}
              />
            </Form.Item>}
            {showAuthMethod && <Form.Item label={fieldLabel('ai_settings.form.auth_method')} name="authMode">
              <Select size="middle" popupMatchSelectWidth={false} classNames={{ popup: { root: 'gonavi-ai-provider-form-popup' } }} options={[
                { value: 'api-key', label: copy('ai_settings.form.auth_api_key') },
                { value: 'bearer', label: copy('ai_settings.form.auth_bearer') },
              ]} />
            </Form.Item>}
            {!usesLocalCLI && <div className="gonavi-ai-provider-connection-row">
              <Form.Item label={fieldLabel(codeBuddyUsesOptionalSecret ? 'ai_settings.form.api_key.codebuddy_optional' : 'ai_settings.form.api_key')} name="apiKey"
                rules={[{ validator: (_, value) => isProviderSecretRequirementSatisfied({ apiKeyInput: value, currentAuthMode: 'api-key', editingProvider,
                  allowEmptySecret: codeBuddyUsesOptionalSecret }) ? Promise.resolve() : Promise.reject(new Error(copy('ai_settings.form.api_key_required'))) }]}>
                <Input.Password size="middle" placeholder={copy(codeBuddyUsesOptionalSecret ? 'ai_settings.form.api_key_placeholder.codebuddy' : 'ai_settings.form.api_key_placeholder')}
                  visibilityToggle={{ visible: primaryPasswordVisible, onVisibleChange: onPrimaryPasswordVisibleChange }} style={{ background: inputBg }} />
              </Form.Item>
              <Form.Item className="gonavi-ai-provider-field-url" label={fieldLabel('ai_settings.form.api_endpoint')} name="baseUrl"
                rules={codeBuddyUsesOptionalSecret ? [] : [{ required: true, message: copy('ai_settings.form.api_endpoint_required') }]}>
                {endpointOptions.length > 0 ? <Select showSearch optionFilterProp="label" size="middle" popupMatchSelectWidth={false}
                  classNames={{ popup: { root: 'gonavi-ai-provider-form-popup' } }}
                  options={endpointOptions.map((endpoint) => ({ label: endpoint.baseUrl, value: endpoint.baseUrl }))}
                  onChange={(baseUrl) => { const endpoint = endpointOptions.find((item) => item.baseUrl === baseUrl); if (endpoint) form.setFieldValue('type', endpoint.backendType); }} />
                  : <Input size="middle" readOnly={!supportsAdvancedEndpoint} placeholder={codeBuddyUsesOptionalSecret ? copy('ai_settings.form.api_endpoint_placeholder.codebuddy') : presetFromForm?.defaultBaseUrl || 'https://...'} />}
              </Form.Item>
            </div>}
            {!usesLocalCLI && <Form.Item label={fieldLabel('ai_settings.form.custom_headers')} name="headerRows"
              extra={copy('ai_settings.form.custom_headers_hint')}>
              <AIProviderKeyValueRows
                namePlaceholder={copy('ai_settings.form.custom_headers_name')}
                valuePlaceholder={copy('ai_settings.form.custom_headers_value')}
                addLabel={copy('ai_settings.form.custom_headers_add')}
                removeLabel={copy('common.cancel')}
              />
            </Form.Item>}
            {!usesLocalCLI && <Form.Item className="gonavi-ai-provider-field-format" label={fieldLabel('ai_settings.form.api_format')}>
              {getProviderEndpointTypes(presetFromForm!).length <= 1
                ? <Input className="gonavi-ai-provider-fixed-value" size="middle" readOnly tabIndex={-1} aria-label={copy('ai_settings.form.api_format')}
                  value={endpointLabel(getProviderEndpointTypes(presetFromForm!)[0] || selectedEndpointType)} />
                : <Select className="gonavi-ai-provider-endpoint-select" aria-label={copy('ai_settings.form.api_format')} size="middle"
                  popupMatchSelectWidth={false} classNames={{ popup: { root: 'gonavi-ai-provider-form-popup' } }}
                  value={selectedEndpointType} disabled={loading} options={getProviderEndpointTypes(presetFromForm!).map((endpoint) => ({ value: endpoint, label: endpointLabel(endpoint) }))}
                  onChange={(endpoint) => onPresetChange(presetKeyFromForm, endpoint)} />}
            </Form.Item>}
            <Form.Item name="model" rules={[requiredModelRule]} label={<span className="gonavi-ai-provider-model-label">
              <span className="gonavi-ai-provider-model-title">
                {hintIcon([
                  (usesLocalCLI || codeBuddyUsesOptionalSecret) && copy('ai_settings.form.default_model_placeholder.local_cli'),
                  copy(modelSourceKey),
                  copy('ai_settings.models.scope'),
                ].filter(Boolean))}
                {fieldLabel('ai_settings.form.default_model')}
              </span>
              <span className="gonavi-ai-provider-model-meta">
                <button type="button" aria-haspopup="dialog" onClick={(event) => {
                  event.preventDefault(); event.stopPropagation();
                  if (usesLocalCLI) { catalogRefreshForced.current = true; setCatalogRefresh((value) => value + 1); }
                  setModelManagementRequest((previous) => ({ scope: editorScope, request: previous.request + 1 }));
                }}
                  aria-label={copy('ai_settings.models.manage')}>{copy('ai_settings.models.enabled_count', { enabled: enabledModelOptions.length, total: modelOptions.length })}</button>
                {usesLocalCLI && hintIcon([
                  copy(presetKeyFromForm === 'codex' ? 'ai_settings.form.local_cli.codex_hint' : presetKeyFromForm === 'grok' ? 'ai_settings.form.local_cli.grok_hint' : presetKeyFromForm === 'cursor-cli' ? 'ai_settings.form.local_cli.cursor_hint' : 'ai_settings.form.local_cli.claude_hint'),
                  activeCLICapability?.command && <>{copy('ai_settings.form.local_cli.command')}: <code>{activeCLICapability.command}</code></>,
                  activeCLICapability?.supportsEffort && !activeCLICapability.effortValuesVerified && copy('ai_settings.form.effort_hint_unverified'),
                  capabilityError && copy('ai_settings.form.cli_capability_unavailable'),
                  copy('ai_settings.models.refresh_hint'),
                ])}
              </span></span>}>
              <AIProviderModelSelect key={`${editorScope}:default`} label={copy('ai_settings.form.default_model')}
                placeholder={copy(usesLocalCLI || codeBuddyUsesOptionalSecret ? 'ai_settings.form.default_model_placeholder.local_cli' : 'ai_settings.form.default_model_placeholder')}
                customLabel={copy('ai_settings.form.model_use_custom')} options={modelOptions} loading={modelsLoading}
                managementRequest={modelManagementRequest.scope === editorScope ? modelManagementRequest.request : 0}
                management={{ disabledModels: watchedDisabledModels, defaultModel: watchedModel || '', completionModel: watchedInlineCompletionModel || '',
                  allowDefaultFallback: Boolean(usesLocalCLI || codeBuddyUsesOptionalSecret), source: copy(modelSourceKey), copy,
                  onToggle: (model, enabled) => patchModels({ disabledModels: enabled ? watchedDisabledModels.filter((item: string) => item !== model) : [...new Set([...watchedDisabledModels, model])] }),
                  onAdd: (model) => patchModels({ customModels: [...new Set([...watchedCustomModels, model])] }),
                }} />
            </Form.Item>
            {!usesLocalCLI && <div className="gonavi-ai-provider-token-row">
              <Form.Item label={fieldLabel('ai_settings.form.max_output_tokens')} name="maxTokens">
                <Input type="number" size="middle" min={0} />
              </Form.Item>
              <Form.Item label={fieldLabel('ai_settings.form.context_window')} name="contextWindow">
                <Input type="number" size="middle" min={0} placeholder={copy('ai_settings.form.context_window_placeholder')} />
              </Form.Item>
            </div>}
            {usesLocalCLI && <Form.Item label={fieldLabel('ai_settings.form.effort')} name="effort">
              {activeCLICapability?.supportsEffort ? <Select allowClear size="middle" placeholder={copy('ai_settings.form.effort_placeholder_empty')}
                popupMatchSelectWidth={false} classNames={{ popup: { root: 'gonavi-ai-provider-form-popup' } }}
                options={(activeCLICapability.effortValues || []).map((value) => ({ label: value, value }))} />
                : <Input size="middle" disabled placeholder={copy(activeCLICapability?.supportsEffort === false ? 'ai_settings.form.effort_unsupported' : 'ai_settings.form.effort_placeholder_empty')} />}
            </Form.Item>}
            <details className="gonavi-ai-provider-more" open={moreOpen}>
              <ProviderDisclosureSummary
                open={moreOpen}
                onToggle={() => setMoreOpen((open) => !open)}
                label={copy('ai_settings.form.more_settings')}
                hint={hintIcon([copy('ai_settings.form.inline_completion_model_hint')])}
              />
              <div className="gonavi-ai-provider-field-grid">
                {supportsModelList && <Form.Item label={fieldLabel('ai_settings.form.favorite_models')} name="models"><Select mode="tags" size="middle"
                  popupMatchSelectWidth={false} classNames={{ popup: { root: 'gonavi-ai-provider-form-popup' } }}
                  maxTagCount="responsive" tokenSeparators={[',']} placeholder={copy('ai_settings.form.model_list_placeholder.local_cli')}
                  options={enabledModelOptions} /></Form.Item>}
                <Form.Item label={fieldLabel('ai_settings.form.inline_completion_model')} name="inlineCompletionModel" rules={[requiredModelRule]}>
                  <AIProviderModelSelect label={copy('ai_settings.form.inline_completion_model')} placeholder={copy('ai_settings.form.inline_completion_model_placeholder')}
                    customLabel={copy('ai_settings.form.model_use_custom')} options={modelOptions} disabledModels={watchedDisabledModels} />
                </Form.Item>
              </div>
            </details>
          </div>
          <div className="gonavi-ai-provider-actions">
            <div className="gonavi-ai-provider-check-action"><Button size="middle" onClick={handleTestProvider} loading={testing} disabled={duplicateCLI}>{copy('ai_settings.action.test')}</Button>
              <small>{copy(dirty ? 'ai_settings.provider.unsaved' : 'ai_settings.provider.saved')}</small></div>
            <div className="gonavi-ai-provider-test-result" role={testStatus === 'error' ? 'alert' : 'status'} data-error={testStatus === 'error'}>
              {testResult && (testResult.success ? <><CheckOutlined /> {copy(`ai_settings.test.${testResult.checkKind}`)}</> : testResult.message)}
            </div>
            <div className="gonavi-ai-provider-save-actions">
              <Button size="middle" onClick={onCancelEdit}>{copy('common.cancel')}</Button>
              {canSaveAsCopy
                ? <Dropdown.Button size="middle" type="primary" onClick={handleSaveProvider} icon={<DownOutlined />}
                  loading={loading && saveMode === 'save'} disabled={duplicateCLI || loading && saveMode === 'copy'}
                  placement="topRight" trigger={['click']} arrow overlayClassName="gonavi-ai-provider-save-as-menu"
                  getPopupContainer={() => document.body}
                  menu={{ items: [{ key: 'save-as', label: <span className="gonavi-ai-provider-save-as-item">
                    <strong>{copy('ai_settings.provider.save_as')}</strong><small>{copy('ai_settings.provider.copy_hint')}</small>
                  </span> }], onClick: () => onSaveProviderAsCopy?.() }}>
                  {saveActionLabel}</Dropdown.Button>
                : <Button size="middle" type="primary" onClick={handleSaveProvider}
                  loading={loading && saveMode === 'save'} disabled={duplicateCLI || loading && saveMode === 'copy'}>{saveActionLabel}</Button>}
            </div>
          </div>
        </Form>}
    </div>
  </div>;
};

export default AISettingsProvidersSection;
