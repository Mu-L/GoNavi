import React from 'react';
import { Select } from 'antd';
import { DownOutlined } from '@ant-design/icons';

import { t as catalogTranslate } from '../../i18n/catalog';
import { useOptionalI18n } from '../../i18n/provider';
import type { AIProviderConfig } from '../../types';
import {
  resolveThinkingIntensityOptions,
  resolveThinkingIntensityProfile,
} from '../../utils/aiThinkingIntensity';

interface AIChatThinkingIntensitySelectProps {
  activeProvider?: AIProviderConfig | null;
  value: string;
  onChange: (value: string) => void;
}

const AIChatThinkingIntensitySelect: React.FC<AIChatThinkingIntensitySelectProps> = ({
  activeProvider,
  value,
  onChange,
}) => {
  const i18n = useOptionalI18n();
  const t = i18n?.t ?? ((key: string, params?: Record<string, string | number | boolean | null | undefined>) =>
    catalogTranslate('en-US', key, params));

  if (!activeProvider) {
    return null;
  }

  const profile = resolveThinkingIntensityProfile({
    type: activeProvider.type,
    apiFormat: activeProvider.apiFormat,
    baseUrl: activeProvider.baseUrl,
    model: activeProvider.model,
  });
  const options = resolveThinkingIntensityOptions(profile).map((item) => ({
    value: item.value,
    label: t(item.labelKey),
  }));

  return (
    <Select
      size="small"
      value={value || undefined}
      onChange={onChange}
      options={options}
      styles={{ popup: { root: { minWidth: 160 } } }}
      placeholder={t('ai_chat.input.thinking_intensity.placeholder')}
      className="gn-v2-ai-thinking-select"
      suffixIcon={<DownOutlined />}
    />
  );
};

export default AIChatThinkingIntensitySelect;
