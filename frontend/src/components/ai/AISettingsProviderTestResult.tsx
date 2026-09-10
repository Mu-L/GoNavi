import React from 'react';
import { CheckOutlined } from '@ant-design/icons';

import type { ProviderCheckResult } from '../../utils/aiProviderManagement';

export const PROVIDER_TEST_ERROR_DETAILS_ID = 'gonavi-ai-provider-test-error-details';

export const copyProviderTestError = async (text: string): Promise<boolean> => {
  const writeText = typeof navigator === 'undefined' ? undefined : navigator.clipboard?.writeText;
  if (typeof writeText !== 'function') return false;
  try {
    await writeText(text);
    return true;
  } catch {
    return false;
  }
};

interface AISettingsProviderTestResultProps {
  testStatus: 'idle' | 'success' | 'error';
  testResult?: ProviderCheckResult | null;
  copy: (key: string, params?: Record<string, string | number>) => string;
  testAction: React.ReactNode;
  saveAction: React.ReactNode;
}

const AISettingsProviderTestResult: React.FC<AISettingsProviderTestResultProps> = ({
  testStatus,
  testResult,
  copy,
  testAction,
  saveAction,
}) => {
  const errorMessage = testStatus === 'error' && testResult && !testResult.success
    ? String(testResult.message || '')
    : '';
  const canInspectError = errorMessage.length > 0;
  const [detailsOpen, setDetailsOpen] = React.useState(false);
  const [copyState, setCopyState] = React.useState<'idle' | 'copied' | 'failed'>('idle');

  React.useEffect(() => {
    setDetailsOpen(false);
    setCopyState('idle');
  }, [errorMessage, testStatus]);

  const handleCopy = async () => {
    setCopyState((await copyProviderTestError(errorMessage)) ? 'copied' : 'failed');
  };

  const summary = testResult
    ? (testResult.success
      ? <><CheckOutlined /> {copy(`ai_settings.test.${testResult.checkKind}`)}</>
      : testResult.message)
    : null;

  return (
    <div className="gonavi-ai-provider-footer">
      <div className="gonavi-ai-provider-actions">
        {testAction}
        <div
          className="gonavi-ai-provider-test-result"
          role={testStatus === 'error' ? 'alert' : 'status'}
          data-error={testStatus === 'error'}
        >
          {summary && <span className="gonavi-ai-provider-test-result-summary">{summary}</span>}
          {canInspectError && (
            <button
              type="button"
              className="gonavi-ai-provider-test-result-toggle"
              aria-expanded={detailsOpen}
              aria-controls={PROVIDER_TEST_ERROR_DETAILS_ID}
              onClick={() => setDetailsOpen((open) => !open)}
            >
              {copy(detailsOpen ? 'ai_settings.test.error.hide' : 'ai_settings.test.error.details')}
            </button>
          )}
        </div>
        {saveAction}
      </div>
      {canInspectError && detailsOpen && (
        <div id={PROVIDER_TEST_ERROR_DETAILS_ID} className="gonavi-ai-provider-test-error-details">
          <pre className="gonavi-ai-provider-test-error-body">{errorMessage}</pre>
          <div className="gonavi-ai-provider-test-error-toolbar">
            <button
              type="button"
              className="gonavi-ai-provider-test-error-copy"
              onClick={() => { void handleCopy(); }}
            >
              {copy(copyState === 'copied' ? 'ai_settings.test.error.copied' : 'ai_settings.test.error.copy')}
            </button>
            {copyState === 'failed' && (
              <span className="gonavi-ai-provider-test-error-copy-failed" role="status">
                {copy('ai_settings.clipboard.error.unsupported')}
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default AISettingsProviderTestResult;
