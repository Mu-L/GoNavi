import React from 'react';
import { act, create, type ReactTestRenderer } from 'react-test-renderer';
import { afterEach, describe, expect, it, vi } from 'vitest';

vi.mock('@ant-design/icons', () => ({
  CheckOutlined: () => <i data-icon="check" />,
}));

import { t as catalogTranslate } from '../../i18n/catalog';
import AISettingsProviderTestResult, {
  copyProviderTestError,
  PROVIDER_TEST_ERROR_DETAILS_ID,
} from './AISettingsProviderTestResult';

const copy = (key: string) => catalogTranslate('en-US', key);
const trailingReason = 'TLS handshake failed: certificate has expired';
const longError = `${'upstream rejected the request at https://api.example.invalid/v1/chat/completions. '.repeat(4)}${trailingReason}`;
const renderedText = (node: any): string => typeof node === 'string' ? node
  : Array.isArray(node) ? node.map(renderedText).join(' ') : renderedText(node?.children || []);

describe('copyProviderTestError', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('copies the full redacted error when the clipboard API is available', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    await expect(copyProviderTestError(longError)).resolves.toBe(true);
    expect(writeText).toHaveBeenCalledWith(longError);
  });

  it('reports failure when the clipboard API is missing', async () => {
    vi.stubGlobal('navigator', {});
    await expect(copyProviderTestError(longError)).resolves.toBe(false);
  });

  it('reports failure when navigator itself is unavailable', async () => {
    vi.stubGlobal('navigator', undefined);
    await expect(copyProviderTestError(longError)).resolves.toBe(false);
  });

  it('reports failure when writing to the clipboard rejects', async () => {
    vi.stubGlobal('navigator', { clipboard: { writeText: vi.fn().mockRejectedValue(new Error('denied')) } });
    await expect(copyProviderTestError(longError)).resolves.toBe(false);
  });
});

describe('AISettingsProviderTestResult', () => {
  let renderer: ReactTestRenderer | undefined;
  afterEach(async () => {
    await act(async () => { renderer?.unmount(); });
    renderer = undefined;
    vi.unstubAllGlobals();
  });

  const render = async (props: Partial<React.ComponentProps<typeof AISettingsProviderTestResult>> = {}) => {
    const element = (
      <AISettingsProviderTestResult
        testStatus="idle"
        testResult={null}
        copy={copy}
        testAction={<button type="button">Test connection</button>}
        saveAction={<button type="button">Save changes</button>}
        {...props}
      />
    );
    await act(async () => {
      if (renderer) renderer.update(element);
      else renderer = create(element);
    });
  };

  it('keeps the action row reachable without a details entry while idle', async () => {
    await render();
    const text = renderedText(renderer!.toJSON());
    expect(text).toContain('Test connection');
    expect(text).toContain('Save changes');
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-result-toggle' })).toHaveLength(0);
    expect(renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result' }).props.role).toBe('status');
  });

  it('renders a successful check beside Test and Save without an inspect action', async () => {
    await render({
      testStatus: 'success',
      testResult: { success: true, checkKind: 'local-auth', modelVerified: false, message: 'fixture' },
    });
    const text = renderedText(renderer!.toJSON());
    expect(text).toContain('Local sign-in checked · model response not verified');
    expect(text).toContain('Test connection');
    expect(text).toContain('Save changes');
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-result-toggle' })).toHaveLength(0);
  });

  it('offers a keyboard-reachable full-error view and copy for a long trailing failure', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal('navigator', { clipboard: { writeText } });
    await render({
      testStatus: 'error',
      testResult: { success: false, checkKind: 'none', modelVerified: false, message: longError },
    });
    const alert = renderer!.root.findByProps({ role: 'alert' });
    expect(alert.props['data-error']).toBe(true);
    expect(renderedText(alert)).toContain(trailingReason);
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-error-details' })).toHaveLength(0);

    const toggle = renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result-toggle' });
    expect(toggle.props.type).toBe('button');
    expect(toggle.props['aria-expanded']).toBe(false);
    expect(toggle.props['aria-controls']).toBe(PROVIDER_TEST_ERROR_DETAILS_ID);
    expect(renderedText(toggle)).toBe('View full error');
    await act(async () => toggle.props.onClick());

    const details = renderer!.root.findByProps({ id: PROVIDER_TEST_ERROR_DETAILS_ID });
    expect(details.props.className).toBe('gonavi-ai-provider-test-error-details');
    expect(renderedText(details)).toContain(trailingReason);
    expect(toggle.props['aria-expanded']).toBe(true);
    expect(renderedText(toggle)).toBe('Hide full error');

    const copyButton = renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-error-copy' });
    expect(copyButton.props.type).toBe('button');
    await act(async () => copyButton.props.onClick());
    expect(writeText).toHaveBeenCalledWith(longError);
    expect(renderedText(copyButton)).toBe('Copied');
    expect(renderedText(renderer!.toJSON())).toContain('Test connection');
    expect(renderedText(renderer!.toJSON())).toContain('Save changes');

    await act(async () => toggle.props.onClick());
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-error-details' })).toHaveLength(0);
  });

  it('surfaces clipboard failure without leaving the alert role', async () => {
    vi.stubGlobal('navigator', { clipboard: { writeText: vi.fn().mockRejectedValue(new Error('denied')) } });
    await render({
      testStatus: 'error',
      testResult: { success: false, checkKind: 'none', modelVerified: false, message: longError },
    });
    await act(async () => renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result-toggle' }).props.onClick());
    await act(async () => renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-error-copy' }).props.onClick());
    const failed = renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-error-copy-failed' });
    expect(failed.props.role).toBe('status');
    expect(renderedText(failed)).toContain('Copying to the clipboard is not supported in this environment');
    expect(renderer!.root.findByProps({ role: 'alert' }).props.role).toBe('alert');
  });

  it('closes details when a later test result replaces the previous error', async () => {
    await render({
      testStatus: 'error',
      testResult: { success: false, checkKind: 'none', modelVerified: false, message: longError },
    });
    await act(async () => renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result-toggle' }).props.onClick());
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-error-details' })).toHaveLength(1);
    await render({
      testStatus: 'error',
      testResult: { success: false, checkKind: 'none', modelVerified: false, message: `next failure ${trailingReason}` },
    });
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-error-details' })).toHaveLength(0);
    expect(renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result-toggle' }).props['aria-expanded']).toBe(false);
  });

  it('does not offer inspect actions for an empty error payload', async () => {
    await render({
      testStatus: 'error',
      testResult: { success: false, checkKind: 'none', modelVerified: false, message: '' },
    });
    expect(renderer!.root.findByProps({ role: 'alert' }).props['data-error']).toBe(true);
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-result-toggle' })).toHaveLength(0);
  });

  it('keeps the alert role when an error arrives without a result payload', async () => {
    await render({ testStatus: 'error', testResult: null });
    expect(renderer!.root.findByProps({ className: 'gonavi-ai-provider-test-result' }).props.role).toBe('alert');
    expect(renderer!.root.findAllByProps({ className: 'gonavi-ai-provider-test-result-toggle' })).toHaveLength(0);
  });
});
