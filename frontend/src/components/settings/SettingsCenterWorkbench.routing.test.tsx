import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { buildSettingsCenterWorkbenchTab, SETTINGS_CENTER_WORKBENCH_TAB_ID } from '../../utils/settingsCenterTab';

const workbenchTabContentSource = readFileSync(
  fileURLToPath(new globalThis.URL('../WorkbenchTabContent.tsx', import.meta.url)),
  'utf8',
);

describe('settings-center workbench routing', () => {
  it('keeps a stable singleton tab id/type', () => {
    const tab = buildSettingsCenterWorkbenchTab();
    expect(tab.id).toBe(SETTINGS_CENTER_WORKBENCH_TAB_ID);
    expect(tab.type).toBe('settings-center');
  });

  it('routes settings-center tabs to SettingsCenterWorkbench in WorkbenchTabContent', () => {
    expect(workbenchTabContentSource).toContain("tab.type === 'settings-center'");
    expect(workbenchTabContentSource).toContain("<SettingsCenterWorkbench tab={tab} isActive={isActive} />");
    expect(workbenchTabContentSource).toContain("import('./settings/SettingsCenterWorkbench')");
  });
});
