import { describe, expect, it } from 'vitest';

import { supportsTableClearAction, supportsTableTruncateAction } from './tableDataDangerActions';

describe('tableDataDangerActions', () => {
  it('supports native truncate for known relational dialects', () => {
    expect(supportsTableTruncateAction('mysql')).toBe(true);
    expect(supportsTableTruncateAction('goldendb')).toBe(true);
    expect(supportsTableTruncateAction('oceanbase')).toBe(true);
    expect(supportsTableTruncateAction('postgres')).toBe(true);
    expect(supportsTableTruncateAction('opengauss')).toBe(true);
    expect(supportsTableTruncateAction('gaussdb')).toBe(true);
    expect(supportsTableTruncateAction('iris')).toBe(true);
    expect(supportsTableTruncateAction('cache')).toBe(true);
    expect(supportsTableTruncateAction('intersystems-cache')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'postgresql')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'greatdb')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'gauss_db')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'kingbase8')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'intersystemsiris')).toBe(true);
    expect(supportsTableTruncateAction('custom', 'cachedb')).toBe(true);
  });

  it('rejects truncate for unsupported or document-style backends', () => {
    expect(supportsTableTruncateAction('sqlite')).toBe(false);
    expect(supportsTableTruncateAction('mongodb')).toBe(false);
    expect(supportsTableTruncateAction('custom', 'sqlite3')).toBe(false);
  });

  it('supports delete-all only for SQL-style and MongoDB table backends', () => {
    expect(supportsTableClearAction('mysql')).toBe(true);
    expect(supportsTableClearAction('sqlite')).toBe(true);
    expect(supportsTableClearAction('mongodb')).toBe(true);
    expect(supportsTableClearAction('tdengine')).toBe(true);
    expect(supportsTableClearAction('custom', 'postgresql')).toBe(true);
    expect(supportsTableClearAction('custom', 'sqlite3')).toBe(true);

    expect(supportsTableClearAction('elasticsearch')).toBe(false);
    expect(supportsTableClearAction('redis')).toBe(false);
    expect(supportsTableClearAction('qdrant')).toBe(false);
    expect(supportsTableClearAction('iotdb')).toBe(false);
    expect(supportsTableClearAction('custom', 'unknown-driver')).toBe(false);
  });
});
