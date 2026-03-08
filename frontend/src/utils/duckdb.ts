export type DuckDBMode = 'database' | 'parquet';

export const looksLikeDuckDBParquetPath = (raw: string): boolean => {
  const text = String(raw || '').trim().toLowerCase();
  return text.endsWith('.parquet') || text.endsWith('.parq');
};

export const normalizeDuckDBMode = (raw: unknown): DuckDBMode => {
  return String(raw || '').trim().toLowerCase() === 'parquet' ? 'parquet' : 'database';
};

export const resolveDuckDBMode = (raw: unknown, path: string): DuckDBMode => {
  const text = String(raw || '').trim().toLowerCase();
  if (text === 'parquet' || text === 'database') {
    return text;
  }
  return looksLikeDuckDBParquetPath(path) ? 'parquet' : 'database';
};