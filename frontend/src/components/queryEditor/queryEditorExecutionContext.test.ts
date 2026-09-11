import { describe, expect, it } from 'vitest';
import { resolveQueryEditorExecutionContext as resolve } from './QueryEditorHelpers';

describe('SQL execution context', () => {
    it.each([
        ['sqlserver', 'SELECT * FROM ZODO_Hmi.dbo.HmiFirstInspectOperate', { dbName: 'ZODO_Hmi' }],
        ['sqlserver', 'SELECT * FROM [ZODO_Hmi].[dbo].[Table]', { dbName: 'ZODO_Hmi' }],
        ['sqlserver', 'SELECT * FROM dbo.HmiModelChangePlan', {}],
        ['mysql', 'SELECT * FROM analytics.orders', { dbName: 'analytics' }],
        ['postgres', 'SELECT * FROM sales.orders', { schemaName: 'sales' }],
        ['postgres', 'SELECT * FROM warehouse.sales.orders', { dbName: 'warehouse', schemaName: 'sales' }],
        ['oracle', 'SELECT * FROM REPORT.orders', { dbName: 'REPORT' }],
        ['dameng', 'SELECT * FROM REPORT.orders', { dbName: 'REPORT' }],
        ['sqlserver', 'USE [ZODO_Hmi]', { dbName: 'ZODO_Hmi' }],
        ['mysql', "SELECT 'FROM fake.orders' FROM users -- JOIN bogus.items", {}],
        ['postgres', 'SELECT sales.id FROM users sales /* FROM bogus.orders */', {}],
        ['sqlite', 'SELECT * FROM main.users', {}],
    ])('%s resolves %s', (dialect, sql, expected) => {
        expect(resolve(sql, dialect, 'main', 'public')).toEqual(expected);
    });

    it('keeps an explicit schema when switching databases even if it matches the previous schema', () => {
        expect(resolve('SELECT * FROM warehouse.public.orders', 'postgres', 'main', 'public'))
            .toEqual({ dbName: 'warehouse', schemaName: 'public' });
    });
});
