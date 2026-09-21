import { describe, expect, it } from 'vitest';

import {
  excelGroupPlanMoveCounts,
  planExcelGroupAssignments,
} from './connectionExcelGroups';

describe('planExcelGroupAssignments', () => {
  it('creates nested groups and assigns connections to the leaf', () => {
    const existing = new Map<string, string>([['\u0000生产', 'tag-prod']]);
    const plan = planExcelGroupAssignments(
      [
        { connectionName: 'db-a', groupPath: '生产/财务' },
        { connectionName: 'db-b', groupPath: '生产/财务' },
        { connectionName: 'db-c', groupPath: '测试' },
        { connectionName: 'skip-me', groupPath: '' },
      ],
      {
        resolveTagId: (name, parentTagId) => existing.get(`${parentTagId || ''}\u0000${name}`),
        nextTagId: (() => {
          let index = 0;
          return () => {
            index += 1;
            return `new-${index}`;
          };
        })(),
      },
    );

    expect(plan.tagsToCreate).toEqual([
      { id: 'new-1', name: '财务', parentTagId: 'tag-prod' },
      { id: 'new-2', name: '测试', parentTagId: undefined },
    ]);
    expect(plan.movesByLeafTagId).toEqual({
      'new-1': ['db-a', 'db-b'],
      'new-2': ['db-c'],
    });
    expect(excelGroupPlanMoveCounts(plan)).toBe(3);
  });
});
