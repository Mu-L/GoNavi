// Excel 批量导入的分组落位规划：根据 Excel 行声明的 "父分组/子分组" 路径，
// 计算需要新建的分组以及每个连接应挂入的叶子分组。纯函数便于单测，
// 实际的建组/挂接由 App 层通过 zustand store 执行。

export type ExcelGroupAssignment = {
  connectionName: string;
  groupPath: string;
};

export type ExcelGroupPlanTag = {
  id: string;
  name: string;
  parentTagId?: string;
};

export type ExcelGroupPlan = {
  tagsToCreate: ExcelGroupPlanTag[];
  /** leaf tag id → 连接名列表（执行时由 App 层按导入视图解析成连接 ID） */
  movesByLeafTagId: Record<string, string[]>;
  /** Excel 中声明过分组的连接名 */
  assignedConnectionNames: string[];
};

export type ExcelGroupPlanContext = {
  /** 在现有分组树中按 (name, parentTagId) 查找分组 ID；找不到返回 undefined */
  resolveTagId: (name: string, parentTagId: string | undefined) => string | undefined;
  /** 生成新分组 ID */
  nextTagId: () => string;
};

const isSameTagName = (left: string, right: string): boolean => (
  String(left || '').trim().localeCompare(String(right || '').trim(), undefined, { sensitivity: 'accent' }) === 0
);

export const planExcelGroupAssignments = (
  assignments: ExcelGroupAssignment[],
  context: ExcelGroupPlanContext,
): ExcelGroupPlan => {
  const plan: ExcelGroupPlan = {
    tagsToCreate: [],
    movesByLeafTagId: {},
    assignedConnectionNames: [],
  };
  const createdTagIdByNameAndParent = new Map<string, string>();
  const createdKey = (name: string, parentTagId: string | undefined) => `${(parentTagId || '')}\u0000${name}`;

  assignments.forEach((assignment) => {
    const connectionName = String(assignment?.connectionName || '').trim();
    const segments = String(assignment?.groupPath || '')
      .split('/')
      .map((segment) => segment.trim())
      .filter(Boolean);
    if (!connectionName || segments.length === 0) {
      return;
    }

    let parentTagId: string | undefined;
    let leafTagId = '';
    segments.forEach((segment) => {
      const existingId = context.resolveTagId(segment, parentTagId);
      if (existingId) {
        leafTagId = existingId;
        parentTagId = existingId;
        return;
      }
      const createdId = createdTagIdByNameAndParent.get(createdKey(segment, parentTagId));
      if (createdId) {
        leafTagId = createdId;
        parentTagId = createdId;
        return;
      }
      const newId = context.nextTagId();
      plan.tagsToCreate.push({ id: newId, name: segment, parentTagId });
      createdTagIdByNameAndParent.set(createdKey(segment, parentTagId), newId);
      leafTagId = newId;
      parentTagId = newId;
    });

    if (!leafTagId) {
      return;
    }
    plan.movesByLeafTagId[leafTagId] = [
      ...(plan.movesByLeafTagId[leafTagId] || []),
      connectionName,
    ];
    plan.assignedConnectionNames.push(connectionName);
  });

  return plan;
};

export const excelGroupPlanMoveCounts = (plan: ExcelGroupPlan): number => (
  Object.values(plan.movesByLeafTagId).reduce((total, names) => total + names.length, 0)
);

export { isSameTagName };
