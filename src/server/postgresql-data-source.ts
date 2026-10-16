import { Context } from '@nocobase/actions';
import {
  CollectionManager,
  CollectionOptions,
  DatabaseDataSource,
  FieldOptions,
  IModel,
  IRepository,
  LoadedCollections,
  LoadTablesOptions,
} from '@nocobase/data-source-manager';
import {
  createPostgresqlConnection,
  loadPostgresqlDriver,
  PostgresqlOptions,
  testPostgresqlConnection,
} from './postgresql';

const MAX_OBJECTS = 500;
const DEFAULT_PAGE_SIZE = 50;
const DEFAULT_QUERY_LIMIT = 2000;
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$#]*$/;
const UNSAFE_OBJECT_NAME = /^(pg_|sql_|information_schema$)/i;
const ROOT_ALIAS = 'postgresql_root';

type OracleColumn = {
  COLUMN_NAME: string;
  DATA_TYPE: string;
  DATA_LENGTH: number;
  DATA_PRECISION: number | null;
  DATA_SCALE: number | null;
  NULLABLE: 'Y' | 'N';
  DATA_DEFAULT: string | null;
};

type ConstraintColumn = {
  COLUMN_NAME: string;
  CONSTRAINT_NAME: string;
  CONSTRAINT_TYPE: 'P' | 'U';
  POSITION: number;
};
type ForeignKeyColumn = {
  CONSTRAINT_NAME: string;
  CHILD_TABLE_NAME: string;
  CHILD_COLUMN_NAME: string;
  PARENT_TABLE_NAME: string;
  PARENT_COLUMN_NAME: string;
  POSITION: number;
};
type OracleObject = { TABLE_NAME: string; OBJECT_TYPE: 'TABLE' | 'VIEW' };

type OracleCollection = {
  name: string;
  tableName: string;
  schema: string;
  view: boolean;
};

const RELATION_TYPES = new Set(['belongsTo', 'belongsToMany', 'belongsToArray', 'hasMany', 'hasOne']);

function row<T>(value: unknown): T {
  return value as T;
}

function quotedIdentifier(identifier: string) {
  if (!IDENTIFIER.test(identifier)) throw new Error(`Invalid PostgreSQL identifier: ${identifier}`);
  return `"${identifier}"`;
}

function assertSafeObjectName(name: string) {
  if (!IDENTIFIER.test(name) || UNSAFE_OBJECT_NAME.test(name)) {
    throw new Error('PostgreSQL system objects and invalid object names cannot be used as collections');
  }
}

type Filter = Record<string, unknown>;

type OracleRelationThrough = {
  schema: string;
  tableName: string;
  sourceKeys: string[];
  targetKeys: string[];
};

type OracleFilterRelation = {
  name: string;
  type: string;
  sourceTableName: string;
  schema: string;
  tableName: string;
  sourceKeys: string[];
  targetKeys: string[];
  targetFields: Set<string>;
  targetFieldTypes: Map<string, string | undefined>;
  targetFieldAliases: Map<string, string>;
  targetFieldNames: Map<string, string>;
  through?: OracleRelationThrough;
  arraySourceKey?: string;
};

type OracleExpression =
  | { type: 'field'; field: string | string[] }
  | { type: 'literal'; value: string | number | boolean | null }
  | {
      type: 'binary';
      operator:
        | 'add'
        | 'subtract'
        | 'multiply'
        | 'divide'
        | 'mod'
        | 'concat'
        | 'eq'
        | 'ne'
        | 'gt'
        | 'gte'
        | 'lt'
        | 'lte'
        | 'and'
        | 'or';
      left: OracleExpression;
      right: OracleExpression;
    }
  | { type: 'unary'; operator: 'not' | 'isNull' | 'isNotNull'; expression: OracleExpression }
  | {
      type: 'function';
      name: 'abs' | 'ceil' | 'floor' | 'round' | 'trunc' | 'lower' | 'upper' | 'length';
      args: OracleExpression[];
    }
  | { type: 'coalesce'; args: OracleExpression[] }
  | {
      type: 'case';
      when: Array<{ if: OracleExpression; then: OracleExpression }>;
      else?: OracleExpression;
    };

type OracleQueryMeasure = {
  field?: string | string[];
  expression?: OracleExpression;
  aggregation?: string;
  alias?: string;
  distinct?: boolean;
};

type OracleQueryDimension = {
  field: string | string[] | OracleExpression;
  type?: string;
  alias?: string;
  format?: string;
  options?: Record<string, unknown>;
};

type OracleQueryOrder = {
  field: string | string[] | OracleExpression;
  alias?: string;
  order?: 'asc' | 'desc';
  nulls?: 'default' | 'first' | 'last';
};

type OracleQueryOptions = {
  measures?: OracleQueryMeasure[];
  dimensions?: OracleQueryDimension[];
  orders?: OracleQueryOrder[];
  filter?: Filter;
  having?: Filter;
  limit?: number;
  offset?: number;
  timezone?: string;
  context?: Context;
};

type OracleAggregateOptions = {
  method: 'avg' | 'count' | 'min' | 'max' | 'sum';
  field?: string;
  expression?: OracleExpression;
  filter?: Filter;
  distinct?: boolean;
  relationPredicate?: { sql: string; binds: Record<string, unknown> };
  optionsTransformer?: (options: Record<string, unknown>) => void;
  context?: Context;
};

type OracleFindOptions = {
  fields?: string[];
  except?: string[];
  filterByTk?: unknown;
  filter?: Filter;
  offset?: number;
  limit?: number;
  paginate?: boolean | string;
  sort?: unknown;
  appends?: unknown;
  context?: Context;
  relationPredicate?: { sql: string; binds: Record<string, unknown> };
};

type OracleRow = Record<string, unknown>;

function isFilter(value: unknown): value is Filter {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function assignNestedFilter(target: Filter, path: string[], value: unknown) {
  let cursor = target;
  path.forEach((part, index) => {
    if (index === path.length - 1) {
      const existing = cursor[part];
      cursor[part] = isFilter(existing) && isFilter(value) ? { ...existing, ...value } : value;
      return;
    }
    if (!isFilter(cursor[part])) cursor[part] = {};
    cursor = cursor[part] as Filter;
  });
}

function normalizeOracleFilter(filter: Filter): Filter {
  const normalized: Filter = {};
  for (const [key, value] of Object.entries(filter)) {
    if (key === '$and' || key === '$or') {
      normalized[key] = Array.isArray(value)
        ? value.map((item) => (isFilter(item) ? normalizeOracleFilter(item) : item))
        : value;
      continue;
    }
    const parts = key.split('.').filter(Boolean);
    if (parts.length > 1) {
      assignNestedFilter(normalized, parts, value);
    } else {
      normalized[key] = isFilter(value) ? normalizeOracleFilter(value) : value;
    }
  }
  return normalized;
}

function fieldOption(field: FieldOptions, key: string): unknown {
  return (field as unknown as Record<string, unknown>)[key];
}

function isScalar(value: unknown): value is string | number | boolean | Date | null {
  return (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value)) ||
    value instanceof Date
  );
}

function resolveFieldName(field: string, knownFields: Set<string>) {
  return [...knownFields].find((candidate) => candidate.toUpperCase() === field.toUpperCase());
}

function normalizeFieldPath(field: string | string[]) {
  const path = Array.isArray(field) ? field : field.split('.');
  return path.map((part) => String(part).trim()).filter(Boolean);
}

function parseDateValue(value: unknown, operator: string) {
  if (value instanceof Date) {
    if (!Number.isNaN(value.getTime())) return value;
  } else if (typeof value === 'string' || typeof value === 'number') {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  throw new Error(`Invalid date for Oracle filter operator ${operator}`);
}

function dateRange(value: unknown, operator: string) {
  if (typeof value !== 'string') return undefined;
  const match = /^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$/.exec(value);
  if (!match) return undefined;
  const year = Number(match[1]);
  const month = match[2] ? Number(match[2]) - 1 : 0;
  const day = match[3] ? Number(match[3]) : 1;
  const start = new Date(Date.UTC(year, month, day));
  const end = match[3]
    ? new Date(Date.UTC(year, month, day + 1))
    : match[2]
      ? new Date(Date.UTC(year, month + 1, 1))
      : new Date(Date.UTC(year + 1, 0, 1));
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error(`Invalid date for Oracle filter operator ${operator}`);
  }
  return { start, end };
}

function formatDatePattern(format: string) {
  if (!format || format.length > 100 || /[';]/.test(format)) {
    throw new Error('Invalid PostgreSQL date format');
  }
  return format.replace(/hh/g, 'HH24').replace(/mm/g, 'MI').replace(/ss/g, 'SS');
}

function isNonPaginated(options: { limit?: number; paginate?: boolean | string; context?: Context }) {
  if (options.limit !== undefined) return false;
  const paginate = options.paginate ?? options.context?.action?.params?.paginate;
  return paginate === false || paginate === 'false';
}

function positiveInteger(value: unknown, fallback: number) {
  if (value === undefined || value === null || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1)
    throw new Error('Oracle pagination values must be positive integers');
  return parsed;
}

function nonNegativeInteger(value: unknown, fallback = 0) {
  if (value === undefined || value === null || value === '') return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error('Oracle offset must be a non-negative integer');
  return parsed;
}

function compileOracleFilter(
  filter: Filter,
  knownFields: Set<string>,
  relations: OracleFilterRelation[] = [],
  fieldExpressions = new Map<string, string>(),
  fieldTypes = new Map<string, string | undefined>(),
  bindPrefix = 'filter',
  rootAlias = ROOT_ALIAS,
  fieldAliases = new Map<string, string>(),
) {
  const normalizedFilter = normalizeOracleFilter(filter);
  const binds: Record<string, unknown> = {};
  let bindIndex = 0;
  const bind = (value: unknown) => {
    const name = `${bindPrefix}_${bindIndex++}`;
    binds[name] = value;
    return `@${name}`;
  };
  const isDateField = (fieldType: string | undefined) =>
    Boolean(fieldType && ['date', 'datetime', 'datetimeTz', 'datetimeNoTz', 'dateOnly', 'time'].includes(fieldType));
  const normalizeScalar = (value: unknown, operator: string, fieldType?: string) =>
    isDateField(fieldType) && value !== null && operator !== '$like' && operator !== '$notLike'
      ? parseDateValue(value, operator)
      : value;
  const scalarPredicate = (column: string, operator: string, value: unknown, fieldType?: string) => {
    if (!isScalar(value)) throw new Error(`Invalid value for Oracle filter operator ${operator}`);
    const normalized = normalizeScalar(value, operator, fieldType);
    if (operator === '$eq') return normalized === null ? `${column} IS NULL` : `${column} = ${bind(normalized)}`;
    if (operator === '$ne') {
      return normalized === null ? `${column} IS NOT NULL` : `(${column} <> ${bind(normalized)} OR ${column} IS NULL)`;
    }
    if (operator === '$gt') return normalized === null ? '1 = 0' : `${column} > ${bind(normalized)}`;
    if (operator === '$gte') return normalized === null ? '1 = 0' : `${column} >= ${bind(normalized)}`;
    if (operator === '$lt') return normalized === null ? '1 = 0' : `${column} < ${bind(normalized)}`;
    if (operator === '$lte') return normalized === null ? '1 = 0' : `${column} <= ${bind(normalized)}`;
    if (operator === '$like' || operator === '$notLike') {
      if (normalized === null || typeof normalized !== 'string') {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const comparison = operator === '$like' ? 'LIKE' : 'NOT LIKE';
      return `${column} ${comparison} ${bind(normalized)}`;
    }
    if (operator === '$startsWith' || operator === '$notStartsWith') {
      if (normalized === null || typeof normalized !== 'string') {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const comparison = operator === '$startsWith' ? '> 0' : '= 0';
      return `STRPOS(${column}, ${bind(normalized)}) ${comparison}`;
    }
    if (operator === '$endWith' || operator === '$notEndWith') {
      if (normalized === null || typeof normalized !== 'string') {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const pattern = operator === '$endWith' ? `%${normalized}` : `%${normalized}`;
      const comparison = operator === '$endWith' ? 'LIKE' : 'NOT LIKE';
      return `${column} ${comparison} ${bind(pattern)}`;
    }
    if (operator === '$includes')
      return normalized === null ? `${column} IS NULL` : `STRPOS(${column}, ${bind(normalized)}) > 0`;
    if (operator === '$notIncludes') {
      return normalized === null ? `${column} IS NOT NULL` : `STRPOS(${column}, ${bind(normalized)}) = 0`;
    }
    throw new Error(`Unsupported Oracle filter operator: ${operator}`);
  };
  const listPredicate = (column: string, operator: string, value: unknown, fieldType?: string) => {
    const values = Array.isArray(value) ? value : [value];
    if (values.some((item) => !isScalar(item))) {
      throw new Error(`Oracle filter operator ${operator} requires an array of scalar values`);
    }
    if (operator === '$includes' || operator === '$notIncludes') {
      if (!values.length) return operator === '$includes' ? '1 = 0' : '1 = 1';
      const predicates = values.map((item) => scalarPredicate(column, operator, item));
      return `(${predicates.join(operator === '$includes' ? ' OR ' : ' AND ')})`;
    }
    const nonNullValues = values
      .filter((item) => item !== null)
      .map((item) => normalizeScalar(item, operator, fieldType));
    const nullPredicate =
      (operator === '$notIn' && values.length > 0) || values.includes(null) ? `${column} IS NULL` : undefined;
    const list = nonNullValues.length
      ? `${column} ${operator === '$in' ? 'IN' : 'NOT IN'} (${nonNullValues.map(bind).join(', ')})`
      : undefined;
    if (!list) return nullPredicate || (operator === '$in' ? '1 = 0' : '1 = 1');
    return nullPredicate ? `(${list} OR ${nullPredicate})` : list;
  };
  const rangePredicate = (column: string, operator: string, value: unknown, fieldType?: string) => {
    if (!Array.isArray(value) || value.length !== 2 || value.some((item) => !isScalar(item) || item === null)) {
      throw new Error(`Oracle filter operator ${operator} requires two scalar values`);
    }
    const [left, right] = value.map((item) => normalizeScalar(item, operator, fieldType));
    if (operator === '$between') return `(${column} >= ${bind(left)} AND ${column} <= ${bind(right)})`;
    return `(${column} < ${bind(left)} OR ${column} > ${bind(right)} OR ${column} IS NULL)`;
  };
  const datePredicate = (column: string, operator: string, value: unknown) => {
    const values = Array.isArray(value) ? value : [value];
    if (operator === '$dateOn' || operator === '$dateNotOn') {
      if (values.length === 2) {
        const start = parseDateValue(values[0], operator);
        const end = parseDateValue(values[1], operator);
        return operator === '$dateOn'
          ? `(${column} >= ${bind(start)} AND ${column} < ${bind(end)})`
          : `(${column} < ${bind(start)} OR ${column} >= ${bind(end)} OR ${column} IS NULL)`;
      }
      const date = parseDateValue(values[0], operator);
      const range = dateRange(values[0], operator);
      if (range) {
        return operator === '$dateOn'
          ? `(${column} >= ${bind(range.start)} AND ${column} < ${bind(range.end)})`
          : `(${column} < ${bind(range.start)} OR ${column} >= ${bind(range.end)} OR ${column} IS NULL)`;
      }
      return operator === '$dateOn' ? `${column} = ${bind(date)}` : `(${column} <> ${bind(date)} OR ${column} IS NULL)`;
    }
    const date = parseDateValue(values[0], operator);
    if (operator === '$dateBefore') return `${column} < ${bind(date)}`;
    if (operator === '$dateNotBefore') return `${column} >= ${bind(date)}`;
    if (operator === '$dateAfter') return `${column} > ${bind(date)}`;
    if (operator === '$dateNotAfter') return `${column} <= ${bind(date)}`;
    if (operator === '$dateBetween') {
      if (values.length !== 2) throw new Error(`Oracle filter operator ${operator} requires two dates`);
      const end = parseDateValue(values[1], operator);
      return `(${column} >= ${bind(date)} AND ${column} < ${bind(end)})`;
    }
    throw new Error(`Unsupported Oracle date filter operator: ${operator}`);
  };
  const fieldPredicate = (
    field: string,
    value: unknown,
    column: string | undefined = undefined,
    fields = knownFields,
    types = fieldTypes,
    aliases = fieldAliases,
  ) => {
    const actualField = resolveFieldName(field, fields);
    if (!actualField) throw new Error(`Unknown Oracle filter field: ${field}`);
    const physicalField = aliases.get(actualField.toUpperCase()) || actualField;
    const expression =
      column ||
      fieldExpressions.get(actualField.toUpperCase()) ||
      `${rootAlias ? `${rootAlias}.` : ''}${quotedIdentifier(physicalField)}`;
    const fieldType = types.get(actualField.toUpperCase());
    if (isScalar(value)) return scalarPredicate(expression, '$eq', value, fieldType);
    if (!isFilter(value)) throw new Error(`Invalid Oracle filter value for ${field}`);
    const predicates = Object.entries(value).map(([operator, operand]) => {
      if (['$in', '$notIn'].includes(operator)) {
        return listPredicate(expression, operator, operand, fieldType);
      }
      if (operator === '$anyOf' || operator === '$noneOf') {
        return listPredicate(expression, operator === '$anyOf' ? '$in' : '$notIn', operand, fieldType);
      }
      if (
        ['$includes', '$notIncludes', '$startsWith', '$notStartsWith', '$endWith', '$notEndWith'].includes(operator) &&
        Array.isArray(operand)
      ) {
        const values = operand;
        return `(${values
          .map((item) => scalarPredicate(expression, operator, item, fieldType))
          .join(operator.startsWith('$not') ? ' AND ' : ' OR ')})`;
      }
      if (['$between', '$notBetween'].includes(operator))
        return rangePredicate(expression, operator, operand, fieldType);
      if (operator.startsWith('$date')) return datePredicate(expression, operator, operand);
      if (operator === '$exists') return operand === false ? `${expression} IS NULL` : `${expression} IS NOT NULL`;
      if (operator === '$notExists') return operand === false ? `${expression} IS NOT NULL` : `${expression} IS NULL`;
      if (operator === '$isTruly') return `COALESCE(${expression}, 0) <> 0`;
      if (operator === '$isFalsy') return `COALESCE(${expression}, 0) = 0`;
      if (operator === '$match' || operator === '$notMatch') {
        if (typeof operand !== 'string') throw new Error(`Oracle filter operator ${operator} requires a string value`);
        return `${expression} ${operator === '$match' ? '~' : '!~'} ${bind(operand)}`;
      }
      if (operator === '$empty')
        return fieldType === 'string' ? `(${expression} IS NULL OR ${expression} = '')` : `${expression} IS NULL`;
      if (operator === '$notEmpty')
        return fieldType === 'string'
          ? `(${expression} IS NOT NULL AND ${expression} <> '')`
          : `${expression} IS NOT NULL`;
      if (
        [
          '$eq',
          '$ne',
          '$gt',
          '$gte',
          '$lt',
          '$lte',
          '$like',
          '$notLike',
          '$includes',
          '$notIncludes',
          '$startsWith',
          '$notStartsWith',
          '$endWith',
          '$notEndWith',
        ].includes(operator)
      ) {
        return scalarPredicate(expression, operator, operand, fieldType);
      }
      throw new Error(`Unsupported Oracle filter operator: ${operator}`);
    });
    if (!predicates.length) return '1 = 1';
    return predicates.length === 1 ? predicates[0] : `(${predicates.join(' AND ')})`;
  };
  const relationPredicate = (path: string, value: unknown) => {
    const [relationName, targetField, ...rest] = path.split('.');
    const relation = relations.find((item) => item.name.toUpperCase() === relationName.toUpperCase());
    if (!relation || (rest.length && !targetField) || (rest.length > 0 && rest.some(Boolean))) {
      throw new Error(`Unknown Oracle filter field: ${path}`);
    }
    const targetFilter = targetField ? { [targetField]: value } : value;
    if (!isFilter(targetFilter)) throw new Error(`Invalid Oracle relation filter: ${relation.name}`);
    const relationIndex = relations.indexOf(relation);
    const targetAlias = quotedIdentifier(
      relation.through ? `relation_${relationIndex}_target` : `relation_${relationIndex}`,
    );
    const targetCondition = conditionPredicate(
      targetFilter,
      relation.targetFields,
      targetAlias,
      relation.targetFieldTypes,
      [],
      relation.targetFieldAliases,
    );
    const joinPredicates = relation.sourceKeys.map((sourceKey, index) => {
      const targetKey = relation.targetKeys[index];
      return `${targetAlias}.${quotedIdentifier(targetKey)} = ${rootAlias ? `${rootAlias}.` : ''}${quotedIdentifier(
        sourceKey,
      )}`;
    });
    if (relation.arraySourceKey) {
      const targetKey = relation.targetKeys[0];
      joinPredicates.splice(
        0,
        joinPredicates.length,
        `EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE((${
          rootAlias ? `${rootAlias}.` : ''
        }${quotedIdentifier(
          relation.arraySourceKey,
        )})::jsonb, '[]'::jsonb)) AS json_item(value) WHERE json_item.value = (${targetAlias}.${quotedIdentifier(
          targetKey,
        )})::text)`,
      );
    }
    if (relation.through) {
      const throughAlias = quotedIdentifier(`relation_${relationIndex}_through`);
      const throughPredicates = relation.sourceKeys.map((sourceKey, index) => {
        const throughKey = relation.through?.sourceKeys[index];
        return `${throughAlias}.${quotedIdentifier(throughKey as string)} = ${
          rootAlias ? `${rootAlias}.` : ''
        }${quotedIdentifier(sourceKey)}`;
      });
      const targetThroughPredicates = relation.targetKeys.map((targetKey, index) => {
        const throughKey = relation.through?.targetKeys[index];
        return `${targetAlias}.${quotedIdentifier(targetKey)} = ${throughAlias}.${quotedIdentifier(
          throughKey as string,
        )}`;
      });
      return `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
        relation.through.tableName,
      )} ${throughAlias} JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
        relation.tableName,
      )} ${targetAlias} ON ${targetThroughPredicates.join(' AND ')} WHERE ${[
        ...throughPredicates,
        targetCondition,
      ].join(' AND ')})`;
    }
    return `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
      relation.tableName,
    )} ${targetAlias} WHERE ${[...joinPredicates, targetCondition].join(' AND ')})`;
  };
  function conditionPredicate(
    condition: Filter,
    fields = knownFields,
    prefix = '',
    types = fieldTypes,
    relationSet = relations,
    aliases = fieldAliases,
  ): string {
    const predicates = Object.entries(condition).map(([field, value]) => {
      if (field === '$and' || field === '$or') {
        if (!Array.isArray(value)) throw new Error(`Oracle filter operator ${field} requires an array of conditions`);
        if (!value.length) return field === '$and' ? '1 = 1' : '1 = 0';
        const children = value.map((item) => {
          if (!isFilter(item)) throw new Error(`Oracle filter operator ${field} requires object conditions`);
          return conditionPredicate(item, fields, prefix, types, relationSet, aliases);
        });
        return `(${children.join(field === '$and' ? ' AND ' : ' OR ')})`;
      }
      if (!prefix && field.includes('.')) return relationPredicate(field, value);
      if (!prefix) {
        const relation = relationSet.find((item) => item.name.toUpperCase() === field.toUpperCase());
        if (relation) return relationPredicate(field, value);
      }
      const resolvedField = aliases.get(field.toUpperCase()) || resolveFieldName(field, fields) || field;
      const column = prefix ? `${prefix}.${quotedIdentifier(resolvedField)}` : undefined;
      return fieldPredicate(field, value, column, fields, types, aliases);
    });
    return predicates.length ? (predicates.length === 1 ? predicates[0] : `(${predicates.join(' AND ')})`) : '1 = 1';
  }

  return { predicate: conditionPredicate(normalizedFilter), binds };
}

function mapOracleField(column: OracleColumn, primaryColumns: Set<string>, uniqueColumns: Set<string>): FieldOptions {
  const rawType = column.DATA_TYPE.toUpperCase();
  let type = 'string';
  let fieldInterface = 'input';
  if (['SMALLINT', 'INTEGER', 'BIGINT', 'INT2', 'INT4', 'INT8', 'SERIAL', 'BIGSERIAL'].includes(rawType)) {
    type = 'integer';
    fieldInterface = 'number';
  } else if (['DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'MONEY'].includes(rawType)) {
    type = 'double';
    fieldInterface = 'number';
  } else if (
    [
      'DATE',
      'TIMESTAMP WITHOUT TIME ZONE',
      'TIMESTAMP WITH TIME ZONE',
      'TIME WITHOUT TIME ZONE',
      'TIME WITH TIME ZONE',
    ].includes(rawType)
  ) {
    type = rawType === 'DATE' ? 'date' : 'datetime';
    fieldInterface = 'datetime';
  } else if (['TEXT', 'CHARACTER VARYING', 'CHARACTER', 'VARCHAR', 'CHAR', 'NAME', 'XML'].includes(rawType)) {
    type = 'text';
    fieldInterface = 'textarea';
  } else if (['BYTEA'].includes(rawType)) {
    type = 'json';
    fieldInterface = 'json';
  } else if (rawType === 'BOOLEAN') {
    type = 'boolean';
    fieldInterface = 'checkbox';
  } else if (rawType === 'UUID') {
    type = 'string';
    fieldInterface = 'input';
  } else if (['JSON', 'JSONB', 'ARRAY'].includes(rawType)) {
    type = 'json';
    fieldInterface = 'json';
  }
  return {
    name: column.COLUMN_NAME,
    field: column.COLUMN_NAME,
    rawType,
    type,
    interface: fieldInterface,
    allowNull: column.NULLABLE === 'Y',
    primaryKey: primaryColumns.has(column.COLUMN_NAME),
    unique: uniqueColumns.has(column.COLUMN_NAME),
    defaultValue: column.DATA_DEFAULT?.trim() || undefined,
  };
}

class OracleRepository implements IRepository {
  constructor(
    private collection: {
      options: CollectionOptions;
      collectionManager: OracleCollectionManager & { dataSource: OracleDataSource };
    },
  ) {}

  private persistedFields() {
    return (this.collection.options.fields || []).filter((field) => !RELATION_TYPES.has(field.type));
  }

  private persistedFieldNames() {
    return new Set(this.persistedFields().flatMap((field) => [String(field.name), String(field.field || field.name)]));
  }

  private persistedFieldTypes() {
    return new Map(
      this.persistedFields().flatMap((field) => [
        [String(field.name).toUpperCase(), field.type] as const,
        [String(field.field || field.name).toUpperCase(), field.type] as const,
      ]),
    );
  }

  private persistedFieldAliases() {
    return new Map(
      this.persistedFields().flatMap((field) => {
        const physical = String(field.field || field.name);
        return [[String(field.name).toUpperCase(), physical] as const, [physical.toUpperCase(), physical] as const];
      }),
    );
  }

  private recordKeyFields() {
    const configured = this.collection.options.filterTargetKey;
    const aliases = this.persistedFieldAliases();
    if (Array.isArray(configured) && configured.length)
      return configured.map((field) => aliases.get(String(field).toUpperCase()) || String(field));
    if (typeof configured === 'string' && configured) return [aliases.get(configured.toUpperCase()) || configured];
    const primary = this.persistedFields()
      .filter((field) => field.primaryKey)
      .map((field) => String(field.field || field.name));
    if (primary.length) return primary;
    return this.persistedFields()
      .filter((field) => field.unique)
      .map((field) => String(field.field || field.name));
  }

  private resolvePersistedField(field: string) {
    const resolved = this.persistedFields().find(
      (item) =>
        String(item.name).toUpperCase() === field.toUpperCase() ||
        String(item.field || item.name).toUpperCase() === field.toUpperCase(),
    );
    if (!resolved) throw new Error(`Unknown Oracle field: ${field}`);
    return String(resolved.field || resolved.name);
  }

  private logicalPersistedField(field: string) {
    const resolved = this.persistedFields().find(
      (item) =>
        String(item.name).toUpperCase() === field.toUpperCase() ||
        String(item.field || item.name).toUpperCase() === field.toUpperCase(),
    );
    return String(resolved?.name || field);
  }

  private normalizeFieldList(value: unknown) {
    if (value === undefined || value === null) return [];
    const values = Array.isArray(value) ? value : [value];
    return values.map((item) => String(item).trim()).filter(Boolean);
  }

  private filterByTargetKey(filterByTk: unknown) {
    if (filterByTk === undefined) return undefined;
    const keys = this.recordKeyFields();
    if (!keys.length)
      throw new Error(`Oracle collection ${this.collection.options.name} has no configured Record Unique Key`);
    if (keys.length === 1) {
      const key = String(keys[0]);
      if (Array.isArray(filterByTk)) {
        if (filterByTk.some((value) => value === null || typeof value === 'object')) {
          throw new Error(`Oracle ${this.collection.options.name} requires scalar Record Unique Keys`);
        }
        return { [key]: { $in: filterByTk } };
      }
      const value = isFilter(filterByTk) && key in filterByTk ? filterByTk[key] : filterByTk;
      if (value === undefined || typeof value === 'object' || value === null) {
        throw new Error(`Oracle ${this.collection.options.name} requires a scalar ${key} Record Unique Key`);
      }
      return { [key]: value };
    }
    if (Array.isArray(filterByTk)) {
      if (filterByTk.every((value) => isFilter(value))) {
        return {
          $or: filterByTk.map((value) =>
            Object.fromEntries(
              keys.map((key) => {
                const item = value[key];
                if (item === undefined || item === null || typeof item === 'object') {
                  throw new Error(`Oracle composite Record Unique Key requires the ${key} value`);
                }
                return [key, item];
              }),
            ),
          ),
        };
      }
      if (filterByTk.length !== keys.length) throw new Error('Oracle composite Record Unique Key has an invalid value');
      return Object.fromEntries(keys.map((key, index) => [key, filterByTk[index]]));
    }
    if (isFilter(filterByTk)) {
      return Object.fromEntries(
        keys.map((key) => {
          const value = filterByTk[key];
          if (value === undefined || value === null || typeof value === 'object') {
            throw new Error(`Oracle composite Record Unique Key requires the ${key} value`);
          }
          return [key, value];
        }),
      );
    }
    throw new Error('Oracle composite Record Unique Key requires an object or array value');
  }

  private mergeFilters(...filters: Array<Filter | undefined>) {
    const active = filters.filter((filter): filter is Filter => Boolean(filter && Object.keys(filter).length));
    if (!active.length) return undefined;
    if (active.length === 1) return active[0];
    return { $and: active };
  }

  private compileFilter(filter: Filter | undefined, bindPrefix = 'filter') {
    if (!filter) return undefined;
    return compileOracleFilter(
      filter,
      this.persistedFieldNames(),
      this.filterRelations(),
      new Map(),
      this.persistedFieldTypes(),
      bindPrefix,
      ROOT_ALIAS,
      this.persistedFieldAliases(),
    );
  }

  private sortClause(sort: unknown) {
    const values = (Array.isArray(sort) ? sort : sort === undefined || sort === null ? [] : [sort]).flatMap((item) =>
      typeof item === 'string'
        ? item
            .split(',')
            .map((value) => value.trim())
            .filter(Boolean)
        : [item],
    );
    const clauses = values.map((item) => {
      let field: unknown;
      let direction = 'ASC';
      let nulls = '';
      if (typeof item === 'string') {
        field = item.startsWith('-') ? item.slice(1) : item;
        direction = item.startsWith('-') ? 'DESC' : 'ASC';
      } else if (isFilter(item)) {
        field = item.field;
        direction = String(item.order || item.direction || 'asc').toUpperCase();
        nulls = item.nulls ? ` NULLS ${String(item.nulls).toUpperCase()}` : '';
      }
      if (typeof field !== 'string' || !['ASC', 'DESC'].includes(direction)) {
        throw new Error('Invalid Oracle sort specification');
      }
      const resolved = this.resolvePersistedField(field);
      if (nulls && ![' NULLS FIRST', ' NULLS LAST'].includes(nulls)) throw new Error('Invalid Oracle null sort order');
      return `${ROOT_ALIAS}.${quotedIdentifier(resolved)} ${direction}${nulls}`;
    });
    return clauses.length ? ` ORDER BY ${clauses.join(', ')}` : '';
  }

  private paginationClause(options: OracleFindOptions, binds: Record<string, unknown>) {
    if (isNonPaginated(options)) return '';
    binds.offset = nonNegativeInteger(options.offset);
    binds.limit = positiveInteger(options.limit, DEFAULT_PAGE_SIZE);
    return ' LIMIT @limit OFFSET @offset';
  }

  async find(options: OracleFindOptions = {}) {
    const collection = this.collection.options;
    const knownFields = this.persistedFieldNames();
    if (!knownFields.size) throw new Error(`Oracle collection ${collection.name} has no persisted field metadata`);
    const requestedInput = this.normalizeFieldList(options.fields);
    const requestedFields = requestedInput.length
      ? requestedInput.map((field) => this.resolvePersistedField(field))
      : [...knownFields];
    const exceptFields = this.normalizeFieldList(options.except).map((field) => this.resolvePersistedField(field));
    const visibleFields = requestedFields.filter((field) => !exceptFields.includes(field));
    const appendFields = this.relationFields(options.appends);
    const requiredFields = appendFields.flatMap((field) => this.relationSourceKeys(field));
    const selectedFields = [...new Set([...visibleFields, ...requiredFields])];
    if (!selectedFields.length) throw new Error('No requested Oracle fields are available in persisted metadata');
    const whereFilter = this.mergeFilters(options.filter, this.filterByTargetKey(options.filterByTk));
    const compiledFilter = this.compileFilter(whereFilter);
    const whereParts = [compiledFilter?.predicate, options.relationPredicate?.sql].filter(Boolean);
    const binds = {
      ...(compiledFilter?.binds || {}),
      ...(options.relationPredicate?.binds || {}),
    };
    const visible = new Set(visibleFields);
    const fields = selectedFields
      .map((field) => {
        const outputName = visible.has(field) ? this.logicalPersistedField(field) : field;
        const expression = `${ROOT_ALIAS}.${quotedIdentifier(field)}`;
        return outputName === field ? expression : `${expression} AS ${quotedIdentifier(outputName)}`;
      })
      .join(', ');
    assertSafeObjectName(collection.tableName);
    const pagination = this.paginationClause(options, binds);
    const sort = this.sortClause(options.sort);
    const sql = `SELECT ${fields} FROM ${this.tableReference()} ${ROOT_ALIAS}${
      whereParts.length ? ` WHERE ${whereParts.join(' AND ')}` : ''
    }${sort}${pagination}`;
    const records = (await this.collection.collectionManager.dataSource.execute(sql, binds)) as unknown as OracleRow[];
    await this.appendRelations(records, appendFields, options.appends);
    for (const record of records) {
      for (const field of selectedFields) {
        if (!visible.has(field)) delete record[field];
      }
    }
    return records as unknown as IModel[];
  }

  async findOne(options: OracleFindOptions = {}) {
    const records = await this.find({ ...options, limit: 1 });
    return (records[0] || null) as IModel;
  }

  async count(options: OracleFindOptions = {}) {
    const collection = this.collection.options;
    const knownFields = this.persistedFieldNames();
    if (!knownFields.size) throw new Error(`Oracle collection ${collection.name} has no persisted field metadata`);
    const whereFilter = this.mergeFilters(options.filter, this.filterByTargetKey(options.filterByTk));
    const compiledFilter = this.compileFilter(whereFilter);
    const whereParts = [compiledFilter?.predicate, options.relationPredicate?.sql].filter(Boolean);
    assertSafeObjectName(collection.tableName);
    const sql = `SELECT COUNT(*) AS COUNT FROM ${this.tableReference()} ${ROOT_ALIAS}${
      whereParts.length ? ` WHERE ${whereParts.join(' AND ')}` : ''
    }`;
    const records = await this.collection.collectionManager.dataSource.execute(sql, {
      ...(compiledFilter?.binds || {}),
      ...(options.relationPredicate?.binds || {}),
    });
    return Number(records[0]?.COUNT ?? records[0]?.count ?? 0);
  }

  async aggregate(options: OracleAggregateOptions) {
    const method = String(options.method || '').toLowerCase();
    if (!['avg', 'count', 'min', 'max', 'sum'].includes(method)) {
      throw new Error(`Unsupported Oracle aggregation: ${options.method}`);
    }
    const relations = this.filterRelations();
    const joins = new Map<string, string>();
    const joinSql: string[] = [];
    const expressionBinds: Record<string, unknown> = {};
    const field = options.expression
      ? this.compileExpression(options.expression, relations, joins, joinSql, expressionBinds)
      : options.field
        ? this.queryFieldExpression(options.field, relations, joins, joinSql)
        : undefined;
    if (method !== 'count' && !field) {
      throw new Error(`Oracle ${method} requires a field`);
    }
    if (options.distinct && !field) {
      throw new Error('Oracle count distinct requires a field');
    }
    const expression = field?.expression || '*';
    const aggregateExpression = `${method.toUpperCase()}(${options.distinct ? 'DISTINCT ' : ''}${expression})`;
    const compiledFilter = options.filter ? this.compileFilter(options.filter) : undefined;
    const whereParts = [compiledFilter?.predicate, options.relationPredicate?.sql].filter(Boolean);
    const binds = {
      ...expressionBinds,
      ...(compiledFilter?.binds || {}),
      ...(options.relationPredicate?.binds || {}),
    };
    const sql = `SELECT ${aggregateExpression} AS "aggregate" FROM ${this.tableReference()} ${ROOT_ALIAS}${joinSql.join(
      '',
    )}${whereParts.length ? ` WHERE ${whereParts.join(' AND ')}` : ''}`;
    const records = await this.collection.collectionManager.dataSource.execute(sql, binds);
    const value = records[0]?.aggregate ?? records[0]?.AGGREGATE ?? records[0]?.COUNT ?? records[0]?.count;
    return Number(value ?? 0);
  }

  async findAndCount(options: OracleFindOptions = {}) {
    return [await this.find(options), await this.count(options)] as [IModel[], Number];
  }

  private queryRelationJoin(relation: OracleFilterRelation, index: number) {
    const targetAlias = quotedIdentifier(`query_relation_${index}_target`);
    if (relation.arraySourceKey) {
      const targetKey = relation.targetKeys[0];
      return {
        alias: targetAlias,
        sql: ` LEFT JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
          relation.tableName,
        )} ${targetAlias} ON EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE((${ROOT_ALIAS}.${quotedIdentifier(
          relation.arraySourceKey,
        )})::jsonb, '[]'::jsonb)) AS json_item(value) WHERE json_item.value = (${targetAlias}.${quotedIdentifier(
          targetKey,
        )})::text)`,
      };
    }
    if (relation.through) {
      const throughAlias = quotedIdentifier(`query_relation_${index}_through`);
      const throughJoin = relation.sourceKeys.map((sourceKey, keyIndex) => {
        const throughKey = relation.through?.sourceKeys[keyIndex];
        return `${throughAlias}.${quotedIdentifier(throughKey as string)} = ${ROOT_ALIAS}.${quotedIdentifier(
          sourceKey,
        )}`;
      });
      const targetJoin = relation.targetKeys.map((targetKey, keyIndex) => {
        const throughKey = relation.through?.targetKeys[keyIndex];
        return `${targetAlias}.${quotedIdentifier(targetKey)} = ${throughAlias}.${quotedIdentifier(
          throughKey as string,
        )}`;
      });
      return {
        alias: targetAlias,
        sql: ` LEFT JOIN ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
          relation.through.tableName,
        )} ${throughAlias} ON ${throughJoin.join(' AND ')} LEFT JOIN ${quotedIdentifier(
          relation.schema,
        )}.${quotedIdentifier(relation.tableName)} ${targetAlias} ON ${targetJoin.join(' AND ')}`,
      };
    }
    const predicates = relation.sourceKeys.map((sourceKey, keyIndex) => {
      const targetKey = relation.targetKeys[keyIndex];
      return `${targetAlias}.${quotedIdentifier(targetKey)} = ${ROOT_ALIAS}.${quotedIdentifier(sourceKey)}`;
    });
    return {
      alias: targetAlias,
      sql: ` LEFT JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
        relation.tableName,
      )} ${targetAlias} ON ${predicates.join(' AND ')}`,
    };
  }

  private compileExpression(
    expression: OracleExpression,
    relations: OracleFilterRelation[],
    joins: Map<string, string>,
    joinSql: string[],
    binds: Record<string, unknown>,
  ): { expression: string; name: string; type?: string } {
    if (!isFilter(expression) || typeof expression.type !== 'string') {
      throw new Error('Invalid Oracle expression');
    }
    if (expression.type === 'field') {
      return this.queryFieldExpression(expression.field, relations, joins, joinSql);
    }
    if (expression.type === 'literal') {
      const name = `expression_${Object.keys(binds).length}`;
      binds[name] = expression.value;
      return { expression: `:${name}`, name: 'literal' };
    }
    if (expression.type === 'binary') {
      const left = this.compileExpression(expression.left, relations, joins, joinSql, binds);
      const right = this.compileExpression(expression.right, relations, joins, joinSql, binds);
      const operators: Record<string, string> = {
        add: '+',
        subtract: '-',
        multiply: '*',
        divide: '/',
        eq: '=',
        ne: '<>',
        gt: '>',
        gte: '>=',
        lt: '<',
        lte: '<=',
        and: 'AND',
        or: 'OR',
      };
      if (expression.operator === 'mod') {
        return { expression: `MOD(${left.expression}, ${right.expression})`, name: 'expression' };
      }
      if (expression.operator === 'concat') {
        return { expression: `CONCAT(${left.expression}, ${right.expression})`, name: 'expression', type: 'string' };
      }
      const operator = operators[expression.operator];
      if (!operator) throw new Error(`Unsupported Oracle expression operator: ${expression.operator}`);
      const rightExpression = expression.operator === 'divide' ? `NULLIF(${right.expression}, 0)` : right.expression;
      return { expression: `(${left.expression} ${operator} ${rightExpression})`, name: 'expression' };
    }
    if (expression.type === 'unary') {
      const child = this.compileExpression(expression.expression, relations, joins, joinSql, binds);
      if (expression.operator === 'not') return { expression: `(NOT ${child.expression})`, name: 'expression' };
      return {
        expression: `${child.expression} IS ${expression.operator === 'isNull' ? '' : 'NOT '}NULL`,
        name: 'expression',
      };
    }
    if (expression.type === 'function') {
      const args = expression.args.map((item) => this.compileExpression(item, relations, joins, joinSql, binds));
      const requestedName = expression.name.toUpperCase();
      const name = requestedName;
      const arity = requestedName === 'ROUND' || requestedName === 'TRUNC' ? [1, 2] : [1];
      if (!arity.includes(args.length)) throw new Error(`Oracle function ${name} has an invalid argument count`);
      return { expression: `${name}(${args.map((item) => item.expression).join(', ')})`, name: 'expression' };
    }
    if (expression.type === 'coalesce') {
      if (expression.args.length < 1) throw new Error('Oracle coalesce requires at least one argument');
      const args = expression.args.map((item) => this.compileExpression(item, relations, joins, joinSql, binds));
      return { expression: `COALESCE(${args.map((item) => item.expression).join(', ')})`, name: 'expression' };
    }
    if (expression.type === 'case') {
      if (!expression.when.length) throw new Error('Oracle case expression requires at least one branch');
      const branches = expression.when.map((item) => {
        const condition = this.compileExpression(item.if, relations, joins, joinSql, binds);
        const result = this.compileExpression(item.then, relations, joins, joinSql, binds);
        return `WHEN ${condition.expression} THEN ${result.expression}`;
      });
      const fallback = expression.else
        ? this.compileExpression(expression.else, relations, joins, joinSql, binds).expression
        : 'NULL';
      return { expression: `(CASE ${branches.join(' ')} ELSE ${fallback} END)`, name: 'expression' };
    }
    throw new Error(`Unsupported Oracle expression type: ${expression.type}`);
  }

  private queryFieldExpression(
    field: string | string[],
    relations: OracleFilterRelation[],
    joins: Map<string, string>,
    joinSql: string[],
  ) {
    const path = normalizeFieldPath(field);
    if (path.length === 1) {
      const physical = this.resolvePersistedField(path[0]);
      return {
        expression: `${ROOT_ALIAS}.${quotedIdentifier(physical)}`,
        name: this.logicalPersistedField(physical),
        type: this.persistedFieldTypes().get(physical.toUpperCase()),
      };
    }
    if (path.length !== 2) throw new Error(`Oracle query supports only one relation path: ${path.join('.')}`);
    const relation = relations.find((item) => item.name.toUpperCase() === path[0].toUpperCase());
    if (!relation) throw new Error(`Unknown Oracle query relation: ${path[0]}`);
    const targetField = resolveFieldName(path[1], relation.targetFields);
    if (!targetField) throw new Error(`Unknown Oracle query field: ${path.join('.')}`);
    let alias = joins.get(relation.name.toUpperCase());
    if (!alias) {
      const join = this.queryRelationJoin(relation, joins.size);
      alias = join.alias;
      joins.set(relation.name.toUpperCase(), alias);
      joinSql.push(join.sql);
    }
    return {
      expression: `${alias}.${quotedIdentifier(targetField)}`,
      name: `${relation.name}_${relation.targetFieldNames.get(targetField.toUpperCase()) || targetField}`,
      type: relation.targetFieldTypes.get(targetField.toUpperCase()),
    };
  }

  private queryAlias(alias: unknown, fallback: string) {
    const value = String(alias || fallback);
    if (!IDENTIFIER.test(value)) throw new Error(`Invalid Oracle query alias: ${value}`);
    return value;
  }

  private queryDimensionExpression(
    expression: string,
    type: string | undefined,
    format: string | undefined,
    timezone?: string,
  ) {
    if (
      !format ||
      !['date', 'datetime', 'datetimeTz', 'datetimeNoTz', 'dateOnly', 'time', 'unixTimestamp'].includes(type || '')
    ) {
      return expression;
    }
    const pattern = formatDatePattern(format);
    const timezoneExpression =
      timezone && type === 'datetimeTz' && /^[A-Za-z][A-Za-z0-9_+:/-]{0,63}$/.test(timezone)
        ? `(${expression} AT TIME ZONE '${timezone}')`
        : expression;
    if (type === 'unixTimestamp') {
      return `TO_CHAR(TIMESTAMP '1970-01-01 00:00:00' + (${expression} * INTERVAL '1 second'), '${pattern}')`;
    }
    return `TO_CHAR(${timezoneExpression}, '${pattern}')`;
  }

  async query(options: OracleQueryOptions = {}) {
    const measures = Array.isArray(options.measures) ? options.measures : [];
    const dimensions = Array.isArray(options.dimensions) ? options.dimensions : [];
    if (!measures.length && !dimensions.length)
      throw new Error('Oracle query requires at least one measure or dimension');
    const relations = this.filterRelations();
    const joins = new Map<string, string>();
    const joinSql: string[] = [];
    const expressions = new Map<string, string>();
    const expressionTypes = new Map<string, string | undefined>();
    const expressionBinds: Record<string, unknown> = {};
    const selectSql: string[] = [];
    const groupSql: string[] = [];
    const nonAggregateSql: string[] = [];
    let hasAggregate = false;

    for (const measure of measures) {
      const aggregation = measure.aggregation?.toLowerCase();
      if (aggregation && !['count', 'sum', 'avg', 'min', 'max'].includes(aggregation)) {
        throw new Error(`Unsupported Oracle aggregation: ${measure.aggregation}`);
      }
      const fieldPath = measure.field === undefined ? ['*'] : normalizeFieldPath(measure.field);
      let fieldExpression = '*';
      let fieldName = 'count';
      let fieldType: string | undefined;
      if (measure.expression) {
        const expression = this.compileExpression(measure.expression, relations, joins, joinSql, expressionBinds);
        fieldExpression = expression.expression;
        fieldName = expression.name;
        fieldType = expression.type;
      } else if (!(fieldPath.length === 1 && fieldPath[0] === '*')) {
        const field = this.queryFieldExpression(measure.field as string | string[], relations, joins, joinSql);
        fieldExpression = field.expression;
        fieldName = field.name;
        fieldType = field.type;
      } else if (aggregation !== 'count') {
        throw new Error('Oracle aggregate measures other than count require a field');
      }
      if (aggregation === 'count' && measure.distinct && fieldExpression === '*') {
        throw new Error('Oracle count distinct requires a field');
      }
      const aggregateExpression = aggregation
        ? `${aggregation.toUpperCase()}(${measure.distinct ? 'DISTINCT ' : ''}${fieldExpression})`
        : fieldExpression;
      hasAggregate = hasAggregate || Boolean(aggregation);
      if (!aggregation) nonAggregateSql.push(aggregateExpression);
      const alias = this.queryAlias(measure.alias, aggregation ? `${aggregation}_${fieldName}` : fieldName);
      const aliasKey = alias.toUpperCase();
      if (expressions.has(aliasKey)) throw new Error(`Duplicate Oracle query alias: ${alias}`);
      expressions.set(aliasKey, aggregateExpression);
      expressionTypes.set(aliasKey, fieldType);
      selectSql.push(`${aggregateExpression} AS ${quotedIdentifier(alias)}`);
    }

    for (const dimension of dimensions) {
      const field = isFilter(dimension.field)
        ? this.compileExpression(dimension.field as OracleExpression, relations, joins, joinSql, expressionBinds)
        : this.queryFieldExpression(dimension.field, relations, joins, joinSql);
      const expression = this.queryDimensionExpression(
        field.expression,
        dimension.type || field.type,
        dimension.format,
        options.timezone,
      );
      const alias = this.queryAlias(dimension.alias, field.name);
      const aliasKey = alias.toUpperCase();
      if (expressions.has(aliasKey)) throw new Error(`Duplicate Oracle query alias: ${alias}`);
      expressions.set(aliasKey, expression);
      expressionTypes.set(aliasKey, dimension.type || field.type);
      selectSql.push(`${expression} AS ${quotedIdentifier(alias)}`);
      if (hasAggregate) groupSql.push(expression);
    }
    if (hasAggregate) groupSql.unshift(...nonAggregateSql);

    const compiledFilter = this.compileFilter(options.filter, 'query_filter');
    const compiledHaving = options.having
      ? compileOracleFilter(
          options.having,
          new Set(expressions.keys()),
          [],
          expressions,
          expressionTypes,
          'query_having',
        )
      : undefined;
    const binds = {
      ...expressionBinds,
      ...(compiledFilter?.binds || {}),
      ...(compiledHaving?.binds || {}),
    };
    const where = compiledFilter ? ` WHERE ${compiledFilter.predicate}` : '';
    const groupBy = groupSql.length ? ` GROUP BY ${groupSql.join(', ')}` : '';
    const having = compiledHaving ? ` HAVING ${compiledHaving.predicate}` : '';
    const orderValues = Array.isArray(options.orders) ? options.orders : [];
    const orderSql = orderValues.map((order) => {
      const aliasKey = order.alias?.toUpperCase();
      const fieldExpression = aliasKey && expressions.get(aliasKey);
      const expression =
        fieldExpression ||
        (isFilter(order.field)
          ? this.compileExpression(order.field as OracleExpression, relations, joins, joinSql, expressionBinds)
              .expression
          : this.queryFieldExpression(order.field, relations, joins, joinSql).expression);
      const direction = String(order.order || 'asc').toUpperCase();
      if (!['ASC', 'DESC'].includes(direction)) throw new Error(`Invalid Oracle query order: ${order.order}`);
      const nulls = order.nulls && order.nulls !== 'default' ? ` NULLS ${order.nulls.toUpperCase()}` : '';
      if (nulls && ![' NULLS FIRST', ' NULLS LAST'].includes(nulls)) throw new Error(`Invalid Oracle query null order`);
      return `${expression} ${direction}${nulls}`;
    });
    Object.assign(binds, expressionBinds);
    const order = orderSql.length ? ` ORDER BY ${orderSql.join(', ')}` : '';
    let pagination = '';
    if (options.limit !== undefined || dimensions.length || !hasAggregate) {
      binds.query_offset = nonNegativeInteger(options.offset);
      binds.query_limit = positiveInteger(options.limit, DEFAULT_QUERY_LIMIT);
      pagination = ' LIMIT @query_limit OFFSET @query_offset';
    }
    const sql = `SELECT ${selectSql.join(', ')} FROM ${this.tableReference()} ${ROOT_ALIAS}${joinSql.join(
      '',
    )}${where}${groupBy}${having}${order}${pagination}`;
    return this.collection.collectionManager.dataSource.execute(sql, binds);
  }

  async findRelated(field: FieldOptions, sourceRecord: OracleRow, options: OracleFindOptions = {}) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return [];
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target as string,
    ) as unknown as OracleRepository;
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.find({ ...options, relationPredicate });
  }

  async countRelated(field: FieldOptions, sourceRecord: OracleRow, options: OracleFindOptions = {}) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return 0;
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target as string,
    ) as unknown as OracleRepository;
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.count({ ...options, relationPredicate });
  }

  async aggregateRelated(field: FieldOptions, sourceRecord: OracleRow, options: OracleAggregateOptions) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return 0;
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target as string,
    ) as unknown as OracleRepository;
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.aggregate({ ...options, relationPredicate });
  }

  private relatedPredicate(relation: OracleFilterRelation, sourceRecord: OracleRow) {
    const getValue = (key: string) => {
      const actual = Object.keys(sourceRecord).find((candidate) => candidate.toUpperCase() === key.toUpperCase());
      return actual ? sourceRecord[actual] : undefined;
    };
    if (relation.arraySourceKey) {
      let values = getValue(relation.arraySourceKey);
      if (typeof values === 'string') {
        try {
          values = JSON.parse(values);
        } catch {
          values = [];
        }
      }
      if (!Array.isArray(values)) return { sql: '1 = 0', binds: {} };
      const scalarValues = values.filter((value) => isScalar(value) && value !== null);
      if (!scalarValues.length) return { sql: '1 = 0', binds: {} };
      const binds = Object.fromEntries(scalarValues.map((value, index) => [`relation_value_${index}`, value]));
      return {
        sql: `${ROOT_ALIAS}.${quotedIdentifier(relation.targetKeys[0])} IN (${scalarValues
          .map((_, index) => `@relation_value_${index}`)
          .join(', ')})`,
        binds,
      };
    }
    const sourceValues = relation.sourceKeys.map((key) => getValue(key));
    if (sourceValues.some((value) => value === undefined || value === null || !isScalar(value))) {
      return { sql: '1 = 0', binds: {} };
    }
    const binds = Object.fromEntries(sourceValues.map((value, index) => [`relation_source_${index}`, value]));
    if (!relation.through) {
      return {
        sql: relation.targetKeys
          .map((key, index) => `${ROOT_ALIAS}.${quotedIdentifier(key)} = @relation_source_${index}`)
          .join(' AND '),
        binds,
      };
    }
    const throughAlias = quotedIdentifier('relation_through');
    const throughPredicates = relation.through.sourceKeys.map(
      (key, index) => `${throughAlias}.${quotedIdentifier(key)} = @relation_source_${index}`,
    );
    const targetPredicates = relation.targetKeys.map(
      (key, index) =>
        `${ROOT_ALIAS}.${quotedIdentifier(key)} = ${throughAlias}.${quotedIdentifier(
          relation.through?.targetKeys[index] as string,
        )}`,
    );
    return {
      sql: `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
        relation.through.tableName,
      )} ${throughAlias} WHERE ${[...throughPredicates, ...targetPredicates].join(' AND ')})`,
      binds,
    };
  }

  async create(options: { values?: Record<string, unknown> }) {
    const values = this.writeValues(options.values || {});
    if (!values.length) throw new Error('No writable PostgreSQL field values were provided');
    const binds = Object.fromEntries(values.map(([name, value], index) => [`value_${index}`, value]));
    await this.collection.collectionManager.dataSource.execute(
      `INSERT INTO ${this.tableReference()} (${values
        .map(([name]) => quotedIdentifier(name))
        .join(', ')}) VALUES (${values.map((_, index) => `@value_${index}`).join(', ')})`,
      binds,
    );
    return Object.fromEntries(values);
  }

  async update(options: { filterByTk?: unknown; filter?: Record<string, unknown>; values?: Record<string, unknown> }) {
    const values = this.writeValues(options.values || {}, true);
    if (!values.length) throw new Error('No writable PostgreSQL field values were provided');
    const { predicate, binds } = this.recordKeyPredicate(options);
    for (const [index, [, value]] of values.entries()) binds[`value_${index}`] = value;
    await this.collection.collectionManager.dataSource.execute(
      `UPDATE ${this.tableReference()} SET ${values
        .map(([name], index) => `${quotedIdentifier(name)} = @value_${index}`)
        .join(', ')} WHERE ${predicate}`,
      binds,
    );
    return { ...Object.fromEntries(values), ...(options.filter || {}) };
  }

  async destroy(options: { filterByTk?: unknown; filter?: Record<string, unknown> }) {
    const { predicate, binds } = this.recordKeyPredicate(options);
    await this.collection.collectionManager.dataSource.execute(
      `DELETE FROM ${this.tableReference()} WHERE ${predicate}`,
      binds,
    );
    return true;
  }

  private writeValues(values: Record<string, unknown>, excludeRecordKey = false) {
    const collection = this.collection.options;
    if (collection.view) throw new Error(`PostgreSQL view ${collection.name} is read-only`);
    const fieldAliases = this.persistedFieldAliases();
    const recordKey = new Set(this.recordKeyFields());
    return Object.entries(values).flatMap(([name, value]) => {
      const physical = fieldAliases.get(name.toUpperCase());
      if (
        !physical ||
        value === undefined ||
        typeof value === 'object' ||
        (excludeRecordKey && recordKey.has(physical))
      ) {
        return [];
      }
      return [[physical, value] as [string, unknown]];
    });
  }

  private recordKeyPredicate(options: { filterByTk?: unknown; filter?: Record<string, unknown> }) {
    const collection = this.collection.options;
    const recordKey = this.recordKeyFields();
    if (!recordKey.length) {
      throw new Error(`PostgreSQL collection ${collection.name} requires a configured Record Unique Key for writes`);
    }
    const targetFilter = options.filterByTk === undefined ? undefined : this.filterByTargetKey(options.filterByTk);
    if (recordKey.length === 1) {
      const key = recordKey[0];
      const value = targetFilter?.[key] ?? options.filter?.[key];
      if (value === undefined || value === null || typeof value === 'object') {
        throw new Error(`PostgreSQL ${collection.name} update and delete require the ${key} Record Unique Key`);
      }
      const predicate = `${quotedIdentifier(key)} = @record_key`;
      return this.withCallerFilter(predicate, { record_key: value }, options.filter);
    }
    const binds: Record<string, unknown> = {};
    const predicate = recordKey
      .map((key, index) => {
        const value = targetFilter?.[key] ?? options.filter?.[key];
        if (value === undefined || value === null || typeof value === 'object') {
          throw new Error(`PostgreSQL ${collection.name} update and delete require the ${key} Record Unique Key`);
        }
        const bind = `record_key_${index}`;
        binds[bind] = value;
        return `${quotedIdentifier(key)} = @${bind}`;
      })
      .join(' AND ');
    return this.withCallerFilter(predicate, binds, options.filter);
  }

  private withCallerFilter(predicate: string, binds: Record<string, unknown>, filter?: Filter) {
    if (!filter || !Object.keys(filter).length) return { predicate, binds };
    const knownFields = new Set(
      (this.collection.options.fields || [])
        .filter((field) => !RELATION_TYPES.has(field.type))
        .map((field) => String(field.field || field.name)),
    );
    const compiled = compileOracleFilter(
      filter,
      knownFields,
      this.filterRelations(),
      new Map(),
      new Map(),
      'filter',
      '',
      this.persistedFieldAliases(),
    );
    const renamedBinds: Record<string, unknown> = {};
    let callerPredicate = compiled.predicate;
    for (const [name, value] of Object.entries(compiled.binds)) {
      const renamed = `caller_${name}`;
      renamedBinds[renamed] = value;
      callerPredicate = callerPredicate.replaceAll(`@${name}`, `@${renamed}`);
    }
    return { predicate: `(${predicate}) AND (${callerPredicate})`, binds: { ...binds, ...renamedBinds } };
  }

  private tableReference() {
    const collection = this.collection.options;
    assertSafeObjectName(collection.tableName);
    return `${quotedIdentifier(collection.schema || 'public')}.${quotedIdentifier(collection.tableName)}`;
  }

  private relationFields(appends: unknown) {
    const requested = Array.isArray(appends) ? appends : typeof appends === 'string' ? [appends] : [];
    const fields = new Map(
      (this.collection.options.fields || [])
        .filter((field) => RELATION_TYPES.has(field.type))
        .map((field) => [field.name.toUpperCase(), field]),
    );
    return requested
      .map((append) => String(append).split('.')[0])
      .map((name) => fields.get(name.toUpperCase()))
      .filter((field): field is FieldOptions => Boolean(field));
  }

  private relationSourceKeys(field: FieldOptions) {
    if (field.type === 'belongsToArray') return [field.foreignKey || field.sourceKey].filter(Boolean).map(String);
    return Array.isArray(field.externalSourceKeys)
      ? field.externalSourceKeys.map(String)
      : [field.sourceKey].filter(Boolean).map(String);
  }

  private filterRelations(): OracleFilterRelation[] {
    const manager = this.collection.collectionManager;
    const schema = this.collection.options.schema || 'public';
    return (this.collection.options.fields || []).flatMap((field) => {
      if (!RELATION_TYPES.has(field.type) || !field.target) return [];
      let target: { options?: CollectionOptions } | undefined;
      try {
        target = manager.getCollection(field.target) as { options?: CollectionOptions } | undefined;
      } catch {
        target = undefined;
      }
      const targetOptions = target?.options;
      const sourceFields = this.persistedFieldNames();
      const sourceKeys = this.relationSourceKeys(field);
      const targetKeys = (
        Array.isArray(field.externalTargetKeys) ? field.externalTargetKeys : [field.targetKey].filter(Boolean)
      ).map(String);
      const targetFields = new Set(
        (targetOptions?.fields || [])
          .filter((item) => !RELATION_TYPES.has(item.type))
          .flatMap((item) => [String(item.name), String(item.field || item.name)]),
      );
      const targetFieldTypes = new Map(
        (targetOptions?.fields || [])
          .filter((item) => !RELATION_TYPES.has(item.type))
          .flatMap((item) => [
            [String(item.name).toUpperCase(), item.type] as const,
            [String(item.field || item.name).toUpperCase(), item.type] as const,
          ]),
      );
      const targetFieldAliases = new Map(
        (targetOptions?.fields || [])
          .filter((item) => !RELATION_TYPES.has(item.type))
          .flatMap((item) => {
            const physical = String(item.field || item.name);
            return [[String(item.name).toUpperCase(), physical] as const, [physical.toUpperCase(), physical] as const];
          }),
      );
      const targetFieldNames = new Map(
        (targetOptions?.fields || [])
          .filter((item) => !RELATION_TYPES.has(item.type))
          .map((item) => [String(item.field || item.name).toUpperCase(), String(item.name)] as const),
      );
      const arraySourceKey = field.type === 'belongsToArray' ? sourceKeys[0] : undefined;
      let through: OracleRelationThrough | undefined;
      if (field.type === 'belongsToMany') {
        const throughName = String(field.through || '').trim();
        let throughCollection: { options?: CollectionOptions } | undefined;
        if (throughName) {
          try {
            throughCollection = manager.getCollection(throughName) as { options?: CollectionOptions } | undefined;
          } catch {
            throughCollection = undefined;
          }
        }
        const throughOptions = throughCollection?.options;
        const configuredSourceKeys = fieldOption(field, 'externalThroughSourceKeys');
        const configuredTargetKeys = fieldOption(field, 'externalThroughTargetKeys');
        const throughSourceKeys = Array.isArray(configuredSourceKeys)
          ? configuredSourceKeys.map(String)
          : [field.foreignKey].filter(Boolean).map(String);
        const throughTargetKeys = Array.isArray(configuredTargetKeys)
          ? configuredTargetKeys.map(String)
          : [field.otherKey].filter(Boolean).map(String);
        const throughFields = new Set(
          (throughOptions?.fields || [])
            .filter((item) => !RELATION_TYPES.has(item.type))
            .map((item) => String(item.field || item.name)),
        );
        if (
          throughOptions?.tableName &&
          throughSourceKeys.length === sourceKeys.length &&
          throughTargetKeys.length === targetKeys.length &&
          throughSourceKeys.every((key) => resolveFieldName(key, throughFields)) &&
          throughTargetKeys.every((key) => resolveFieldName(key, throughFields))
        ) {
          through = {
            schema: throughOptions.schema || schema,
            tableName: throughOptions.tableName,
            sourceKeys: throughSourceKeys.map((key) => resolveFieldName(key, throughFields) as string),
            targetKeys: throughTargetKeys.map((key) => resolveFieldName(key, throughFields) as string),
          };
        }
      }
      if (
        !targetOptions ||
        !targetOptions.tableName ||
        (!arraySourceKey && !sourceKeys.length) ||
        sourceKeys.length !== targetKeys.length ||
        (!arraySourceKey && sourceKeys.some((key) => !resolveFieldName(key, sourceFields))) ||
        targetKeys.some((key) => !resolveFieldName(key, targetFields)) ||
        (field.type === 'belongsToMany' && !through)
      ) {
        return [];
      }
      const resolvedSourceKeys = sourceKeys.map((key) => resolveFieldName(key, sourceFields) || key);
      const resolvedTargetKeys = targetKeys.map((key) => resolveFieldName(key, targetFields) || key);
      return [
        {
          name: field.name,
          type: field.type,
          sourceTableName: this.collection.options.tableName,
          schema: targetOptions.schema || schema,
          tableName: targetOptions.tableName,
          sourceKeys: resolvedSourceKeys,
          targetKeys: resolvedTargetKeys,
          targetFields,
          targetFieldTypes,
          targetFieldAliases,
          targetFieldNames,
          through,
          arraySourceKey,
        },
      ];
    });
  }

  private async appendRelations(records: OracleRow[], fields: FieldOptions[], appends?: unknown) {
    const requested = Array.isArray(appends) ? appends.map(String) : typeof appends === 'string' ? [appends] : [];
    for (const field of fields) {
      const target = this.collection.collectionManager.getRepository(
        `${this.collection.options.name}.${field.name}`,
        undefined,
      ) as unknown as OracleRelationRepository;
      for (const record of records) {
        const values = this.relationSourceKeys(field).map((key) => record[key]);
        if (field.type !== 'belongsToArray' && values.some((value) => value === null || value === undefined)) {
          record[field.name] =
            field.type === 'hasMany' || field.type === 'belongsToMany' || field.type === 'belongsToArray' ? [] : null;
          continue;
        }
        const relation = new OracleRelationRepository(target.parent, target.target, field, record);
        record[field.name] =
          field.type === 'hasMany' || field.type === 'belongsToMany' || field.type === 'belongsToArray'
            ? await relation.find()
            : await relation.findOne();
        const nested = requested
          .filter((append) => append.split('.')[0].toUpperCase() === field.name.toUpperCase() && append.includes('.'))
          .map((append) => append.split('.').slice(1).join('.'));
        if (nested.length && record[field.name]) {
          const relatedRecords = Array.isArray(record[field.name]) ? record[field.name] : [record[field.name]];
          const targetRepository = target.target.repository;
          const nestedFields = targetRepository.relationFields(nested);
          await targetRepository.appendRelations(relatedRecords as OracleRow[], nestedFields, nested);
        }
      }
    }
  }
}

class OracleRelationRepository implements IRepository {
  public collection: { options: CollectionOptions };

  constructor(
    public parent: { options: CollectionOptions; repository: OracleRepository },
    public target: { options: CollectionOptions; repository: OracleRepository },
    private field: FieldOptions,
    private sourceId: unknown,
  ) {
    // NocoBase list actions read repository.collection.options for pagination and tree metadata.
    this.collection = { options: target.options };
  }

  async find(options: OracleFindOptions = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return [];
    if (this.field.type === 'belongsToMany' || this.field.type === 'belongsToArray') {
      return this.parent.repository.findRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return [];
    return this.target.repository.find({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter),
    });
  }

  async findOne(options: OracleFindOptions = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return null as IModel;
    if (this.field.type === 'belongsToMany' || this.field.type === 'belongsToArray') {
      const records = await this.parent.repository.findRelated(this.field, source, { ...options, limit: 1 });
      return (records[0] || null) as IModel;
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return null as IModel;
    return this.target.repository.findOne({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter),
    });
  }

  async count(options: OracleFindOptions = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return 0;
    if (this.field.type === 'belongsToMany' || this.field.type === 'belongsToArray') {
      return this.parent.repository.countRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return 0;
    return this.target.repository.count({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter),
    });
  }

  async aggregate(options: OracleAggregateOptions) {
    const source = await this.sourceRecord(options.context);
    if (!source) return 0;
    if (this.field.type === 'belongsToMany' || this.field.type === 'belongsToArray') {
      return this.parent.repository.aggregateRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return 0;
    return this.target.repository.aggregate({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter),
    });
  }

  async findAndCount(options: OracleFindOptions = {}) {
    return [await this.find(options), await this.count(options)] as [IModel[], Number];
  }

  private mergeFilter(filter: Filter | undefined, relationFilter: Filter): Filter {
    return filter && Object.keys(filter).length ? { $and: [filter, relationFilter] } : relationFilter;
  }

  private relationFilterFromSource(source: Record<string, unknown>): Record<string, unknown> | undefined {
    const sourceKeys = Array.isArray(this.field.externalSourceKeys)
      ? this.field.externalSourceKeys
      : [this.field.sourceKey].filter(Boolean);
    const targetKeys = Array.isArray(this.field.externalTargetKeys)
      ? this.field.externalTargetKeys
      : [this.field.targetKey].filter(Boolean);
    if (!source || !sourceKeys.length || sourceKeys.length !== targetKeys.length) return undefined;
    const knownTargetFields = new Set(
      (this.target.options.fields || [])
        .filter((field) => !RELATION_TYPES.has(field.type))
        .map((field) => field.field || field.name),
    );
    const filterEntries = targetKeys.map((targetKey, index) => [targetKey, source[sourceKeys[index]]] as const);
    if (
      filterEntries.some(
        ([targetKey, value]) =>
          !targetKey ||
          !knownTargetFields.has(targetKey) ||
          value === null ||
          value === undefined ||
          typeof value === 'object',
      )
    ) {
      return undefined;
    }
    return Object.fromEntries(filterEntries);
  }

  private async sourceRecord(context?: Context): Promise<Record<string, unknown> | undefined> {
    if (this.sourceId !== null && typeof this.sourceId === 'object' && !Array.isArray(this.sourceId)) {
      return this.sourceId as Record<string, unknown>;
    }
    const keys = Array.isArray(this.parent.options.filterTargetKey)
      ? this.parent.options.filterTargetKey
      : [this.parent.options.filterTargetKey].filter(Boolean);
    const referer = context?.request?.headers?.referer || '';
    const routeKey = /\/filterbytk\/([^/?#]+)/.exec(referer)?.[1];
    const sourceId = routeKey ?? this.sourceId;
    // Composite target keys arrive from associated URLs as a URI-encoded JSON object or array.
    const decodedSourceId = typeof sourceId === 'string' ? this.parseCompositeSourceId(sourceId) : sourceId;
    if (decodedSourceId !== null && typeof decodedSourceId === 'object' && !Array.isArray(decodedSourceId)) {
      return decodedSourceId as Record<string, unknown>;
    }
    const values = Array.isArray(decodedSourceId) ? decodedSourceId : [decodedSourceId];
    if (
      !keys.length ||
      keys.length !== values.length ||
      values.some((value) => value === null || value === undefined || typeof value === 'object')
    ) {
      return undefined;
    }
    const filter = Object.fromEntries(keys.map((key, index) => [key, values[index]]));
    return await this.parent.repository.findOne({ filter });
  }

  private parseCompositeSourceId(sourceId: string) {
    try {
      const parsed = JSON.parse(decodeURIComponent(sourceId));
      return Array.isArray(parsed) || (parsed && typeof parsed === 'object') ? parsed : sourceId;
    } catch {
      return sourceId;
    }
  }

  async create(
    options: { values?: Record<string, unknown>; context?: { request?: { headers?: Record<string, string> } } } = {},
  ) {
    const source = await this.sourceRecord(options.context as unknown as Context);
    const relationFilter = source ? this.relationFilterFromSource(source) : undefined;
    if (!relationFilter || this.field.type !== 'hasMany') {
      throw new Error(`Oracle relation ${this.field.name} does not support create`);
    }
    return this.target.repository.create({ values: { ...options.values, ...relationFilter } });
  }

  async update(
    options: {
      filterByTk?: unknown;
      filter?: Record<string, unknown>;
      values?: Record<string, unknown>;
      context?: { request?: { headers?: Record<string, string> } };
    } = {},
  ) {
    const source = await this.sourceRecord(options.context as unknown as Context);
    const relationFilter = source ? this.relationFilterFromSource(source) : undefined;
    if (!relationFilter || this.field.type !== 'hasMany') {
      throw new Error(`Oracle relation ${this.field.name} does not support update`);
    }
    return this.target.repository.update({ ...options, filter: this.mergeFilter(options.filter, relationFilter) });
  }

  async destroy(
    options: {
      filterByTk?: unknown;
      filter?: Record<string, unknown>;
      context?: { request?: { headers?: Record<string, string> } };
    } = {},
  ) {
    const source = await this.sourceRecord(options.context as unknown as Context);
    const relationFilter = source ? this.relationFilterFromSource(source) : undefined;
    if (!relationFilter || this.field.type !== 'hasMany') {
      throw new Error(`Oracle relation ${this.field.name} does not support delete`);
    }
    return this.target.repository.destroy({ ...options, filter: this.mergeFilter(options.filter, relationFilter) });
  }
}

class OracleCollectionManager extends CollectionManager {
  public readonly db = {
    getRepository: (name: string, sourceId?: unknown) => this.getRepository(name, sourceId),
    getCollection: (name: string) => this.getCollection(name),
    getCollections: () => this.getCollections(),
    getFieldByPath: (path: string) => {
      const [collectionName, ...fieldPath] = path.split('.');
      const collection = this.getCollection(collectionName);
      return fieldPath.length ? collection?.getField(fieldPath.join('.')) : undefined;
    },
  };

  constructor() {
    super();
    this.registerRepositories({ Repository: OracleRepository });
  }

  getRepository(name: string, sourceId?: unknown): IRepository {
    const [parentName, relationName] = name.split('.');
    if (!relationName) return super.getRepository(name, sourceId as never);
    const parent = this.getCollection(parentName) as unknown as
      | { options: CollectionOptions; repository: OracleRepository }
      | undefined;
    const field = parent?.options.fields?.find(
      (item) => item.name.toUpperCase() === relationName.toUpperCase() && RELATION_TYPES.has(item.type),
    );
    if (!parent || !field || !field.target) throw new Error(`Unknown Oracle relation: ${name}`);
    const target = this.getCollection(field.target) as unknown as
      | { options: CollectionOptions; repository: OracleRepository }
      | undefined;
    if (!target) throw new Error(`Unknown Oracle relation target: ${field.target}`);
    return new OracleRelationRepository(parent, target, field, sourceId);
  }

  getCollection(name: string) {
    const [parentName, relationName] = name.split('.');
    if (!relationName) return super.getCollection(name);
    const parent = super.getCollection(parentName) as { options?: CollectionOptions } | undefined;
    const field = parent?.options?.fields?.find(
      (item) => item.name.toUpperCase() === relationName.toUpperCase() && RELATION_TYPES.has(item.type),
    );
    return field?.target ? super.getCollection(field.target) : undefined;
  }
}

export class OracleDataSource extends DatabaseDataSource {
  static async testConnection(options: PostgresqlOptions) {
    await testPostgresqlConnection(options);
    return true;
  }

  createCollectionManager() {
    return new OracleCollectionManager();
  }

  async load(options: { localData?: LoadedCollections } = {}) {
    for (const collection of Object.values(options.localData || {})) {
      this.collectionManager.defineCollection(collection as CollectionOptions);
    }
  }

  async execute(sql: string, binds?: Record<string, unknown>, _options: Record<string, unknown> = {}) {
    const { connection } = await this.connect();
    try {
      const names = Object.keys(binds || {});
      const query = sql.replace(/@([A-Za-z_][A-Za-z0-9_]*)/g, (_match, name: string) => {
        const index = names.indexOf(name);
        if (index < 0) throw new Error(`Unknown PostgreSQL bind parameter: ${name}`);
        return `$${index + 1}`;
      });
      const result = await connection.query(
        query,
        names.map((name) => binds?.[name] ?? null),
      );
      return await Promise.all(
        (result.rows || []).map((value) => this.materializeRow(row<Record<string, unknown>>(value))),
      );
    } finally {
      await connection.end();
    }
  }

  private async materializeRow(value: Record<string, unknown>) {
    const entries = await Promise.all(
      Object.entries(value).map(async ([key, item]) => [key, await this.materializeValue(item)] as const),
    );
    return Object.fromEntries(entries);
  }

  private async materializeValue(value: unknown): Promise<unknown> {
    if (Buffer.isBuffer(value)) return value.toString('base64');
    if (value && typeof value === 'object' && 'getData' in value && typeof value.getData === 'function') {
      const data = await (value.getData as () => Promise<unknown>)();
      return Buffer.isBuffer(data) ? data.toString('base64') : data;
    }
    return value;
  }

  async readTables() {
    const prefix = this.options.tablePrefix?.trim();
    const schema = this.schemaName();
    const objects = await this.execute(
      `SELECT table_name AS TABLE_NAME, CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE
       FROM information_schema.tables
       WHERE table_schema = @schema AND table_type IN ('BASE TABLE', 'VIEW')
         AND (@tablePrefix IS NULL OR table_name LIKE @tablePrefix)
       ORDER BY table_name LIMIT @limit`,
      { schema, tablePrefix: prefix ? `${prefix}%` : null, limit: MAX_OBJECTS },
    );
    const names = new Set<string>();
    return objects.flatMap((value) => {
      const object = row<OracleObject>(value);
      if (UNSAFE_OBJECT_NAME.test(object.TABLE_NAME)) return [];
      if (prefix && !object.TABLE_NAME.startsWith(prefix)) return [];
      const name = prefix ? object.TABLE_NAME.slice(prefix.length) : object.TABLE_NAME;
      if (!name || names.has(name)) {
        throw new Error(`PostgreSQL table prefix creates an ambiguous collection name: ${object.TABLE_NAME}`);
      }
      names.add(name);
      return {
        name,
        tableName: object.TABLE_NAME,
        schema,
        view: object.OBJECT_TYPE === 'VIEW',
      };
    });
  }

  async loadTables(ctx: Context, tables: string[] = [], options: LoadTablesOptions = {}) {
    if (tables.length > MAX_OBJECTS) throw new Error(`A maximum of ${MAX_OBJECTS} PostgreSQL objects can be selected`);
    const selectedNames = new Set(tables.map((name) => String(name).trim()));
    if (selectedNames.size !== tables.length) throw new Error('PostgreSQL collections must be selected only once');
    for (const name of selectedNames) assertSafeObjectName(name);
    const discovered = await this.readTables();
    const selected = discovered.filter((object) => selectedNames.has(object.name));
    // Preserve support for a manually entered object if discovery is unavailable to the connected user.
    if (!discovered.length && selected.length !== selectedNames.size && !this.options.tablePrefix) {
      selected.push(
        ...(await this.readExplicitTables([...selectedNames])).filter(
          (object) => !selected.some((item) => item.name === object.name),
        ),
      );
    }
    if (selected.length !== selectedNames.size)
      throw new Error('One or more selected PostgreSQL objects are unavailable');
    const collections = await Promise.all(selected.map((object) => this.loadCollection(object)));
    await this.addForeignKeyRelations(collections);
    const merged = options.localData
      ? this.mergeWithLoadedCollections(collections, options.localData as LoadedCollections)
      : collections;
    for (const collection of merged) {
      const configured = options.localData?.[collection.name] as
        | (CollectionOptions & { recordUniqueKey?: string[] })
        | undefined;
      const fieldNames = new Set((collection.fields || []).map((field) => field.name));
      const configuredKey = configured?.filterTargetKey || configured?.recordUniqueKey;
      const automaticKey = collections.find((item) => item.name === collection.name)?.filterTargetKey;
      if (Array.isArray(configuredKey) && configuredKey.length && configuredKey.every((name) => fieldNames.has(name))) {
        collection.filterTargetKey = configuredKey;
      } else if (automaticKey) {
        collection.filterTargetKey = automaticKey;
      } else {
        delete collection.filterTargetKey;
      }
    }
    for (const collection of merged) this.collectionManager.defineCollection(collection);
    // The data-source manager persists these association values with the data source transaction.
    ctx.action.params.values.collections = merged.map(({ fields, ...collection }) => ({
      name: collection.name,
      options: collection,
      fields: fields.map((field) => ({ name: field.name, options: field })),
    }));
    ctx.body = merged;
  }

  publicOptions() {
    const { password: _password, ...safeOptions } = this.options;
    return safeOptions;
  }

  private async connect() {
    const driver = await loadPostgresqlDriver();
    return {
      driver,
      connection: await createPostgresqlConnection(this.options as PostgresqlOptions, Promise.resolve(driver)),
    };
  }

  private schemaName() {
    const schema = String(this.options.schema || 'public').trim();
    if (!IDENTIFIER.test(schema)) throw new Error('PostgreSQL schema must be a valid identifier');
    return schema;
  }

  private async readExplicitTables(names: string[]): Promise<OracleCollection[]> {
    const result: OracleCollection[] = [];
    for (const name of names) {
      const rows = await this.execute(
        "SELECT table_name AS TABLE_NAME, CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE FROM information_schema.tables WHERE table_schema = @schema AND table_name = @tableName AND table_type IN ('BASE TABLE', 'VIEW')",
        { schema: this.schemaName(), tableName: name },
      );
      const object = row<OracleObject>(rows[0]);
      if (!object?.TABLE_NAME || UNSAFE_OBJECT_NAME.test(object.TABLE_NAME)) continue;
      result.push({
        name: object.TABLE_NAME,
        tableName: object.TABLE_NAME,
        schema: this.schemaName(),
        view: object.OBJECT_TYPE === 'VIEW',
      });
    }
    return result;
  }

  private async loadCollection(object: OracleCollection): Promise<CollectionOptions> {
    const binds = { schema: object.schema, tableName: object.tableName };
    const [columns, constraintColumns] = await Promise.all([
      this.execute(
        "SELECT column_name AS COLUMN_NAME, data_type AS DATA_TYPE, COALESCE(character_maximum_length, 0) AS DATA_LENGTH, numeric_precision AS DATA_PRECISION, numeric_scale AS DATA_SCALE, CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS NULLABLE, column_default AS DATA_DEFAULT FROM information_schema.columns WHERE table_schema = @schema AND table_name = @tableName ORDER BY ordinal_position",
        binds,
      ),
      this.execute(
        "SELECT kcu.column_name AS COLUMN_NAME, kcu.constraint_name AS CONSTRAINT_NAME, CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'P' ELSE 'U' END AS CONSTRAINT_TYPE, kcu.ordinal_position AS POSITION FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name WHERE tc.table_schema = @schema AND tc.table_name = @tableName AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') ORDER BY CONSTRAINT_TYPE, kcu.constraint_name, kcu.ordinal_position",
        binds,
      ),
    ]);
    const primaryConstraints = new Map<string, ConstraintColumn[]>();
    const uniqueConstraints = new Map<string, ConstraintColumn[]>();
    for (const value of constraintColumns.map((item) => row<ConstraintColumn>(item))) {
      const constraints = value.CONSTRAINT_TYPE === 'P' ? primaryConstraints : uniqueConstraints;
      constraints.set(value.CONSTRAINT_NAME, [...(constraints.get(value.CONSTRAINT_NAME) || []), value]);
    }
    const constraintKeys = (constraints: Map<string, ConstraintColumn[]>) =>
      [...constraints.entries()]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([, columns]) =>
          columns.sort((left, right) => left.POSITION - right.POSITION).map(({ COLUMN_NAME }) => COLUMN_NAME),
        );
    const primaryKeys = constraintKeys(primaryConstraints);
    const uniqueKeys = constraintKeys(uniqueConstraints);
    const primaryColumns = new Set(primaryKeys.filter((columns) => columns.length === 1).flat());
    const uniqueColumns = new Set(uniqueKeys.filter((columns) => columns.length === 1).flat());
    const automaticUniqueKey = primaryKeys[0] || uniqueKeys[0];
    return {
      ...object,
      title: object.name,
      ...(automaticUniqueKey ? { filterTargetKey: automaticUniqueKey } : {}),
      fields: columns.map((value) => mapOracleField(row<OracleColumn>(value), primaryColumns, uniqueColumns)),
    };
  }

  private async addForeignKeyRelations(collections: CollectionOptions[]) {
    if (collections.length < 2) return;
    const collectionsByTableName = new Map(collections.map((collection) => [collection.tableName, collection]));
    const rows = await this.execute(
      'SELECT child.constraint_name AS CONSTRAINT_NAME, child.table_name AS CHILD_TABLE_NAME, child.column_name AS CHILD_COLUMN_NAME, parent.table_name AS PARENT_TABLE_NAME, parent.column_name AS PARENT_COLUMN_NAME, child.ordinal_position AS POSITION FROM information_schema.key_column_usage child JOIN information_schema.referential_constraints rc ON rc.constraint_schema = child.constraint_schema AND rc.constraint_name = child.constraint_name JOIN information_schema.key_column_usage parent ON parent.constraint_schema = rc.unique_constraint_schema AND parent.constraint_name = rc.unique_constraint_name AND parent.ordinal_position = child.position_in_unique_constraint WHERE child.table_schema = @schema ORDER BY child.constraint_name, child.ordinal_position',
      { schema: this.schemaName() },
    );
    const foreignKeys = new Map<string, ForeignKeyColumn[]>();
    for (const value of rows.map((item) => row<ForeignKeyColumn>(item))) {
      const child = collectionsByTableName.get(value.CHILD_TABLE_NAME);
      const parent = collectionsByTableName.get(value.PARENT_TABLE_NAME);
      if (!child || !parent || !value.CONSTRAINT_NAME) continue;
      foreignKeys.set(value.CONSTRAINT_NAME, [...(foreignKeys.get(value.CONSTRAINT_NAME) || []), value]);
    }
    const fieldNames = new Map(
      collections.map((collection) => [collection.name, new Set((collection.fields || []).map((field) => field.name))]),
    );
    for (const foreignKey of foreignKeys.values()) {
      const mappings = foreignKey.sort((left, right) => left.POSITION - right.POSITION);
      const [mapping] = mappings;
      const child = collectionsByTableName.get(mapping.CHILD_TABLE_NAME);
      const parent = collectionsByTableName.get(mapping.PARENT_TABLE_NAME);
      if (!child || !parent) continue;
      const childFieldNames = fieldNames.get(child.name);
      const parentFieldNames = fieldNames.get(parent.name);
      const belongsToName = this.relationFieldName(
        childFieldNames,
        mapping.CHILD_COLUMN_NAME.toUpperCase().endsWith('_ID') ? mapping.CHILD_COLUMN_NAME.slice(0, -3) : parent.name,
      );
      const hasManyName = this.relationFieldName(parentFieldNames, child.name, mapping.CHILD_COLUMN_NAME);
      child.fields?.push({
        name: belongsToName,
        field: belongsToName,
        rawType: 'relation',
        type: 'belongsTo',
        interface: 'm2o',
        target: parent.name,
        foreignKey: mapping.CHILD_COLUMN_NAME,
        sourceKey: mapping.CHILD_COLUMN_NAME,
        targetKey: mapping.PARENT_COLUMN_NAME,
        externalForeignKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalSourceKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalTargetKeys: mappings.map((item) => item.PARENT_COLUMN_NAME),
      });
      parent.fields?.push({
        name: hasManyName,
        field: hasManyName,
        rawType: 'relation',
        type: 'hasMany',
        interface: 'o2m',
        target: child.name,
        foreignKey: mapping.CHILD_COLUMN_NAME,
        sourceKey: mapping.PARENT_COLUMN_NAME,
        targetKey: mapping.CHILD_COLUMN_NAME,
        externalForeignKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalSourceKeys: mappings.map((item) => item.PARENT_COLUMN_NAME),
        externalTargetKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
      });
    }
  }

  private relationFieldName(fieldNames: Set<string> | undefined, name: string, suffix?: string) {
    if (!fieldNames?.has(name)) {
      fieldNames?.add(name);
      return name;
    }
    const uniqueName = `${name}_${suffix || 'RELATION'}`;
    fieldNames.add(uniqueName);
    return uniqueName;
  }
}

export { OracleDataSource as PostgresqlDataSource };
