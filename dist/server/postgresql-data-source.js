/**
 * This file is part of the NocoBase (R) project.
 * Copyright (c) 2020-2024 NocoBase Co., Ltd.
 * Authors: NocoBase Team.
 *
 * This project is dual-licensed under AGPL-3.0 and NocoBase Commercial License.
 * For more information, please refer to: https://www.nocobase.com/agreement.
 */

var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var postgresql_data_source_exports = {};
__export(postgresql_data_source_exports, {
  OracleDataSource: () => OracleDataSource,
  PostgresqlDataSource: () => OracleDataSource
});
module.exports = __toCommonJS(postgresql_data_source_exports);
var import_data_source_manager = require("@nocobase/data-source-manager");
var import_postgresql = require("./postgresql");
const MAX_OBJECTS = 500;
const DEFAULT_PAGE_SIZE = 50;
const DEFAULT_QUERY_LIMIT = 2e3;
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$#]*$/;
const UNSAFE_OBJECT_NAME = /^(pg_|sql_|information_schema$)/i;
const ROOT_ALIAS = "postgresql_root";
const RELATION_TYPES = /* @__PURE__ */ new Set(["belongsTo", "belongsToMany", "belongsToArray", "hasMany", "hasOne"]);
function row(value) {
  return value;
}
function quotedIdentifier(identifier) {
  if (!IDENTIFIER.test(identifier)) throw new Error(`Invalid PostgreSQL identifier: ${identifier}`);
  return `"${identifier}"`;
}
function assertSafeObjectName(name) {
  if (!IDENTIFIER.test(name) || UNSAFE_OBJECT_NAME.test(name)) {
    throw new Error("PostgreSQL system objects and invalid object names cannot be used as collections");
  }
}
function isFilter(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
function assignNestedFilter(target, path, value) {
  let cursor = target;
  path.forEach((part, index) => {
    if (index === path.length - 1) {
      const existing = cursor[part];
      cursor[part] = isFilter(existing) && isFilter(value) ? { ...existing, ...value } : value;
      return;
    }
    if (!isFilter(cursor[part])) cursor[part] = {};
    cursor = cursor[part];
  });
}
function normalizeOracleFilter(filter) {
  const normalized = {};
  for (const [key, value] of Object.entries(filter)) {
    if (key === "$and" || key === "$or") {
      normalized[key] = Array.isArray(value) ? value.map((item) => isFilter(item) ? normalizeOracleFilter(item) : item) : value;
      continue;
    }
    const parts = key.split(".").filter(Boolean);
    if (parts.length > 1) {
      assignNestedFilter(normalized, parts, value);
    } else {
      normalized[key] = isFilter(value) ? normalizeOracleFilter(value) : value;
    }
  }
  return normalized;
}
function fieldOption(field, key) {
  return field[key];
}
function isScalar(value) {
  return value === null || typeof value === "string" || typeof value === "boolean" || typeof value === "number" && Number.isFinite(value) || value instanceof Date;
}
function resolveFieldName(field, knownFields) {
  return [...knownFields].find((candidate) => candidate.toUpperCase() === field.toUpperCase());
}
function normalizeFieldPath(field) {
  const path = Array.isArray(field) ? field : field.split(".");
  return path.map((part) => String(part).trim()).filter(Boolean);
}
function parseDateValue(value, operator) {
  if (value instanceof Date) {
    if (!Number.isNaN(value.getTime())) return value;
  } else if (typeof value === "string" || typeof value === "number") {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  throw new Error(`Invalid date for Oracle filter operator ${operator}`);
}
function dateRange(value, operator) {
  if (typeof value !== "string") return void 0;
  const match = /^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$/.exec(value);
  if (!match) return void 0;
  const year = Number(match[1]);
  const month = match[2] ? Number(match[2]) - 1 : 0;
  const day = match[3] ? Number(match[3]) : 1;
  const start = new Date(Date.UTC(year, month, day));
  const end = match[3] ? new Date(Date.UTC(year, month, day + 1)) : match[2] ? new Date(Date.UTC(year, month + 1, 1)) : new Date(Date.UTC(year + 1, 0, 1));
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error(`Invalid date for Oracle filter operator ${operator}`);
  }
  return { start, end };
}
function formatDatePattern(format) {
  if (!format || format.length > 100 || /[';]/.test(format)) {
    throw new Error("Invalid PostgreSQL date format");
  }
  return format.replace(/hh/g, "HH24").replace(/mm/g, "MI").replace(/ss/g, "SS");
}
function isNonPaginated(options) {
  var _a, _b, _c;
  if (options.limit !== void 0) return false;
  const paginate = options.paginate ?? ((_c = (_b = (_a = options.context) == null ? void 0 : _a.action) == null ? void 0 : _b.params) == null ? void 0 : _c.paginate);
  return paginate === false || paginate === "false";
}
function positiveInteger(value, fallback) {
  if (value === void 0 || value === null || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1)
    throw new Error("Oracle pagination values must be positive integers");
  return parsed;
}
function nonNegativeInteger(value, fallback = 0) {
  if (value === void 0 || value === null || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) throw new Error("Oracle offset must be a non-negative integer");
  return parsed;
}
function compileOracleFilter(filter, knownFields, relations = [], fieldExpressions = /* @__PURE__ */ new Map(), fieldTypes = /* @__PURE__ */ new Map(), bindPrefix = "filter", rootAlias = ROOT_ALIAS, fieldAliases = /* @__PURE__ */ new Map()) {
  const normalizedFilter = normalizeOracleFilter(filter);
  const binds = {};
  let bindIndex = 0;
  const bind = (value) => {
    const name = `${bindPrefix}_${bindIndex++}`;
    binds[name] = value;
    return `@${name}`;
  };
  const isDateField = (fieldType) => Boolean(fieldType && ["date", "datetime", "datetimeTz", "datetimeNoTz", "dateOnly", "time"].includes(fieldType));
  const normalizeScalar = (value, operator, fieldType) => isDateField(fieldType) && value !== null && operator !== "$like" && operator !== "$notLike" ? parseDateValue(value, operator) : value;
  const scalarPredicate = (column, operator, value, fieldType) => {
    if (!isScalar(value)) throw new Error(`Invalid value for Oracle filter operator ${operator}`);
    const normalized = normalizeScalar(value, operator, fieldType);
    if (operator === "$eq") return normalized === null ? `${column} IS NULL` : `${column} = ${bind(normalized)}`;
    if (operator === "$ne") {
      return normalized === null ? `${column} IS NOT NULL` : `(${column} <> ${bind(normalized)} OR ${column} IS NULL)`;
    }
    if (operator === "$gt") return normalized === null ? "1 = 0" : `${column} > ${bind(normalized)}`;
    if (operator === "$gte") return normalized === null ? "1 = 0" : `${column} >= ${bind(normalized)}`;
    if (operator === "$lt") return normalized === null ? "1 = 0" : `${column} < ${bind(normalized)}`;
    if (operator === "$lte") return normalized === null ? "1 = 0" : `${column} <= ${bind(normalized)}`;
    if (operator === "$like" || operator === "$notLike") {
      if (normalized === null || typeof normalized !== "string") {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const comparison = operator === "$like" ? "LIKE" : "NOT LIKE";
      return `${column} ${comparison} ${bind(normalized)}`;
    }
    if (operator === "$startsWith" || operator === "$notStartsWith") {
      if (normalized === null || typeof normalized !== "string") {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const comparison = operator === "$startsWith" ? "> 0" : "= 0";
      return `STRPOS(${column}, ${bind(normalized)}) ${comparison}`;
    }
    if (operator === "$endWith" || operator === "$notEndWith") {
      if (normalized === null || typeof normalized !== "string") {
        throw new Error(`Oracle filter operator ${operator} requires a string value`);
      }
      const pattern = operator === "$endWith" ? `%${normalized}` : `%${normalized}`;
      const comparison = operator === "$endWith" ? "LIKE" : "NOT LIKE";
      return `${column} ${comparison} ${bind(pattern)}`;
    }
    if (operator === "$includes")
      return normalized === null ? `${column} IS NULL` : `STRPOS(${column}, ${bind(normalized)}) > 0`;
    if (operator === "$notIncludes") {
      return normalized === null ? `${column} IS NOT NULL` : `STRPOS(${column}, ${bind(normalized)}) = 0`;
    }
    throw new Error(`Unsupported Oracle filter operator: ${operator}`);
  };
  const listPredicate = (column, operator, value, fieldType) => {
    const values = Array.isArray(value) ? value : [value];
    if (values.some((item) => !isScalar(item))) {
      throw new Error(`Oracle filter operator ${operator} requires an array of scalar values`);
    }
    if (operator === "$includes" || operator === "$notIncludes") {
      if (!values.length) return operator === "$includes" ? "1 = 0" : "1 = 1";
      const predicates = values.map((item) => scalarPredicate(column, operator, item));
      return `(${predicates.join(operator === "$includes" ? " OR " : " AND ")})`;
    }
    const nonNullValues = values.filter((item) => item !== null).map((item) => normalizeScalar(item, operator, fieldType));
    const nullPredicate = operator === "$notIn" && values.length > 0 || values.includes(null) ? `${column} IS NULL` : void 0;
    const list = nonNullValues.length ? `${column} ${operator === "$in" ? "IN" : "NOT IN"} (${nonNullValues.map(bind).join(", ")})` : void 0;
    if (!list) return nullPredicate || (operator === "$in" ? "1 = 0" : "1 = 1");
    return nullPredicate ? `(${list} OR ${nullPredicate})` : list;
  };
  const rangePredicate = (column, operator, value, fieldType) => {
    if (!Array.isArray(value) || value.length !== 2 || value.some((item) => !isScalar(item) || item === null)) {
      throw new Error(`Oracle filter operator ${operator} requires two scalar values`);
    }
    const [left, right] = value.map((item) => normalizeScalar(item, operator, fieldType));
    if (operator === "$between") return `(${column} >= ${bind(left)} AND ${column} <= ${bind(right)})`;
    return `(${column} < ${bind(left)} OR ${column} > ${bind(right)} OR ${column} IS NULL)`;
  };
  const datePredicate = (column, operator, value) => {
    const values = Array.isArray(value) ? value : [value];
    if (operator === "$dateOn" || operator === "$dateNotOn") {
      if (values.length === 2) {
        const start = parseDateValue(values[0], operator);
        const end = parseDateValue(values[1], operator);
        return operator === "$dateOn" ? `(${column} >= ${bind(start)} AND ${column} < ${bind(end)})` : `(${column} < ${bind(start)} OR ${column} >= ${bind(end)} OR ${column} IS NULL)`;
      }
      const date2 = parseDateValue(values[0], operator);
      const range = dateRange(values[0], operator);
      if (range) {
        return operator === "$dateOn" ? `(${column} >= ${bind(range.start)} AND ${column} < ${bind(range.end)})` : `(${column} < ${bind(range.start)} OR ${column} >= ${bind(range.end)} OR ${column} IS NULL)`;
      }
      return operator === "$dateOn" ? `${column} = ${bind(date2)}` : `(${column} <> ${bind(date2)} OR ${column} IS NULL)`;
    }
    const date = parseDateValue(values[0], operator);
    if (operator === "$dateBefore") return `${column} < ${bind(date)}`;
    if (operator === "$dateNotBefore") return `${column} >= ${bind(date)}`;
    if (operator === "$dateAfter") return `${column} > ${bind(date)}`;
    if (operator === "$dateNotAfter") return `${column} <= ${bind(date)}`;
    if (operator === "$dateBetween") {
      if (values.length !== 2) throw new Error(`Oracle filter operator ${operator} requires two dates`);
      const end = parseDateValue(values[1], operator);
      return `(${column} >= ${bind(date)} AND ${column} < ${bind(end)})`;
    }
    throw new Error(`Unsupported Oracle date filter operator: ${operator}`);
  };
  const fieldPredicate = (field, value, column = void 0, fields = knownFields, types = fieldTypes, aliases = fieldAliases) => {
    const actualField = resolveFieldName(field, fields);
    if (!actualField) throw new Error(`Unknown Oracle filter field: ${field}`);
    const physicalField = aliases.get(actualField.toUpperCase()) || actualField;
    const expression = column || fieldExpressions.get(actualField.toUpperCase()) || `${rootAlias ? `${rootAlias}.` : ""}${quotedIdentifier(physicalField)}`;
    const fieldType = types.get(actualField.toUpperCase());
    if (isScalar(value)) return scalarPredicate(expression, "$eq", value, fieldType);
    if (!isFilter(value)) throw new Error(`Invalid Oracle filter value for ${field}`);
    const predicates = Object.entries(value).map(([operator, operand]) => {
      if (["$in", "$notIn"].includes(operator)) {
        return listPredicate(expression, operator, operand, fieldType);
      }
      if (operator === "$anyOf" || operator === "$noneOf") {
        return listPredicate(expression, operator === "$anyOf" ? "$in" : "$notIn", operand, fieldType);
      }
      if (["$includes", "$notIncludes", "$startsWith", "$notStartsWith", "$endWith", "$notEndWith"].includes(operator) && Array.isArray(operand)) {
        const values = operand;
        return `(${values.map((item) => scalarPredicate(expression, operator, item, fieldType)).join(operator.startsWith("$not") ? " AND " : " OR ")})`;
      }
      if (["$between", "$notBetween"].includes(operator))
        return rangePredicate(expression, operator, operand, fieldType);
      if (operator.startsWith("$date")) return datePredicate(expression, operator, operand);
      if (operator === "$exists") return operand === false ? `${expression} IS NULL` : `${expression} IS NOT NULL`;
      if (operator === "$notExists") return operand === false ? `${expression} IS NOT NULL` : `${expression} IS NULL`;
      if (operator === "$isTruly") return `COALESCE(${expression}, 0) <> 0`;
      if (operator === "$isFalsy") return `COALESCE(${expression}, 0) = 0`;
      if (operator === "$match" || operator === "$notMatch") {
        if (typeof operand !== "string") throw new Error(`Oracle filter operator ${operator} requires a string value`);
        return `${expression} ${operator === "$match" ? "~" : "!~"} ${bind(operand)}`;
      }
      if (operator === "$empty")
        return fieldType === "string" ? `(${expression} IS NULL OR ${expression} = '')` : `${expression} IS NULL`;
      if (operator === "$notEmpty")
        return fieldType === "string" ? `(${expression} IS NOT NULL AND ${expression} <> '')` : `${expression} IS NOT NULL`;
      if ([
        "$eq",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$like",
        "$notLike",
        "$includes",
        "$notIncludes",
        "$startsWith",
        "$notStartsWith",
        "$endWith",
        "$notEndWith"
      ].includes(operator)) {
        return scalarPredicate(expression, operator, operand, fieldType);
      }
      throw new Error(`Unsupported Oracle filter operator: ${operator}`);
    });
    if (!predicates.length) return "1 = 1";
    return predicates.length === 1 ? predicates[0] : `(${predicates.join(" AND ")})`;
  };
  const relationPredicate = (path, value) => {
    const [relationName, targetField, ...rest] = path.split(".");
    const relation = relations.find((item) => item.name.toUpperCase() === relationName.toUpperCase());
    if (!relation || rest.length && !targetField || rest.length > 0 && rest.some(Boolean)) {
      throw new Error(`Unknown Oracle filter field: ${path}`);
    }
    const targetFilter = targetField ? { [targetField]: value } : value;
    if (!isFilter(targetFilter)) throw new Error(`Invalid Oracle relation filter: ${relation.name}`);
    const relationIndex = relations.indexOf(relation);
    const targetAlias = quotedIdentifier(
      relation.through ? `relation_${relationIndex}_target` : `relation_${relationIndex}`
    );
    const targetCondition = conditionPredicate(
      targetFilter,
      relation.targetFields,
      targetAlias,
      relation.targetFieldTypes,
      [],
      relation.targetFieldAliases
    );
    const joinPredicates = relation.sourceKeys.map((sourceKey, index) => {
      const targetKey = relation.targetKeys[index];
      return `${targetAlias}.${quotedIdentifier(targetKey)} = ${rootAlias ? `${rootAlias}.` : ""}${quotedIdentifier(
        sourceKey
      )}`;
    });
    if (relation.arraySourceKey) {
      const targetKey = relation.targetKeys[0];
      joinPredicates.splice(
        0,
        joinPredicates.length,
        `EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE((${rootAlias ? `${rootAlias}.` : ""}${quotedIdentifier(
          relation.arraySourceKey
        )})::jsonb, '[]'::jsonb)) AS json_item(value) WHERE json_item.value = (${targetAlias}.${quotedIdentifier(
          targetKey
        )})::text)`
      );
    }
    if (relation.through) {
      const throughAlias = quotedIdentifier(`relation_${relationIndex}_through`);
      const throughPredicates = relation.sourceKeys.map((sourceKey, index) => {
        var _a;
        const throughKey = (_a = relation.through) == null ? void 0 : _a.sourceKeys[index];
        return `${throughAlias}.${quotedIdentifier(throughKey)} = ${rootAlias ? `${rootAlias}.` : ""}${quotedIdentifier(sourceKey)}`;
      });
      const targetThroughPredicates = relation.targetKeys.map((targetKey, index) => {
        var _a;
        const throughKey = (_a = relation.through) == null ? void 0 : _a.targetKeys[index];
        return `${targetAlias}.${quotedIdentifier(targetKey)} = ${throughAlias}.${quotedIdentifier(
          throughKey
        )}`;
      });
      return `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
        relation.through.tableName
      )} ${throughAlias} JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
        relation.tableName
      )} ${targetAlias} ON ${targetThroughPredicates.join(" AND ")} WHERE ${[
        ...throughPredicates,
        targetCondition
      ].join(" AND ")})`;
    }
    return `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
      relation.tableName
    )} ${targetAlias} WHERE ${[...joinPredicates, targetCondition].join(" AND ")})`;
  };
  function conditionPredicate(condition, fields = knownFields, prefix = "", types = fieldTypes, relationSet = relations, aliases = fieldAliases) {
    const predicates = Object.entries(condition).map(([field, value]) => {
      if (field === "$and" || field === "$or") {
        if (!Array.isArray(value)) throw new Error(`Oracle filter operator ${field} requires an array of conditions`);
        if (!value.length) return field === "$and" ? "1 = 1" : "1 = 0";
        const children = value.map((item) => {
          if (!isFilter(item)) throw new Error(`Oracle filter operator ${field} requires object conditions`);
          return conditionPredicate(item, fields, prefix, types, relationSet, aliases);
        });
        return `(${children.join(field === "$and" ? " AND " : " OR ")})`;
      }
      if (!prefix && field.includes(".")) return relationPredicate(field, value);
      if (!prefix) {
        const relation = relationSet.find((item) => item.name.toUpperCase() === field.toUpperCase());
        if (relation) return relationPredicate(field, value);
      }
      const resolvedField = aliases.get(field.toUpperCase()) || resolveFieldName(field, fields) || field;
      const column = prefix ? `${prefix}.${quotedIdentifier(resolvedField)}` : void 0;
      return fieldPredicate(field, value, column, fields, types, aliases);
    });
    return predicates.length ? predicates.length === 1 ? predicates[0] : `(${predicates.join(" AND ")})` : "1 = 1";
  }
  return { predicate: conditionPredicate(normalizedFilter), binds };
}
function mapOracleField(column, primaryColumns, uniqueColumns) {
  var _a;
  const rawType = column.DATA_TYPE.toUpperCase();
  let type = "string";
  let fieldInterface = "input";
  if (["SMALLINT", "INTEGER", "BIGINT", "INT2", "INT4", "INT8", "SERIAL", "BIGSERIAL"].includes(rawType)) {
    type = "integer";
    fieldInterface = "number";
  } else if (["DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "MONEY"].includes(rawType)) {
    type = "double";
    fieldInterface = "number";
  } else if ([
    "DATE",
    "TIMESTAMP WITHOUT TIME ZONE",
    "TIMESTAMP WITH TIME ZONE",
    "TIME WITHOUT TIME ZONE",
    "TIME WITH TIME ZONE"
  ].includes(rawType)) {
    type = rawType === "DATE" ? "date" : "datetime";
    fieldInterface = "datetime";
  } else if (["TEXT", "CHARACTER VARYING", "CHARACTER", "VARCHAR", "CHAR", "NAME", "XML"].includes(rawType)) {
    type = "text";
    fieldInterface = "textarea";
  } else if (["BYTEA"].includes(rawType)) {
    type = "json";
    fieldInterface = "json";
  } else if (rawType === "BOOLEAN") {
    type = "boolean";
    fieldInterface = "checkbox";
  } else if (rawType === "UUID") {
    type = "string";
    fieldInterface = "input";
  } else if (["JSON", "JSONB", "ARRAY"].includes(rawType)) {
    type = "json";
    fieldInterface = "json";
  }
  return {
    name: column.COLUMN_NAME,
    field: column.COLUMN_NAME,
    rawType,
    type,
    interface: fieldInterface,
    allowNull: column.NULLABLE === "Y",
    primaryKey: primaryColumns.has(column.COLUMN_NAME),
    unique: uniqueColumns.has(column.COLUMN_NAME),
    defaultValue: ((_a = column.DATA_DEFAULT) == null ? void 0 : _a.trim()) || void 0
  };
}
class OracleRepository {
  constructor(collection) {
    this.collection = collection;
  }
  persistedFields() {
    return (this.collection.options.fields || []).filter((field) => !RELATION_TYPES.has(field.type));
  }
  persistedFieldNames() {
    return new Set(this.persistedFields().flatMap((field) => [String(field.name), String(field.field || field.name)]));
  }
  persistedFieldTypes() {
    return new Map(
      this.persistedFields().flatMap((field) => [
        [String(field.name).toUpperCase(), field.type],
        [String(field.field || field.name).toUpperCase(), field.type]
      ])
    );
  }
  persistedFieldAliases() {
    return new Map(
      this.persistedFields().flatMap((field) => {
        const physical = String(field.field || field.name);
        return [[String(field.name).toUpperCase(), physical], [physical.toUpperCase(), physical]];
      })
    );
  }
  recordKeyFields() {
    const configured = this.collection.options.filterTargetKey;
    const aliases = this.persistedFieldAliases();
    if (Array.isArray(configured) && configured.length)
      return configured.map((field) => aliases.get(String(field).toUpperCase()) || String(field));
    if (typeof configured === "string" && configured) return [aliases.get(configured.toUpperCase()) || configured];
    const primary = this.persistedFields().filter((field) => field.primaryKey).map((field) => String(field.field || field.name));
    if (primary.length) return primary;
    return this.persistedFields().filter((field) => field.unique).map((field) => String(field.field || field.name));
  }
  resolvePersistedField(field) {
    const resolved = this.persistedFields().find(
      (item) => String(item.name).toUpperCase() === field.toUpperCase() || String(item.field || item.name).toUpperCase() === field.toUpperCase()
    );
    if (!resolved) throw new Error(`Unknown Oracle field: ${field}`);
    return String(resolved.field || resolved.name);
  }
  logicalPersistedField(field) {
    const resolved = this.persistedFields().find(
      (item) => String(item.name).toUpperCase() === field.toUpperCase() || String(item.field || item.name).toUpperCase() === field.toUpperCase()
    );
    return String((resolved == null ? void 0 : resolved.name) || field);
  }
  normalizeFieldList(value) {
    if (value === void 0 || value === null) return [];
    const values = Array.isArray(value) ? value : [value];
    return values.map((item) => String(item).trim()).filter(Boolean);
  }
  filterByTargetKey(filterByTk) {
    if (filterByTk === void 0) return void 0;
    const keys = this.recordKeyFields();
    if (!keys.length)
      throw new Error(`Oracle collection ${this.collection.options.name} has no configured Record Unique Key`);
    if (keys.length === 1) {
      const key = String(keys[0]);
      if (Array.isArray(filterByTk)) {
        if (filterByTk.some((value2) => value2 === null || typeof value2 === "object")) {
          throw new Error(`Oracle ${this.collection.options.name} requires scalar Record Unique Keys`);
        }
        return { [key]: { $in: filterByTk } };
      }
      const value = isFilter(filterByTk) && key in filterByTk ? filterByTk[key] : filterByTk;
      if (value === void 0 || typeof value === "object" || value === null) {
        throw new Error(`Oracle ${this.collection.options.name} requires a scalar ${key} Record Unique Key`);
      }
      return { [key]: value };
    }
    if (Array.isArray(filterByTk)) {
      if (filterByTk.every((value) => isFilter(value))) {
        return {
          $or: filterByTk.map(
            (value) => Object.fromEntries(
              keys.map((key) => {
                const item = value[key];
                if (item === void 0 || item === null || typeof item === "object") {
                  throw new Error(`Oracle composite Record Unique Key requires the ${key} value`);
                }
                return [key, item];
              })
            )
          )
        };
      }
      if (filterByTk.length !== keys.length) throw new Error("Oracle composite Record Unique Key has an invalid value");
      return Object.fromEntries(keys.map((key, index) => [key, filterByTk[index]]));
    }
    if (isFilter(filterByTk)) {
      return Object.fromEntries(
        keys.map((key) => {
          const value = filterByTk[key];
          if (value === void 0 || value === null || typeof value === "object") {
            throw new Error(`Oracle composite Record Unique Key requires the ${key} value`);
          }
          return [key, value];
        })
      );
    }
    throw new Error("Oracle composite Record Unique Key requires an object or array value");
  }
  mergeFilters(...filters) {
    const active = filters.filter((filter) => Boolean(filter && Object.keys(filter).length));
    if (!active.length) return void 0;
    if (active.length === 1) return active[0];
    return { $and: active };
  }
  compileFilter(filter, bindPrefix = "filter") {
    if (!filter) return void 0;
    return compileOracleFilter(
      filter,
      this.persistedFieldNames(),
      this.filterRelations(),
      /* @__PURE__ */ new Map(),
      this.persistedFieldTypes(),
      bindPrefix,
      ROOT_ALIAS,
      this.persistedFieldAliases()
    );
  }
  sortClause(sort) {
    const values = (Array.isArray(sort) ? sort : sort === void 0 || sort === null ? [] : [sort]).flatMap(
      (item) => typeof item === "string" ? item.split(",").map((value) => value.trim()).filter(Boolean) : [item]
    );
    const clauses = values.map((item) => {
      let field;
      let direction = "ASC";
      let nulls = "";
      if (typeof item === "string") {
        field = item.startsWith("-") ? item.slice(1) : item;
        direction = item.startsWith("-") ? "DESC" : "ASC";
      } else if (isFilter(item)) {
        field = item.field;
        direction = String(item.order || item.direction || "asc").toUpperCase();
        nulls = item.nulls ? ` NULLS ${String(item.nulls).toUpperCase()}` : "";
      }
      if (typeof field !== "string" || !["ASC", "DESC"].includes(direction)) {
        throw new Error("Invalid Oracle sort specification");
      }
      const resolved = this.resolvePersistedField(field);
      if (nulls && ![" NULLS FIRST", " NULLS LAST"].includes(nulls)) throw new Error("Invalid Oracle null sort order");
      return `${ROOT_ALIAS}.${quotedIdentifier(resolved)} ${direction}${nulls}`;
    });
    return clauses.length ? ` ORDER BY ${clauses.join(", ")}` : "";
  }
  paginationClause(options, binds) {
    if (isNonPaginated(options)) return "";
    binds.offset = nonNegativeInteger(options.offset);
    binds.limit = positiveInteger(options.limit, DEFAULT_PAGE_SIZE);
    return " LIMIT @limit OFFSET @offset";
  }
  async find(options = {}) {
    var _a, _b;
    const collection = this.collection.options;
    const knownFields = this.persistedFieldNames();
    if (!knownFields.size) throw new Error(`Oracle collection ${collection.name} has no persisted field metadata`);
    const requestedInput = this.normalizeFieldList(options.fields);
    const requestedFields = requestedInput.length ? requestedInput.map((field) => this.resolvePersistedField(field)) : [...knownFields];
    const exceptFields = this.normalizeFieldList(options.except).map((field) => this.resolvePersistedField(field));
    const visibleFields = requestedFields.filter((field) => !exceptFields.includes(field));
    const appendFields = this.relationFields(options.appends);
    const requiredFields = appendFields.flatMap((field) => this.relationSourceKeys(field));
    const selectedFields = [.../* @__PURE__ */ new Set([...visibleFields, ...requiredFields])];
    if (!selectedFields.length) throw new Error("No requested Oracle fields are available in persisted metadata");
    const whereFilter = this.mergeFilters(options.filter, this.filterByTargetKey(options.filterByTk));
    const compiledFilter = this.compileFilter(whereFilter);
    const whereParts = [compiledFilter == null ? void 0 : compiledFilter.predicate, (_a = options.relationPredicate) == null ? void 0 : _a.sql].filter(Boolean);
    const binds = {
      ...(compiledFilter == null ? void 0 : compiledFilter.binds) || {},
      ...((_b = options.relationPredicate) == null ? void 0 : _b.binds) || {}
    };
    const visible = new Set(visibleFields);
    const fields = selectedFields.map((field) => {
      const outputName = visible.has(field) ? this.logicalPersistedField(field) : field;
      const expression = `${ROOT_ALIAS}.${quotedIdentifier(field)}`;
      return outputName === field ? expression : `${expression} AS ${quotedIdentifier(outputName)}`;
    }).join(", ");
    assertSafeObjectName(collection.tableName);
    const pagination = this.paginationClause(options, binds);
    const sort = this.sortClause(options.sort);
    const sql = `SELECT ${fields} FROM ${this.tableReference()} ${ROOT_ALIAS}${whereParts.length ? ` WHERE ${whereParts.join(" AND ")}` : ""}${sort}${pagination}`;
    const records = await this.collection.collectionManager.dataSource.execute(sql, binds);
    await this.appendRelations(records, appendFields, options.appends);
    for (const record of records) {
      for (const field of selectedFields) {
        if (!visible.has(field)) delete record[field];
      }
    }
    return records;
  }
  async findOne(options = {}) {
    const records = await this.find({ ...options, limit: 1 });
    return records[0] || null;
  }
  async count(options = {}) {
    var _a, _b, _c, _d;
    const collection = this.collection.options;
    const knownFields = this.persistedFieldNames();
    if (!knownFields.size) throw new Error(`Oracle collection ${collection.name} has no persisted field metadata`);
    const whereFilter = this.mergeFilters(options.filter, this.filterByTargetKey(options.filterByTk));
    const compiledFilter = this.compileFilter(whereFilter);
    const whereParts = [compiledFilter == null ? void 0 : compiledFilter.predicate, (_a = options.relationPredicate) == null ? void 0 : _a.sql].filter(Boolean);
    assertSafeObjectName(collection.tableName);
    const sql = `SELECT COUNT(*) AS COUNT FROM ${this.tableReference()} ${ROOT_ALIAS}${whereParts.length ? ` WHERE ${whereParts.join(" AND ")}` : ""}`;
    const records = await this.collection.collectionManager.dataSource.execute(sql, {
      ...(compiledFilter == null ? void 0 : compiledFilter.binds) || {},
      ...((_b = options.relationPredicate) == null ? void 0 : _b.binds) || {}
    });
    return Number(((_c = records[0]) == null ? void 0 : _c.COUNT) ?? ((_d = records[0]) == null ? void 0 : _d.count) ?? 0);
  }
  async aggregate(options) {
    var _a, _b, _c, _d, _e, _f;
    const method = String(options.method || "").toLowerCase();
    if (!["avg", "count", "min", "max", "sum"].includes(method)) {
      throw new Error(`Unsupported Oracle aggregation: ${options.method}`);
    }
    const relations = this.filterRelations();
    const joins = /* @__PURE__ */ new Map();
    const joinSql = [];
    const expressionBinds = {};
    const field = options.expression ? this.compileExpression(options.expression, relations, joins, joinSql, expressionBinds) : options.field ? this.queryFieldExpression(options.field, relations, joins, joinSql) : void 0;
    if (method !== "count" && !field) {
      throw new Error(`Oracle ${method} requires a field`);
    }
    if (options.distinct && !field) {
      throw new Error("Oracle count distinct requires a field");
    }
    const expression = (field == null ? void 0 : field.expression) || "*";
    const aggregateExpression = `${method.toUpperCase()}(${options.distinct ? "DISTINCT " : ""}${expression})`;
    const compiledFilter = options.filter ? this.compileFilter(options.filter) : void 0;
    const whereParts = [compiledFilter == null ? void 0 : compiledFilter.predicate, (_a = options.relationPredicate) == null ? void 0 : _a.sql].filter(Boolean);
    const binds = {
      ...expressionBinds,
      ...(compiledFilter == null ? void 0 : compiledFilter.binds) || {},
      ...((_b = options.relationPredicate) == null ? void 0 : _b.binds) || {}
    };
    const sql = `SELECT ${aggregateExpression} AS "aggregate" FROM ${this.tableReference()} ${ROOT_ALIAS}${joinSql.join(
      ""
    )}${whereParts.length ? ` WHERE ${whereParts.join(" AND ")}` : ""}`;
    const records = await this.collection.collectionManager.dataSource.execute(sql, binds);
    const value = ((_c = records[0]) == null ? void 0 : _c.aggregate) ?? ((_d = records[0]) == null ? void 0 : _d.AGGREGATE) ?? ((_e = records[0]) == null ? void 0 : _e.COUNT) ?? ((_f = records[0]) == null ? void 0 : _f.count);
    return Number(value ?? 0);
  }
  async findAndCount(options = {}) {
    return [await this.find(options), await this.count(options)];
  }
  queryRelationJoin(relation, index) {
    const targetAlias = quotedIdentifier(`query_relation_${index}_target`);
    if (relation.arraySourceKey) {
      const targetKey = relation.targetKeys[0];
      return {
        alias: targetAlias,
        sql: ` LEFT JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
          relation.tableName
        )} ${targetAlias} ON EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE((${ROOT_ALIAS}.${quotedIdentifier(
          relation.arraySourceKey
        )})::jsonb, '[]'::jsonb)) AS json_item(value) WHERE json_item.value = (${targetAlias}.${quotedIdentifier(
          targetKey
        )})::text)`
      };
    }
    if (relation.through) {
      const throughAlias = quotedIdentifier(`query_relation_${index}_through`);
      const throughJoin = relation.sourceKeys.map((sourceKey, keyIndex) => {
        var _a;
        const throughKey = (_a = relation.through) == null ? void 0 : _a.sourceKeys[keyIndex];
        return `${throughAlias}.${quotedIdentifier(throughKey)} = ${ROOT_ALIAS}.${quotedIdentifier(
          sourceKey
        )}`;
      });
      const targetJoin = relation.targetKeys.map((targetKey, keyIndex) => {
        var _a;
        const throughKey = (_a = relation.through) == null ? void 0 : _a.targetKeys[keyIndex];
        return `${targetAlias}.${quotedIdentifier(targetKey)} = ${throughAlias}.${quotedIdentifier(
          throughKey
        )}`;
      });
      return {
        alias: targetAlias,
        sql: ` LEFT JOIN ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
          relation.through.tableName
        )} ${throughAlias} ON ${throughJoin.join(" AND ")} LEFT JOIN ${quotedIdentifier(
          relation.schema
        )}.${quotedIdentifier(relation.tableName)} ${targetAlias} ON ${targetJoin.join(" AND ")}`
      };
    }
    const predicates = relation.sourceKeys.map((sourceKey, keyIndex) => {
      const targetKey = relation.targetKeys[keyIndex];
      return `${targetAlias}.${quotedIdentifier(targetKey)} = ${ROOT_ALIAS}.${quotedIdentifier(sourceKey)}`;
    });
    return {
      alias: targetAlias,
      sql: ` LEFT JOIN ${quotedIdentifier(relation.schema)}.${quotedIdentifier(
        relation.tableName
      )} ${targetAlias} ON ${predicates.join(" AND ")}`
    };
  }
  compileExpression(expression, relations, joins, joinSql, binds) {
    if (!isFilter(expression) || typeof expression.type !== "string") {
      throw new Error("Invalid Oracle expression");
    }
    if (expression.type === "field") {
      return this.queryFieldExpression(expression.field, relations, joins, joinSql);
    }
    if (expression.type === "literal") {
      const name = `expression_${Object.keys(binds).length}`;
      binds[name] = expression.value;
      return { expression: `:${name}`, name: "literal" };
    }
    if (expression.type === "binary") {
      const left = this.compileExpression(expression.left, relations, joins, joinSql, binds);
      const right = this.compileExpression(expression.right, relations, joins, joinSql, binds);
      const operators = {
        add: "+",
        subtract: "-",
        multiply: "*",
        divide: "/",
        eq: "=",
        ne: "<>",
        gt: ">",
        gte: ">=",
        lt: "<",
        lte: "<=",
        and: "AND",
        or: "OR"
      };
      if (expression.operator === "mod") {
        return { expression: `MOD(${left.expression}, ${right.expression})`, name: "expression" };
      }
      if (expression.operator === "concat") {
        return { expression: `CONCAT(${left.expression}, ${right.expression})`, name: "expression", type: "string" };
      }
      const operator = operators[expression.operator];
      if (!operator) throw new Error(`Unsupported Oracle expression operator: ${expression.operator}`);
      const rightExpression = expression.operator === "divide" ? `NULLIF(${right.expression}, 0)` : right.expression;
      return { expression: `(${left.expression} ${operator} ${rightExpression})`, name: "expression" };
    }
    if (expression.type === "unary") {
      const child = this.compileExpression(expression.expression, relations, joins, joinSql, binds);
      if (expression.operator === "not") return { expression: `(NOT ${child.expression})`, name: "expression" };
      return {
        expression: `${child.expression} IS ${expression.operator === "isNull" ? "" : "NOT "}NULL`,
        name: "expression"
      };
    }
    if (expression.type === "function") {
      const args = expression.args.map((item) => this.compileExpression(item, relations, joins, joinSql, binds));
      const requestedName = expression.name.toUpperCase();
      const name = requestedName;
      const arity = requestedName === "ROUND" || requestedName === "TRUNC" ? [1, 2] : [1];
      if (!arity.includes(args.length)) throw new Error(`Oracle function ${name} has an invalid argument count`);
      return { expression: `${name}(${args.map((item) => item.expression).join(", ")})`, name: "expression" };
    }
    if (expression.type === "coalesce") {
      if (expression.args.length < 1) throw new Error("Oracle coalesce requires at least one argument");
      const args = expression.args.map((item) => this.compileExpression(item, relations, joins, joinSql, binds));
      return { expression: `COALESCE(${args.map((item) => item.expression).join(", ")})`, name: "expression" };
    }
    if (expression.type === "case") {
      if (!expression.when.length) throw new Error("Oracle case expression requires at least one branch");
      const branches = expression.when.map((item) => {
        const condition = this.compileExpression(item.if, relations, joins, joinSql, binds);
        const result = this.compileExpression(item.then, relations, joins, joinSql, binds);
        return `WHEN ${condition.expression} THEN ${result.expression}`;
      });
      const fallback = expression.else ? this.compileExpression(expression.else, relations, joins, joinSql, binds).expression : "NULL";
      return { expression: `(CASE ${branches.join(" ")} ELSE ${fallback} END)`, name: "expression" };
    }
    throw new Error(`Unsupported Oracle expression type: ${expression.type}`);
  }
  queryFieldExpression(field, relations, joins, joinSql) {
    const path = normalizeFieldPath(field);
    if (path.length === 1) {
      const physical = this.resolvePersistedField(path[0]);
      return {
        expression: `${ROOT_ALIAS}.${quotedIdentifier(physical)}`,
        name: this.logicalPersistedField(physical),
        type: this.persistedFieldTypes().get(physical.toUpperCase())
      };
    }
    if (path.length !== 2) throw new Error(`Oracle query supports only one relation path: ${path.join(".")}`);
    const relation = relations.find((item) => item.name.toUpperCase() === path[0].toUpperCase());
    if (!relation) throw new Error(`Unknown Oracle query relation: ${path[0]}`);
    const targetField = resolveFieldName(path[1], relation.targetFields);
    if (!targetField) throw new Error(`Unknown Oracle query field: ${path.join(".")}`);
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
      type: relation.targetFieldTypes.get(targetField.toUpperCase())
    };
  }
  queryAlias(alias, fallback) {
    const value = String(alias || fallback);
    if (!IDENTIFIER.test(value)) throw new Error(`Invalid Oracle query alias: ${value}`);
    return value;
  }
  queryDimensionExpression(expression, type, format, timezone) {
    if (!format || !["date", "datetime", "datetimeTz", "datetimeNoTz", "dateOnly", "time", "unixTimestamp"].includes(type || "")) {
      return expression;
    }
    const pattern = formatDatePattern(format);
    const timezoneExpression = timezone && type === "datetimeTz" && /^[A-Za-z][A-Za-z0-9_+:/-]{0,63}$/.test(timezone) ? `(${expression} AT TIME ZONE '${timezone}')` : expression;
    if (type === "unixTimestamp") {
      return `TO_CHAR(TIMESTAMP '1970-01-01 00:00:00' + (${expression} * INTERVAL '1 second'), '${pattern}')`;
    }
    return `TO_CHAR(${timezoneExpression}, '${pattern}')`;
  }
  async query(options = {}) {
    var _a;
    const measures = Array.isArray(options.measures) ? options.measures : [];
    const dimensions = Array.isArray(options.dimensions) ? options.dimensions : [];
    if (!measures.length && !dimensions.length)
      throw new Error("Oracle query requires at least one measure or dimension");
    const relations = this.filterRelations();
    const joins = /* @__PURE__ */ new Map();
    const joinSql = [];
    const expressions = /* @__PURE__ */ new Map();
    const expressionTypes = /* @__PURE__ */ new Map();
    const expressionBinds = {};
    const selectSql = [];
    const groupSql = [];
    const nonAggregateSql = [];
    let hasAggregate = false;
    for (const measure of measures) {
      const aggregation = (_a = measure.aggregation) == null ? void 0 : _a.toLowerCase();
      if (aggregation && !["count", "sum", "avg", "min", "max"].includes(aggregation)) {
        throw new Error(`Unsupported Oracle aggregation: ${measure.aggregation}`);
      }
      const fieldPath = measure.field === void 0 ? ["*"] : normalizeFieldPath(measure.field);
      let fieldExpression = "*";
      let fieldName = "count";
      let fieldType;
      if (measure.expression) {
        const expression = this.compileExpression(measure.expression, relations, joins, joinSql, expressionBinds);
        fieldExpression = expression.expression;
        fieldName = expression.name;
        fieldType = expression.type;
      } else if (!(fieldPath.length === 1 && fieldPath[0] === "*")) {
        const field = this.queryFieldExpression(measure.field, relations, joins, joinSql);
        fieldExpression = field.expression;
        fieldName = field.name;
        fieldType = field.type;
      } else if (aggregation !== "count") {
        throw new Error("Oracle aggregate measures other than count require a field");
      }
      if (aggregation === "count" && measure.distinct && fieldExpression === "*") {
        throw new Error("Oracle count distinct requires a field");
      }
      const aggregateExpression = aggregation ? `${aggregation.toUpperCase()}(${measure.distinct ? "DISTINCT " : ""}${fieldExpression})` : fieldExpression;
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
      const field = isFilter(dimension.field) ? this.compileExpression(dimension.field, relations, joins, joinSql, expressionBinds) : this.queryFieldExpression(dimension.field, relations, joins, joinSql);
      const expression = this.queryDimensionExpression(
        field.expression,
        dimension.type || field.type,
        dimension.format,
        options.timezone
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
    const compiledFilter = this.compileFilter(options.filter, "query_filter");
    const compiledHaving = options.having ? compileOracleFilter(
      options.having,
      new Set(expressions.keys()),
      [],
      expressions,
      expressionTypes,
      "query_having"
    ) : void 0;
    const binds = {
      ...expressionBinds,
      ...(compiledFilter == null ? void 0 : compiledFilter.binds) || {},
      ...(compiledHaving == null ? void 0 : compiledHaving.binds) || {}
    };
    const where = compiledFilter ? ` WHERE ${compiledFilter.predicate}` : "";
    const groupBy = groupSql.length ? ` GROUP BY ${groupSql.join(", ")}` : "";
    const having = compiledHaving ? ` HAVING ${compiledHaving.predicate}` : "";
    const orderValues = Array.isArray(options.orders) ? options.orders : [];
    const orderSql = orderValues.map((order2) => {
      var _a2;
      const aliasKey = (_a2 = order2.alias) == null ? void 0 : _a2.toUpperCase();
      const fieldExpression = aliasKey && expressions.get(aliasKey);
      const expression = fieldExpression || (isFilter(order2.field) ? this.compileExpression(order2.field, relations, joins, joinSql, expressionBinds).expression : this.queryFieldExpression(order2.field, relations, joins, joinSql).expression);
      const direction = String(order2.order || "asc").toUpperCase();
      if (!["ASC", "DESC"].includes(direction)) throw new Error(`Invalid Oracle query order: ${order2.order}`);
      const nulls = order2.nulls && order2.nulls !== "default" ? ` NULLS ${order2.nulls.toUpperCase()}` : "";
      if (nulls && ![" NULLS FIRST", " NULLS LAST"].includes(nulls)) throw new Error(`Invalid Oracle query null order`);
      return `${expression} ${direction}${nulls}`;
    });
    Object.assign(binds, expressionBinds);
    const order = orderSql.length ? ` ORDER BY ${orderSql.join(", ")}` : "";
    let pagination = "";
    if (options.limit !== void 0 || dimensions.length || !hasAggregate) {
      binds.query_offset = nonNegativeInteger(options.offset);
      binds.query_limit = positiveInteger(options.limit, DEFAULT_QUERY_LIMIT);
      pagination = " LIMIT @query_limit OFFSET @query_offset";
    }
    const sql = `SELECT ${selectSql.join(", ")} FROM ${this.tableReference()} ${ROOT_ALIAS}${joinSql.join(
      ""
    )}${where}${groupBy}${having}${order}${pagination}`;
    return this.collection.collectionManager.dataSource.execute(sql, binds);
  }
  async findRelated(field, sourceRecord, options = {}) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return [];
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target
    );
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.find({ ...options, relationPredicate });
  }
  async countRelated(field, sourceRecord, options = {}) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return 0;
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target
    );
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.count({ ...options, relationPredicate });
  }
  async aggregateRelated(field, sourceRecord, options) {
    const relation = this.filterRelations().find((item) => item.name.toUpperCase() === field.name.toUpperCase());
    if (!relation) return 0;
    const targetRepository = this.collection.collectionManager.getRepository(
      field.target
    );
    const relationPredicate = this.relatedPredicate(relation, sourceRecord);
    return targetRepository.aggregate({ ...options, relationPredicate });
  }
  relatedPredicate(relation, sourceRecord) {
    const getValue = (key) => {
      const actual = Object.keys(sourceRecord).find((candidate) => candidate.toUpperCase() === key.toUpperCase());
      return actual ? sourceRecord[actual] : void 0;
    };
    if (relation.arraySourceKey) {
      let values = getValue(relation.arraySourceKey);
      if (typeof values === "string") {
        try {
          values = JSON.parse(values);
        } catch {
          values = [];
        }
      }
      if (!Array.isArray(values)) return { sql: "1 = 0", binds: {} };
      const scalarValues = values.filter((value) => isScalar(value) && value !== null);
      if (!scalarValues.length) return { sql: "1 = 0", binds: {} };
      const binds2 = Object.fromEntries(scalarValues.map((value, index) => [`relation_value_${index}`, value]));
      return {
        sql: `${ROOT_ALIAS}.${quotedIdentifier(relation.targetKeys[0])} IN (${scalarValues.map((_, index) => `@relation_value_${index}`).join(", ")})`,
        binds: binds2
      };
    }
    const sourceValues = relation.sourceKeys.map((key) => getValue(key));
    if (sourceValues.some((value) => value === void 0 || value === null || !isScalar(value))) {
      return { sql: "1 = 0", binds: {} };
    }
    const binds = Object.fromEntries(sourceValues.map((value, index) => [`relation_source_${index}`, value]));
    if (!relation.through) {
      return {
        sql: relation.targetKeys.map((key, index) => `${ROOT_ALIAS}.${quotedIdentifier(key)} = @relation_source_${index}`).join(" AND "),
        binds
      };
    }
    const throughAlias = quotedIdentifier("relation_through");
    const throughPredicates = relation.through.sourceKeys.map(
      (key, index) => `${throughAlias}.${quotedIdentifier(key)} = @relation_source_${index}`
    );
    const targetPredicates = relation.targetKeys.map(
      (key, index) => {
        var _a;
        return `${ROOT_ALIAS}.${quotedIdentifier(key)} = ${throughAlias}.${quotedIdentifier(
          (_a = relation.through) == null ? void 0 : _a.targetKeys[index]
        )}`;
      }
    );
    return {
      sql: `EXISTS (SELECT 1 FROM ${quotedIdentifier(relation.through.schema)}.${quotedIdentifier(
        relation.through.tableName
      )} ${throughAlias} WHERE ${[...throughPredicates, ...targetPredicates].join(" AND ")})`,
      binds
    };
  }
  async create(options) {
    const values = this.writeValues(options.values || {});
    if (!values.length) throw new Error("No writable PostgreSQL field values were provided");
    const binds = Object.fromEntries(values.map(([name, value], index) => [`value_${index}`, value]));
    await this.collection.collectionManager.dataSource.execute(
      `INSERT INTO ${this.tableReference()} (${values.map(([name]) => quotedIdentifier(name)).join(", ")}) VALUES (${values.map((_, index) => `@value_${index}`).join(", ")})`,
      binds
    );
    return Object.fromEntries(values);
  }
  async update(options) {
    const values = this.writeValues(options.values || {}, true);
    if (!values.length) throw new Error("No writable PostgreSQL field values were provided");
    const { predicate, binds } = this.recordKeyPredicate(options);
    for (const [index, [, value]] of values.entries()) binds[`value_${index}`] = value;
    await this.collection.collectionManager.dataSource.execute(
      `UPDATE ${this.tableReference()} SET ${values.map(([name], index) => `${quotedIdentifier(name)} = @value_${index}`).join(", ")} WHERE ${predicate}`,
      binds
    );
    return { ...Object.fromEntries(values), ...options.filter || {} };
  }
  async destroy(options) {
    const { predicate, binds } = this.recordKeyPredicate(options);
    await this.collection.collectionManager.dataSource.execute(
      `DELETE FROM ${this.tableReference()} WHERE ${predicate}`,
      binds
    );
    return true;
  }
  writeValues(values, excludeRecordKey = false) {
    const collection = this.collection.options;
    if (collection.view) throw new Error(`PostgreSQL view ${collection.name} is read-only`);
    const fieldAliases = this.persistedFieldAliases();
    const recordKey = new Set(this.recordKeyFields());
    return Object.entries(values).flatMap(([name, value]) => {
      const physical = fieldAliases.get(name.toUpperCase());
      if (!physical || value === void 0 || typeof value === "object" || excludeRecordKey && recordKey.has(physical)) {
        return [];
      }
      return [[physical, value]];
    });
  }
  recordKeyPredicate(options) {
    var _a;
    const collection = this.collection.options;
    const recordKey = this.recordKeyFields();
    if (!recordKey.length) {
      throw new Error(`PostgreSQL collection ${collection.name} requires a configured Record Unique Key for writes`);
    }
    const targetFilter = options.filterByTk === void 0 ? void 0 : this.filterByTargetKey(options.filterByTk);
    if (recordKey.length === 1) {
      const key = recordKey[0];
      const value = (targetFilter == null ? void 0 : targetFilter[key]) ?? ((_a = options.filter) == null ? void 0 : _a[key]);
      if (value === void 0 || value === null || typeof value === "object") {
        throw new Error(`PostgreSQL ${collection.name} update and delete require the ${key} Record Unique Key`);
      }
      const predicate2 = `${quotedIdentifier(key)} = @record_key`;
      return this.withCallerFilter(predicate2, { record_key: value }, options.filter);
    }
    const binds = {};
    const predicate = recordKey.map((key, index) => {
      var _a2;
      const value = (targetFilter == null ? void 0 : targetFilter[key]) ?? ((_a2 = options.filter) == null ? void 0 : _a2[key]);
      if (value === void 0 || value === null || typeof value === "object") {
        throw new Error(`PostgreSQL ${collection.name} update and delete require the ${key} Record Unique Key`);
      }
      const bind = `record_key_${index}`;
      binds[bind] = value;
      return `${quotedIdentifier(key)} = @${bind}`;
    }).join(" AND ");
    return this.withCallerFilter(predicate, binds, options.filter);
  }
  withCallerFilter(predicate, binds, filter) {
    if (!filter || !Object.keys(filter).length) return { predicate, binds };
    const knownFields = new Set(
      (this.collection.options.fields || []).filter((field) => !RELATION_TYPES.has(field.type)).map((field) => String(field.field || field.name))
    );
    const compiled = compileOracleFilter(
      filter,
      knownFields,
      this.filterRelations(),
      /* @__PURE__ */ new Map(),
      /* @__PURE__ */ new Map(),
      "filter",
      "",
      this.persistedFieldAliases()
    );
    const renamedBinds = {};
    let callerPredicate = compiled.predicate;
    for (const [name, value] of Object.entries(compiled.binds)) {
      const renamed = `caller_${name}`;
      renamedBinds[renamed] = value;
      callerPredicate = callerPredicate.replaceAll(`@${name}`, `@${renamed}`);
    }
    return { predicate: `(${predicate}) AND (${callerPredicate})`, binds: { ...binds, ...renamedBinds } };
  }
  tableReference() {
    const collection = this.collection.options;
    assertSafeObjectName(collection.tableName);
    return `${quotedIdentifier(collection.schema || "public")}.${quotedIdentifier(collection.tableName)}`;
  }
  relationFields(appends) {
    const requested = Array.isArray(appends) ? appends : typeof appends === "string" ? [appends] : [];
    const fields = new Map(
      (this.collection.options.fields || []).filter((field) => RELATION_TYPES.has(field.type)).map((field) => [field.name.toUpperCase(), field])
    );
    return requested.map((append) => String(append).split(".")[0]).map((name) => fields.get(name.toUpperCase())).filter((field) => Boolean(field));
  }
  relationSourceKeys(field) {
    if (field.type === "belongsToArray") return [field.foreignKey || field.sourceKey].filter(Boolean).map(String);
    return Array.isArray(field.externalSourceKeys) ? field.externalSourceKeys.map(String) : [field.sourceKey].filter(Boolean).map(String);
  }
  filterRelations() {
    const manager = this.collection.collectionManager;
    const schema = this.collection.options.schema || "public";
    return (this.collection.options.fields || []).flatMap((field) => {
      if (!RELATION_TYPES.has(field.type) || !field.target) return [];
      let target;
      try {
        target = manager.getCollection(field.target);
      } catch {
        target = void 0;
      }
      const targetOptions = target == null ? void 0 : target.options;
      const sourceFields = this.persistedFieldNames();
      const sourceKeys = this.relationSourceKeys(field);
      const targetKeys = (Array.isArray(field.externalTargetKeys) ? field.externalTargetKeys : [field.targetKey].filter(Boolean)).map(String);
      const targetFields = new Set(
        ((targetOptions == null ? void 0 : targetOptions.fields) || []).filter((item) => !RELATION_TYPES.has(item.type)).flatMap((item) => [String(item.name), String(item.field || item.name)])
      );
      const targetFieldTypes = new Map(
        ((targetOptions == null ? void 0 : targetOptions.fields) || []).filter((item) => !RELATION_TYPES.has(item.type)).flatMap((item) => [
          [String(item.name).toUpperCase(), item.type],
          [String(item.field || item.name).toUpperCase(), item.type]
        ])
      );
      const targetFieldAliases = new Map(
        ((targetOptions == null ? void 0 : targetOptions.fields) || []).filter((item) => !RELATION_TYPES.has(item.type)).flatMap((item) => {
          const physical = String(item.field || item.name);
          return [[String(item.name).toUpperCase(), physical], [physical.toUpperCase(), physical]];
        })
      );
      const targetFieldNames = new Map(
        ((targetOptions == null ? void 0 : targetOptions.fields) || []).filter((item) => !RELATION_TYPES.has(item.type)).map((item) => [String(item.field || item.name).toUpperCase(), String(item.name)])
      );
      const arraySourceKey = field.type === "belongsToArray" ? sourceKeys[0] : void 0;
      let through;
      if (field.type === "belongsToMany") {
        const throughName = String(field.through || "").trim();
        let throughCollection;
        if (throughName) {
          try {
            throughCollection = manager.getCollection(throughName);
          } catch {
            throughCollection = void 0;
          }
        }
        const throughOptions = throughCollection == null ? void 0 : throughCollection.options;
        const configuredSourceKeys = fieldOption(field, "externalThroughSourceKeys");
        const configuredTargetKeys = fieldOption(field, "externalThroughTargetKeys");
        const throughSourceKeys = Array.isArray(configuredSourceKeys) ? configuredSourceKeys.map(String) : [field.foreignKey].filter(Boolean).map(String);
        const throughTargetKeys = Array.isArray(configuredTargetKeys) ? configuredTargetKeys.map(String) : [field.otherKey].filter(Boolean).map(String);
        const throughFields = new Set(
          ((throughOptions == null ? void 0 : throughOptions.fields) || []).filter((item) => !RELATION_TYPES.has(item.type)).map((item) => String(item.field || item.name))
        );
        if ((throughOptions == null ? void 0 : throughOptions.tableName) && throughSourceKeys.length === sourceKeys.length && throughTargetKeys.length === targetKeys.length && throughSourceKeys.every((key) => resolveFieldName(key, throughFields)) && throughTargetKeys.every((key) => resolveFieldName(key, throughFields))) {
          through = {
            schema: throughOptions.schema || schema,
            tableName: throughOptions.tableName,
            sourceKeys: throughSourceKeys.map((key) => resolveFieldName(key, throughFields)),
            targetKeys: throughTargetKeys.map((key) => resolveFieldName(key, throughFields))
          };
        }
      }
      if (!targetOptions || !targetOptions.tableName || !arraySourceKey && !sourceKeys.length || sourceKeys.length !== targetKeys.length || !arraySourceKey && sourceKeys.some((key) => !resolveFieldName(key, sourceFields)) || targetKeys.some((key) => !resolveFieldName(key, targetFields)) || field.type === "belongsToMany" && !through) {
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
          arraySourceKey
        }
      ];
    });
  }
  async appendRelations(records, fields, appends) {
    const requested = Array.isArray(appends) ? appends.map(String) : typeof appends === "string" ? [appends] : [];
    for (const field of fields) {
      const target = this.collection.collectionManager.getRepository(
        `${this.collection.options.name}.${field.name}`,
        void 0
      );
      for (const record of records) {
        const values = this.relationSourceKeys(field).map((key) => record[key]);
        if (field.type !== "belongsToArray" && values.some((value) => value === null || value === void 0)) {
          record[field.name] = field.type === "hasMany" || field.type === "belongsToMany" || field.type === "belongsToArray" ? [] : null;
          continue;
        }
        const relation = new OracleRelationRepository(target.parent, target.target, field, record);
        record[field.name] = field.type === "hasMany" || field.type === "belongsToMany" || field.type === "belongsToArray" ? await relation.find() : await relation.findOne();
        const nested = requested.filter((append) => append.split(".")[0].toUpperCase() === field.name.toUpperCase() && append.includes(".")).map((append) => append.split(".").slice(1).join("."));
        if (nested.length && record[field.name]) {
          const relatedRecords = Array.isArray(record[field.name]) ? record[field.name] : [record[field.name]];
          const targetRepository = target.target.repository;
          const nestedFields = targetRepository.relationFields(nested);
          await targetRepository.appendRelations(relatedRecords, nestedFields, nested);
        }
      }
    }
  }
}
class OracleRelationRepository {
  constructor(parent, target, field, sourceId) {
    this.parent = parent;
    this.target = target;
    this.field = field;
    this.sourceId = sourceId;
    this.collection = { options: target.options };
  }
  collection;
  async find(options = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return [];
    if (this.field.type === "belongsToMany" || this.field.type === "belongsToArray") {
      return this.parent.repository.findRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return [];
    return this.target.repository.find({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter)
    });
  }
  async findOne(options = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return null;
    if (this.field.type === "belongsToMany" || this.field.type === "belongsToArray") {
      const records = await this.parent.repository.findRelated(this.field, source, { ...options, limit: 1 });
      return records[0] || null;
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return null;
    return this.target.repository.findOne({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter)
    });
  }
  async count(options = {}) {
    const source = await this.sourceRecord(options.context);
    if (!source) return 0;
    if (this.field.type === "belongsToMany" || this.field.type === "belongsToArray") {
      return this.parent.repository.countRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return 0;
    return this.target.repository.count({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter)
    });
  }
  async aggregate(options) {
    const source = await this.sourceRecord(options.context);
    if (!source) return 0;
    if (this.field.type === "belongsToMany" || this.field.type === "belongsToArray") {
      return this.parent.repository.aggregateRelated(this.field, source, options);
    }
    const relationFilter = this.relationFilterFromSource(source);
    if (!relationFilter) return 0;
    return this.target.repository.aggregate({
      ...options,
      filter: this.mergeFilter(options.filter, relationFilter)
    });
  }
  async findAndCount(options = {}) {
    return [await this.find(options), await this.count(options)];
  }
  mergeFilter(filter, relationFilter) {
    return filter && Object.keys(filter).length ? { $and: [filter, relationFilter] } : relationFilter;
  }
  relationFilterFromSource(source) {
    const sourceKeys = Array.isArray(this.field.externalSourceKeys) ? this.field.externalSourceKeys : [this.field.sourceKey].filter(Boolean);
    const targetKeys = Array.isArray(this.field.externalTargetKeys) ? this.field.externalTargetKeys : [this.field.targetKey].filter(Boolean);
    if (!source || !sourceKeys.length || sourceKeys.length !== targetKeys.length) return void 0;
    const knownTargetFields = new Set(
      (this.target.options.fields || []).filter((field) => !RELATION_TYPES.has(field.type)).map((field) => field.field || field.name)
    );
    const filterEntries = targetKeys.map((targetKey, index) => [targetKey, source[sourceKeys[index]]]);
    if (filterEntries.some(
      ([targetKey, value]) => !targetKey || !knownTargetFields.has(targetKey) || value === null || value === void 0 || typeof value === "object"
    )) {
      return void 0;
    }
    return Object.fromEntries(filterEntries);
  }
  async sourceRecord(context) {
    var _a, _b, _c;
    if (this.sourceId !== null && typeof this.sourceId === "object" && !Array.isArray(this.sourceId)) {
      return this.sourceId;
    }
    const keys = Array.isArray(this.parent.options.filterTargetKey) ? this.parent.options.filterTargetKey : [this.parent.options.filterTargetKey].filter(Boolean);
    const referer = ((_b = (_a = context == null ? void 0 : context.request) == null ? void 0 : _a.headers) == null ? void 0 : _b.referer) || "";
    const routeKey = (_c = /\/filterbytk\/([^/?#]+)/.exec(referer)) == null ? void 0 : _c[1];
    const sourceId = routeKey ?? this.sourceId;
    const decodedSourceId = typeof sourceId === "string" ? this.parseCompositeSourceId(sourceId) : sourceId;
    if (decodedSourceId !== null && typeof decodedSourceId === "object" && !Array.isArray(decodedSourceId)) {
      return decodedSourceId;
    }
    const values = Array.isArray(decodedSourceId) ? decodedSourceId : [decodedSourceId];
    if (!keys.length || keys.length !== values.length || values.some((value) => value === null || value === void 0 || typeof value === "object")) {
      return void 0;
    }
    const filter = Object.fromEntries(keys.map((key, index) => [key, values[index]]));
    return await this.parent.repository.findOne({ filter });
  }
  parseCompositeSourceId(sourceId) {
    try {
      const parsed = JSON.parse(decodeURIComponent(sourceId));
      return Array.isArray(parsed) || parsed && typeof parsed === "object" ? parsed : sourceId;
    } catch {
      return sourceId;
    }
  }
  async create(options = {}) {
    const source = await this.sourceRecord(options.context);
    const relationFilter = source ? this.relationFilterFromSource(source) : void 0;
    if (!relationFilter || this.field.type !== "hasMany") {
      throw new Error(`Oracle relation ${this.field.name} does not support create`);
    }
    return this.target.repository.create({ values: { ...options.values, ...relationFilter } });
  }
  async update(options = {}) {
    const source = await this.sourceRecord(options.context);
    const relationFilter = source ? this.relationFilterFromSource(source) : void 0;
    if (!relationFilter || this.field.type !== "hasMany") {
      throw new Error(`Oracle relation ${this.field.name} does not support update`);
    }
    return this.target.repository.update({ ...options, filter: this.mergeFilter(options.filter, relationFilter) });
  }
  async destroy(options = {}) {
    const source = await this.sourceRecord(options.context);
    const relationFilter = source ? this.relationFilterFromSource(source) : void 0;
    if (!relationFilter || this.field.type !== "hasMany") {
      throw new Error(`Oracle relation ${this.field.name} does not support delete`);
    }
    return this.target.repository.destroy({ ...options, filter: this.mergeFilter(options.filter, relationFilter) });
  }
}
class OracleCollectionManager extends import_data_source_manager.CollectionManager {
  db = {
    getRepository: (name, sourceId) => this.getRepository(name, sourceId),
    getCollection: (name) => this.getCollection(name),
    getCollections: () => this.getCollections(),
    getFieldByPath: (path) => {
      const [collectionName, ...fieldPath] = path.split(".");
      const collection = this.getCollection(collectionName);
      return fieldPath.length ? collection == null ? void 0 : collection.getField(fieldPath.join(".")) : void 0;
    }
  };
  constructor() {
    super();
    this.registerRepositories({ Repository: OracleRepository });
  }
  getRepository(name, sourceId) {
    var _a;
    const [parentName, relationName] = name.split(".");
    if (!relationName) return super.getRepository(name, sourceId);
    const parent = this.getCollection(parentName);
    const field = (_a = parent == null ? void 0 : parent.options.fields) == null ? void 0 : _a.find(
      (item) => item.name.toUpperCase() === relationName.toUpperCase() && RELATION_TYPES.has(item.type)
    );
    if (!parent || !field || !field.target) throw new Error(`Unknown Oracle relation: ${name}`);
    const target = this.getCollection(field.target);
    if (!target) throw new Error(`Unknown Oracle relation target: ${field.target}`);
    return new OracleRelationRepository(parent, target, field, sourceId);
  }
  getCollection(name) {
    var _a, _b;
    const [parentName, relationName] = name.split(".");
    if (!relationName) return super.getCollection(name);
    const parent = super.getCollection(parentName);
    const field = (_b = (_a = parent == null ? void 0 : parent.options) == null ? void 0 : _a.fields) == null ? void 0 : _b.find(
      (item) => item.name.toUpperCase() === relationName.toUpperCase() && RELATION_TYPES.has(item.type)
    );
    return (field == null ? void 0 : field.target) ? super.getCollection(field.target) : void 0;
  }
}
class OracleDataSource extends import_data_source_manager.DatabaseDataSource {
  static async testConnection(options) {
    await (0, import_postgresql.testPostgresqlConnection)(options);
    return true;
  }
  createCollectionManager() {
    return new OracleCollectionManager();
  }
  async load(options = {}) {
    for (const collection of Object.values(options.localData || {})) {
      this.collectionManager.defineCollection(collection);
    }
  }
  async execute(sql, binds, _options = {}) {
    const { connection } = await this.connect();
    try {
      const names = Object.keys(binds || {});
      const query = sql.replace(/@([A-Za-z_][A-Za-z0-9_]*)/g, (_match, name) => {
        const index = names.indexOf(name);
        if (index < 0) throw new Error(`Unknown PostgreSQL bind parameter: ${name}`);
        return `$${index + 1}`;
      });
      const result = await connection.query(
        query,
        names.map((name) => (binds == null ? void 0 : binds[name]) ?? null)
      );
      return await Promise.all(
        (result.rows || []).map((value) => this.materializeRow(row(value)))
      );
    } finally {
      await connection.end();
    }
  }
  async materializeRow(value) {
    const entries = await Promise.all(
      Object.entries(value).map(async ([key, item]) => [key, await this.materializeValue(item)])
    );
    return Object.fromEntries(entries);
  }
  async materializeValue(value) {
    if (Buffer.isBuffer(value)) return value.toString("base64");
    if (value && typeof value === "object" && "getData" in value && typeof value.getData === "function") {
      const data = await value.getData();
      return Buffer.isBuffer(data) ? data.toString("base64") : data;
    }
    return value;
  }
  async readTables() {
    var _a;
    const prefix = (_a = this.options.tablePrefix) == null ? void 0 : _a.trim();
    const schema = this.schemaName();
    const objects = await this.execute(
      `SELECT table_name AS TABLE_NAME, CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE
       FROM information_schema.tables
       WHERE table_schema = @schema AND table_type IN ('BASE TABLE', 'VIEW')
         AND (CAST(@tablePrefix AS text) IS NULL OR table_name LIKE CAST(@tablePrefix AS text))
       ORDER BY table_name LIMIT @limit`,
      { schema, tablePrefix: prefix ? `${prefix}%` : null, limit: MAX_OBJECTS }
    );
    const names = /* @__PURE__ */ new Set();
    return objects.flatMap((value) => {
      const object = row(value);
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
        view: object.OBJECT_TYPE === "VIEW"
      };
    });
  }
  async loadTables(ctx, tables = [], options = {}) {
    var _a, _b;
    if (tables.length > MAX_OBJECTS) throw new Error(`A maximum of ${MAX_OBJECTS} PostgreSQL objects can be selected`);
    const selectedNames = new Set(tables.map((name) => String(name).trim()));
    if (selectedNames.size !== tables.length) throw new Error("PostgreSQL collections must be selected only once");
    for (const name of selectedNames) assertSafeObjectName(name);
    const discovered = await this.readTables();
    const selected = discovered.filter((object) => selectedNames.has(object.name));
    if (!discovered.length && selected.length !== selectedNames.size && !this.options.tablePrefix) {
      selected.push(
        ...(await this.readExplicitTables([...selectedNames])).filter(
          (object) => !selected.some((item) => item.name === object.name)
        )
      );
    }
    if (selected.length !== selectedNames.size)
      throw new Error("One or more selected PostgreSQL objects are unavailable");
    const collections = await Promise.all(selected.map((object) => this.loadCollection(object)));
    await this.addForeignKeyRelations(collections);
    const merged = options.localData ? this.mergeWithLoadedCollections(collections, options.localData) : collections;
    for (const collection of merged) {
      const configured = (_a = options.localData) == null ? void 0 : _a[collection.name];
      const fieldNames = new Set((collection.fields || []).map((field) => field.name));
      const configuredKey = (configured == null ? void 0 : configured.filterTargetKey) || (configured == null ? void 0 : configured.recordUniqueKey);
      const automaticKey = (_b = collections.find((item) => item.name === collection.name)) == null ? void 0 : _b.filterTargetKey;
      if (Array.isArray(configuredKey) && configuredKey.length && configuredKey.every((name) => fieldNames.has(name))) {
        collection.filterTargetKey = configuredKey;
      } else if (automaticKey) {
        collection.filterTargetKey = automaticKey;
      } else {
        delete collection.filterTargetKey;
      }
    }
    for (const collection of merged) this.collectionManager.defineCollection(collection);
    ctx.action.params.values.collections = merged.map(({ fields, ...collection }) => ({
      name: collection.name,
      options: collection,
      fields: fields.map((field) => ({ name: field.name, options: field }))
    }));
    ctx.body = merged;
  }
  publicOptions() {
    const { password: _password, ...safeOptions } = this.options;
    return safeOptions;
  }
  async connect() {
    const driver = await (0, import_postgresql.loadPostgresqlDriver)();
    return {
      driver,
      connection: await (0, import_postgresql.createPostgresqlConnection)(this.options, Promise.resolve(driver))
    };
  }
  schemaName() {
    const schema = String(this.options.schema || "public").trim();
    if (!IDENTIFIER.test(schema)) throw new Error("PostgreSQL schema must be a valid identifier");
    return schema;
  }
  async readExplicitTables(names) {
    const result = [];
    for (const name of names) {
      const rows = await this.execute(
        "SELECT table_name AS TABLE_NAME, CASE WHEN table_type = 'VIEW' THEN 'VIEW' ELSE 'TABLE' END AS OBJECT_TYPE FROM information_schema.tables WHERE table_schema = @schema AND table_name = @tableName AND table_type IN ('BASE TABLE', 'VIEW')",
        { schema: this.schemaName(), tableName: name }
      );
      const object = row(rows[0]);
      if (!(object == null ? void 0 : object.TABLE_NAME) || UNSAFE_OBJECT_NAME.test(object.TABLE_NAME)) continue;
      result.push({
        name: object.TABLE_NAME,
        tableName: object.TABLE_NAME,
        schema: this.schemaName(),
        view: object.OBJECT_TYPE === "VIEW"
      });
    }
    return result;
  }
  async loadCollection(object) {
    const binds = { schema: object.schema, tableName: object.tableName };
    const [columns, constraintColumns] = await Promise.all([
      this.execute(
        "SELECT column_name AS COLUMN_NAME, data_type AS DATA_TYPE, COALESCE(character_maximum_length, 0) AS DATA_LENGTH, numeric_precision AS DATA_PRECISION, numeric_scale AS DATA_SCALE, CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS NULLABLE, column_default AS DATA_DEFAULT FROM information_schema.columns WHERE table_schema = @schema AND table_name = @tableName ORDER BY ordinal_position",
        binds
      ),
      this.execute(
        "SELECT kcu.column_name AS COLUMN_NAME, kcu.constraint_name AS CONSTRAINT_NAME, CASE WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'P' ELSE 'U' END AS CONSTRAINT_TYPE, kcu.ordinal_position AS POSITION FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name WHERE tc.table_schema = @schema AND tc.table_name = @tableName AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') ORDER BY CONSTRAINT_TYPE, kcu.constraint_name, kcu.ordinal_position",
        binds
      )
    ]);
    const primaryConstraints = /* @__PURE__ */ new Map();
    const uniqueConstraints = /* @__PURE__ */ new Map();
    for (const value of constraintColumns.map((item) => row(item))) {
      const constraints = value.CONSTRAINT_TYPE === "P" ? primaryConstraints : uniqueConstraints;
      constraints.set(value.CONSTRAINT_NAME, [...constraints.get(value.CONSTRAINT_NAME) || [], value]);
    }
    const constraintKeys = (constraints) => [...constraints.entries()].sort(([left], [right]) => left.localeCompare(right)).map(
      ([, columns2]) => columns2.sort((left, right) => left.POSITION - right.POSITION).map(({ COLUMN_NAME }) => COLUMN_NAME)
    );
    const primaryKeys = constraintKeys(primaryConstraints);
    const uniqueKeys = constraintKeys(uniqueConstraints);
    const primaryColumns = new Set(primaryKeys.filter((columns2) => columns2.length === 1).flat());
    const uniqueColumns = new Set(uniqueKeys.filter((columns2) => columns2.length === 1).flat());
    const automaticUniqueKey = primaryKeys[0] || uniqueKeys[0];
    return {
      ...object,
      title: object.name,
      ...automaticUniqueKey ? { filterTargetKey: automaticUniqueKey } : {},
      fields: columns.map((value) => mapOracleField(row(value), primaryColumns, uniqueColumns))
    };
  }
  async addForeignKeyRelations(collections) {
    var _a, _b;
    if (collections.length < 2) return;
    const collectionsByTableName = new Map(collections.map((collection) => [collection.tableName, collection]));
    const rows = await this.execute(
      "SELECT child.constraint_name AS CONSTRAINT_NAME, child.table_name AS CHILD_TABLE_NAME, child.column_name AS CHILD_COLUMN_NAME, parent.table_name AS PARENT_TABLE_NAME, parent.column_name AS PARENT_COLUMN_NAME, child.ordinal_position AS POSITION FROM information_schema.key_column_usage child JOIN information_schema.referential_constraints rc ON rc.constraint_schema = child.constraint_schema AND rc.constraint_name = child.constraint_name JOIN information_schema.key_column_usage parent ON parent.constraint_schema = rc.unique_constraint_schema AND parent.constraint_name = rc.unique_constraint_name AND parent.ordinal_position = child.position_in_unique_constraint WHERE child.table_schema = @schema ORDER BY child.constraint_name, child.ordinal_position",
      { schema: this.schemaName() }
    );
    const foreignKeys = /* @__PURE__ */ new Map();
    for (const value of rows.map((item) => row(item))) {
      const child = collectionsByTableName.get(value.CHILD_TABLE_NAME);
      const parent = collectionsByTableName.get(value.PARENT_TABLE_NAME);
      if (!child || !parent || !value.CONSTRAINT_NAME) continue;
      foreignKeys.set(value.CONSTRAINT_NAME, [...foreignKeys.get(value.CONSTRAINT_NAME) || [], value]);
    }
    const fieldNames = new Map(
      collections.map((collection) => [collection.name, new Set((collection.fields || []).map((field) => field.name))])
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
        mapping.CHILD_COLUMN_NAME.toUpperCase().endsWith("_ID") ? mapping.CHILD_COLUMN_NAME.slice(0, -3) : parent.name
      );
      const hasManyName = this.relationFieldName(parentFieldNames, child.name, mapping.CHILD_COLUMN_NAME);
      (_a = child.fields) == null ? void 0 : _a.push({
        name: belongsToName,
        field: belongsToName,
        rawType: "relation",
        type: "belongsTo",
        interface: "m2o",
        target: parent.name,
        foreignKey: mapping.CHILD_COLUMN_NAME,
        sourceKey: mapping.CHILD_COLUMN_NAME,
        targetKey: mapping.PARENT_COLUMN_NAME,
        externalForeignKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalSourceKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalTargetKeys: mappings.map((item) => item.PARENT_COLUMN_NAME)
      });
      (_b = parent.fields) == null ? void 0 : _b.push({
        name: hasManyName,
        field: hasManyName,
        rawType: "relation",
        type: "hasMany",
        interface: "o2m",
        target: child.name,
        foreignKey: mapping.CHILD_COLUMN_NAME,
        sourceKey: mapping.PARENT_COLUMN_NAME,
        targetKey: mapping.CHILD_COLUMN_NAME,
        externalForeignKeys: mappings.map((item) => item.CHILD_COLUMN_NAME),
        externalSourceKeys: mappings.map((item) => item.PARENT_COLUMN_NAME),
        externalTargetKeys: mappings.map((item) => item.CHILD_COLUMN_NAME)
      });
    }
  }
  relationFieldName(fieldNames, name, suffix) {
    if (!(fieldNames == null ? void 0 : fieldNames.has(name))) {
      fieldNames == null ? void 0 : fieldNames.add(name);
      return name;
    }
    const uniqueName = `${name}_${suffix || "RELATION"}`;
    fieldNames.add(uniqueName);
    return uniqueName;
  }
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  OracleDataSource,
  PostgresqlDataSource
});
