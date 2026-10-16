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
var plugin_exports = {};
__export(plugin_exports, {
  PluginPostgresqlConnectorServer: () => PluginPostgresqlConnectorServer,
  default: () => plugin_default,
  mergePostgresqlPassword: () => mergePostgresqlPassword,
  normalizePostgresqlSelection: () => normalizePostgresqlSelection
});
module.exports = __toCommonJS(plugin_exports);
var import_server = require("@nocobase/server");
var import_postgresql_data_source = require("./postgresql-data-source");
var import_postgresql = require("./postgresql");
function normalizePostgresqlSelection(values, fallbackOptions = {}) {
  const options = values.options || {};
  const rawSelectedTables = options.selectedTables ?? fallbackOptions.selectedTables ?? values.collections;
  const selectedTables = Array.isArray(rawSelectedTables) ? rawSelectedTables.map((name) => String(name).trim()).filter(Boolean) : [];
  return {
    ...values,
    options: { ...options, selectedTables },
    collections: selectedTables
  };
}
function mergePostgresqlPassword(currentOptions, submittedOptions) {
  if (submittedOptions.password) return submittedOptions;
  const { password } = currentOptions;
  return password ? { ...submittedOptions, password } : submittedOptions;
}
class PluginPostgresqlConnectorServer extends import_server.Plugin {
  async beforeLoad() {
    this.app.dataSourceManager.registerDataSourceType("postgresql", import_postgresql_data_source.PostgresqlDataSource);
    this.app.db.on("dataSources.afterSaveWithAssociations", async (model, { transaction }) => {
      if (model.get("type") !== "postgresql") return;
      const options = model.get("options") || {};
      const selectedTables = new Set(
        Array.isArray(options.selectedTables) ? options.selectedTables.map((name) => String(name).trim()).filter(Boolean) : []
      );
      const collections = await this.app.db.getRepository("dataSourcesCollections").find({
        filter: { dataSourceKey: model.get("key") },
        transaction
      });
      for (const collection of collections) {
        if (!selectedTables.has(String(collection.get("name")))) await collection.destroy({ transaction });
      }
      await model.loadIntoApplication({ app: this.app, transaction, refresh: true });
    });
    this.app.resourceManager.use(
      async (ctx, next) => {
        var _a, _b;
        const action = ctx.action;
        if ((action == null ? void 0 : action.resourceName) === "dataSources" && action.actionName === "update") {
          const values = action.params.values;
          const model = await ctx.db.getRepository("dataSources").findByTargetKey(action.params.filterByTk);
          if ((model == null ? void 0 : model.get("type")) === "postgresql" && (values == null ? void 0 : values.options)) {
            Object.assign(values, normalizePostgresqlSelection(values, model.get("options") || {}));
            values.options = mergePostgresqlPassword(model.get("options") || {}, values.options);
          }
        }
        if ((action == null ? void 0 : action.resourceName) === "dataSources" && action.actionName === "create" && ((_a = action.params.values) == null ? void 0 : _a.type) === "postgresql") {
          Object.assign(action.params.values, normalizePostgresqlSelection(action.params.values));
        }
        await next();
        if (((action == null ? void 0 : action.actionName) === "create" || (action == null ? void 0 : action.actionName) === "update") && action.resourceName === "dataSources" && ((_b = action.params.values) == null ? void 0 : _b.type) === "postgresql") {
          await this.persistPostgresqlMetadata(ctx.db, action.params.values.key);
        }
      },
      { tag: "normalizePostgresqlDataSource", before: "default" }
    );
  }
  async persistPostgresqlMetadata(db, dataSourceKey) {
    const dataSources = db.getRepository("dataSources");
    const model = await dataSources.findByTargetKey(dataSourceKey);
    if (!model || model.get("type") !== "postgresql") return;
    const options = model.get("options") || {};
    const selectedTables = Array.isArray(options.selectedTables) ? options.selectedTables.map((name) => String(name).trim()).filter(Boolean) : [];
    const dataSource = this.app.dataSourceManager.factory.create("postgresql", {
      name: dataSourceKey,
      ...options
    });
    const metadataContext = { action: { params: { values: {} } } };
    await dataSource.loadTables(metadataContext, selectedTables);
    const collections = metadataContext.action.params.values.collections;
    const collectionRepository = db.getRepository("dataSourcesCollections");
    const fieldRepository = db.getRepository("dataSourcesFields");
    const existing = await collectionRepository.find({ filter: { dataSourceKey } });
    for (const collection of existing) {
      if (!collections.some((item) => item.name === collection.get("name"))) await collection.destroy();
    }
    for (const collection of collections) {
      const current = existing.find((item) => item.get("name") === collection.name);
      if (current) {
        await fieldRepository.destroy({ filter: { dataSourceKey, collectionName: collection.name } });
        await collectionRepository.update({
          filter: { dataSourceKey, name: collection.name },
          values: { name: collection.name, options: collection.options }
        });
        for (const field of collection.fields) {
          await fieldRepository.create({
            values: { ...field, collectionName: collection.name, dataSourceKey, collectionKey: current.get("key") }
          });
        }
      } else {
        await collectionRepository.create({ values: { ...collection, dataSourceKey } });
      }
    }
    await model.loadIntoApplication({ app: this.app, refresh: true });
  }
  async load() {
    this.app.resourceManager.define({
      name: "postgresqlConnector",
      actions: {
        testConnection: async (ctx, next) => {
          const values = ctx.action.params.values ?? {};
          await (0, import_postgresql.testPostgresqlConnection)(values);
          ctx.body = { success: true, driver: "postgresql" };
          await next();
        },
        readTables: async (ctx, next) => {
          const values = ctx.action.params.values ?? {};
          const model = values.dataSourceKey ? await ctx.db.getRepository("dataSources").findByTargetKey(values.dataSourceKey) : void 0;
          const savedOptions = (model == null ? void 0 : model.get("options")) || {};
          const options = mergePostgresqlPassword(savedOptions, { ...savedOptions, ...values.options });
          const dataSource = this.app.dataSourceManager.factory.create("postgresql", {
            name: values.dataSourceKey,
            ...options
          });
          ctx.body = await dataSource.readTables();
          await next();
        }
      }
    });
    this.app.acl.allow("postgresqlConnector", "testConnection", "loggedIn");
    this.app.acl.allow("postgresqlConnector", "readTables", "loggedIn");
  }
}
var plugin_default = PluginPostgresqlConnectorServer;
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  PluginPostgresqlConnectorServer,
  mergePostgresqlPassword,
  normalizePostgresqlSelection
});
