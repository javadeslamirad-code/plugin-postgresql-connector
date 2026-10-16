import { Context } from '@nocobase/actions';
import { Application, Plugin } from '@nocobase/server';
import { PostgresqlDataSource } from './postgresql-data-source';
import { PostgresqlOptions, testPostgresqlConnection } from './postgresql';

type PostgresqlOptionsWithPassword = PostgresqlOptions & { password?: string };

export function normalizePostgresqlSelection(
  values: Record<string, unknown>,
  fallbackOptions: Record<string, unknown> = {},
) {
  const options = (values.options || {}) as Record<string, unknown>;
  const rawSelectedTables = options.selectedTables ?? fallbackOptions.selectedTables ?? values.collections;
  const selectedTables = Array.isArray(rawSelectedTables)
    ? rawSelectedTables.map((name) => String(name).trim()).filter(Boolean)
    : [];
  return {
    ...values,
    options: { ...options, selectedTables },
    collections: selectedTables,
  };
}

export function mergePostgresqlPassword(
  currentOptions: PostgresqlOptionsWithPassword,
  submittedOptions: PostgresqlOptionsWithPassword,
) {
  if (submittedOptions.password) return submittedOptions;
  const { password } = currentOptions;
  return password ? { ...submittedOptions, password } : submittedOptions;
}

export class PluginPostgresqlConnectorServer extends Plugin {
  declare app: Application;

  async beforeLoad() {
    this.app.dataSourceManager.registerDataSourceType('postgresql', PostgresqlDataSource);
    this.app.db.on('dataSources.afterSaveWithAssociations', async (model, { transaction }) => {
      if (model.get('type') !== 'postgresql') return;
      const options = (model.get('options') || {}) as Record<string, unknown>;
      const selectedTables = new Set(
        Array.isArray(options.selectedTables)
          ? options.selectedTables.map((name) => String(name).trim()).filter(Boolean)
          : [],
      );
      const collections = await this.app.db.getRepository('dataSourcesCollections').find({
        filter: { dataSourceKey: model.get('key') },
        transaction,
      });
      for (const collection of collections) {
        if (!selectedTables.has(String(collection.get('name')))) await collection.destroy({ transaction });
      }
      await model.loadIntoApplication({ app: this.app, transaction, refresh: true });
    });
    this.app.resourceManager.use(
      async (ctx, next) => {
        const action = ctx.action;
        if (action?.resourceName === 'dataSources' && action.actionName === 'update') {
          const values = action.params.values;
          const model = await ctx.db.getRepository('dataSources').findByTargetKey(action.params.filterByTk);
          if (model?.get('type') === 'postgresql' && values?.options) {
            Object.assign(values, normalizePostgresqlSelection(values, model.get('options') || {}));
            values.options = mergePostgresqlPassword(model.get('options') || {}, values.options);
          }
        }
        if (
          action?.resourceName === 'dataSources' &&
          action.actionName === 'create' &&
          action.params.values?.type === 'postgresql'
        ) {
          Object.assign(action.params.values, normalizePostgresqlSelection(action.params.values));
        }
        await next();
        if (
          (action?.actionName === 'create' || action?.actionName === 'update') &&
          action.resourceName === 'dataSources' &&
          action.params.values?.type === 'postgresql'
        ) {
          await this.persistPostgresqlMetadata(ctx.db, action.params.values.key);
        }
      },
      { tag: 'normalizePostgresqlDataSource', before: 'default' },
    );
  }

  private async persistPostgresqlMetadata(db: Context['db'], dataSourceKey: string) {
    const dataSources = db.getRepository('dataSources');
    const model = await dataSources.findByTargetKey(dataSourceKey);
    if (!model || model.get('type') !== 'postgresql') return;
    const options = (model.get('options') || {}) as Record<string, unknown>;
    const selectedTables = Array.isArray(options.selectedTables)
      ? options.selectedTables.map((name) => String(name).trim()).filter(Boolean)
      : [];
    const dataSource = this.app.dataSourceManager.factory.create('postgresql', {
      name: dataSourceKey,
      ...options,
    }) as PostgresqlDataSource;
    const metadataContext = { action: { params: { values: {} } } } as unknown as Context;
    await dataSource.loadTables(metadataContext, selectedTables);
    const collections = metadataContext.action.params.values.collections as Array<{
      name: string;
      options: Record<string, unknown>;
      fields: Array<{ name: string; options: Record<string, unknown> }>;
    }>;
    const collectionRepository = db.getRepository('dataSourcesCollections');
    const fieldRepository = db.getRepository('dataSourcesFields');
    const existing = await collectionRepository.find({ filter: { dataSourceKey } });
    for (const collection of existing) {
      if (!collections.some((item) => item.name === collection.get('name'))) await collection.destroy();
    }
    for (const collection of collections) {
      const current = existing.find((item) => item.get('name') === collection.name);
      if (current) {
        await fieldRepository.destroy({ filter: { dataSourceKey, collectionName: collection.name } });
        await collectionRepository.update({
          filter: { dataSourceKey, name: collection.name },
          values: { name: collection.name, options: collection.options },
        });
        for (const field of collection.fields) {
          await fieldRepository.create({
            values: { ...field, collectionName: collection.name, dataSourceKey, collectionKey: current.get('key') },
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
      name: 'postgresqlConnector',
      actions: {
        testConnection: async (ctx, next) => {
          const values = ctx.action.params.values ?? {};
          await testPostgresqlConnection(values as PostgresqlOptions);
          ctx.body = { success: true, driver: 'postgresql' };
          await next();
        },
        readTables: async (ctx, next) => {
          const values = (ctx.action.params.values ?? {}) as {
            dataSourceKey?: string;
            options?: PostgresqlOptionsWithPassword;
          };
          const model = values.dataSourceKey
            ? await ctx.db.getRepository('dataSources').findByTargetKey(values.dataSourceKey)
            : undefined;
          const savedOptions = (model?.get('options') || {}) as PostgresqlOptionsWithPassword;
          const options = mergePostgresqlPassword(savedOptions, { ...savedOptions, ...values.options });
          const dataSource = this.app.dataSourceManager.factory.create('postgresql', {
            name: values.dataSourceKey,
            ...options,
          }) as PostgresqlDataSource;
          ctx.body = await dataSource.readTables();
          await next();
        },
      },
    });
    this.app.acl.allow('postgresqlConnector', 'testConnection', 'loggedIn');
    this.app.acl.allow('postgresqlConnector', 'readTables', 'loggedIn');
  }
}

export default PluginPostgresqlConnectorServer;
