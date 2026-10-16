import { Context } from '@nocobase/actions';
import { createPostgresqlConnection, PostgresqlDriver } from '../postgresql';
import { PostgresqlDataSource } from '../postgresql-data-source';
import { mergePostgresqlPassword, normalizePostgresqlSelection } from '../plugin';

const options = {
  name: 'postgresql',
  host: 'localhost',
  database: 'app',
  username: 'app',
  password: 'secret',
  schema: 'public',
};

describe('PostgresqlDataSource', () => {
  it('creates a configured pg client and connects before use', async () => {
    const connect = vi.fn(async () => undefined);
    const Client = vi.fn(() => ({ connect, query: vi.fn(), end: vi.fn(async () => undefined) }));
    await createPostgresqlConnection({ ...options, port: 5433, ssl: true }, { Client } as unknown as PostgresqlDriver);

    expect(Client).toHaveBeenCalledWith({
      host: 'localhost',
      port: 5433,
      database: 'app',
      user: 'app',
      password: 'secret',
      connectionTimeoutMillis: 15000,
      ssl: { rejectUnauthorized: false },
    });
    expect(connect).toHaveBeenCalledOnce();
  });

  it('normalizes selected tables and preserves an omitted password', () => {
    expect(normalizePostgresqlSelection({ options: { selectedTables: [' orders ', 'customers'] } })).toMatchObject({
      options: { selectedTables: ['orders', 'customers'] },
      collections: ['orders', 'customers'],
    });
    expect(mergePostgresqlPassword({ password: 'saved' }, { host: 'new-host' })).toEqual({
      host: 'new-host',
      password: 'saved',
    });
  });

  it('discovers PostgreSQL tables and views with a schema and prefix', async () => {
    const dataSource = new PostgresqlDataSource({ ...options, tablePrefix: 'nb_' });
    const execute = vi.spyOn(dataSource, 'execute').mockResolvedValue([
      { TABLE_NAME: 'nb_orders', OBJECT_TYPE: 'TABLE' },
      { TABLE_NAME: 'nb_order_view', OBJECT_TYPE: 'VIEW' },
    ]);

    await expect(dataSource.readTables()).resolves.toEqual([
      { name: 'orders', tableName: 'nb_orders', schema: 'public', view: false },
      { name: 'order_view', tableName: 'nb_order_view', schema: 'public', view: true },
    ]);
    expect(execute).toHaveBeenCalledWith(expect.stringContaining('information_schema.tables'), {
      schema: 'public',
      tablePrefix: 'nb_%',
      limit: 500,
    });
    expect(execute.mock.calls[0][0]).toContain('AS "TABLE_NAME"');
    expect(execute.mock.calls[0][0]).toContain('AS "OBJECT_TYPE"');
  });

  it('casts an omitted table prefix so PostgreSQL can infer the null bind type', async () => {
    const dataSource = new PostgresqlDataSource(options);
    const execute = vi.spyOn(dataSource, 'execute').mockResolvedValue([]);

    await expect(dataSource.readTables()).resolves.toEqual([]);
    expect(execute.mock.calls[0][0]).toContain(
      'CAST(@tablePrefix AS text) IS NULL OR table_name LIKE CAST(@tablePrefix AS text)',
    );
  });

  it('maps metadata, keys, and foreign-key relations from PostgreSQL catalogs', async () => {
    const dataSource = new PostgresqlDataSource(options);
    vi.spyOn(dataSource, 'execute').mockImplementation(async (sql) => {
      if (sql.includes('information_schema.tables')) {
        return [
          { TABLE_NAME: 'customers', OBJECT_TYPE: 'TABLE' },
          { TABLE_NAME: 'orders', OBJECT_TYPE: 'TABLE' },
        ];
      }
      if (sql.includes('information_schema.columns')) {
        return [
          { COLUMN_NAME: 'id', DATA_TYPE: 'integer', NULLABLE: 'N', DATA_DEFAULT: null },
          { COLUMN_NAME: 'customer_id', DATA_TYPE: 'integer', NULLABLE: 'N', DATA_DEFAULT: null },
        ];
      }
      if (sql.includes('information_schema.table_constraints')) {
        return [{ COLUMN_NAME: 'id', CONSTRAINT_NAME: 'orders_pkey', CONSTRAINT_TYPE: 'P', POSITION: 1 }];
      }
      return [
        {
          CONSTRAINT_NAME: 'orders_customer_id_fkey',
          CHILD_TABLE_NAME: 'orders',
          CHILD_COLUMN_NAME: 'customer_id',
          PARENT_TABLE_NAME: 'customers',
          PARENT_COLUMN_NAME: 'id',
          POSITION: 1,
        },
      ];
    });
    const ctx = { action: { params: { values: {} } } } as unknown as Context;

    await dataSource.loadTables(ctx, ['customers', 'orders']);

    const collections = ctx.action.params.values.collections;
    expect(collections.find((item) => item.name === 'orders')?.fields).toContainEqual(
      expect.objectContaining({
        name: 'customer',
        options: expect.objectContaining({ type: 'belongsTo', target: 'customers', targetKey: 'id' }),
      }),
    );
    expect(collections.find((item) => item.name === 'customers')?.fields).toContainEqual(
      expect.objectContaining({
        name: 'orders',
        options: expect.objectContaining({ type: 'hasMany', target: 'orders', sourceKey: 'id' }),
      }),
    );
  });

  it('uses bound filters, PostgreSQL pagination, aggregate queries, and CRUD operations', async () => {
    const dataSource = new PostgresqlDataSource(options);
    await dataSource.load({
      localData: {
        orders: {
          name: 'orders',
          tableName: 'orders',
          schema: 'public',
          filterTargetKey: ['id'],
          fields: [
            { name: 'id', field: 'id', type: 'integer', primaryKey: true },
            { name: 'status', field: 'status', type: 'string' },
            { name: 'amount', field: 'amount', type: 'double' },
          ],
        },
      },
    });
    const execute = vi.spyOn(dataSource, 'execute').mockImplementation(async (sql) => {
      if (sql.includes('COUNT(*)')) return [{ COUNT: 2 }];
      return [];
    });
    const repository = dataSource.collectionManager.getRepository('orders');

    await repository.find({ filter: { status: { $includes: 'open' } }, sort: ['-id'], offset: 5, limit: 10 });
    await repository.query({ measures: [{ field: 'amount', aggregation: 'sum', alias: 'total' }] });
    await repository.create({ values: { id: 1, status: 'open', ignored: 'x' } });
    await repository.update({ filterByTk: 1, values: { status: 'closed' } });
    await repository.destroy({ filterByTk: 1 });

    expect(execute.mock.calls[0][0]).toContain('STRPOS(postgresql_root."status", @filter_0) > 0');
    expect(execute.mock.calls[0][0]).toContain('LIMIT @limit OFFSET @offset');
    expect(execute.mock.calls[1][0]).toContain('SUM(postgresql_root."amount")');
    expect(execute.mock.calls.map(([sql]) => sql).join('\n')).toContain(
      'INSERT INTO "public"."orders" ("id", "status") VALUES (@value_0, @value_1)',
    );
    expect(execute.mock.calls.map(([sql]) => sql).join('\n')).toContain(
      'UPDATE "public"."orders" SET "status" = @value_0 WHERE "id" = @record_key',
    );
    expect(execute.mock.calls.map(([sql]) => sql).join('\n')).toContain(
      'DELETE FROM "public"."orders" WHERE "id" = @record_key',
    );
  });
});
