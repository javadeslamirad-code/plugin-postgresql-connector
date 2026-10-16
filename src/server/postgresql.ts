export type PostgresqlOptions = {
  host: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  schema?: string;
  tablePrefix?: string;
  ssl?: boolean;
  connectTimeout?: number;
};

export type PostgresqlResult = { rows?: unknown[] };

export type PostgresqlConnection = {
  query: (sql: string, values?: unknown[]) => Promise<PostgresqlResult>;
  end: () => Promise<void>;
};

export type PostgresqlDriver = {
  Client: new (options: Record<string, unknown>) => PostgresqlConnection & { connect: () => Promise<void> };
};

export async function loadPostgresqlDriver(): Promise<PostgresqlDriver> {
  try {
    const imported = await import('pg');
    return (imported.default ?? imported) as unknown as PostgresqlDriver;
  } catch {
    throw new Error(
      'The pg driver is not installed. Install the plugin runtime dependencies before testing a connection.',
    );
  }
}

export async function createPostgresqlConnection(
  options: PostgresqlOptions,
  driver = loadPostgresqlDriver(),
): Promise<PostgresqlConnection> {
  if (!options.host || !options.database || !options.username || !options.password) {
    throw new Error('Host, database, username, and password are required');
  }
  const postgresql = await driver;
  const connection = new postgresql.Client({
    host: options.host,
    port: options.port || 5432,
    database: options.database,
    user: options.username,
    password: options.password,
    connectionTimeoutMillis: options.connectTimeout || 15000,
    ...(options.ssl ? { ssl: { rejectUnauthorized: false } } : {}),
  });
  await connection.connect();
  return connection;
}

export async function testPostgresqlConnection(options: PostgresqlOptions) {
  const connection = await createPostgresqlConnection(options);
  try {
    await connection.query('SELECT 1 AS ok');
    return true;
  } finally {
    await connection.end();
  }
}
