# @rad-org/plugin-postgresql-connector

NocoBase external data source connector for PostgreSQL.

The connector discovers tables and views, persists selected collection metadata, maps keys and foreign-key relations, and exposes safe list, count, aggregate, query, create, update, and destroy operations through the NocoBase data source manager.

## Configuration

- Host, port, database, username, and password
- Schema, table prefix, and optional SSL

The default port is `5432` and the default schema is `public`. Views are read-only; base tables support CRUD operations.

## NocoBase

Enable the plugin in a NocoBase 2.x application with the Data Source Manager plugin enabled. The data source type appears as `PostgreSQL`, and the connection diagnostics page is available under the modern admin settings UI.
