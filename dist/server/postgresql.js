/**
 * This file is part of the NocoBase (R) project.
 * Copyright (c) 2020-2024 NocoBase Co., Ltd.
 * Authors: NocoBase Team.
 *
 * This project is dual-licensed under AGPL-3.0 and NocoBase Commercial License.
 * For more information, please refer to: https://www.nocobase.com/agreement.
 */

var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
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
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);
var postgresql_exports = {};
__export(postgresql_exports, {
  createPostgresqlConnection: () => createPostgresqlConnection,
  loadPostgresqlDriver: () => loadPostgresqlDriver,
  testPostgresqlConnection: () => testPostgresqlConnection
});
module.exports = __toCommonJS(postgresql_exports);
async function loadPostgresqlDriver() {
  try {
    const imported = await import("pg");
    return imported.default ?? imported;
  } catch {
    throw new Error(
      "The pg driver is not installed. Install the plugin runtime dependencies before testing a connection."
    );
  }
}
async function createPostgresqlConnection(options, driver = loadPostgresqlDriver()) {
  if (!options.host || !options.database || !options.username || !options.password) {
    throw new Error("Host, database, username, and password are required");
  }
  const postgresql = await driver;
  const connection = new postgresql.Client({
    host: options.host,
    port: options.port || 5432,
    database: options.database,
    user: options.username,
    password: options.password,
    connectionTimeoutMillis: options.connectTimeout || 15e3,
    ...options.ssl ? { ssl: { rejectUnauthorized: false } } : {}
  });
  await connection.connect();
  return connection;
}
async function testPostgresqlConnection(options) {
  const connection = await createPostgresqlConnection(options);
  try {
    await connection.query("SELECT 1 AS ok");
    return true;
  } finally {
    await connection.end();
  }
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  createPostgresqlConnection,
  loadPostgresqlDriver,
  testPostgresqlConnection
});
