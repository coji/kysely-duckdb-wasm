import {
  type DatabaseIntrospector,
  type Dialect,
  type DialectAdapter,
  type Driver,
  Kysely,
  type QueryCompiler,
  type Simplify
} from "kysely";

import { DuckDbAdapter } from "./adapter";
import { DuckDbWasmDriver, type DuckDbWasmDriverConfig } from "./driver-wasm";
import { DuckDbIntrospector } from "./introspector";
import {
  DuckDbQueryCompiler,
  type DuckDbQueryCompilerConfigs
} from "./query-compiler";

export type DuckDbDialectConfig = Simplify<
  DuckDbWasmDriverConfig & DuckDbQueryCompilerConfigs
>;
/**
 * Kysely dialect for duckdb.
 *
 * ## Quick Start and Usage Example
 * Please see also [Kysely Docs](https://kysely.dev/docs/intro) and [Duckdb Docs](https://duckdb.org/docs/)
 *
 * ### Install
 * ```bash
 * $ npm install --save kysely @duckdb/duckdb-wasm @coji/kysely-duckdb-wasm
 * ```
 *
 * ### Basic Usage Example
 * reading data from json file.
 * ```ts
 * import * as duckdb from "@duckdb/duckdb-wasm";
 * import { Kysely } from "kysely";
 * import { DuckDbDialect } from "@coji/kysely-duckdb-wasm";
 *
 * interface PersonTable {
 *   first_name: string,
 *   gender: string,
 *   last_name: string,
 * };
 * interface DatabaseSchema {
 *   person: PersonTable,
 * };
 *
 * const worker = new Worker('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js');
 * const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR);
 * const db = new duckdb.AsyncDuckDB(logger, worker);
 * await db.instantiate('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm');
 * const duckdbDialect = new DuckDbDialect({
 *   database: db,
 *   tableMappings: {
 *     person:
 *       `read_json('./person.json', columns={"first_name": "STRING", "gender": "STRING", "last_name": "STRING"})`,
 *   },
 * });
 * const kysely = new Kysely<DatabaseSchema>({ dialect: duckdbDialect });
 *
 * const res = await kysely.selectFrom("person").selectAll().execute();
 * ```
 */
export class DuckDbDialect implements Dialect {
  /**
   * @param config configurations for DuckDbDialect
   */
  constructor(private readonly config: DuckDbDialectConfig) {}
  createQueryCompiler(): QueryCompiler {
    return new DuckDbQueryCompiler(this.config);
  }
  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new DuckDbIntrospector(db);
  }
  createDriver(): Driver {
    return new DuckDbWasmDriver(this.config);
  }
  createAdapter(): DialectAdapter {
    return new DuckDbAdapter();
  }
}

export * as datatypes from "./helper/datatypes";
export type { DuckDBWasmDataTypes } from "./helper/datatypes";
export { DuckDbWasmDriver, type DuckDbWasmDriverConfig } from "./driver-wasm";
