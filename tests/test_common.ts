import * as duckdb from "@duckdb/duckdb-wasm";
import personData from "./person.json";
import type { DuckDBWasmDataTypes } from "../src/helper/datatypes";
import { datatypes } from "../src/index";
import { DuckDbDialect } from "../src/index";

import type { ColumnType, Generated } from "kysely";
import { CompiledQuery, Kysely } from "kysely";

export interface Database {
  person: PersonTable;
  t1: { a: number; b: number };
  t2: {
    int_list: number[];
    string_list: string[];
    m: string;
    st: {
      x: number;
      y: string;
    };
    bs: DuckDBWasmDataTypes["BIT"];
    bl: DuckDBWasmDataTypes["BLOB"];
    bool: DuckDBWasmDataTypes["BOOLEAN"];
    dt: DuckDBWasmDataTypes["DATE"];
    ts: DuckDBWasmDataTypes["TIMESTAMP"];
    tsz: DuckDBWasmDataTypes["TIMESTAMPTZ"];
    enm: string;
    delta: DuckDBWasmDataTypes["INTERVAL"];
  };
}

export interface PersonTable {
  id: Generated<number>;
  first_name: string;
  gender: "man" | "woman" | "other";
  last_name: string | null;
  created_at: ColumnType<Date, string | undefined, never>;
}

export const setupDb = async () => {
  // Try using local npm bundle instead of CDN to avoid CORS issues
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm",
      mainWorker:
        "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js"
    },
    eh: {
      mainModule: "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm",
      mainWorker:
        "/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js"
    }
  });

  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  // Register test JSON file into DuckDB's virtual FS for browser environment
  await db.registerFileText("person.json", JSON.stringify(personData));

  const duckdbDialect = new DuckDbDialect({
    database: db,
    tableMappings: {
      person: `read_json('person.json', columns={"first_name": "STRING", "gender": "STRING", "last_name": "STRING"})`
    }
  });
  const kysely = new Kysely<Database>({ dialect: duckdbDialect });
  // t1
  await kysely.executeQuery(
    CompiledQuery.raw("CREATE TABLE t1 (a INT, b INT);")
  );
  await kysely.executeQuery(CompiledQuery.raw("INSERT INTO t1 VALUES (1, 2);"));

  // t2
  await kysely.executeQuery(
    CompiledQuery.raw("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
  );
  await kysely.executeQuery(
    CompiledQuery.raw(
      "CREATE TABLE t2 (" +
        [
          "int_list INT[]",
          "string_list STRING[]",
          "m MAP(STRING, STRING)",
          "st STRUCT(x INT, y STRING)",
          "bs BIT",
          "bl BLOB",
          "bool BOOLEAN",
          "dt DATE",
          "ts TIMESTAMP",
          "tsz TIMESTAMPTZ",
          "enm mood",
          "delta INTERVAL"
        ].join(", ") +
        ");"
    )
  );

  await kysely.executeQuery(
    CompiledQuery.raw(
      "INSERT INTO t2 VALUES (" +
        [
          "[1, 2, 3]",
          "['a', 'b', 'c']",
          "map {'a': 'text', 'b': 'text'}",
          "{'x': 1, 'y': 'a'}",
          "'010101'",
          "'\\xAA'",
          "true",
          "'1992-09-20'",
          "'1992-09-20 11:30:00.123'",
          "'1992-09-20 11:30:00.123+03:00'",
          "'sad'",
          "INTERVAL '1' YEAR"
        ].join(", ") +
        ");"
    )
  );

  return kysely;
};
