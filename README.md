# Kysely Dialect for DuckDB Wasm

[![test](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml/badge.svg)](https://github.com/runoshun/kysely-duckdb/actions/workflows/test.yml)

This dialect allows you to use [Kysely](https://kysely.dev/) with [DuckDB Wasm](https://duckdb.org/docs/api/wasm/overview.html).

This is forked from [kysely-duckdb](https://github.com/runoshun/kysely-duckdb).

### Installation

```bash
npm install --save kysely @duckdb/duckdb-wasm @coji/kysely-duckdb-wasm
```

### Usage

```ts
import * as duckdb from '@duckdb/duckdb-wasm'
import duckdbWorker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?worker'
import duckdbWasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url'
import { Kysely } from 'kysely'
import { DuckDbDialect } from '@coji/kysely-duckdb-wasm'

const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.ERROR)
const worker = new duckdbWorker()
const db = new duckdb.AsyncDuckDB(logger, worker)
await db.instantiate(duckdbWasm)

const duckdbDialect = new DuckDbDialect({
  database: db,
  tableMappings: {},
})

const kysely = new Kysely<DatabaseSchema>({ dialect: duckdbDialect })
const res = await kysely.selectFrom('person').selectAll().execute()
```

### Configurations

The configuration object of `DuckDbDialect` can contain the following properties:

- `tableMappings`: A mapping of table names in Kysely to DuckDB table expressions. This is useful if you want to use DuckDB's external data sources, such as JSON files or CSV files.

### DuckDB DataTypes Supports (Experimental Feature)

DuckDB supports various data types like arrays, structs, blobs and more.
Kysely has not built in supports for these types, but it can handle almost
of these using [raw SQL](https://kysely.dev/docs/recipes/raw-sql) feature.

This package includes some shallow helper for these types.

```ts
import type { DuckDBWasmDataTypes } from '@coji/kysely-duckdb-wasm'
import { datatypes } from '@coji/kysely-duckdb-wasm'

// DuckDBWasmDataTypes: type mappings for table schema
export interface Database {
  t1: {
    int_list: number[];
    string_list: string[];
    map1: DuckDBWasmDataTypes["MAP"]; // `map` is alias of string now. The returned value from duckdb is like '{a=1,b=2}'
    struct1: {
      x: number;
      y: string;
    };
    bitstring1: DuckDBWasmDataTypes["BIT"];
    blob1: DuckDBWasmDataTypes["BLOB"];
    bool1: DuckDBWasmDataTypes["BOOLEAN"];
    date1: DuckDBWasmDataTypes["DATE"];
    timestamp1: DuckDBWasmDataTypes["TIMESTAMP"];
    timestamptz1: DuckDBWasmDataTypes["TIMESTAMPTZ"];
    interval1: DuckDBWasmDataTypes["INTERVAL"];
  };
}

...

// datatypes: type constructors
const kysely = new Kysely<Database>({ dialect: duckDbDialect })
await kysely
  .insertInto("t1")
  .values([{
    int_list: datatypes.list([3, 4, 5]),
    string_list: datatypes.list(["d", "e", "f"]),
    map1: datatypes.map([[1, 2], [3, 4]]),
    struct1: datatypes.struct({
      x: sql`${1}`,
      y: sql`${"aaa"}`,
    }),
    bitstring1: datatypes.bit('010101'),
    blob1: datatypes.blob(new Uint8Array([0xBB, 0xCC])),
    bool1: true,
    date1: datatypes.date(new Date()),
    timestamp1: datatypes.timestamp(new Date()),
    timestamptz1: datatypes.timestamptz(new Date().toISOString().slice(0, -1) + '+03:00'),
    interval1: sql`INTERVAL 1 YEAR`,
  }])
  .execute();
```
