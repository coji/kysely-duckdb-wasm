import * as duckdb from '@duckdb/duckdb-wasm'
import { expect, test } from 'vitest'
import { DuckDbDialect } from '../src'
import { KyselyDuckDbExtension } from '../src/extension'

interface DB {
  src: { a: number; b: number }
}

async function setupExtensionDb() {
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm',
      mainWorker:
        '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js',
    },
    eh: {
      mainModule: '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-eh.wasm',
      mainWorker:
        '/node_modules/@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js',
    },
  })

  const worker = new Worker(bundle.mainWorker!)
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING)
  const db = new duckdb.AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)

  const dialect = new DuckDbDialect({ database: db, tableMappings: {} })
  const ext = new KyselyDuckDbExtension<DB>({ dialect })
  return ext
}

test('createTablesAsSelect creates table with safe identifier', async () => {
  const ext = await setupExtensionDb()

  await ext.executeQuery(
    // Create source table and seed a row
    { sql: 'CREATE TABLE src(a INT, b INT);', parameters: [] } as any,
  )
  await ext.executeQuery({
    sql: 'INSERT INTO src VALUES (1, 2);',
    parameters: [],
  } as any)

  // Create table via CTAS using validated/quoted identifier
  const next = await ext.createTablesAsSelect({
    target_ctas: ext.selectFrom('src').select(['a', 'b']),
  })

  const rows = await next.selectFrom('target_ctas').selectAll().execute()
  expect(rows).toEqual([{ a: 1, b: 2 }])
})

test('createTablesAsSelect rejects invalid table name', async () => {
  const ext = await setupExtensionDb()

  await ext.executeQuery({
    sql: 'CREATE TABLE src(a INT, b INT);',
    parameters: [],
  } as any)

  await expect(async () => {
    await ext.createTablesAsSelect({
      // invalid identifier (hyphen)
      'bad-name': ext.selectFrom('src').select(['a']),
    } as any)
  }).rejects.toThrow(/Invalid table name/)
})
