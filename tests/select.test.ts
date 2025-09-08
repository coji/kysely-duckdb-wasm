import { CompiledQuery, sql } from 'kysely'
import { expect, test } from 'vitest'
import * as types from '../src/helper/datatypes'
import { setupDb } from './test_common'

test('select from json file', async () => {
  const kysely = await setupDb()
  const results = await kysely
    .selectFrom('person')
    .select(['first_name'])
    .execute()
  expect(results).toEqual([{ first_name: 'foo' }])

  const results2 = await kysely
    .selectFrom('person')
    .select(['gender'])
    .execute()
  expect(results2).toEqual([{ gender: 'man' }])
})

test('select from table', async () => {
  const kysely = await setupDb()

  const results = await kysely.selectFrom('t1').selectAll().execute()
  expect(results).toEqual([{ a: 1, b: 2 }])
})

test('select complex data types', async () => {
  const kysely = await setupDb()

  const results = await kysely.selectFrom('t2').selectAll().execute()
  expect(results.length).toBe(1)
  const row = results[0]
  expect(row.int_list).toEqual([1, 2, 3])
  expect(row.string_list).toEqual(['a', 'b', 'c'])
  expect(row.m).toEqual('{a=text, b=text}')
  expect(row.st).toEqual({ x: 1, y: 'a' })
  expect(row.bs).toEqual('010101')
  expect(row.bl).toEqual(new Uint8Array([0xaa]))
  expect(row.bool).toEqual(true)
  expect(row.dt).toEqual(new Date(Date.UTC(1992, 8, 20)))
  expect(row.ts).toEqual(new Date(Date.UTC(1992, 8, 20, 11, 30, 0, 123)))
  expect(row.enm).toEqual('sad')
  expect(row.delta).toEqual({ months: 12, days: 0, micros: 0 })
})

test('select complex data types with where', async () => {
  const kysely = await setupDb()

  const results = await kysely
    .selectFrom('t2')
    .selectAll()
    .where('bs', '=', types.bit('010101'))
    .where('bl', '=', types.blob(new Uint8Array([0xaa])))
    .where('bool', '=', true)
    .where('dt', '=', types.date(new Date(Date.UTC(1992, 8, 20))))
    .where('int_list', '=', types.list([1, 2, 3]))
    .where('string_list', '=', types.list(['a', 'b', 'c']))
    .where('st', '=', types.struct({ x: sql.val(1), y: sql.val('a') }))
    .where(
      'ts',
      '=',
      types.timestamp(new Date(Date.UTC(1992, 8, 20, 11, 30, 0, 123))),
    )
    .where('tsz', '=', types.timestamptz('1992-09-20 11:30:00.123+03:00'))
    .execute()
  expect(results.length).toBe(1)
})

test('select struct with plain values in where', async () => {
  const kysely = await setupDb()

  const results = await kysely
    .selectFrom('t2')
    .selectAll()
    .where('st', '=', types.struct({ x: 1, y: 'a' }))
    .execute()
  expect(results.length).toBe(1)
})

test('BLOB helper accepts number[]', async () => {
  const kysely = await setupDb()

  const results = await kysely
    .selectFrom('t2')
    .select(['bl'])
    .where('bl', '=', types.blob([0xaa]))
    .execute()

  expect(results.length).toBe(1)
  expect(results[0].bl).toEqual(new Uint8Array([0xaa]))
})

test('BIT literal conversion returns bit string', async () => {
  const kysely = await setupDb()

  const r = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '010101'::BIT AS b;"),
  )
  expect(r.rows.length).toBe(1)
  expect(r.rows[0]['b']).toEqual('010101')
})

test('BLOB literal conversion returns Uint8Array', async () => {
  const kysely = await setupDb()

  const r = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '\\xAA\\xBB\\xCC'::BLOB AS b;"),
  )
  expect(r.rows.length).toBe(1)
  expect(r.rows[0]['b']).toEqual(new Uint8Array([0xaa, 0xbb, 0xcc]))
})

test('BLOB leading zero bytes preserved', async () => {
  const kysely = await setupDb()

  const r1 = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '\\x00'::BLOB AS b;"),
  )
  expect(r1.rows.length).toBe(1)
  expect(r1.rows[0]['b']).toEqual(new Uint8Array([0x00]))

  const r2 = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '\\x00\\x0A'::BLOB AS b;"),
  )
  expect(r2.rows.length).toBe(1)
  expect(r2.rows[0]['b']).toEqual(new Uint8Array([0x00, 0x0a]))
})

/*
  NOTE: BIT literal variation tests commented out intentionally.

  Why: DuckDB Wasm -> Arrow sometimes omits logical type metadata for BIT
  when selecting bare literals like '1'::BIT, '1010'::BIT, etc. Without
  metadata (e.g., BIT(n)), Arrow exposes the value as Binary and the driver
  cannot safely distinguish BIT from BLOB purely from bytes.

  We keep the driver conservative to avoid misclassifying BLOBs as BIT.
  For stable assertions, either:
    - specify width: '1'::BIT(1), '1010'::BIT(4) so BIT(n) metadata is present, or
    - cast to text in SQL: (expr::BIT)::VARCHAR and assert the string result.

  If needed later, re-enable with explicit width or VARCHAR casts.

test("BIT literal variations convert to bit strings", async () => {
  const kysely = await setupDb();

  const r1 = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '1'::BIT AS b;")
  );
  expect(r1.rows[0]["b"]).toEqual("1");

  const r2 = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '0000'::BIT AS b;")
  );
  expect(r2.rows[0]["b"]).toEqual("0");

  const r3 = await kysely.executeQuery(
    CompiledQuery.raw("SELECT '1010'::BIT AS b;")
  );
  expect(r3.rows[0]["b"]).toEqual("1010");
});
*/

test('MAP literal conversion formats as key=value pairs', async () => {
  const kysely = await setupDb()

  const r = await kysely.executeQuery(
    CompiledQuery.raw("SELECT map {'x': 'y', 'z': 'w'} AS m;"),
  )
  expect(r.rows.length).toBe(1)
  expect(r.rows[0]['m']).toEqual('{x=y, z=w}')
})

test('TIMESTAMP microseconds are truncated to milliseconds', async () => {
  const kysely = await setupDb()

  const r = await kysely.executeQuery(
    CompiledQuery.raw("SELECT TIMESTAMP '1970-01-01 00:00:00.001234' AS t;"),
  )
  expect(r.rows.length).toBe(1)
  expect(r.rows[0]['t']).toEqual(new Date(Date.UTC(1970, 0, 1, 0, 0, 0, 1)))
})

test('TIMESTAMPTZ with offset converts to UTC', async () => {
  const kysely = await setupDb()

  const r = await kysely.executeQuery(
    CompiledQuery.raw(
      "SELECT TIMESTAMPTZ '1992-09-20 11:30:00.123+03:00' AS t;",
    ),
  )
  expect(r.rows.length).toBe(1)
  expect(r.rows[0]['t']).toEqual(new Date(Date.UTC(1992, 8, 20, 8, 30, 0, 123)))
})
