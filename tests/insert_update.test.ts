import { sql } from "kysely";
import { expect, test } from "vitest";
import * as types from "../src/helper/datatypes";
import { setupDb } from "./test_common";

test("inset into table", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t1")
    .values([{
      a: 1,
      b: 2,
    }, {
      a: 2,
      b: 3,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(2));

  const selectRes = await kysely.selectFrom("t1").selectAll().execute();
  expect(selectRes.length).toBe(3);
});

test("insert into complex types", async () => {
  const kysely = await setupDb();

  const res = await kysely.insertInto("t2")
    .values([{
      int_list: types.list([3, 4, 5]),
      string_list: types.list(["d", "e", "f"]),
      m: types.map([[1, 2], [3, 4]]),
      st: types.struct({
        x: sql`${1}`,
        y: sql`${"aaa"}`,
      }),
      bs: types.bit("010101"),
      bl: types.blob(new Uint8Array([0xBB, 0xCC])),
      bool: true,
      dt: types.date(new Date()),
      ts: types.timestamp(new Date()),
      tsz: types.timestamptz(new Date().toISOString().slice(0, -1) + "+03:00"),
      enm: "sad",
      delta: sql`INTERVAL 1 YEAR`,
    }])
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numInsertedOrUpdatedRows).toBe(BigInt(1));
});

test("update table", async () => {
  const kysely = await setupDb();

  const res = await kysely.updateTable("t1")
    .set({
      a: 10,
    })
    .where("a", "=", 1)
    .execute();

  expect(res.length).toBe(1);
  expect(res[0].numUpdatedRows).toBe(BigInt(1));
});

test("insert returning rows", async () => {
  const kysely = await setupDb();

  const inserted = await kysely
    .insertInto("t1")
    .values({ a: 20, b: 21 })
    .returningAll()
    .execute();

  expect(inserted).toEqual([{ a: 20, b: 21 }]);

  const all = await kysely.selectFrom("t1").selectAll().execute();
  expect(all.find((r) => r.a === 20 && r.b === 21)).toBeTruthy();
});
