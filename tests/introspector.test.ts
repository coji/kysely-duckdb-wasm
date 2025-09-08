import { expect, test } from "vitest";
import { CompiledQuery } from "kysely";
import { DuckDbIntrospector } from "../src/introspector";
import { setupDb } from "./test_common";

test("introspector: getSchemas includes current schema", async () => {
  const db = await setupDb();

  const cur = await db.executeQuery(
    CompiledQuery.raw("SELECT current_schema() AS s;")
  );
  const current = (cur.rows[0] as any)["s"] as string;

  const introspector = new DuckDbIntrospector(db);
  const schemas = await introspector.getSchemas();

  expect(schemas.find((sc) => sc.name === current)).toBeTruthy();
});

test("introspector: getTables returns created tables with columns", async () => {
  const db = await setupDb();
  const introspector = new DuckDbIntrospector(db);

  const cur = await db.executeQuery(
    CompiledQuery.raw("SELECT current_schema() AS s;")
  );
  const current = (cur.rows[0] as any)["s"] as string;

  const tables = await introspector.getTables();

  const t1 = tables.find((t) => t.name === "t1");
  const t2 = tables.find((t) => t.name === "t2");

  expect(t1).toBeTruthy();
  expect(t2).toBeTruthy();

  // Basic table properties
  expect(t1!.schema).toBe(current);
  expect(t1!.isView).toBe(false);
  expect(Array.isArray(t1!.columns)).toBe(true);
  expect(t1!.columns.map((c) => c.name).sort()).toEqual(["a", "b"]);

  // t2 must contain all declared columns
  const t2Cols = new Set(t2!.columns.map((c) => c.name));
  [
    "int_list",
    "string_list",
    "m",
    "st",
    "bs",
    "bl",
    "bool",
    "dt",
    "ts",
    "tsz",
    "enm",
    "delta"
  ].forEach((c) => expect(t2Cols.has(c)).toBe(true));
});

test("introspector: getMetadata aggregates tables", async () => {
  const db = await setupDb();
  const introspector = new DuckDbIntrospector(db);

  const md = await introspector.getMetadata();
  expect(md.tables.length).toBeGreaterThanOrEqual(2);
  expect(md.tables.some((t) => t.name === "t1")).toBe(true);
});
