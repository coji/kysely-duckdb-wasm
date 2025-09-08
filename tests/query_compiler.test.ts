import { expect, test } from "vitest";
import { Kysely } from "kysely";
import { DuckDbDialect } from "../src";

interface DB {
  person: { id: number };
  t1: { a: number };
}

const dialect = new DuckDbDialect({
  // Driver is not used for compile-only tests; provide a dummy.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  database: {} as any,
  tableMappings: {
    person: "read_json('person.json')",
  },
});

test("tableMappings preserves alias in selectFrom", () => {
  const db = new Kysely<DB>({ dialect });
  const q = db.selectFrom("person as p").selectAll();
  const c = q.compile();

  expect(c.sql.toLowerCase()).toContain(
    "from read_json('person.json') as \"p\""
  );
});

test("tableMappings preserves alias in join", () => {
  const db = new Kysely<DB>({ dialect });
  const q = db
    .selectFrom("t1")
    .innerJoin("person as pp", "pp.id", "t1.a")
    .selectAll();
  const c = q.compile();

  expect(c.sql.toLowerCase()).toContain(
    "join read_json('person.json') as \"pp\""
  );
});

