import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE, Kysely, sql } from "kysely";
import type {
  DatabaseIntrospector,
  DatabaseMetadata,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
} from "kysely";

export class DuckDbIntrospector implements DatabaseIntrospector {
  readonly #db: Kysely<any>;

  constructor(db: Kysely<any>) {
    this.#db = db;
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    const rawSchemas = await this.#db
      .selectFrom("information_schema.schemata")
      .select("schema_name")
      .$castTo<RawSchemaMetadata>()
      .execute();

    return rawSchemas.map((it) => ({ name: it.schema_name }));
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    let query = this.#db
      .selectFrom("information_schema.columns as columns")
      .innerJoin("information_schema.tables as tables", (b) =>
        b
          .onRef("columns.table_catalog", "=", "tables.table_catalog")
          .onRef("columns.table_schema", "=", "tables.table_schema")
          .onRef("columns.table_name", "=", "tables.table_name"))
      .select([
        "columns.column_name",
        "columns.column_default",
        "columns.table_name",
        "columns.table_schema",
        "tables.table_type",
        "columns.is_nullable",
        "columns.data_type",
      ])
      .where("columns.table_schema", "=", sql`current_schema()`)
      .orderBy("columns.table_name")
      .orderBy("columns.ordinal_position")
      .$castTo<RawColumnMetadata>();

    if (!options.withInternalKyselyTables) {
      query = query
        .where("columns.table_name", "!=", DEFAULT_MIGRATION_TABLE)
        .where("columns.table_name", "!=", DEFAULT_MIGRATION_LOCK_TABLE);
    }

    const rawColumns = await query.execute();
    return this.#parseTableMetadata(rawColumns);
  }

  async getMetadata(
    options?: DatabaseMetadataOptions,
  ): Promise<DatabaseMetadata> {
    return {
      tables: await this.getTables(options),
    };
  }

  #parseTableMetadata(columns: RawColumnMetadata[]): TableMetadata[] {
    // Build mutable structures first, then freeze at the end to avoid
    // mutating frozen objects during construction.
    const tableMap = new Map<string, {
      name: string;
      isView: boolean;
      schema: string | undefined;
      columns: Array<{
        name: string;
        dataType: string;
        isNullable: boolean;
        isAutoIncrementing: boolean;
        hasDefaultValue: boolean;
      }>;
    }>();

    for (const it of columns) {
      const key = `${it.table_schema}.${it.table_name}`;
      let tbl = tableMap.get(key);
      if (!tbl) {
        tbl = {
          name: it.table_name,
          isView: it.table_type === "view",
          schema: it.table_schema,
          columns: [],
        };
        tableMap.set(key, tbl);
      }

      tbl.columns.push({
        name: it.column_name,
        dataType: it.data_type,
        isNullable: it.is_nullable === "YES",
        isAutoIncrementing: false,
        hasDefaultValue: it.column_default !== null,
      });
    }

    // Freeze structures for external consumers.
    const tables: TableMetadata[] = [];
    for (const tbl of tableMap.values()) {
      const frozen = Object.freeze({
        name: tbl.name,
        isView: tbl.isView,
        schema: tbl.schema,
        columns: Object.freeze(
          tbl.columns.map((c) =>
            Object.freeze({
              name: c.name,
              dataType: c.dataType,
              isNullable: c.isNullable,
              isAutoIncrementing: c.isAutoIncrementing,
              hasDefaultValue: c.hasDefaultValue,
            }),
          ),
        ),
      });
      tables.push(frozen as unknown as TableMetadata);
    }

    return tables;
  }
}

interface RawSchemaMetadata {
  schema_name: string;
}

interface RawColumnMetadata {
  column_name: string;
  column_default: any;
  table_name: string;
  table_schema: string;
  table_type: string;
  is_nullable: "YES" | "NO";
  data_type: string;
  extra: string;
}
