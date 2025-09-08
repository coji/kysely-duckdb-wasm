import type { SelectQueryBuilder, Simplify } from 'kysely'
import { Kysely, sql } from 'kysely'

type CompiledQuerySchema<T> =
  T extends SelectQueryBuilder<any, any, infer O> ? Simplify<O> : never

/**
 * @alpha
 * Kysely extension methods.
 */
export class KyselyDuckDbExtension<DB> extends Kysely<DB> {
  /**
   * @param tables selectQueries for CTAS.
   * @returns Kysely instance with CTAS tables.
   * @example
   * ```ts
   * const db = new KyselyDuckDbExtension<{
   *   users: {
   *     id: number;
   *     name: string;
   *   };
   * };
   * const db2 = await db.createTablesAsSelect({
   *    userNames: db.selectFrom('users').select(['name']),
   * });
   *
   * // db2 is now a Kysely instance with CTAS tables.
   * console.log(db2.selectFrom('userNames').selectAll().execute());
   * ```
   */
  public async createTablesAsSelect<
    T extends Record<string, SelectQueryBuilder<DB, keyof DB, unknown>>,
  >(
    tables: T,
  ): Promise<Kysely<DB & { [K in keyof T]: CompiledQuerySchema<T[K]> }>> {
    const tableNames = Object.keys(tables) as (keyof T)[]

    for (const tableName of tableNames) {
      const name = String(tableName)
      const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/
      if (!IDENTIFIER_RE.test(name)) {
        throw new Error(
          `Invalid table name: ${name}. Names must match ${IDENTIFIER_RE.source}`,
        )
      }

      const ctas = sql`CREATE TABLE ${sql.id(name)} AS (${tables[tableName]})`
      await this.executeQuery(ctas.compile(this))
    }

    return this as Kysely<DB & { [K in keyof T]: CompiledQuerySchema<T[K]> }>
  }
}
