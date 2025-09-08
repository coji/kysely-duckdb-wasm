import { DefaultQueryCompiler, type TableNode } from 'kysely'

const ID_WRAP_REGEX = /"/g

export interface DuckDbQueryCompilerConfigs {
  /**
   * Mappings of table name in kysely to duckdb table expressions.
   *
   * Duckdb can read external source(file, url or database) as table
   * like: `SELECT * FROM read_json_objects('path/to/file/*.json')`.
   * You can use raw duckdb table expression as table name, but it may be too
   * long, preserving too many implementation details.
   *
   * This mappings is used to replace table name string to duckdb table expression.
   *
   * @example
   * ```ts
   * const dialect = new DuckDbDialect({
   *  database: db,
   *  tableMappings: {
   *    person: 'read_json_object("s3://my-bucket/person.json?s3_access_key_id=key&s3_secret_access_key=secret")'
   *  }
   * });
   *
   * const db = new Kysely<{
   *   person: { first_name: string, last_name: string }, // 'person' is defined in tableMappings
   *   pet: { name: string, species: 'cat' | 'dog' },     // 'pet' is *not* defined in tableMappings
   * >({ dialect });
   *
   * await db.selectFrom("person").selectAll().execute();
   * // => Executed query is: `SELECT * FROM read_json_object("s3://my-bucket/person.json?s3_access_key_id=key&s3_secret_access_key=secret");`
   * ```
   *
   * await db.selectFrom("pet").selectAll().execute();
   * // => Executed query is: `SELECT * FROM pet;`
   */
  tableMappings: {
    [tableName: string]: string
  }
}

export class DuckDbQueryCompiler extends DefaultQueryCompiler {
  #configs: DuckDbQueryCompilerConfigs

  constructor(configs: DuckDbQueryCompilerConfigs) {
    super()
    this.#configs = configs
  }

  protected override getCurrentParameterPlaceholder() {
    return '?'
  }

  protected override getLeftExplainOptionsWrapper(): string {
    return ''
  }

  protected override getRightExplainOptionsWrapper(): string {
    return ''
  }

  protected override getLeftIdentifierWrapper(): string {
    return '"'
  }

  protected override getRightIdentifierWrapper(): string {
    return '"'
  }

  protected override getAutoIncrement(): string {
    throw new Error('Can not use auto increment in DuckDB')
  }

  protected override sanitizeIdentifier(identifier: string): string {
    return identifier.replace(ID_WRAP_REGEX, '""')
  }

  protected visitTable(node: TableNode): void {
    const name = node.table.identifier.name
    if (Object.hasOwn(this.#configs.tableMappings, name)) {
      // Append the mapped table expression
      this.append(this.#configs.tableMappings[name])

      // Preserve alias if present and not already handled by an AliasNode parent
      const parent: any = (this as any).parentNode
      const parentIsAlias = parent && parent.kind === 'AliasNode'

      // Some Kysely versions carry alias as a separate AliasNode (preferred),
      // but if it exists on the table node, append it here.
      if (!parentIsAlias) {
        const alias: any = (node as any).alias ?? (node.table as any).alias
        if (alias) {
          this.append(' as ')
          // alias may be an operation node or a plain identifier-like node
          if (alias.kind) {
            this.visitNode(alias)
          } else if (typeof alias === 'string') {
            this.append(this.getLeftIdentifierWrapper())
            this.append(this.sanitizeIdentifier(alias))
            this.append(this.getRightIdentifierWrapper())
          } else if (alias.identifier?.name) {
            // Handle IdentifierNode-like shape
            this.append(this.getLeftIdentifierWrapper())
            this.append(this.sanitizeIdentifier(alias.identifier.name))
            this.append(this.getRightIdentifierWrapper())
          }
        }
      }
      return
    }
    super.visitTable(node)
  }
}
