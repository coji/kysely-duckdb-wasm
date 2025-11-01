# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a Kysely dialect implementation for DuckDB WebAssembly. It provides a type-safe SQL query builder interface for DuckDB's WebAssembly version, enabling SQL queries in browser and Node.js environments.

## Development Commands

```bash
# Install dependencies (using pnpm)
pnpm install

# Run tests
pnpm test

# Build the project (outputs to dist/)
pnpm run build

# Type check without emitting
pnpm run check

# Run all checks (type check, tests, build, docs)
pnpm run all

# Generate documentation
pnpm run docs
```

## Release & Publishing

This project uses **release-please** for automated versioning and npm publishing.

### Commit Message Convention

**IMPORTANT**: Always use Conventional Commits format for commit messages:

```bash
# Feature (minor version bump: 0.2.0 → 0.3.0)
feat: add new DuckDB helper functions

# Bug fix (patch version bump: 0.2.0 → 0.2.1)
fix: correct type inference for struct fields

# Breaking change (major version bump: 0.2.0 → 1.0.0)
feat!: remove deprecated API methods

# Other types (no version bump, but included in changelog)
chore: update dependencies
docs: improve API documentation
test: add integration tests
refactor: simplify driver connection logic
```

### Release Workflow

1. **Develop**: Make changes and commit using Conventional Commits
2. **Create Pull Request**: Open a PR to merge your changes
3. **Merge PR using "Create a merge commit"**:
   - **IMPORTANT**: Use "Create a merge commit" (not squash merge)
   - This prevents duplicate entries in CHANGELOG
   - Repository is configured to only allow merge commits
4. **release-please creates Release PR**: Automatically created after merge to main
5. **Review Release PR**: Check version bump and CHANGELOG.md
6. **Merge Release PR**: Triggers automatic:
   - GitHub Release creation
   - npm publish with OIDC provenance (no secrets required)
   - Documentation deployment to GitHub Pages

### Manual Release (if needed)

```bash
# Update version manually
pnpm version patch|minor|major

# Build and validate
pnpm run validate

# Publish to npm (requires npm login)
npm publish --provenance --access public
```

### npm Trusted Publishing (OIDC)

This package uses npm trusted publishing with OIDC for secure, transparent publishing:

- No `NPM_TOKEN` secrets required
- Cryptographic proof of package origin
- Verifiable build attestation on npm package page
- Provenance is automatically enabled (no `--provenance` flag needed)

#### Setup Requirements

**npm Package Configuration** (one-time setup):

1. Visit https://www.npmjs.com/package/@coji/kysely-duckdb-wasm/access
2. Navigate to **Publishing Access** settings
3. Click **Add Trusted Publisher**
4. Configure:
   - **Provider**: GitHub Actions
   - **Organization/User**: `coji`
   - **Repository**: `kysely-duckdb-wasm`
   - **Workflow filename**: `release-please.yml`
   - **Environment**: (leave empty)
5. Save the configuration

**Technical Requirements**:

- npm CLI v11.5.1 or later (automatically installed in CI)
- `id-token: write` permission in GitHub Actions
- GitHub-hosted runner (not self-hosted)

## Architecture

The codebase follows the Kysely dialect pattern with four main components:

1. **DuckDbWasmDriver** (`src/driver-wasm.ts`): Manages connections to DuckDB WebAssembly instances
2. **DuckDbAdapter** (`src/adapter.ts`): Provides DuckDB-specific adaptations for Kysely
3. **DuckDbQueryCompiler** (`src/query-compiler.ts`): Compiles Kysely query AST to DuckDB SQL
4. **DuckDbIntrospector** (`src/introspector.ts`): Introspects database schema for type generation

The main entry point is `src/index.ts` which exports the `DuckDbDialect` class.

## Key Features & Patterns

### Table Mappings

The dialect supports mapping external data sources (JSON, CSV files) as tables through the `tableMappings` configuration option. See `tests/select.test.ts` for examples.

### DuckDB-Specific Data Types

Helper functions for DuckDB's advanced types are in `src/helper/datatypes.ts`:

- Arrays: `duckArray()`, `duckArrayAggregate()`
- Structs: `duckStruct()`, `duckRowStruct()`
- Maps: `duckMap()`, `duckMapFromEntries()`
- Unions: `duckUnion()`

### Testing Pattern

Tests use Vitest and follow a pattern of:

1. Creating a DuckDB instance with test data
2. Setting up Kysely with the dialect
3. Running queries and asserting results

See `tests/test_common.ts` for shared test utilities.

## Common Development Tasks

### Adding New DuckDB Functions

1. Add the function to `src/helper/functions.ts`
2. Follow the existing pattern using `sql` template literals
3. Add tests in the appropriate test file

### Modifying Query Compilation

1. Edit `src/query-compiler.ts`
2. Override the appropriate visit method from the base `DefaultQueryCompiler`
3. Test with complex queries to ensure correctness

### Working with WebAssembly Driver

The driver implementation in `src/driver-wasm.ts` handles:

- Connection lifecycle
- Query execution
- Result transformation from Arrow format

When modifying driver behavior, ensure compatibility with both browser and Node.js environments.
