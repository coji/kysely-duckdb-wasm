import { defineConfig } from 'tsdown'

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  sourcemap: true,
  clean: true,
  // Keep ESM as .js (package.json `type: module`) to match the exports map.
  outExtensions: ({ format }) => ({ js: format === 'es' ? '.js' : '.cjs' }),
})
