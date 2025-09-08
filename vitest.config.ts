import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    browser: {
      enabled: true,
      name: "chromium",
      provider: "playwright",
      headless: true
    }
  },
  optimizeDeps: {
    exclude: ["@duckdb/duckdb-wasm"]
  }
});
