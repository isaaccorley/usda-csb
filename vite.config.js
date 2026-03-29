import { defineConfig } from "vite";

export default defineConfig({
  base: "/usda-csb/",
  server: {
    watch: {
      ignored: [
        "**/.git/**",
        "**/.venv/**",
        "**/.pytest_cache/**",
        "**/.ruff_cache/**",
        "**/node_modules/**",
        "**/data/**",
        "**/csb.parquet/**",
        "**/pmtiles/**",
        "**/dist/**",
      ],
    },
  },
});
