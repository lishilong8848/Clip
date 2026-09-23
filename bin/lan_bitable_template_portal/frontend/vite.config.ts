import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { fileURLToPath, URL } from "node:url";

const rootDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  root: rootDir,
  plugins: [vue()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "assets/index-[hash].js",
        chunkFileNames: "assets/c-[hash].js",
        assetFileNames: "assets/a-[hash][extname]",
      },
    },
  },
});
