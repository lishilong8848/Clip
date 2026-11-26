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
      input: { usage: fileURLToPath(new URL('./src/usageHtml.ts', import.meta.url)), main: fileURLToPath(new URL('./index.html', import.meta.url)), assistant: fileURLToPath(new URL('./assistant.html', import.meta.url)) },
      output: {
        entryFileNames: chunk => chunk.name === 'usage' ? 'assets/usage-pages.js' : 'assets/index-[hash].js',
        chunkFileNames: "assets/c-[hash].js",
        assetFileNames: "assets/a-[hash][extname]",
      },
    },
  },
});
