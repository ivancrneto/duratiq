import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// In dev, proxy API calls to the backend so the frontend can use relative URLs
// (the same shape nginx serves in the Docker build). Override the target with
// VITE_API_TARGET if the backend runs elsewhere.
const apiTarget = process.env.VITE_API_TARGET ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: apiTarget, changeOrigin: true },
      "/health": { target: apiTarget, changeOrigin: true },
    },
  },
});
