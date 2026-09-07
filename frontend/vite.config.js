import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/login': 'http://localhost:4000',
      '/api': 'http://localhost:4000',
      '/infinite': 'http://localhost:4000',
      '/cacheMessage': 'http://localhost:4000',
      // LangGraph financial-agent: strip /agent prefix so /agent/runs/stream → /runs/stream
      '/agent': {
        target: 'http://localhost:8123',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/agent/, '')
      },
      '/socket.io': {
        target: 'http://localhost:4000',
        ws: true
      }
    }
  }
})

