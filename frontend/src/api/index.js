import axios from 'axios';

// Rely on Vite proxy to route relative requests to localhost:4000
const api = axios.create();

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export const login = (username, password) =>
  api.post('/login', { username, password });

export const fetchIndiaNews = (page = 1) =>
  api.get('/api/news/india', { params: { page } });

export const fetchSimilarNews = (newsid) =>
  api.get('/api/news/similar', { params: { newsid } });

export const fetchInfiniteNews = (pageNo) =>
  api.get('/infinite', { params: { pageNo } });

export const publishEvent = (payload) =>
  api.post('/api/publish', payload);

export const fetchCachedMessages = (groupId) =>
  api.post('/cacheMessage', { groupId });

/**
 * Stream a financial analysis from the LangGraph financial-agent.
 *
 * @param {string} news - The article title + description to analyse.
 * @param {(text: string) => void} onMessageUpdate - Called with the cumulative text of the current message.
 * @param {() => void} onDone - Called when the stream finishes cleanly.
 * @param {(err: string) => void} onError - Called if an error occurs.
 * @returns {AbortController} - Call .abort() to cancel the stream.
 */
export function streamAgentAnalysis(news, onMessageUpdate, onDone, onError) {
  const controller = new AbortController();

  (async () => {
    try {
      const res = await fetch('/agent/runs/stream', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          assistant_id: 'FinAgent',
          input: { news },
          stream_mode: ['messages']
        }),
        signal: controller.signal,
      });

      if (!res.ok) {
        const errText = await res.text();
        onError(`Server error ${res.status}: ${errText}`);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop(); // keep incomplete last line

        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || !trimmed.startsWith('data:')) continue;

          const raw = trimmed.slice(5).trim();
          if (raw === '[DONE]') { onDone(); return; }

          try {
            const parsed = JSON.parse(raw);
            if (Array.isArray(parsed)) {
              for (const chunk of parsed) {
                if (chunk.type === 'ai' || chunk.type === 'AIMessageChunk') {
                  let textContent = '';
                  if (typeof chunk.content === 'string') {
                    textContent = chunk.content;
                  } else if (Array.isArray(chunk.content)) {
                    for (const block of chunk.content) {
                      if (block.type === 'text' && typeof block.text === 'string') {
                        textContent += block.text;
                      }
                    }
                  }
                  if (textContent) {
                    // Filter out raw JSON from intermediate nodes
                    const trimmed = textContent.trim();
                    if (!trimmed.startsWith('{') && !trimmed.startsWith('```json')) {
                      onMessageUpdate(textContent);
                    }
                  }
                }
              }
            }
          } catch {
            // Non-JSON lines (e.g., event: metadata) — skip silently
          }
        }
      }

      onDone();
    } catch (err) {
      if (err.name === 'AbortError') return; // user cancelled, not an error
      onError(err.message || 'Stream failed');
    }
  })();

  return controller;
}

export default api;

