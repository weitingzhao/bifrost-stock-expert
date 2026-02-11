const API_BASE = import.meta.env.VITE_API_URL || '';

async function request(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });
  if (!res.ok) throw new Error(await res.text().catch(() => res.statusText));
  return res.json();
}

export const api = {
  health: () => request('/health'),
  stock: {
    get: (code) => request(`/api/stock/${code}`),
    kline: (code, limit = 120) => request(`/api/stock/${code}/kline?limit=${limit}`),
    technicals: (code, limit = 60) => request(`/api/stock/${code}/technicals?limit=${limit}`),
    fundamentals: (code, limit = 20) => request(`/api/stock/${code}/fundamentals?limit=${limit}`),
    moneyflow: (code, limit = 120) => request(`/api/stock/${code}/moneyflow?limit=${limit}`),
    financial: (code, limit = 10) => request(`/api/stock/${code}/financial?limit=${limit}`),
    signals: (code, limit = 50) => request(`/api/stock/${code}/signals?limit=${limit}`),
    news: (code, limit = 20) => request(`/api/stock/${code}/news?limit=${limit}`),
    newsOpinionRecords: (code, limit = 50) => request(`/api/stock/${code}/news-opinion-records?limit=${limit}`),
    watch: (code, track = true) => request(`/api/stock/${code}/watch`, { method: 'POST', body: JSON.stringify({ track }) }),
    unwatch: (code) => request(`/api/stock/${code}/watch`, { method: 'DELETE' }),
  },
  selection: {
    filter: (params) => request(`/api/selection/filter?${new URLSearchParams(params)}`),
    strategy: (strategy, limit = 10000) => request(`/api/selection/strategy?strategy=${encodeURIComponent(strategy)}&limit=${limit}`),
    watchlist: () => request('/api/selection/watchlist'),
    watchlistSummary: () => request('/api/selection/watchlist-summary'),
    watchlistSignals: (refDate) =>
      request(refDate ? `/api/selection/watchlist-signals?ref_date=${encodeURIComponent(refDate)}` : '/api/selection/watchlist-signals'),
    indexSignals: (refDate) =>
      request(refDate ? `/api/selection/index-signals?ref_date=${encodeURIComponent(refDate)}` : '/api/selection/index-signals'),
    industries: () => request('/api/selection/industries'),
    corps: (q, limit = 30) => request(`/api/selection/corps?q=${encodeURIComponent(q)}&limit=${limit}`),
  },
  workflow: {
    logs: (limit = 50) => request(`/api/workflow/logs?limit=${limit}`),
    trigger: (body) => request('/api/workflow/trigger', { method: 'POST', body: JSON.stringify(body) }),
  },
  indices: {
    daily: (limit = 30) => request(`/api/indices/daily?limit=${limit}`),
  },
  config: {
    getInvestmentSummaryPrompt: () => request('/api/config/investment-summary-prompt'),
    setInvestmentSummaryPrompt: (prompt) =>
      request('/api/config/investment-summary-prompt', { method: 'PUT', body: JSON.stringify({ prompt }) }),
    getScoreWeights: () => request('/api/config/score-weights'),
    setScoreWeights: (weights) =>
      request('/api/config/score-weights', { method: 'PUT', body: JSON.stringify({ weights }) }),
  },
};
