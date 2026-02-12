import { useState, useEffect, useMemo } from 'react';
import { api } from '../api';
const PAGE_SIZE = 30;

const SELECTION_MODE = { data: 'data', strategy: 'strategy' };

// 可多选参与组合的策略（不含 all_combined）
const STRATEGIES_MULTI = [
  { key: 'growth', name: '企业增长策略', desc: '关注企业财务数据，财报披露：营收和利润连续 2 个季度增长且最新季度数据同比超过去年' },
  { key: 'tech_competition', name: '中美科技竞争战略', desc: '在 AI、芯片、太空航天、新能源、机器人等领域具有战略地位、能避免卡脖子的企业' },
  { key: 'classic_pattern', name: '经典形态策略', desc: '计算K线图：K 线杯柄形态、上升三法等经典形态，识别“上涨中继”的特定K线模式' },
  { key: 'trend_following', name: '趋势跟踪策略', desc: '计算K线图：均线多头排列且回踩不破，系统化捕捉创新高行情（海龟交易思路）' },
  { key: 'low_vol_breakout', name: '低量横盘放量突破', desc: '前 7～10 日低量横盘（量低于 20 日均量、振幅＜5%），当日放量至少 30% 且收涨突破' },
];
const STRATEGIES_SINGLE = [
  ...STRATEGIES_MULTI,
  { key: 'all_combined', name: '全策略综合', desc: '企业增长、科技竞争、经典形态、趋势跟踪 4 种策略中至少满足 3 种的股票' },
];

export function Selection() {
  const [selectionMode, setSelectionMode] = useState(SELECTION_MODE.data);
  const [watchlist, setWatchlist] = useState([]);
  const [filterResult, setFilterResult] = useState([]);
  const [strategyResult, setStrategyResult] = useState({ strategyName: '', strategies: [], combine: '', list: [] });
  const [selectedStrategies, setSelectedStrategies] = useState([]);
  const [combineMode, setCombineMode] = useState('or');
  const [industries, setIndustries] = useState({ industries: [], sectors: [], markets: [] });
  const [loading, setLoading] = useState(false);
  const [strategyLoading, setStrategyLoading] = useState(false);
  const [collecting, setCollecting] = useState(false);
  const [collectMsg, setCollectMsg] = useState('');
  const [page, setPage] = useState(1);
  const [params, setParams] = useState({
    code: '',
    market: '',
    industry: '',
    market_cap_min: '',
    market_cap_max: '',
    pe_min: '',
    pe_max: '',
    limit: 6000,
  });

  useEffect(() => {
    api.selection.watchlist().then(setWatchlist).catch(() => setWatchlist([]));
    api.selection.industries().then(setIndustries).catch(() => setIndustries({ industries: [], sectors: [], markets: [] }));
  }, []);

  const runFilter = () => {
    setLoading(true);
    setPage(1);
    const q = Object.fromEntries(Object.entries(params).filter(([, v]) => v != null && v !== ''));
    if (!q.limit) q.limit = params.limit;
    api.selection.filter(q).then(setFilterResult).catch(() => setFilterResult([])).finally(() => setLoading(false));
  };

  useEffect(() => {
    setLoading(true);
    api.selection.filter({ limit: 6000 }).then(setFilterResult).catch(() => setFilterResult([])).finally(() => setLoading(false));
  }, []);

  const runStrategy = (strategyKey) => {
    setStrategyLoading(true);
    setPage(1);
    api.selection.strategy(strategyKey, 10000)
      .then((res) => setStrategyResult({
        strategyName: res.strategyName || STRATEGIES_SINGLE.find(s => s.key === res.strategy)?.name || '',
        strategies: res.strategies || [],
        combine: res.combine || '',
        list: res.list || [],
      }))
      .catch(() => setStrategyResult({ strategyName: '', strategies: [], combine: '', list: [] }))
      .finally(() => setStrategyLoading(false));
  };

  const toggleStrategy = (key) => {
    setSelectedStrategies((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
  };

  const runSelectedStrategies = () => {
    if (selectedStrategies.length === 0) return;
    setStrategyLoading(true);
    setPage(1);
    if (selectedStrategies.length === 1) {
      api.selection.strategy(selectedStrategies[0], 10000)
        .then((res) => setStrategyResult({
          strategyName: res.strategyName || STRATEGIES_SINGLE.find(s => s.key === res.strategy)?.name || '',
          strategies: [],
          combine: '',
          list: res.list || [],
        }))
        .catch(() => setStrategyResult({ strategyName: '', strategies: [], combine: '', list: [] }))
        .finally(() => setStrategyLoading(false));
    } else {
      api.selection.strategies(selectedStrategies, combineMode, 10000)
        .then((res) => setStrategyResult({
          strategyName: res.strategies?.join(combineMode === 'and' ? ' + ' : '、') || '',
          strategies: res.strategies || [],
          combine: res.combine || combineMode,
          list: res.list || [],
        }))
        .catch(() => setStrategyResult({ strategyName: '', strategies: [], combine: '', list: [] }))
        .finally(() => setStrategyLoading(false));
    }
  };

  const displayList = selectionMode === SELECTION_MODE.data ? filterResult : strategyResult.list;
  const watchlistCodeSet = useMemo(() => new Set(watchlist.map(w => w.code)), [watchlist]);
  const sortedResult = useMemo(() => {
    return [...displayList].sort((a, b) => {
      const aIn = watchlistCodeSet.has(a.code);
      const bIn = watchlistCodeSet.has(b.code);
      if (aIn && !bIn) return -1;
      if (!aIn && bIn) return 1;
      return 0;
    });
  }, [displayList, watchlistCodeSet]);

  const totalPages = Math.max(1, Math.ceil(sortedResult.length / PAGE_SIZE));
  const pageResult = useMemo(() => {
    const start = (page - 1) * PAGE_SIZE;
    return sortedResult.slice(start, start + PAGE_SIZE);
  }, [sortedResult, page]);

  const runCollectCorp = () => {
    setCollecting(true);
    setCollectMsg('');
    api.workflow.trigger({ action: 'collect_corp' })
      .then(res => setCollectMsg(res.result ? `已入库 ${res.result.total_upserted || 0} 条，行业 ${res.result.industries || 0} 个` : (res.error || JSON.stringify(res))))
      .catch(err => setCollectMsg('采集失败: ' + (err.message || err)))
      .finally(() => {
        setCollecting(false);
        api.selection.industries().then(setIndustries).catch(() => {});
      });
  };

  const addWatch = (code) => {
    api.stock.watch(code).then(() => api.selection.watchlist().then(setWatchlist)).catch(console.error);
  };

  const removeWatch = (code, e) => {
    e.preventDefault();
    api.stock.unwatch(code).then(() => api.selection.watchlist().then(setWatchlist)).catch(console.error);
  };

  return (
    <div className="page selection">
      <section className="section watchlist-section">
        <h2>收藏 / 跟踪</h2>
        <div className="watchlist-chips">
          {watchlist.map(row => (
            <span key={row.code} className="watchlist-chip">
              <a href={`/stock/${row.code}`} target="_blank" rel="noopener noreferrer" className="watchlist-chip-link">{row.code} {row.name || ''}</a>
              <button type="button" className="watchlist-chip-remove" onClick={e => removeWatch(row.code, e)} title="取消跟踪" aria-label="取消跟踪">×</button>
            </span>
          ))}
          {watchlist.length === 0 && <span className="muted">暂无收藏</span>}
        </div>
      </section>

      <section className="section selection-mode-section">
        <h2>选股方式</h2>
        <div className="selection-mode-tabs">
          <button
            type="button"
            className={`selection-mode-tab ${selectionMode === SELECTION_MODE.data ? 'active' : ''}`}
            onClick={() => setSelectionMode(SELECTION_MODE.data)}
          >
            数据筛选
          </button>
          <button
            type="button"
            className={`selection-mode-tab ${selectionMode === SELECTION_MODE.strategy ? 'active' : ''}`}
            onClick={() => setSelectionMode(SELECTION_MODE.strategy)}
          >
            策略选股
          </button>
        </div>

        {selectionMode === SELECTION_MODE.data && (
          <>
            <p className="muted">按市场、行业、市值、市盈率等条件筛选股票。基础数据来自「采集股票基础数据」；默认返回 6000 条，可调大「条数」后点击筛选以查看全市场（最多 10000 条）。</p>
            <div className="selection-filter">
              <label>代码</label>
              <input type="text" value={params.code} onChange={e => setParams(p => ({ ...p, code: e.target.value }))} placeholder="代码或名称" />
              <label>市场</label>
              <select value={params.market} onChange={e => setParams(p => ({ ...p, market: e.target.value }))}>
                <option value="">全部</option>
                {industries.markets?.map(name => <option key={name} value={name}>{name}</option>)}
              </select>
              <label>行业</label>
              <select value={params.industry} onChange={e => setParams(p => ({ ...p, industry: e.target.value }))}>
                <option value="">全部</option>
                {industries.industries?.map(name => <option key={name} value={name}>{name}</option>)}
              </select>
              <label>市值(亿)</label>
              <input type="number" value={params.market_cap_min} onChange={e => setParams(p => ({ ...p, market_cap_min: e.target.value }))} placeholder="最小" className="range-min" />
              <span className="range-sep">-</span>
              <input type="number" value={params.market_cap_max} onChange={e => setParams(p => ({ ...p, market_cap_max: e.target.value }))} placeholder="最大" className="range-max" />
              <label>市盈率</label>
              <input type="number" value={params.pe_min} onChange={e => setParams(p => ({ ...p, pe_min: e.target.value }))} placeholder="最小" className="range-min" />
              <span className="range-sep">-</span>
              <input type="number" value={params.pe_max} onChange={e => setParams(p => ({ ...p, pe_max: e.target.value }))} placeholder="最大" className="range-max" />
              <label>条数</label>
              <input type="number" value={params.limit} onChange={e => setParams(p => ({ ...p, limit: e.target.value }))} min={1} max={10000} title="最多 10000 条" />
              <button onClick={runFilter} disabled={loading}>{loading ? '查询中…' : '筛选'}</button>
            </div>
          </>
        )}

        {selectionMode === SELECTION_MODE.strategy && (
          <>
            <p className="muted">按既定策略筛选股票，可多选策略组合筛选（满足全部/满足任一）。若需覆盖全市场，请先在「工作流」中多次执行「全市场数据采集」以拉取更多股票的日线/财务/技术数据。</p>
            <div className="strategy-multi-section">
              <div className="strategy-multi-row">
                <span className="strategy-multi-label">多选策略：</span>
                <div className="strategy-checkboxes">
                  {STRATEGIES_MULTI.map((s) => (
                    <label key={s.key} className="strategy-checkbox-label">
                      <input
                        type="checkbox"
                        checked={selectedStrategies.includes(s.key)}
                        onChange={() => toggleStrategy(s.key)}
                      />
                      <span>{s.name}</span>
                    </label>
                  ))}
                </div>
              </div>
              <div className="strategy-multi-row">
                <span className="strategy-multi-label">组合方式：</span>
                <label className="strategy-radio-label">
                  <input
                    type="radio"
                    name="combine"
                    checked={combineMode === 'or'}
                    onChange={() => setCombineMode('or')}
                  />
                  满足任一
                </label>
                <label className="strategy-radio-label">
                  <input
                    type="radio"
                    name="combine"
                    checked={combineMode === 'and'}
                    onChange={() => setCombineMode('and')}
                  />
                  满足全部
                </label>
              </div>
              <button
                type="button"
                className="strategy-card-btn strategy-run-selected"
                onClick={runSelectedStrategies}
                disabled={strategyLoading || selectedStrategies.length === 0}
              >
                {strategyLoading ? '运行中…' : selectedStrategies.length === 0 ? '请至少勾选一个策略' : `运行所选策略（${selectedStrategies.length} 个）`}
              </button>
            </div>
            <div className="strategy-cards">
              {STRATEGIES_SINGLE.map((s) => (
                <div key={s.key} className={`strategy-card strategy-card--${s.key}`}>
                  <div className="strategy-card-head">{s.name}</div>
                  <p className="strategy-card-desc">{s.desc}</p>
                  <button
                    type="button"
                    className="strategy-card-btn"
                    onClick={() => runStrategy(s.key)}
                    disabled={strategyLoading}
                  >
                    {strategyLoading ? '运行中…' : '运行该策略'}
                  </button>
                </div>
              ))}
            </div>
            {strategyResult.strategyName && strategyResult.list.length >= 0 && (
              <p className="muted strategy-result-label">
                当前结果：{strategyResult.strategyName}
                {strategyResult.combine ? `（${strategyResult.combine === 'and' ? '满足全部' : '满足任一'}）` : ''}，共 {strategyResult.list.length} 只
              </p>
            )}
          </>
        )}
      </section>

      <section className="section">
        <h2>{selectionMode === SELECTION_MODE.data ? '筛选结果' : '策略结果'}</h2>
        <div className="selection-result-summary">
          <span className="total-count">共 {sortedResult.length} 条</span>
          <span className="muted">第 {page} / {totalPages} 页，每页 {PAGE_SIZE} 条</span>
          <span className="muted">已收藏跟踪的股票排在前面</span>
        </div>
        <div className="table-wrap selection-result-table">
          <table>
            <thead>
              <tr>
                <th>代码</th>
                <th>名称</th>
                <th>市场</th>
                <th>行业</th>
                <th>板块</th>
                <th>最新股价</th>
                <th>市值(亿)</th>
                <th>市盈率</th>
                <th>市净率</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {pageResult.map(row => (
                <tr key={row.code}>
                  <td><a href={`/stock/${row.code}`} target="_blank" rel="noopener noreferrer">{row.code}</a></td>
                  <td>{row.name}</td>
                  <td>{row.market || '-'}</td>
                  <td>{row.industry}</td>
                  <td>{row.sector}</td>
                  <td>{row.latest_close != null ? Number(row.latest_close).toFixed(2) : '-'}</td>
                  <td>{row.market_cap != null ? (Number(row.market_cap) / 1e8).toFixed(2) : '-'}</td>
                  <td>{row.pe != null ? Number(row.pe).toFixed(2) : '-'}</td>
                  <td>{row.pb != null ? Number(row.pb).toFixed(2) : '-'}</td>
                  <td className="selection-action-cell">
                    {watchlistCodeSet.has(row.code) ? (
                      <button type="button" className="selection-btn selection-btn-unwatch" onClick={e => removeWatch(row.code, e)}>取消跟踪</button>
                    ) : (
                      <button type="button" className="selection-btn selection-btn-watch" onClick={() => addWatch(row.code)}>加入跟踪</button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {sortedResult.length === 0 && !loading && !strategyLoading && (
            <p className="muted">
              {selectionMode === SELECTION_MODE.data ? '暂无数据或未执行筛选。' : '请勾选一个或多个策略后点击「运行所选策略」，或点击某策略卡片上的「运行该策略」。经典形态策略需先执行形态识别任务。'}
            </p>
          )}
          {sortedResult.length > 0 && (
            <div className="pagination">
              <span className="pagination-total">共 {sortedResult.length} 条</span>
              <button type="button" disabled={page <= 1} onClick={() => setPage(p => Math.max(1, p - 1))}>上一页</button>
              <span className="page-info">第 {page} / {totalPages} 页</span>
              <button type="button" disabled={page >= totalPages} onClick={() => setPage(p => Math.min(totalPages, p + 1))}>下一页</button>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
