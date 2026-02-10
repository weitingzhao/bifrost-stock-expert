import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { Tiny } from '@ant-design/charts';
import { api } from '../api';

function usePrefersDark() {
  const [dark, setDark] = useState(() =>
    typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches
  );
  useEffect(() => {
    const m = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e) => setDark(e.matches);
    m.addEventListener('change', handler);
    return () => m.removeEventListener('change', handler);
  }, []);
  return dark;
}

const TAB_KEYS = { summary: 'summary', signals: 'signals', indexSignals: 'indexSignals' };

// 大盘指数小图：每个指数对应一种颜色（标题+折线一致）
const INDEX_CHART_COLORS = ['#e74c3c', '#3498db', '#2ecc71', '#9b59b6', '#f39c12'];

// 投资信号表头（与后端 signal_agent 一致，外加新闻舆论、换手率）
const SIGNAL_COLUMNS = [
  '成交量资金MA20',
  '成交量涨跌幅',
  '持续资金流向',
  '均线金叉死叉',
  '主力资金',
  '支撑阻力位',
  '新闻舆论',
  '换手率',
];

// 大盘信号表头（无资金流数据，仅日线可算）
const INDEX_SIGNAL_COLUMNS = [
  '成交量MA20',
  '成交量涨跌幅',
  '均线金叉死叉',
  '均线多空排列',
  '支撑阻力位',
  '量价背离',
  '波动率突破',
];

const directionClass = (d) => {
  if (!d || d === '-' || d === '无信号') return '';
  if (d === '看涨' || d === '正常活跃' || d === '异常活跃') return 'signal-bull';
  if (d === '看跌' || d === '交投清淡') return 'signal-bear';
  if (d === '中性') return 'signal-neutral';
  return '';
};

export function StockList() {
  const prefersDark = usePrefersDark();
  const chartTheme = prefersDark ? 'dark' : undefined;
  const [activeTab, setActiveTab] = useState(TAB_KEYS.summary);
  const [summary, setSummary] = useState([]);
  const [signalsRows, setSignalsRows] = useState([]);
  const [indexDaily, setIndexDaily] = useState([]);
  const [loading, setLoading] = useState(true);
  const [signalsLoading, setSignalsLoading] = useState(false);
  const [pulling, setPulling] = useState(false);
  const [pullMsg, setPullMsg] = useState('');
  const [parsingAll, setParsingAll] = useState(false);
  const [computingSignals, setComputingSignals] = useState(false);
  const [updatingCode, setUpdatingCode] = useState(null);
  const [signalDateFilter, setSignalDateFilter] = useState('');
  const [availableSignalDates, setAvailableSignalDates] = useState([]);
  const [indexSignalsRows, setIndexSignalsRows] = useState([]);
  const [indexSignalDateFilter, setIndexSignalDateFilter] = useState('');
  const [availableIndexSignalDates, setAvailableIndexSignalDates] = useState([]);
  const [indexSignalsLoading, setIndexSignalsLoading] = useState(false);
  const [computingIndexSignals, setComputingIndexSignals] = useState(false);

  const loadSummary = () => {
    api.selection.watchlistSummary().then(setSummary).catch(() => setSummary([])).finally(() => setLoading(false));
  };

  const loadSignals = (refDate) => {
    setSignalsLoading(true);
    const dateParam = refDate !== undefined ? refDate : signalDateFilter;
    api.selection
      .watchlistSignals(dateParam || null)
      .then((data) => {
        const rows = data?.rows ?? data;
        const dates = data?.available_dates ?? [];
        setSignalsRows(Array.isArray(rows) ? rows : []);
        setAvailableSignalDates(Array.isArray(dates) ? dates : []);
      })
      .catch(() => setSignalsRows([]))
      .finally(() => setSignalsLoading(false));
  };

  useEffect(() => {
    loadSummary();
  }, []);

  useEffect(() => {
    api.indices.daily(30).then(setIndexDaily).catch(() => setIndexDaily([]));
  }, []);

  useEffect(() => {
    if (activeTab === TAB_KEYS.signals) loadSignals();
  }, [activeTab, signalDateFilter]);

  const loadIndexSignals = (refDate) => {
    setIndexSignalsLoading(true);
    const dateParam = refDate !== undefined ? refDate : indexSignalDateFilter;
    api.selection
      .indexSignals(dateParam || null)
      .then((data) => {
        const rows = data?.rows ?? data;
        const dates = data?.available_dates ?? [];
        setIndexSignalsRows(Array.isArray(rows) ? rows : []);
        setAvailableIndexSignalDates(Array.isArray(dates) ? dates : []);
      })
      .catch(() => setIndexSignalsRows([]))
      .finally(() => setIndexSignalsLoading(false));
  };

  useEffect(() => {
    if (activeTab === TAB_KEYS.indexSignals) loadIndexSignals();
  }, [activeTab, indexSignalDateFilter]);

  const runCollectWatchlist = () => {
    setPulling(true);
    setPullMsg('');
    api.workflow.trigger({ action: 'collect_watchlist' })
      .then(res => {
        setPullMsg(res.result ? `已处理 ${res.result.codes_processed || 0} 只，日线 ${res.result.days_updated || 0} 条` : (res.error || JSON.stringify(res)));
        if (res.result?.ok) loadSummary();
      })
      .catch(err => setPullMsg('更新失败: ' + (err.message || err)))
      .finally(() => setPulling(false));
  };

  const runParseAllStocks = () => {
    if (parsingAll) return;
    const codes = (summary || []).map(r => r.code).filter(Boolean);
    if (!codes.length) {
      setPullMsg('暂无收藏股票可解析');
      return;
    }
    setParsingAll(true);
    setPullMsg('');
    api.workflow.trigger({ action: 'parse_corp', codes })
      .then(res => {
        if (res?.result) {
          const r = res.result;
          setPullMsg(`解析完成：处理 ${r.codes_processed || 0}/${r.codes_requested || codes.length} 只，成功 ${r.codes_ok || 0}，失败 ${r.codes_failed || 0}`);
        } else {
          setPullMsg(res.error || JSON.stringify(res));
        }
      })
      .catch(err => setPullMsg('解析失败: ' + (err.message || err)))
      .finally(() => setParsingAll(false));
  };

  const runComputeSignals = () => {
    if (computingSignals) return;
    const codes = (signalsRows.length ? signalsRows : summary).map(r => r.code).filter(Boolean);
    if (!codes.length) {
      setPullMsg('暂无收藏股票，无法计算信号');
      return;
    }
    setComputingSignals(true);
    setPullMsg('');
    api.workflow.trigger({ action: 'compute_signals', codes })
      .then(res => {
        if (res?.result) {
          const r = res.result;
          setPullMsg(`信号计算完成：处理 ${r.codes_processed || 0} 只`);
          if (activeTab === TAB_KEYS.signals) loadSignals();
        } else {
          setPullMsg(res.error || JSON.stringify(res));
        }
      })
      .catch(err => setPullMsg('计算失败: ' + (err.message || err)))
      .finally(() => setComputingSignals(false));
  };

  const runComputeIndexSignals = () => {
    if (computingIndexSignals) return;
    setComputingIndexSignals(true);
    setPullMsg('');
    api.workflow.trigger({ action: 'compute_index_signals' })
      .then(res => {
        if (res?.result) {
          const r = res.result;
          setPullMsg(`大盘信号计算完成：处理 ${r.indices_processed || 0} 个指数`);
          if (activeTab === TAB_KEYS.indexSignals) loadIndexSignals();
        } else {
          setPullMsg(res.error || JSON.stringify(res));
        }
      })
      .catch(err => setPullMsg('大盘信号计算失败: ' + (err.message || err)))
      .finally(() => setComputingIndexSignals(false));
  };

  const runCollectStock = (code) => {
    setUpdatingCode(code);
    api.workflow.trigger({ action: 'collect_stock', codes: [code] })
      .then(res => {
        if (res.result?.ok) loadSummary();
      })
      .catch(() => {})
      .finally(() => setUpdatingCode(null));
  };

  const fmtDate = (d) => (d && String(d).length >= 8) ? `${String(d).slice(0,4)}-${String(d).slice(4,6)}-${String(d).slice(6,8)}` : (d || '-');
  const fmtPct = (v) => v != null ? (Number(v).toFixed(2) + '%') : '-';
  const pctClass = (v) => v == null ? '' : (Number(v) >= 0 ? 'pct-up' : 'pct-down');
  const pctIcon = (v) => v == null ? '' : (Number(v) > 0 ? '↑' : Number(v) < 0 ? '↓' : '');
  const fmtPrice = (v) => v != null ? `￥${Number(v).toFixed(2)}` : '-';
  const fmtPe = (v) => v != null ? Number(v).toFixed(2) : '-';

  const indexChartData = (item) => (item?.data || []).map((d) => ({
    date: (d.trade_date || '').slice(0, 10),
    value: d.close != null ? Number(d.close) : null,
  })).filter((d) => d.date && d.value != null);

  return (
    <div className="page stock-list">
      {indexDaily.length > 0 && (
        <section className="section index-charts-section">
          <h3 className="index-charts-title">大盘指数近期走势</h3>
          <div className="index-charts-row">
            {indexDaily.map((item, idx) => {
              const data = indexChartData(item);
              if (data.length === 0) return null;
              const lineColor = INDEX_CHART_COLORS[idx % INDEX_CHART_COLORS.length];
              return (
                <div key={item.index_code} className="index-chart-cell">
                  <div className="index-chart-head" style={{ color: lineColor }}>{item.name}</div>
                  <div className="index-chart-wrap">
                    <Tiny.Line
                      theme={chartTheme}
                      data={data}
                      xField="date"
                      yField="value"
                      color={[lineColor]}
                      tooltip={{
                        title: (d) => d?.date ?? '',
                        items: [(d) => ({ name: '收盘', value: d?.value != null ? Number(d.value).toFixed(2) : '-' })],
                      }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </section>
      )}
      <section className="section">
        <h2>跟踪股票</h2>
        <div className="stock-list-tabs" role="tablist" aria-label="跟踪股票">
          <button
            type="button"
            role="tab"
            aria-selected={activeTab === TAB_KEYS.summary}
            aria-controls="stock-list-panel-summary"
            id="stock-list-tab-summary"
            className={`stock-list-tab ${activeTab === TAB_KEYS.summary ? 'active' : ''}`}
            onClick={() => setActiveTab(TAB_KEYS.summary)}
          >
            跟踪列表
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activeTab === TAB_KEYS.signals}
            aria-controls="stock-list-panel-signals"
            id="stock-list-tab-signals"
            className={`stock-list-tab ${activeTab === TAB_KEYS.signals ? 'active' : ''}`}
            onClick={() => setActiveTab(TAB_KEYS.signals)}
          >
            投资信号
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activeTab === TAB_KEYS.indexSignals}
            aria-controls="stock-list-panel-index-signals"
            id="stock-list-tab-index-signals"
            className={`stock-list-tab ${activeTab === TAB_KEYS.indexSignals ? 'active' : ''}`}
            onClick={() => setActiveTab(TAB_KEYS.indexSignals)}
          >
            大盘信号
          </button>
        </div>

        <div
          id={activeTab === TAB_KEYS.summary ? 'stock-list-panel-summary' : activeTab === TAB_KEYS.signals ? 'stock-list-panel-signals' : 'stock-list-panel-index-signals'}
          className="stock-list-tab-content"
          role="tabpanel"
          aria-labelledby={`stock-list-tab-${activeTab}`}
        >
        {activeTab === TAB_KEYS.summary && (
          <>
            <p className="muted">表格展示已收藏跟踪的股票及最新价、日/周/月/3月涨跌幅。</p>
            <div className="form-row" style={{ marginBottom: '0.5rem' }}>
              <button onClick={runCollectWatchlist} disabled={pulling}>{pulling ? '更新中…' : '更新股票数据'}</button>
              <button onClick={runParseAllStocks} disabled={parsingAll || pulling}>{parsingAll ? '解析中…' : '解析股票企业'}</button>
              {pullMsg && <span className="collect-msg">{pullMsg}</span>}
            </div>
            {loading ? (
              <p className="muted">加载中…</p>
            ) : summary.length > 0 ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>代码</th>
                      <th>名称</th>
                      <th>市场</th>
                      <th>最新价</th>
                      <th>市盈率</th>
                      <th>日期</th>
                      <th>日涨跌幅</th>
                      <th>周涨跌幅</th>
                      <th>月涨跌幅</th>
                      <th>3月涨跌幅</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {summary.map(row => (
                      <tr key={row.code}>
                        <td><Link to={`/stock/${row.code}`}>{row.code}</Link></td>
                        <td>{row.name || '-'}</td>
                        <td>{row.market || '-'}</td>
                        <td>{fmtPrice(row.close)}</td>
                        <td>{fmtPe(row.pe)}</td>
                        <td>{fmtDate(String(row.latest_date || '').replace(/-/g, ''))}</td>
                        <td className={pctClass(row.daily_pct)}><span className="pct-icon" aria-hidden>{pctIcon(row.daily_pct)}</span>{fmtPct(row.daily_pct)}</td>
                        <td className={pctClass(row.weekly_pct)}><span className="pct-icon" aria-hidden>{pctIcon(row.weekly_pct)}</span>{fmtPct(row.weekly_pct)}</td>
                        <td className={pctClass(row.monthly_pct)}><span className="pct-icon" aria-hidden>{pctIcon(row.monthly_pct)}</span>{fmtPct(row.monthly_pct)}</td>
                        <td className={pctClass(row.monthly_3_pct)}><span className="pct-icon" aria-hidden>{pctIcon(row.monthly_3_pct)}</span>{fmtPct(row.monthly_3_pct)}</td>
                        <td className="table-actions">
                          <Link to={`/stock/${row.code}`} className="action-link">详情</Link>
                          <span className="table-action-sep"> </span>
                          <button type="button" className="action-link" onClick={() => runCollectStock(row.code)} disabled={updatingCode === row.code}>
                            {updatingCode === row.code ? '更新中…' : '更新'}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="muted">暂无收藏。请先将股票加入跟踪（如通过详情页「加入跟踪」）。</p>
            )}
          </>
        )}

        {activeTab === TAB_KEYS.signals && (
          <>
            <p className="muted">每行展示一只收藏股票的投资信号与次日涨跌幅；综合分数由同期大盘信号与个股信号计算，表格按综合分数从高到低排序。可切换信号日期对比。</p>
            <div className="form-row" style={{ marginBottom: '0.5rem', flexWrap: 'wrap', gap: '0.5rem', alignItems: 'center' }}>
              <label className="form-label" style={{ marginBottom: 0 }}>
                信号日期：
              </label>
              <select
                value={signalDateFilter}
                onChange={(e) => setSignalDateFilter(e.target.value)}
                className="form-select"
                style={{ width: 'auto', minWidth: '10rem' }}
                title="选择日期查看该日的投资信号与次日股价"
              >
                <option value="">最近（每只股票取最新一日）</option>
                {availableSignalDates.map((d) => (
                  <option key={d} value={d}>
                    {fmtDate(String(d).replace(/-/g, ''))}
                  </option>
                ))}
              </select>
              <button onClick={runComputeSignals} disabled={computingSignals || signalsLoading}>
                {computingSignals ? '计算中…' : '计算投资信号'}
              </button>
              {pullMsg && <span className="collect-msg">{pullMsg}</span>}
            </div>
            {signalsLoading ? (
              <p className="muted">加载中…</p>
            ) : signalsRows.length > 0 ? (
              <div className="table-wrap table-wrap-signals">
                <table>
                  <thead>
                    <tr>
                      <th>代码</th>
                      <th>名称</th>
                      <th>信号日期</th>
                      <th>次日涨跌幅</th>
                      <th title="个股信号×同期大盘分数，按此列从高到低排序">综合分数</th>
                      {SIGNAL_COLUMNS.map(col => <th key={col}>{col}</th>)}
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {signalsRows.map(row => (
                      <tr key={row.code}>
                        <td><Link to={`/stock/${row.code}`}>{row.code}</Link></td>
                        <td>{row.name || '-'}</td>
                        <td>{fmtDate(String(row.ref_date || '').replace(/-/g, ''))}</td>
                        <td className={pctClass(row.next_day_pct)}>{fmtPct(row.next_day_pct)}</td>
                        <td>{row.composite_score != null ? Number(row.composite_score).toFixed(2) : '-'}</td>
                        {SIGNAL_COLUMNS.map(col => (
                          <td key={col} className={directionClass(row[col])}>{row[col] ?? '-'}</td>
                        ))}
                        <td className="table-actions">
                          <Link to={`/stock/${row.code}`} className="action-link">详情</Link>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="muted">暂无收藏或暂无信号数据。请先加入跟踪并执行「计算全部投资信号」。</p>
            )}
          </>
        )}

        {activeTab === TAB_KEYS.indexSignals && (
          <>
            <p className="muted">大盘各指数（上证/深证/创业板/沪深300/中证500）的投资信号与次日涨跌幅。因无指数资金流数据，仅展示基于日线的 7 类信号：成交量MA20、成交量涨跌幅、均线金叉死叉、均线多空排列、支撑阻力位、量价背离、波动率突破。可切换信号日期对比。</p>
            <div className="form-row" style={{ marginBottom: '0.5rem', flexWrap: 'wrap', gap: '0.5rem', alignItems: 'center' }}>
              <label className="form-label" style={{ marginBottom: 0 }}>
                信号日期：
              </label>
              <select
                value={indexSignalDateFilter}
                onChange={(e) => setIndexSignalDateFilter(e.target.value)}
                className="form-select"
                style={{ width: 'auto', minWidth: '10rem' }}
                title="选择日期查看该日的指数信号与次日涨跌幅"
              >
                <option value="">最近（每个指数取最新一日）</option>
                {availableIndexSignalDates.map((d) => (
                  <option key={d} value={d}>
                    {fmtDate(String(d).replace(/-/g, ''))}
                  </option>
                ))}
              </select>
              <button onClick={runComputeIndexSignals} disabled={computingIndexSignals || indexSignalsLoading}>
                {computingIndexSignals ? '计算中…' : '计算大盘信号'}
              </button>
              {pullMsg && <span className="collect-msg">{pullMsg}</span>}
            </div>
            {indexSignalsLoading ? (
              <p className="muted">加载中…</p>
            ) : indexSignalsRows.length > 0 ? (
              <div className="table-wrap table-wrap-signals">
                <table>
                  <thead>
                    <tr>
                      <th>指数</th>
                      <th>名称</th>
                      <th>信号日期</th>
                      <th>次日涨跌幅</th>
                      {INDEX_SIGNAL_COLUMNS.map(col => <th key={col}>{col}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {indexSignalsRows.map(row => (
                      <tr key={row.code}>
                        <td>{row.code}</td>
                        <td>{row.name || '-'}</td>
                        <td>{fmtDate(String(row.ref_date || '').replace(/-/g, ''))}</td>
                        <td className={pctClass(row.next_day_pct)}>{fmtPct(row.next_day_pct)}</td>
                        {INDEX_SIGNAL_COLUMNS.map(col => (
                          <td key={col} className={directionClass(row[col])}>{row[col] ?? '-'}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <p className="muted">暂无大盘信号数据。请先在工作流执行「采集大盘指数」拉取指数日线，再点击「计算大盘信号」。</p>
            )}
          </>
        )}
        </div>
      </section>
    </div>
  );
}
