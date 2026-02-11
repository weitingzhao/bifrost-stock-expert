import { useEffect, useState } from 'react';
import { api } from '../api';

const SIGNAL_KEYS = [
  '成交量资金MA20',
  '成交量涨跌幅',
  '持续资金流向',
  '均线金叉死叉',
  '主力资金',
  '支撑阻力位',
  '新闻舆论',
];

// 与后端计算逻辑一致的默认值（当前使用的计算值），用于填空与参照
const DEFAULT_SIGNAL_ROW = { bull: 1, bear: -2 };

const DEFAULT_WEIGHTS = {
  marketBase: 1,
  marketBullPerIndex: 0.1,
  marketBearPerIndex: -0.1,
  stockBull: 1,
  stockBear: -2,
  stockNeutralUp: 0.2,
  stockNeutralDown: -0.2,
  signalWeights: SIGNAL_KEYS.reduce((acc, key) => {
    acc[key] = { ...DEFAULT_SIGNAL_ROW };
    return acc;
  }, {}),
  turnover: {
    low: -0.5,
    normal: 0.3,
    high: 0.1,
  },
};

function mergeWeightsFromApi(apiWeights) {
  if (!apiWeights || typeof apiWeights !== 'object') return DEFAULT_WEIGHTS;
  const base = { ...DEFAULT_WEIGHTS, ...apiWeights };
  base.signalWeights = SIGNAL_KEYS.reduce((acc, key) => {
    acc[key] = { ...DEFAULT_SIGNAL_ROW, ...(apiWeights.signalWeights && apiWeights.signalWeights[key]) };
    return acc;
  }, {});
  base.turnover = { ...DEFAULT_WEIGHTS.turnover, ...(apiWeights.turnover || {}) };
  return base;
}

export function Weights() {
  const [weights, setWeights] = useState(DEFAULT_WEIGHTS);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState('');
  const [backtest, setBacktest] = useState(null);
  const [backtesting, setBacktesting] = useState(false);

  useEffect(() => {
    api.config
      .getScoreWeights()
      .then((res) => {
        if (res && res.weights) setWeights(mergeWeightsFromApi(res.weights));
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const updateField = (field, value) => {
    setWeights((prev) => ({ ...prev, [field]: value }));
  };

  const updateTurnover = (field, value) => {
    setWeights((prev) => ({
      ...prev,
      turnover: { ...(prev.turnover || {}), [field]: value },
    }));
  };

  const updateSignalWeight = (signalKey, field, value) => {
    setWeights((prev) => ({
      ...prev,
      signalWeights: {
        ...(prev.signalWeights || {}),
        [signalKey]: {
          ...(prev.signalWeights?.[signalKey] || {}),
          [field]: value,
        },
      },
    }));
  };

  const parseNumber = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };

  const handleSave = () => {
    setSaving(true);
    setMessage('');
    const payload = {
      ...weights,
      marketBase: parseNumber(weights.marketBase),
      marketBullPerIndex: parseNumber(weights.marketBullPerIndex),
      marketBearPerIndex: parseNumber(weights.marketBearPerIndex),
      stockBull: parseNumber(weights.stockBull),
      stockBear: parseNumber(weights.stockBear),
      stockNeutralUp: parseNumber(weights.stockNeutralUp),
      stockNeutralDown: parseNumber(weights.stockNeutralDown),
      signalWeights: SIGNAL_KEYS.reduce((acc, key) => {
        const sw = (weights.signalWeights && weights.signalWeights[key]) || {};
        acc[key] = {
          bull: parseNumber(sw.bull),
          bear: parseNumber(sw.bear),
        };
        return acc;
      }, {}),
      turnover: {
        low: parseNumber(weights.turnover?.low),
        normal: parseNumber(weights.turnover?.normal),
        high: parseNumber(weights.turnover?.high),
      },
    };
    api.config
      .setScoreWeights(payload)
      .then(() => setMessage('已保存权重配置（新的综合得分将按此计算）'))
      .catch((err) => setMessage('保存失败: ' + (err.message || err)))
      .finally(() => setSaving(false));
  };

  const runBacktest = async (windowDays) => {
    setBacktesting(true);
    setBacktest(null);
    setMessage('');
    try {
      const first = await api.selection.watchlistSignals(null);
      const dates = (first?.available_dates || []).filter(Boolean);
      if (!dates.length) {
        setBacktest({ windowDays, total: 0, hits: 0, rate: null, details: [] });
        return;
      }
      const usedDates = dates.slice(0, windowDays);
      let total = 0;
      let hits = 0;
      const details = [];

      for (const d of usedDates) {
        const data = await api.selection.watchlistSignals(d);
        const rows = data?.rows || [];
        let dayTotal = 0;
        let dayHits = 0;
        for (const r of rows) {
          const score = r.composite_score;
          const pct = r.next_day_pct;
          if (score == null || pct == null) continue;
          if (score === 0) continue;
          dayTotal += 1;
          if ((score > 0 && pct > 0) || (score < 0 && pct < 0)) dayHits += 1;
        }
        total += dayTotal;
        hits += dayHits;
        details.push({
          date: d,
          total: dayTotal,
          hits: dayHits,
          rate: dayTotal > 0 ? dayHits / dayTotal : null,
        });
      }

      const rate = total > 0 ? hits / total : null;
      setBacktest({ windowDays, total, hits, rate, details });
    } catch (e) {
      setMessage('回测失败: ' + (e.message || e));
    } finally {
      setBacktesting(false);
    }
  };

  if (loading) {
    return <div className="page page--weights"><p className="weights-loading">加载中…</p></div>;
  }

  return (
    <div className="page page--weights">
      <header className="weights-header">
        <h1 className="weights-title">权重配置</h1>
        <p className="weights-desc">调整「跟踪列表 → 投资信号」综合得分的各项权重，保存后全局生效。</p>
      </header>

      <section className="weights-config">
        <div className="weights-layout">
          <aside className="weights-side">
            <div className="weights-card">
              <h3 className="weights-card-title">大盘分数</h3>
              <p className="weights-card-desc">指数信号对综合分的乘数</p>
              <div className="weights-fields">
                <label className="weights-label">
                  <span>基础分</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.marketBase}
                    onChange={(e) => updateField('marketBase', e.target.value)}
                  />
                </label>
                <label className="weights-label">
                  <span>每看涨指数</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.marketBullPerIndex}
                    onChange={(e) => updateField('marketBullPerIndex', e.target.value)}
                  />
                </label>
                <label className="weights-label">
                  <span>每看跌指数</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.marketBearPerIndex}
                    onChange={(e) => updateField('marketBearPerIndex', e.target.value)}
                  />
                </label>
              </div>
            </div>
            <div className="weights-card">
              <h3 className="weights-card-title">换手率</h3>
              <p className="weights-card-desc">按档位得分</p>
              <div className="weights-fields">
                <label className="weights-label">
                  <span>交投清淡</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.turnover?.low}
                    onChange={(e) => updateTurnover('low', e.target.value)}
                  />
                </label>
                <label className="weights-label">
                  <span>正常活跃</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.turnover?.normal}
                    onChange={(e) => updateTurnover('normal', e.target.value)}
                  />
                </label>
                <label className="weights-label">
                  <span>异常活跃</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.turnover?.high}
                    onChange={(e) => updateTurnover('high', e.target.value)}
                  />
                </label>
              </div>
            </div>
          </aside>

          <div className="weights-main">
            <div className="weights-card weights-card--wide">
              <h3 className="weights-card-title">个股信号（分项权重）</h3>
              <p className="weights-card-desc">每类信号单独设置看涨/看跌得分；中性信号用下方全局值。</p>
              <div className="weights-signals-wrap">
                <table className="weights-signals-table">
                  <thead>
                    <tr>
                      <th>信号</th>
                      <th>看涨</th>
                      <th>看跌</th>
                    </tr>
                  </thead>
                  <tbody>
                    {SIGNAL_KEYS.map((key) => {
                      const sw = { ...DEFAULT_SIGNAL_ROW, ...weights.signalWeights?.[key] };
                      return (
                        <tr key={key}>
                          <td>{key}</td>
                          <td>
                            <input
                              type="number"
                              step="any"
                              value={sw.bull}
                              onChange={(e) => updateSignalWeight(key, 'bull', e.target.value)}
                            />
                          </td>
                          <td>
                            <input
                              type="number"
                              step="any"
                              value={sw.bear}
                              onChange={(e) => updateSignalWeight(key, 'bear', e.target.value)}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <div className="weights-neutral">
                <label className="weights-label">
                  <span>中性（大盘&gt;1）</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.stockNeutralUp}
                    onChange={(e) => updateField('stockNeutralUp', e.target.value)}
                  />
                </label>
                <label className="weights-label">
                  <span>中性（大盘&lt;1）</span>
                  <input
                    type="number"
                    step="any"
                    value={weights.stockNeutralDown}
                    onChange={(e) => updateField('stockNeutralDown', e.target.value)}
                  />
                </label>
              </div>
            </div>
          </div>
        </div>
        <div className="weights-bar">
          <button type="button" className="weights-save" onClick={handleSave} disabled={saving}>
            {saving ? '保存中…' : '保存配置'}
          </button>
          {message && <span className="weights-msg">{message}</span>}
          <div className="weights-formula" title="综合得分 = 大盘分数 × 个股原始分">
            <span className="weights-formula-label">计算公式：</span>
            <span className="weights-formula-body">
              综合得分 = 大盘分数 × 个股原始分；大盘分数 = 基础分 + 看涨指数数×每看涨权重 + 看跌指数数×每看跌权重；个股原始分 = 各信号得分之和（看涨/看跌/中性/换手率按上表权重）。
            </span>
          </div>
        </div>
      </section>

      <section className="weights-backtest">
        <h2 className="weights-section-title">回测</h2>
        <p className="weights-backtest-desc">
          按当前权重统计：得分与次日涨跌方向一致（得分&gt;0 且涨、或得分&lt;0 且跌）视为正相关。
        </p>
        <p className="weights-backtest-sample-desc">
          <strong>样本数</strong>：该日跟踪列表中「有综合得分且存在次日涨跌幅」的股票数；得分为 0 的不计入（无方向）。
        </p>
        <div className="weights-backtest-btns">
          <button type="button" disabled={backtesting} onClick={() => runBacktest(5)}>1 周</button>
          <button type="button" disabled={backtesting} onClick={() => runBacktest(10)}>2 周</button>
          <button type="button" disabled={backtesting} onClick={() => runBacktest(20)}>1 月</button>
        </div>
        {backtesting && <p className="weights-backtest-status">回测中…</p>}
        {backtest && (
          <div className="weights-backtest-result">
            {backtest.total === 0 ? (
              <p className="muted">最近 {backtest.windowDays} 个交易日内暂无可回测数据。</p>
            ) : (
              <>
                <p className="weights-backtest-summary">
                  合计：样本 <strong>{backtest.total}</strong>，正相关 <strong>{backtest.hits}</strong>，
                  比例 <strong>{backtest.rate != null ? (backtest.rate * 100).toFixed(1) : '-'}%</strong>
                </p>
                {backtest.details && backtest.details.length > 0 && (
                  <div className="weights-backtest-table-wrap table-wrap">
                    <table className="weights-backtest-table">
                      <thead>
                        <tr>
                          <th>信号日期</th>
                          <th>样本数</th>
                          <th>正相关数</th>
                          <th>正相关比例</th>
                        </tr>
                      </thead>
                      <tbody>
                        {backtest.details.map((row) => (
                          <tr key={row.date}>
                            <td>{row.date}</td>
                            <td>{row.total}</td>
                            <td>{row.hits}</td>
                            <td>{row.rate != null ? (row.rate * 100).toFixed(1) + '%' : '-'}</td>
                          </tr>
                        ))}
                        <tr className="weights-backtest-total-row">
                          <td>合计</td>
                          <td>{backtest.total}</td>
                          <td>{backtest.hits}</td>
                          <td>{backtest.rate != null ? (backtest.rate * 100).toFixed(1) + '%' : '-'}</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            )}
          </div>
        )}
      </section>
    </div>
  );
}

