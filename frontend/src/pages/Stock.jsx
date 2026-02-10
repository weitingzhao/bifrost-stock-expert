import React, { useState, useEffect, useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import { Stock as StockChart, Line, Column } from '@ant-design/charts';
import { api } from '../api';

/** 将文本中的数字（含小数、百分号）包成 span.investment-summary-num 以便高亮；递归处理 React 子节点 */
function wrapNumbers(children) {
  if (children == null) return null;
  if (typeof children === 'string') {
    const parts = children.split(/(\d+\.?\d*%?)/g);
    return parts.map((part, i) =>
      /^\d/.test(part) ? <span key={i} className="investment-summary-num">{part}</span> : part
    );
  }
  if (Array.isArray(children)) {
    return children.map((child, i) => <React.Fragment key={i}>{wrapNumbers(child)}</React.Fragment>);
  }
  if (typeof children === 'object' && children.props?.children !== undefined) {
    return React.cloneElement(children, {}, wrapNumbers(children.props.children));
  }
  return children;
}

const fmtDate = (d) => (d && String(d).length >= 8) ? `${String(d).slice(0,4)}-${String(d).slice(4,6)}-${String(d).slice(6,8)}` : (d || '');

/** 详情页头部按钮用的小图标（SVG 继承 currentColor，由 CSS 分类色控制） */
const IconStar = ({ filled }) => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill={filled ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg></span>
);
const IconRefresh = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg></span>
);
const IconBuilding = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M2 22h20M5 22V7l7-4 7 4v15M5 11h14M9 15h6"/></svg></span>
);
const IconTrending = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg></span>
);
const IconNews = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"/><path d="M18 14h-8M15 18h-8M10 6h8"/></svg></span>
);
const IconList = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg></span>
);
const IconFileText = () => (
  <span className="stock-btn-icon" aria-hidden><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><line x1="10" y1="9" x2="8" y2="9"/></svg></span>
);

/** 检测系统深色模式，用于图表 theme（图例/坐标轴等适配深色） */
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

export function Stock() {
  const { code } = useParams();
  const prefersDark = usePrefersDark();
  const chartTheme = prefersDark ? 'dark' : undefined;
  const [data, setData] = useState(null);
  const [kline, setKline] = useState([]);
  const [klineLimit, setKlineLimit] = useState(90); // 90 | 120 | 180 天
  const [technicals, setTechnicals] = useState([]);
  const [fundamentals, setFundamentals] = useState([]);
  const [moneyflow, setMoneyflow] = useState([]);
  const [financial, setFinancial] = useState([]);
  const [loading, setLoading] = useState(true);
  const [watching, setWatching] = useState(false);
  const [updating, setUpdating] = useState(false);
  const [parsing, setParsing] = useState(false);
  const [computingSignals, setComputingSignals] = useState(false);
  const [newsSignaling, setNewsSignaling] = useState(false);
  const [summaryGenerating, setSummaryGenerating] = useState(false);
  const [signalTooltip, setSignalTooltip] = useState(null); // { text, left, top } 用于固定定位 tooltip，避免被 table-wrap 裁剪
  const [newsModalOpen, setNewsModalOpen] = useState(false);
  const [promptModalOpen, setPromptModalOpen] = useState(false);
  const [promptText, setPromptText] = useState('');
  const [promptLoading, setPromptLoading] = useState(false);
  const [promptSaving, setPromptSaving] = useState(false);
  const [newsRecords, setNewsRecords] = useState([]);
  const [newsRecordsLoading, setNewsRecordsLoading] = useState(false);

  const refreshData = () => {
    if (!code) return;
    Promise.all([
      api.stock.get(code),
      api.stock.kline(code, klineLimit),
      api.stock.technicals(code),
      api.stock.fundamentals(code),
      api.stock.moneyflow(code),
      api.stock.financial(code),
    ]).then(([d, k, t, f, mf, fin]) => {
      setData(d);
      setKline(Array.isArray(k) ? k : []);
      setTechnicals(Array.isArray(t) ? t : []);
      setFundamentals(Array.isArray(f) ? f : []);
      setMoneyflow(Array.isArray(mf) ? mf : []);
      setFinancial(Array.isArray(fin) ? fin : []);
      setWatching(!!d?.watchlist);
    }).catch(() => {}).finally(() => setLoading(false));
  };

  const setKlinePeriod = (limit) => {
    setKlineLimit(limit);
    if (code) api.stock.kline(code, limit).then((k) => setKline(Array.isArray(k) ? k : [])).catch(() => {});
  };

  useEffect(() => {
    if (!code) return;
    setLoading(true);
    Promise.all([
      api.stock.get(code),
      api.stock.kline(code, klineLimit),
      api.stock.technicals(code),
      api.stock.fundamentals(code),
      api.stock.moneyflow(code),
      api.stock.financial(code),
    ]).then(([d, k, t, f, mf, fin]) => {
      setData(d);
      setKline(Array.isArray(k) ? k : []);
      setTechnicals(Array.isArray(t) ? t : []);
      setFundamentals(Array.isArray(f) ? f : []);
      setMoneyflow(Array.isArray(mf) ? mf : []);
      setFinancial(Array.isArray(fin) ? fin : []);
      setWatching(!!d?.watchlist);
    }).catch(() => setData(null)).finally(() => setLoading(false));
  }, [code]);

  const toggleWatch = () => {
    if (watching) api.stock.unwatch(code).then(() => setWatching(false)).catch(console.error);
    else api.stock.watch(code).then(() => setWatching(true)).catch(console.error);
  };

  const runUpdateData = () => {
    if (!code || updating) return;
    setUpdating(true);
    api.workflow.trigger({ action: 'collect_stock', codes: [code] })
      .then((res) => {
        if (res.result?.ok) refreshData();
      })
      .catch(console.error)
      .finally(() => setUpdating(false));
  };

  const runParseCorp = () => {
    if (!code || parsing) return;
    setParsing(true);
    api.workflow.trigger({ action: 'parse_corp', codes: [code] })
      .then((res) => {
        if (res.result?.ok) refreshData();
      })
      .catch(console.error)
      .finally(() => setParsing(false));
  };

  const runComputeSignals = () => {
    if (!code || computingSignals) return;
    setComputingSignals(true);
    api.workflow.trigger({ action: 'compute_signals', codes: [code] })
      .then((res) => {
        if (res.result?.ok) refreshData();
      })
      .catch(console.error)
      .finally(() => setComputingSignals(false));
  };

  const runNewsSignal = () => {
    if (!code || newsSignaling) return;
    setNewsSignaling(true);
    api.workflow.trigger({ action: 'news_signal', codes: [code] })
      .then((res) => {
        if (res.result?.ok) refreshData();
      })
      .catch(console.error)
      .finally(() => setNewsSignaling(false));
  };

  const openNewsModal = () => {
    if (!code) return;
    setNewsModalOpen(true);
    setNewsRecordsLoading(true);
    api.stock.newsOpinionRecords(code, 80)
      .then((list) => setNewsRecords(Array.isArray(list) ? list : []))
      .catch(() => setNewsRecords([]))
      .finally(() => setNewsRecordsLoading(false));
  };

  const openPromptModal = () => {
    setPromptModalOpen(true);
    setPromptLoading(true);
    api.config.getInvestmentSummaryPrompt()
      .then((r) => setPromptText(r?.prompt ?? ''))
      .catch(() => setPromptText(''))
      .finally(() => setPromptLoading(false));
  };

  const savePrompt = () => {
    setPromptSaving(true);
    api.config.setInvestmentSummaryPrompt(promptText)
      .then(() => setPromptModalOpen(false))
      .catch(console.error)
      .finally(() => setPromptSaving(false));
  };

  const runInvestmentSummary = () => {
    if (!code || summaryGenerating) return;
    setSummaryGenerating(true);
    api.workflow.trigger({ action: 'investment_summary', codes: [code] })
      .then((res) => {
        if (res.result?.ok) refreshData();
      })
      .catch(console.error)
      .finally(() => setSummaryGenerating(false));
  };

  const fmt = (v) => v != null && v !== '' ? Number(v).toLocaleString(undefined, { maximumFractionDigits: 4 }) : '-';

  /** 财务金额友好展示：亿/万 或千分位 */
  const fmtAmount = (v) => {
    if (v == null || v === '') return '-';
    const n = Number(v);
    if (Number.isNaN(n)) return '-';
    const abs = Math.abs(n);
    if (abs >= 1e8) return (n / 1e8).toFixed(2) + '亿';
    if (abs >= 1e4) return (n / 1e4).toFixed(2) + '万';
    return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  };

  /** 数量友好展示：万/亿（用于成交量、成交额） */
  const fmtVolumeOrAmount = (v) => {
    if (v == null || v === '') return '-';
    const n = Number(v);
    if (Number.isNaN(n)) return '-';
    const abs = Math.abs(n);
    if (abs >= 1e8) return (n / 1e8).toFixed(2) + '亿';
    if (abs >= 1e4) return (n / 1e4).toFixed(2) + '万';
    return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  };

  const klineChartData = useMemo(() => {
    return [...kline]
      .map(row => {
        const date = fmtDate(String(row.trade_date || '').replace(/-/g, '').slice(0, 8));
        const open = row.open != null ? Number(row.open) : null;
        const close = row.close != null ? Number(row.close) : null;
        const trend = open != null && close != null ? (close >= open ? 1 : -1) : 0;
        const changePct = open != null && close != null && open !== 0
          ? ((close - open) / open) * 100
          : null;
        return {
          date,
          open,
          close,
          high: row.high != null ? Number(row.high) : null,
          low: row.low != null ? Number(row.low) : null,
          volume: row.volume != null ? Number(row.volume) : null,
          amount: row.amount != null ? Number(row.amount) : null,
          trend,
          changePct,
        };
      })
      .filter(row => row.date && row.open != null)
      .sort((a, b) => a.date.localeCompare(b.date));
  }, [kline]);

  /** 信号日期 -> 次日涨跌幅(%)（下一交易日相对当日的涨跌），用于对比信号与次日结果 */
  const nextDayPctMap = useMemo(() => {
    const m = {};
    const sorted = [...klineChartData].sort((a, b) => a.date.localeCompare(b.date));
    for (let i = 0; i + 1 < sorted.length; i++) {
      const todayClose = sorted[i].close;
      const nextClose = sorted[i + 1].close;
      if (todayClose != null && nextClose != null && todayClose !== 0) {
        m[sorted[i].date] = ((nextClose - todayClose) / todayClose) * 100;
      }
    }
    return m;
  }, [klineChartData]);

  const volumeChartData = useMemo(() => {
    return klineChartData.map(row => {
      const rise = row.close != null && row.open != null && row.close >= row.open;
      return {
        date: row.date,
        volume: row.volume,
        amount: row.amount,
        成交量: row.volume,
        rise,
        riseLabel: rise ? 'up' : 'down',
      };
    });
  }, [klineChartData]);

  const technicalsLineData = useMemo(() => {
    const ma = [];
    const macd = [];
    const rsi = [];
    const kdj = [];
    technicals.forEach(row => {
      const date = fmtDate(String(row.trade_date || '').replace(/-/g, ''));
      if (row.ma5 != null) ma.push({ date, value: Number(row.ma5), name: 'MA5' });
      if (row.ma10 != null) ma.push({ date, value: Number(row.ma10), name: 'MA10' });
      if (row.ma20 != null) ma.push({ date, value: Number(row.ma20), name: 'MA20' });
      if (row.macd != null) macd.push({ date, value: Number(row.macd), name: 'MACD' });
      if (row.rsi != null) rsi.push({ date, value: Number(row.rsi), name: 'RSI' });
      if (row.kdj_k != null) kdj.push({ date, value: Number(row.kdj_k), name: 'K' });
      if (row.kdj_d != null) kdj.push({ date, value: Number(row.kdj_d), name: 'D' });
      if (row.kdj_j != null) kdj.push({ date, value: Number(row.kdj_j), name: 'J' });
    });
    return { ma, macd, rsi, kdj };
  }, [technicals]);

  const fundamentalsLineData = useMemo(() => {
    const list = [];
    fundamentals.forEach(row => {
      const date = fmtDate(String(row.report_date || '').replace(/-/g, ''));
      if (row.pe != null) list.push({ date, value: Number(row.pe), name: 'PE' });
      if (row.pb != null) list.push({ date, value: Number(row.pb), name: 'PB' });
      if (row.ps != null) list.push({ date, value: Number(row.ps), name: 'PS' });
      if (row.market_cap != null) list.push({ date, value: Number(row.market_cap) / 1e8, name: '市值(亿)' });
      if (row.roe != null) list.push({ date, value: Number(row.roe), name: 'ROE%' });
    });
    return list;
  }, [fundamentals]);

  /** 资金净流入图表：总净流入 + 小/中/大/特大单净流入（万元），用于柱状图与 tooltip */
  const moneyflowChartData = useMemo(() => {
    const net = (b, s) => {
      const buy = b != null ? Number(b) : 0;
      const sell = s != null ? Number(s) : 0;
      return buy - sell;
    };
    return moneyflow.map(row => {
      const date = fmtDate(String(row.trade_date || '').replace(/-/g, ''));
      const amt = row.net_mf_amount != null ? Number(row.net_mf_amount) : null;
      const netSm = row.buy_sm_amount != null || row.sell_sm_amount != null ? net(row.buy_sm_amount, row.sell_sm_amount) : null;
      const netMd = row.buy_md_amount != null || row.sell_md_amount != null ? net(row.buy_md_amount, row.sell_md_amount) : null;
      const netLg = row.buy_lg_amount != null || row.sell_lg_amount != null ? net(row.buy_lg_amount, row.sell_lg_amount) : null;
      const netElg = row.buy_elg_amount != null || row.sell_elg_amount != null ? net(row.buy_elg_amount, row.sell_elg_amount) : null;
      return {
        date,
        net_mf_amount: amt,
        direction: amt != null ? (amt >= 0 ? '流入' : '流出') : '-',
        net_sm: netSm,
        net_md: netMd,
        net_lg: netLg,
        net_elg: netElg,
      };
    }).filter(row => row.date);
  }, [moneyflow]);

  /** 财报柱状图数据：营业收入、净利润（单位亿），长格式供 Column 使用 */
  const financialChartData = useMemo(() => {
    const list = [...financial].slice(0, 12).reverse();
    const rows = [];
    list.forEach((row) => {
      const date = fmtDate(String(row.report_date || '').replace(/-/g, ''));
      if (row.revenue != null) rows.push({ date, name: '营业收入', value: Number(row.revenue) / 1e8 });
      if (row.net_profit != null) rows.push({ date, name: '净利润', value: Number(row.net_profit) / 1e8 });
    });
    return rows;
  }, [financial]);

  /** 财务季度数据 + 营收/净利润的同比、环比（环比=较上季，同比=较去年同期同季） */
  const financialWithChange = useMemo(() => {
    const list = [...financial];
    const rev = (i) => (list[i]?.revenue != null ? Number(list[i].revenue) : null);
    const net = (i) => (list[i]?.net_profit != null ? Number(list[i].net_profit) : null);
    const pct = (cur, prev) => (prev != null && prev !== 0 ? ((cur - prev) / Math.abs(prev)) * 100 : null);
    return list.map((row, i) => ({
      ...row,
      revenue_yoy: pct(rev(i), rev(i + 4)),
      revenue_qoq: pct(rev(i), rev(i + 1)),
      net_profit_yoy: pct(net(i), net(i + 4)),
      net_profit_qoq: pct(net(i), net(i + 1)),
    }));
  }, [financial]);

  const fmtPctChange = (v) => v != null ? (v >= 0 ? '+' : '') + Number(v).toFixed(1) + '%' : '-';
  const pctChangeClass = (v) => v == null ? '' : (Number(v) >= 0 ? 'pct-up' : 'pct-down');

  if (loading) return <div className="page"><p>加载中…</p></div>;
  if (!data) return <div className="page"><p>未找到该股票或接口异常</p></div>;

  const { corp, latestDay, signals, corpAnalysis, indexSnapshot, investmentSummary } = data;
  // 当日涨跌：先用开盘→收盘（与 K 线一致），用于展示；跑赢大盘用日涨跌幅（昨收→今收）与指数一致
  const lastK = klineChartData.length > 0 ? klineChartData[klineChartData.length - 1] : null;
  const prevClose = klineChartData.length >= 2 ? klineChartData[klineChartData.length - 2].close : null;
  const stockPct = lastK?.changePct != null ? lastK.changePct : null;
  const stockDailyPct = lastK?.close != null && prevClose != null && prevClose !== 0
    ? ((lastK.close - prevClose) / prevClose) * 100
    : null;
  const pctForVs = stockDailyPct != null ? stockDailyPct : stockPct;
  const shIndex = indexSnapshot?.indices?.find((i) => i.index_code === '000001.SH');
  const cybIndex = indexSnapshot?.indices?.find((i) => i.index_code === '399006.SZ');
  const vsSh = pctForVs != null && shIndex?.pct_chg != null ? pctForVs - shIndex.pct_chg : null;
  const vsCyb = pctForVs != null && cybIndex?.pct_chg != null ? pctForVs - cybIndex.pct_chg : null;

  const lineChartConfig = (palette) => {
    const colors = palette || ['#5B8FF9', '#F6BD16', '#5AD8A6', '#E86452', '#9254DE'];
    return {
      theme: chartTheme,
      xField: 'date',
      yField: 'value',
      seriesField: 'name',
      colorField: 'name',
      smooth: true,
      animation: { appear: { duration: 400 } },
      legend: { position: 'top-left' },
      scale: { color: { type: 'ordinal', range: colors } },
    };
  };

  return (
    <div className="page stock">
      <div className="stock-header">
        <h1>{code} {corp?.name || '-'}</h1>
        <div className="stock-header-actions">
          <button type="button" className="stock-btn stock-btn-watch" onClick={toggleWatch}>
            <IconStar filled={watching} />{watching ? '取消跟踪' : '加入跟踪'}
          </button>
          <button type="button" className="stock-btn stock-btn-data" onClick={runUpdateData} disabled={updating}>
            <IconRefresh />{updating ? '更新中…' : '更新数据'}
          </button>
          <button type="button" className="stock-btn stock-btn-parse" onClick={runParseCorp} disabled={parsing}>
            <IconBuilding />{parsing ? '解析中…' : '解析企业'}
          </button>
          <button type="button" className="stock-btn stock-btn-signal" onClick={runComputeSignals} disabled={computingSignals}>
            <IconTrending />{computingSignals ? '计算中…' : '计算信号'}
          </button>
          <button type="button" className="stock-btn stock-btn-news" onClick={runNewsSignal} disabled={newsSignaling}>
            <IconNews />{newsSignaling ? '收集中…' : '收集情报'}
          </button>
          <button type="button" className="stock-btn stock-btn-view-news" onClick={openNewsModal}>
            <IconList />查看新闻
          </button>
          <button type="button" className="stock-btn stock-btn-summary" onClick={runInvestmentSummary} disabled={summaryGenerating}>
            <IconFileText />{summaryGenerating ? '生成中…' : '投资总结'}
          </button>
        </div>
      </div>
      {corp && (
        <p className="meta">
          {corp.market && `${corp.market} · `}{corp.industry || ''}{corp.sector && corp.sector !== corp.industry ? ` / ${corp.sector}` : ''}
          {corp.list_date && (
            <span className="meta-sep"> · 上市日期：{typeof corp.list_date === 'string' ? corp.list_date.slice(0, 10) : (corp.list_date.toISOString && corp.list_date.toISOString().slice(0, 10)) || String(corp.list_date).slice(0, 10)}</span>
          )}
          {(corp.pe != null && corp.pe !== '') && (
            <span className="meta-sep"> · 市盈率：{Number(corp.pe).toFixed(2)}</span>
          )}
        </p>
      )}
      {(corpAnalysis?.business_intro || corpAnalysis?.competitiveness_analysis) && (
        <section className="section corp-analysis-section corp-analysis-header">
          <div className="corp-analysis-top">
            <span className="corp-analysis-heading">企业解析</span>
            {corpAnalysis.updated_at && (
              <span className="muted corp-analysis-meta">更新：{new Date(corpAnalysis.updated_at).toLocaleString()}</span>
            )}
          </div>
          <div className="corp-analysis-header-grid">
            {corpAnalysis.business_intro && (
              <div className="corp-analysis-block">
                <h3 className="corp-analysis-title">主营业务介绍</h3>
                <div className="corp-analysis-content" style={{ whiteSpace: 'pre-wrap' }}>{corpAnalysis.business_intro}</div>
              </div>
            )}
            {corpAnalysis.competitiveness_analysis && (
              <div className="corp-analysis-block">
                <h3 className="corp-analysis-title">核心竞争力分析</h3>
                <div className="corp-analysis-content" style={{ whiteSpace: 'pre-wrap' }}>{corpAnalysis.competitiveness_analysis}</div>
              </div>
            )}
          </div>
        </section>
      )}
      {(investmentSummary?.content || summaryGenerating) && (
        <section className="section corp-analysis-section investment-summary-section">
          <div className="corp-analysis-top">
            <span className="corp-analysis-heading">投资总结</span>
            {investmentSummary?.updated_at && (
              <span className="muted corp-analysis-meta">更新：{new Date(investmentSummary.updated_at).toLocaleString()}</span>
            )}
            <button
              type="button"
              className="investment-summary-refresh"
              onClick={runInvestmentSummary}
              disabled={summaryGenerating}
            >
              {summaryGenerating ? '生成中…' : '重新生成'}
            </button>
            <button type="button" className="investment-summary-refresh" onClick={openPromptModal}>
              修改提示词
            </button>
          </div>
          <div className="corp-analysis-block investment-summary-block">
            <div className="corp-analysis-content investment-summary-content">
              {summaryGenerating ? (
                '正在调用 AI 生成投资建议…'
              ) : (
                <ReactMarkdown
                  components={{
                    p: ({ children }) => <p>{wrapNumbers(children)}</p>,
                    li: ({ children }) => <li>{wrapNumbers(children)}</li>,
                    strong: ({ children }) => <strong>{wrapNumbers(children)}</strong>,
                    em: ({ children }) => <em>{wrapNumbers(children)}</em>,
                    blockquote: ({ children }) => <blockquote>{wrapNumbers(children)}</blockquote>,
                    h1: ({ children }) => <h1>{wrapNumbers(children)}</h1>,
                    h2: ({ children }) => <h2>{wrapNumbers(children)}</h2>,
                    h3: ({ children }) => <h3>{wrapNumbers(children)}</h3>,
                  }}
                >
                  {investmentSummary?.content ?? ''}
                </ReactMarkdown>
              )}
            </div>
          </div>
        </section>
      )}
      {!investmentSummary?.content && !summaryGenerating && (
        <section className="section investment-summary-cta">
          <p className="muted">暂无投资总结。将综合系统信号、日线、技术指标、企业分析、大盘与财务数据，由 AI 输出建仓价位区间、持仓时间及应关注的波动与交易信号。</p>
          <div className="form-row" style={{ gap: '0.5rem', flexWrap: 'wrap' }}>
            <button type="button" className="investment-summary-cta-btn" onClick={runInvestmentSummary} disabled={summaryGenerating}>
              {summaryGenerating ? '生成中…' : '生成投资总结'}
            </button>
            <button type="button" className="investment-summary-refresh" onClick={openPromptModal}>修改提示词</button>
          </div>
        </section>
      )}
      {latestDay && (
        <div className="latest-day">
          <span className="latest-day-item"><span className="latest-day-label">数据日期</span> {fmtDate(String(latestDay.trade_date).replace(/-/g, ''))}</span>
          <span className="latest-day-sep">·</span>
          <span className="latest-day-item"><span className="latest-day-label">收盘</span> <strong>{fmt(latestDay.close)}</strong></span>
          <span className="latest-day-sep">·</span>
          <span className="latest-day-item"><span className="latest-day-label">成交量</span> {fmtVolumeOrAmount(latestDay.volume)}</span>
          <span className="latest-day-sep">·</span>
          <span className="latest-day-item"><span className="latest-day-label">成交额</span> {fmtVolumeOrAmount(latestDay.amount)}</span>
          <span className="latest-day-sep">·</span>
          <span className={`latest-day-item ${stockPct != null ? (stockPct >= 0 ? 'pct-up' : 'pct-down') : ''}`}>
            <span className="latest-day-label">当日涨跌</span>{' '}
            {stockPct != null ? `${stockPct >= 0 ? '+' : ''}${stockPct.toFixed(2)}%` : '-'}
          </span>
        </div>
      )}
      {indexSnapshot?.indices?.length > 0 && (
        <div className="index-snapshot">
          <span className="index-snapshot-label">大盘（同日）</span>
          {indexSnapshot.indices.map((i) => (
            <span key={i.index_code} className={`index-snapshot-item ${i.pct_chg != null ? (i.pct_chg >= 0 ? 'pct-up' : 'pct-down') : ''}`}>
              {i.name} {i.pct_chg != null ? `${i.pct_chg >= 0 ? '+' : ''}${Number(i.pct_chg).toFixed(2)}%` : '-'}
            </span>
          ))}
          {(vsSh != null || vsCyb != null) && (
            <>
              <span className="index-snapshot-sep">|</span>
              <span className="index-snapshot-label">本股 vs 大盘</span>
              {vsSh != null && (
                <span className={`index-snapshot-item ${vsSh >= 0 ? 'pct-up' : 'pct-down'}`}>
                  较上证 {vsSh >= 0 ? '跑赢' : '跑输'} {Math.abs(vsSh).toFixed(2)}%
                </span>
              )}
              {vsCyb != null && (
                <span className={`index-snapshot-item ${vsCyb >= 0 ? 'pct-up' : 'pct-down'}`}>
                  较创业板 {vsCyb >= 0 ? '跑赢' : '跑输'} {Math.abs(vsCyb).toFixed(2)}%
                </span>
              )}
            </>
          )}
        </div>
      )}
      {klineChartData.length > 0 && (
        <p className="muted stock-data-hint">
          日线数据最新到 <strong>{klineChartData[klineChartData.length - 1].date}</strong>。
          若需更新到今日，请点「更新数据」；若仍只到前一交易日，可能是 Tushare 该只仅更新到该日，或终端「最新交易日（来自股票 xxx）」来自其他股票。
        </p>
      )}

      <section className="section chart-section">
        <div className="chart-section-head">
          <h2>日线 K 线（蜡烛图）</h2>
          <div className="kline-period-tabs">
            <button type="button" className={klineLimit === 90 ? 'active' : ''} onClick={() => setKlinePeriod(90)}>90天</button>
            <button type="button" className={klineLimit === 120 ? 'active' : ''} onClick={() => setKlinePeriod(120)}>120天</button>
            <button type="button" className={klineLimit === 180 ? 'active' : ''} onClick={() => setKlinePeriod(180)}>180天</button>
          </div>
        </div>
        {klineChartData.length > 0 ? (
          <>
            <div className="chart-wrap" style={{ height: 320 }}>
              <StockChart
                theme={chartTheme}
                data={klineChartData}
                xField="date"
                yField={['open', 'close', 'low', 'high']}
                colorField="trend"
                scale={{ color: { domain: [-1, 0, 1], range: ['#26a69a', '#999999', '#ef5350'] } }}
                tooltip={{
                  title: (d) => d?.date ?? '',
                  items: [
                    (d) => ({ name: '日期', value: d?.date ?? '-' }),
                    (d) => ({ name: '开盘', value: d?.open != null ? fmt(d.open) : '-' }),
                    (d) => ({ name: '收盘', value: d?.close != null ? fmt(d.close) : '-' }),
                    (d) => ({ name: '最高', value: d?.high != null ? fmt(d.high) : '-' }),
                    (d) => ({ name: '最低', value: d?.low != null ? fmt(d.low) : '-' }),
                    (d) => ({ name: '涨跌幅', value: d?.changePct != null ? `${d.changePct >= 0 ? '+' : ''}${d.changePct.toFixed(2)}%` : '-' }),
                  ],
                }}
              />
            </div>
            {volumeChartData.some(d => d.volume != null) && (
              <div className="chart-wrap chart-wrap-sm" style={{ height: 160 }}>
                <Column
                  theme={chartTheme}
                  data={volumeChartData}
                  xField="date"
                  yField="volume"
                  colorField="riseLabel"
                  scale={{ color: { domain: ['down', 'up'], range: ['#26a69a', '#ef5350'] } }}
                  columnStyle={{ radius: [2, 2, 0, 0] }}
                  legend={false}
                  tooltip={{
                    title: (d) => d?.date ?? '',
                    items: [
                      (d) => ({ name: '日期', value: d?.date ?? '-' }),
                      (d) => ({ name: '成交量（股）', value: d?.volume != null ? fmtVolumeOrAmount(d.volume) : '-' }),
                      (d) => ({ name: '成交额（万/亿）', value: d?.amount != null ? fmtVolumeOrAmount(d.amount) : '-' }),
                    ],
                  }}
                />
              </div>
            )}
          </>
        ) : (
          <p className="muted">暂无日线。请在工作流中执行「拉取收藏股票日线/技术/基本面/财务」。</p>
        )}
      </section>

      <section className="section chart-section chart-section-indicators">
        <h2>技术指标 / 财报图表</h2>
        <div className="chart-row-half">
          <div className="chart-col-half">
            <h3 className="chart-block-title">均线 MA5 / MA10 / MA20</h3>
            {technicalsLineData.ma.length > 0 ? (
              <div className="chart-wrap" style={{ height: 280 }}>
                <Line data={technicalsLineData.ma} {...lineChartConfig(['#5B8FF9', '#F6BD16', '#5AD8A6'])} />
              </div>
            ) : (
              <p className="muted">暂无均线数据</p>
            )}
          </div>
          <div className="chart-col-half">
            <h3 className="chart-block-title">财报数据（季度）</h3>
            {financialChartData.length > 0 ? (
              <div className="chart-wrap" style={{ height: 280 }}>
                <Column
                  theme={chartTheme}
                  data={financialChartData}
                  xField="date"
                  yField="value"
                  seriesField="name"
                  colorField="name"
                  color={['#5B8FF9', '#E86452']}
                  legend={{ position: 'top-right' }}
                  columnStyle={{ radius: [2, 2, 0, 0] }}
                  tooltip={{
                    title: (d) => d?.date ?? '',
                    items: [
                      (d) => ({ name: '报告期', value: d?.date ?? '-' }),
                      (d) => ({ name: d?.name ?? '指标', value: d?.value != null ? `${Number(d.value).toFixed(2)} 亿` : '-' }),
                    ],
                  }}
                />
              </div>
            ) : (
              <p className="muted">暂无财报数据</p>
            )}
          </div>
        </div>
        <div className="chart-row-half">
          <div className="chart-col-half">
            <h3 className="chart-block-title">每日资金净流入（万元）</h3>
            {moneyflowChartData.length > 0 ? (
              <div className="chart-wrap" style={{ height: 280 }}>
                <Column
                  theme={chartTheme}
                  data={moneyflowChartData}
                  xField="date"
                  yField="net_mf_amount"
                  colorField="direction"
                  color={['#ef5350', '#26a69a']}
                  legend={{ position: 'top-right' }}
                  columnStyle={{ radius: [2, 2, 0, 0] }}
                  tooltip={{
                    title: (d) => d?.date ?? '',
                    items: [
                      (d) => ({ name: '日期', value: d?.date ?? '-' }),
                      (d) => ({ name: '净流入额', value: d?.net_mf_amount != null ? `${Number(d.net_mf_amount).toFixed(2)} 万元` : '-' }),
                      (d) => ({ name: '小单净流入', value: d?.net_sm != null ? `${Number(d.net_sm).toFixed(2)} 万元` : '-' }),
                      (d) => ({ name: '中单净流入', value: d?.net_md != null ? `${Number(d.net_md).toFixed(2)} 万元` : '-' }),
                      (d) => ({ name: '大单净流入', value: d?.net_lg != null ? `${Number(d.net_lg).toFixed(2)} 万元` : '-' }),
                      (d) => ({ name: '特大单净流入', value: d?.net_elg != null ? `${Number(d.net_elg).toFixed(2)} 万元` : '-' }),
                    ],
                  }}
                />
              </div>
            ) : (
              <p className="muted">暂无资金流向数据（需 Tushare 2000+ 积分并执行更新数据）</p>
            )}
          </div>
          <div className="chart-col-half">
            <h3 className="chart-block-title">基本面（每日）</h3>
            {fundamentalsLineData.length > 0 ? (
              <div className="chart-wrap" style={{ height: 280 }}>
                <Line data={fundamentalsLineData} {...lineChartConfig(['#5B8FF9', '#F6BD16', '#5AD8A6', '#E86452', '#9254DE'])} />
              </div>
            ) : (
              <p className="muted">暂无基本面数据</p>
            )}
          </div>
        </div>
      </section>

      <section className="section">
        <h2>财报数据（季度）</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>报告期</th>
                <th>类型</th>
                <th>营收</th>
                <th>营收同比</th>
                <th>营收环比</th>
                <th>净利润</th>
                <th>净利润同比</th>
                <th>净利润环比</th>
                <th>总资产</th>
              </tr>
            </thead>
            <tbody>
              {financialWithChange.slice(0, 10).map((row, i) => (
                <tr key={i}>
                  <td>{fmtDate(String(row.report_date).replace(/-/g, ''))}</td>
                  <td>{row.report_type || '-'}</td>
                  <td>{fmtAmount(row.revenue)}</td>
                  <td className={pctChangeClass(row.revenue_yoy)}>{fmtPctChange(row.revenue_yoy)}</td>
                  <td className={pctChangeClass(row.revenue_qoq)}>{fmtPctChange(row.revenue_qoq)}</td>
                  <td>{fmtAmount(row.net_profit)}</td>
                  <td className={pctChangeClass(row.net_profit_yoy)}>{fmtPctChange(row.net_profit_yoy)}</td>
                  <td className={pctChangeClass(row.net_profit_qoq)}>{fmtPctChange(row.net_profit_qoq)}</td>
                  <td>{fmtAmount(row.total_assets)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {financial.length === 0 && <p className="muted">暂无财务数据</p>}
        </div>
      </section>

      <section className="section">
        <h2>投资信号</h2>
        {(() => {
          const SIGNAL_COLUMNS = ['成交量资金MA20', '成交量涨跌幅', '持续资金流向', '均线金叉死叉', '主力资金', '支撑阻力位', '新闻舆论', '换手率'];
          const SIGNAL_TOOLTIPS = {
            '日期': '信号对应的交易日',
            '次日涨跌幅': '该交易日下一交易日的涨跌幅（%），用于对比信号与次日实际表现',
            '成交量资金MA20': '结合当日是否放量（量比前5日均量）、资金净流入/流出、股价相对MA20高低位。高位放量净流入→中性，低位放量净流入→看涨，低位放量净流出→看跌',
            '成交量涨跌幅': '当日成交量相对前5日均量是否放量 + 当日涨跌。放量上涨→看涨，缩量下跌→看跌，放量下跌/缩量上涨→中性',
            '持续资金流向': '连续5日资金净流入→看涨，连续5日净流出→看跌，否则中性',
            '均线金叉死叉': 'MA5或MA10上穿MA20为金叉→看涨，下穿为死叉→看跌，否则中性',
            '主力资金': '当日特大单（单笔≥100万）净流入→看涨，净流出→看跌，否则中性',
            '支撑阻力位': '股价触及或接近近期20日低点（支撑）→看涨，触及高点（阻力）→看跌，否则中性',
            '新闻舆论': '基于互联网搜索的企业新闻、市场热点、政策，由AI判断利好/利空得出的看涨/看跌/中性；无有效新闻时为无信号',
            '换手率': '0-3%交投清淡减分，3-10%正常活跃加分，>10%异常活跃小幅加分',
          };
          const byDate = {};
          (signals || []).forEach((s) => {
            const d = s.ref_date ? String(s.ref_date).slice(0, 10) : '';
            if (!d) return;
            if (!byDate[d]) byDate[d] = {};
            byDate[d][s.signal_type] = s.direction ?? '-';
          });
          const dates = Object.keys(byDate).sort((a, b) => b.localeCompare(a));
          if (dates.length === 0) return <p className="muted">暂无信号</p>;
          return (
            <div className="table-wrap">
              <table className="signals-table">
                <thead>
                  <tr>
                    <th>
                      <span
                        className="signal-th-tooltip"
                        data-tooltip={SIGNAL_TOOLTIPS['日期']}
                        onMouseEnter={(e) => {
                          const el = e.currentTarget;
                          const r = el.getBoundingClientRect();
                          setSignalTooltip({ text: el.getAttribute('data-tooltip'), left: r.left + r.width / 2, top: r.top });
                        }}
                        onMouseLeave={() => setSignalTooltip(null)}
                      >日期</span>
                    </th>
                    <th>
                      <span
                        className="signal-th-tooltip"
                        data-tooltip={SIGNAL_TOOLTIPS['次日涨跌幅']}
                        onMouseEnter={(e) => {
                          const el = e.currentTarget;
                          const r = el.getBoundingClientRect();
                          setSignalTooltip({ text: el.getAttribute('data-tooltip'), left: r.left + r.width / 2, top: r.top });
                        }}
                        onMouseLeave={() => setSignalTooltip(null)}
                      >次日涨跌幅</span>
                    </th>
                    {SIGNAL_COLUMNS.map((col) => (
                      <th key={col}>
                        <span
                          className="signal-th-tooltip"
                          data-tooltip={SIGNAL_TOOLTIPS[col]}
                          onMouseEnter={(e) => {
                            const el = e.currentTarget;
                            const r = el.getBoundingClientRect();
                            setSignalTooltip({ text: el.getAttribute('data-tooltip'), left: r.left + r.width / 2, top: r.top });
                          }}
                          onMouseLeave={() => setSignalTooltip(null)}
                        >{col}</span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {dates.map((d) => {
                    const pct = nextDayPctMap[d];
                    const pctCls = pct != null ? (pct >= 0 ? 'pct-up' : 'pct-down') : '';
                    return (
                      <tr key={d}>
                        <td>{d}</td>
                        <td className={pctCls}>
                          {pct != null ? `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%` : '-'}
                        </td>
                        {SIGNAL_COLUMNS.map((col) => {
                          const val = byDate[d][col];
                          const v = val != null && val !== '' ? val : '-';
                          const cls = (v === '看涨' || v === '正常活跃' || v === '异常活跃') ? 'pct-up' : (v === '看跌' || v === '交投清淡') ? 'pct-down' : '';
                          return (
                            <td key={col} className={cls}>
                              {v}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          );
        })()}
      </section>

      {signalTooltip && (
        <div
          className="signal-tooltip-fixed"
          style={{ left: signalTooltip.left, top: signalTooltip.top }}
          role="tooltip"
        >
          {signalTooltip.text}
        </div>
      )}

      {promptModalOpen && (
        <div className="news-opinion-modal-overlay prompt-modal-overlay" onClick={() => !promptSaving && setPromptModalOpen(false)} role="dialog" aria-modal="true" aria-label="修改投资总结提示词">
          <div className="news-opinion-modal prompt-modal" onClick={(e) => e.stopPropagation()}>
            <div className="news-opinion-modal-header">
              <h2>修改投资总结提示词</h2>
              <button type="button" className="news-opinion-modal-close" onClick={() => !promptSaving && setPromptModalOpen(false)}>×</button>
            </div>
            <div className="news-opinion-modal-body">
              <p className="muted" style={{ marginBottom: '0.75rem' }}>该提示词用于生成所有跟踪股票的投资总结，修改后对后续「生成/重新生成」生效。</p>
              {promptLoading ? (
                <p className="muted">加载中…</p>
              ) : (
                <>
                  <textarea
                    className="prompt-modal-textarea"
                    value={promptText}
                    onChange={(e) => setPromptText(e.target.value)}
                    placeholder="输入用于 AI 生成投资总结的系统提示词…"
                    rows={14}
                    spellCheck={false}
                  />
                  <div className="prompt-modal-actions">
                    <button type="button" onClick={() => !promptSaving && setPromptModalOpen(false)} disabled={promptSaving}>取消</button>
                    <button type="button" onClick={savePrompt} disabled={promptSaving}>{promptSaving ? '保存中…' : '保存'}</button>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {newsModalOpen && (
        <div className="news-opinion-modal-overlay" onClick={() => setNewsModalOpen(false)} role="dialog" aria-modal="true" aria-label="新闻舆论拉取记录">
          <div className="news-opinion-modal" onClick={(e) => e.stopPropagation()}>
            <div className="news-opinion-modal-header">
              <h2>新闻舆论拉取记录</h2>
              <button type="button" className="news-opinion-modal-close" onClick={() => setNewsModalOpen(false)}>×</button>
            </div>
            <div className="news-opinion-modal-body">
              {newsRecordsLoading ? (
                <p className="muted">加载中…</p>
              ) : newsRecords.length === 0 ? (
                <p className="muted">暂无记录。请先点击「收集情报」拉取并分析新闻。</p>
              ) : (
                <div className="table-wrap">
                  <table className="news-opinion-table">
                    <thead>
                      <tr>
                        <th>拉取日期</th>
                        <th>信号日期</th>
                        <th>信号结果</th>
                        <th>理由</th>
                        <th>新闻摘要</th>
                      </tr>
                    </thead>
                    <tbody>
                      {newsRecords.map((r) => (
                        <tr key={r.id}>
                          <td>{r.fetch_date || '-'}</td>
                          <td>{r.ref_date || '-'}</td>
                          <td className={r.direction === '看涨' ? 'pct-up' : r.direction === '看跌' ? 'pct-down' : ''}>{r.direction || '-'}</td>
                          <td className="news-opinion-reason">{(r.reason || '-').slice(0, 80)}{(r.reason && r.reason.length > 80) ? '…' : ''}</td>
                          <td className="news-opinion-content">{(r.news_content || '-').slice(0, 200)}{(r.news_content && r.news_content.length > 200) ? '…' : ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      <p className="muted"><Link to="/stock">← 返回股票列表</Link></p>
    </div>
  );
}
