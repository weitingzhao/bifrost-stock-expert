import { Router } from 'express';
import { db } from '../db.js';

export const selectionRouter = Router();

// 选股：支持市场、股票代码、行业、市值区间、市盈率区间等
selectionRouter.get('/filter', async (req, res) => {
  const {
    code,
    market,
    industry,
    market_cap_min,
    market_cap_max,
    pe_min,
    pe_max,
    limit = 6000,
  } = req.query;
  let sql = `
    SELECT c.code, c.name, c.market, c.industry, c.sector,
           COALESCE(c.market_cap, f.market_cap) AS market_cap,
           COALESCE(c.pe, f.pe) AS pe,
           c.pb,
           f.net_profit, f.profit_growth,
           d.latest_close
    FROM stex.corp c
    LEFT JOIN LATERAL (
      SELECT market_cap, pe, net_profit, profit_growth
      FROM stex.fundamentals
      WHERE code = c.code
      ORDER BY report_date DESC
      LIMIT 1
    ) f ON true
    LEFT JOIN LATERAL (
      SELECT close AS latest_close
      FROM stex.stock_day
      WHERE code = c.code
      ORDER BY trade_date DESC
      LIMIT 1
    ) d ON true
    WHERE 1=1
  `;
  const params = [];
  let i = 1;
  const capExpr = 'COALESCE(c.market_cap, f.market_cap)';
  const peExpr = 'COALESCE(c.pe, f.pe)';
  if (code && String(code).trim()) {
    const codeVal = String(code).trim();
    const pattern = codeVal.includes('%') ? codeVal : `%${codeVal}%`;
    params.push(pattern);
    sql += ` AND (c.code LIKE $${i} OR c.name LIKE $${i})`;
    i += 1;
  }
  if (market) {
    params.push(String(market));
    sql += ` AND c.market = $${i++}`;
  }
  if (industry) {
    params.push(String(industry));
    sql += ` AND c.industry = $${i++}`;
  }
  if (market_cap_min != null && market_cap_min !== '') {
    params.push(Number(market_cap_min) * 1e8);
    sql += ` AND ${capExpr} >= $${i++}`;
  }
  if (market_cap_max != null && market_cap_max !== '') {
    params.push(Number(market_cap_max) * 1e8);
    sql += ` AND ${capExpr} <= $${i++}`;
  }
  if (pe_min != null && pe_min !== '') {
    params.push(Number(pe_min));
    sql += ` AND ${peExpr} >= $${i++}`;
  }
  if (pe_max != null && pe_max !== '') {
    params.push(Number(pe_max));
    sql += ` AND ${peExpr} <= $${i++}`;
  }
  params.push(Math.min(Number(limit) || 6000, 10000));
  sql += ` ORDER BY c.code LIMIT $${i}`;
  const { rows } = await db.query(sql, params);
  res.json(rows);
});

// 市场/行业/板块列表（用于选股下拉，来自 stex.corp）
selectionRouter.get('/industries', async (req, res) => {
  const { rows: ind } = await db.query(`
    SELECT DISTINCT industry AS name FROM stex.corp WHERE industry IS NOT NULL AND industry <> '' ORDER BY name
  `);
  const { rows: sec } = await db.query(`
    SELECT DISTINCT sector AS name FROM stex.corp WHERE sector IS NOT NULL AND sector <> '' ORDER BY name
  `);
  const { rows: mkt } = await db.query(`
    SELECT DISTINCT market AS name FROM stex.corp WHERE market IS NOT NULL AND market <> '' ORDER BY name
  `);
  res.json({
    industries: ind.map(r => r.name),
    sectors: sec.map(r => r.name),
    markets: mkt.map(r => r.name),
  });
});

// 收藏/跟踪列表
selectionRouter.get('/watchlist', async (req, res) => {
  const { rows } = await db.query(`
    SELECT w.code, w.track, c.name, c.market, c.industry, c.sector
    FROM stex.watchlist w
    LEFT JOIN stex.corp c ON c.code = w.code
    ORDER BY w.updated_at DESC
  `);
  res.json(rows);
});

// 收藏/跟踪列表汇总：最新价、日/周/月涨跌幅（基于 stex.stock_day）
selectionRouter.get('/watchlist-summary', async (req, res) => {
  const { rows: watchlist } = await db.query(`
    SELECT w.code, c.name, c.market, c.pe
    FROM stex.watchlist w
    LEFT JOIN stex.corp c ON c.code = w.code
    ORDER BY w.updated_at DESC
  `);
  const summary = [];
  for (const w of watchlist) {
    // 市盈率：优先 corp.pe，没有则取 fundamentals 最新 pe
    let pe = w.pe != null ? Number(w.pe) : null;
    if (pe == null) {
      const { rows: peRows } = await db.query(
        `SELECT pe FROM stex.fundamentals WHERE code = $1 AND pe IS NOT NULL ORDER BY report_date DESC LIMIT 1`,
        [w.code]
      );
      pe = peRows[0]?.pe != null ? Number(peRows[0].pe) : null;
    }

    const { rows: days } = await db.query(
      `SELECT trade_date, close FROM stex.stock_day WHERE code = $1 ORDER BY trade_date DESC LIMIT 65`,
      [w.code]
    );
    const latest = days[0];
    if (!latest || latest.close == null) {
      summary.push({ code: w.code, name: w.name, market: w.market, pe, latest_date: null, close: null, daily_pct: null, weekly_pct: null, monthly_pct: null, monthly_3_pct: null });
      continue;
    }
    const c0 = Number(latest.close);
    const prev1 = days[1] ? Number(days[1].close) : null;
    const prev5 = days[5] ? Number(days[5].close) : null;
    const prev20 = days[20] ? Number(days[20].close) : null;
    const prev63 = days[63] ? Number(days[63].close) : null; // ~3 个月（约 63 个交易日）
    const pct = (cur, prev) => (prev != null && prev !== 0 ? ((cur - prev) / prev * 100) : null);
    summary.push({
      code: w.code,
      name: w.name,
      market: w.market,
      pe,
      latest_date: latest.trade_date,
      close: c0,
      daily_pct: pct(c0, prev1),
      weekly_pct: pct(c0, prev5),
      monthly_pct: pct(c0, prev20),
      monthly_3_pct: pct(c0, prev63),
    });
  }
  res.json(summary);
});

// 大盘指数代码（用于同期大盘分数计算，与 index-signals 一致）
const INDEX_CODES_FOR_SCORING = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000905.SH'];
// 参与综合分数计算的信号列（与前端 SIGNAL_COLUMNS 一致）
const SIGNAL_COLS_FOR_SCORE = [
  '成交量资金MA20', '成交量涨跌幅', '持续资金流向', '均线金叉死叉', '主力资金', '支撑阻力位', '新闻舆论', '换手率',
];

// 综合分数权重默认值（与 /config/score-weights 一致）
const DEFAULT_SCORE_WEIGHTS = {
  marketBase: 1,
  marketBullPerIndex: 0.1,
  marketBearPerIndex: -0.1,
  stockBull: 1,
  stockBear: -2,
  stockNeutralUp: 0.2,
  stockNeutralDown: -0.2,
  signalWeights: {
    '成交量资金MA20': { bull: 1, bear: -2 },
    '成交量涨跌幅': { bull: 1, bear: -2 },
    '持续资金流向': { bull: 1, bear: -2 },
    '均线金叉死叉': { bull: 1, bear: -2 },
    '主力资金': { bull: 1, bear: -2 },
    '支撑阻力位': { bull: 1, bear: -2 },
    '新闻舆论': { bull: 1, bear: -2 },
  },
  turnover: {
    low: -0.5,
    normal: 0.3,
    high: 0.1,
  },
};

async function loadScoreWeights() {
  const { rows } = await db.query(
    'SELECT value FROM stex.app_config WHERE key = $1',
    ['score_weights'],
  );
  const raw = rows[0]?.value;
  let saved = {};
  if (raw) {
    try {
      saved = JSON.parse(raw);
    } catch {
      saved = {};
    }
  }
  return {
    ...DEFAULT_SCORE_WEIGHTS,
    ...saved,
    turnover: {
      ...DEFAULT_SCORE_WEIGHTS.turnover,
      ...(saved.turnover || {}),
    },
    signalWeights: {
      ...DEFAULT_SCORE_WEIGHTS.signalWeights,
      ...(saved.signalWeights || {}),
    },
  };
}

// 收藏列表每只股票的投资信号（用于列表页「投资信号」Tab）。可选 ref_date 筛选指定日期的信号并对比次日股价
selectionRouter.get('/watchlist-signals', async (req, res) => {
  const refDateParam = req.query.ref_date ? String(req.query.ref_date).trim().slice(0, 10) : null;
  const { rows: watchlist } = await db.query(`
    SELECT w.code, c.name
    FROM stex.watchlist w
    LEFT JOIN stex.corp c ON c.code = w.code
    ORDER BY w.updated_at DESC
  `);
  const { rows: availableDates } = await db.query(
    `SELECT DISTINCT s.ref_date
     FROM stex.signals s
     INNER JOIN stex.watchlist w ON w.code = s.code
     WHERE s.ref_date IS NOT NULL
     ORDER BY s.ref_date DESC
     LIMIT 60`,
    []
  );
  const available_dates = (availableDates || []).map((r) => String(r.ref_date).slice(0, 10));

  if (watchlist.length === 0) {
    return res.json({ rows: [], available_dates });
  }
  const { rows: signals } = await db.query(
    `SELECT s.code, s.ref_date, s.signal_type, s.direction
     FROM stex.signals s
     INNER JOIN stex.watchlist w ON w.code = s.code
     WHERE s.ref_date IS NOT NULL
     ORDER BY s.code, s.ref_date DESC`,
    []
  );
  const byCode = {};
  for (const w of watchlist) {
    byCode[w.code] = { code: w.code, name: w.name || '-', ref_date: null, signals: {} };
  }
  for (const s of signals) {
    const code = s.code;
    const refDate = s.ref_date ? String(s.ref_date).slice(0, 10) : null;
    if (!byCode[code]) continue;
    const rec = byCode[code];
    if (refDateParam) {
      if (refDate !== refDateParam) continue;
      rec.ref_date = refDateParam;
      rec.signals[s.signal_type] = s.direction || '-';
    } else {
      if (rec.ref_date == null) rec.ref_date = refDate;
      if (refDate === rec.ref_date) rec.signals[s.signal_type] = s.direction || '-';
    }
  }
  if (refDateParam) {
    for (const w of watchlist) {
      if (byCode[w.code].ref_date === null) byCode[w.code].ref_date = refDateParam;
    }
  }
  // 批量查「信号日期」的下一交易日收盘价（参考详情页次日涨跌幅逻辑）
  const codesWithRef = watchlist.filter((w) => byCode[w.code].ref_date).map((w) => w.code);
  const refDates = codesWithRef.map((c) => byCode[c].ref_date);
  if (codesWithRef.length > 0) {
    const { rows: nextDayRows } = await db.query(
      `WITH list AS (
        SELECT code, ref_date FROM unnest($1::text[], $2::date[]) AS t(code, ref_date)
      ),
      signal_close AS (
        SELECT sd.code, sd.close AS c FROM stex.stock_day sd
        INNER JOIN list ON list.code = sd.code AND sd.trade_date = list.ref_date
      ),
      next_close AS (
        SELECT list.code,
          (SELECT sd.close FROM stex.stock_day sd
           WHERE sd.code = list.code AND sd.trade_date > list.ref_date
           ORDER BY sd.trade_date ASC LIMIT 1) AS c
        FROM list
      )
      SELECT list.code,
        CASE WHEN sc.c IS NOT NULL AND sc.c <> 0 AND nc.c IS NOT NULL
          THEN ((nc.c - sc.c) / sc.c) * 100 ELSE NULL END AS next_day_pct
      FROM list
      LEFT JOIN signal_close sc ON sc.code = list.code
      LEFT JOIN next_close nc ON nc.code = list.code`,
      [codesWithRef, refDates]
    );
    for (const r of nextDayRows) {
      if (byCode[r.code]) byCode[r.code].next_day_pct = r.next_day_pct != null ? Number(r.next_day_pct) : null;
    }
  }
  let out = watchlist.map((w) => {
    const rec = byCode[w.code];
    return {
      code: rec.code,
      name: rec.name,
      ref_date: rec.ref_date,
      next_day_pct: rec.next_day_pct ?? null,
      ...rec.signals,
    };
  });

  // 综合分数：大盘分数（可配置：base + 看涨*权重/看跌*权重）× 个股分数（每个信号看涨/看跌/中性权重可配置，换手率单独权重），按综合分从高到低排序
  const weights = await loadScoreWeights();
  const marketBase = Number.isFinite(Number(weights.marketBase)) ? Number(weights.marketBase) : 1;
  const marketBullPerIndex = Number.isFinite(Number(weights.marketBullPerIndex))
    ? Number(weights.marketBullPerIndex)
    : 0.1;
  const marketBearPerIndex = Number.isFinite(Number(weights.marketBearPerIndex))
    ? Number(weights.marketBearPerIndex)
    : -0.1;

  const refDatesDistinct = [...new Set(out.map((r) => r.ref_date).filter(Boolean))];
  const marketScoreByDate = {};
  if (refDatesDistinct.length > 0) {
    const { rows: indexSignals } = await db.query(
      `SELECT ref_date, direction FROM stex.signals
       WHERE code = ANY($1::text[]) AND ref_date = ANY($2::date[])`,
      [INDEX_CODES_FOR_SCORING, refDatesDistinct]
    );
    for (const d of refDatesDistinct) {
      let bull = 0;
      let bear = 0;
      for (const s of indexSignals || []) {
        if (String(s.ref_date).slice(0, 10) !== d) continue;
        if (s.direction === '看涨') bull += 1;
        else if (s.direction === '看跌') bear += 1;
      }
      marketScoreByDate[d] = marketBase + marketBullPerIndex * bull + marketBearPerIndex * bear;
    }
  }

  const stockBull = Number.isFinite(Number(weights.stockBull)) ? Number(weights.stockBull) : 1;
  const stockBear = Number.isFinite(Number(weights.stockBear)) ? Number(weights.stockBear) : -2;
  const stockNeutralUp = Number.isFinite(Number(weights.stockNeutralUp))
    ? Number(weights.stockNeutralUp)
    : 0.2;
  const stockNeutralDown = Number.isFinite(Number(weights.stockNeutralDown))
    ? Number(weights.stockNeutralDown)
    : -0.2;
  const turnoverWeights = weights.turnover || {};
  const turnoverLow = Number.isFinite(Number(turnoverWeights.low)) ? Number(turnoverWeights.low) : -0.5;
  const turnoverNormal = Number.isFinite(Number(turnoverWeights.normal)) ? Number(turnoverWeights.normal) : 0.3;
  const turnoverHigh = Number.isFinite(Number(turnoverWeights.high)) ? Number(turnoverWeights.high) : 0.1;
  const perSignalWeights = weights.signalWeights || {};
  for (const row of out) {
    const marketScore = row.ref_date ? (marketScoreByDate[row.ref_date] ?? 1) : 1;
    let stockRaw = 0;
    for (const col of SIGNAL_COLS_FOR_SCORE) {
      const v = row[col];
      if (col === '换手率') {
        if (v === '交投清淡') stockRaw += turnoverLow;
        else if (v === '正常活跃') stockRaw += turnoverNormal;
        else if (v === '异常活跃') stockRaw += turnoverHigh;
        continue;
      }
      const sw = perSignalWeights[col] || {};
      const bullW = Number.isFinite(Number(sw.bull)) ? Number(sw.bull) : stockBull;
      const bearW = Number.isFinite(Number(sw.bear)) ? Number(sw.bear) : stockBear;
      if (v === '看涨') stockRaw += bullW;
      else if (v === '看跌') stockRaw += bearW;
      else if (v === '中性') stockRaw += marketScore > 1 ? stockNeutralUp : marketScore < 1 ? stockNeutralDown : 0;
    }
    const composite = marketScore * stockRaw;
    row.composite_score = Number.isFinite(composite) ? Math.round(composite * 100) / 100 : null;
  }
  out.sort((a, b) => (b.composite_score ?? -Infinity) - (a.composite_score ?? -Infinity));

  res.json({ rows: out, available_dates });
});

// 大盘指数列表（与 indices.js / index_data_agent 一致）
const INDEX_SIGNAL_CODES = ['000001.SH', '399001.SZ', '399006.SZ', '000300.SH', '000905.SH'];
const INDEX_SIGNAL_NAMES = {
  '000001.SH': '上证指数',
  '399001.SZ': '深证成指',
  '399006.SZ': '创业板指',
  '000300.SH': '沪深300',
  '000905.SH': '中证500',
};

/** 大盘指数投资信号（与个股投资信号表格结构一致：信号日期、次日涨跌幅、各信号列），可选 ref_date 筛选 */
selectionRouter.get('/index-signals', async (req, res) => {
  const refDateParam = req.query.ref_date ? String(req.query.ref_date).trim().slice(0, 10) : null;
  const { rows: availableDates } = await db.query(
    `SELECT DISTINCT ref_date FROM stex.signals WHERE code = ANY($1::text[]) AND ref_date IS NOT NULL ORDER BY ref_date DESC LIMIT 60`,
    [INDEX_SIGNAL_CODES]
  );
  const available_dates = (availableDates || []).map((r) => String(r.ref_date).slice(0, 10));

  const { rows: signals } = await db.query(
    `SELECT code, ref_date, signal_type, direction FROM stex.signals
     WHERE code = ANY($1::text[]) AND ref_date IS NOT NULL ORDER BY code, ref_date DESC`,
    [INDEX_SIGNAL_CODES]
  );
  const byCode = {};
  for (const code of INDEX_SIGNAL_CODES) {
    byCode[code] = { code, name: INDEX_SIGNAL_NAMES[code] || code, ref_date: null, signals: {} };
  }
  for (const s of signals) {
    const refDate = s.ref_date ? String(s.ref_date).slice(0, 10) : null;
    if (!byCode[s.code]) continue;
    const rec = byCode[s.code];
    if (refDateParam) {
      if (refDate !== refDateParam) continue;
      rec.ref_date = refDateParam;
      rec.signals[s.signal_type] = s.direction || '-';
    } else {
      if (rec.ref_date == null) rec.ref_date = refDate;
      if (refDate === rec.ref_date) rec.signals[s.signal_type] = s.direction || '-';
    }
  }
  if (refDateParam) {
    for (const code of INDEX_SIGNAL_CODES) {
      if (byCode[code].ref_date === null) byCode[code].ref_date = refDateParam;
    }
  }
  const codesWithRef = INDEX_SIGNAL_CODES.filter((c) => byCode[c].ref_date);
  const refDates = codesWithRef.map((c) => byCode[c].ref_date);
  if (codesWithRef.length > 0) {
    const { rows: nextDayRows } = await db.query(
      `WITH list AS (
        SELECT code, ref_date FROM unnest($1::text[], $2::date[]) AS t(code, ref_date)
      ),
      signal_close AS (
        SELECT d.index_code AS code, d.close AS c FROM stex.index_day d
        INNER JOIN list ON list.code = d.index_code AND d.trade_date = list.ref_date
      ),
      next_close AS (
        SELECT list.code,
          (SELECT d2.close FROM stex.index_day d2
           WHERE d2.index_code = list.code AND d2.trade_date > list.ref_date
           ORDER BY d2.trade_date ASC LIMIT 1) AS c
        FROM list
      )
      SELECT list.code,
        CASE WHEN sc.c IS NOT NULL AND sc.c <> 0 AND nc.c IS NOT NULL
          THEN ((nc.c - sc.c) / sc.c) * 100 ELSE NULL END AS next_day_pct
      FROM list
      LEFT JOIN signal_close sc ON sc.code = list.code
      LEFT JOIN next_close nc ON nc.code = list.code`,
      [codesWithRef, refDates]
    );
    for (const r of nextDayRows) {
      if (byCode[r.code]) byCode[r.code].next_day_pct = r.next_day_pct != null ? Number(r.next_day_pct) : null;
    }
  }
  const out = INDEX_SIGNAL_CODES.map((code) => {
    const rec = byCode[code];
    return {
      code: rec.code,
      name: rec.name,
      ref_date: rec.ref_date,
      next_day_pct: rec.next_day_pct ?? null,
      ...rec.signals,
    };
  });
  res.json({ rows: out, available_dates });
});

// 企业列表（按代码或名称模糊）
selectionRouter.get('/corps', async (req, res) => {
  const { q, limit = 30 } = req.query;
  if (!q || String(q).length < 1) {
    return res.json([]);
  }
  const pattern = `%${String(q).trim()}%`;
  const { rows } = await db.query(
    `SELECT code, name, market, industry, sector FROM stex.corp
     WHERE code LIKE $1 OR name LIKE $1
     ORDER BY code LIMIT $2`,
    [pattern, Math.min(Number(limit), 100)]
  );
  res.json(rows);
});

// 策略选股：返回符合策略的股票列表（与 filter 同结构便于共用表格）
const STRATEGY_NAMES = {
  growth: '企业增长策略',
  tech_competition: '中美科技竞争战略',
  classic_pattern: '经典形态策略',
  trend_following: '趋势跟踪策略',
  low_vol_breakout: '低量横盘放量突破',
  all_combined: '全策略综合',
};

const VALID_STRATEGY_KEYS = Object.keys(STRATEGY_NAMES);

function getStrategyCodeSubquery(strategyKey) {
  let codeSubquery = '';
  if (strategyKey === 'growth') {
    // 营收和利润：连续3季度环比增长 r1>r2>r3、p1>p2>p3；且最近一季同比去年同季增长 r1>r4、p1>p4（r4/p4 为第4期即去年同期）
    codeSubquery = `
      WITH last4 AS (
        SELECT code, report_date, revenue, net_profit,
               ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
        FROM stex.financial
        WHERE revenue IS NOT NULL AND net_profit IS NOT NULL
      ),
      last4_limited AS (
        SELECT * FROM last4 WHERE rn <= 4
      )
      SELECT code FROM (
        SELECT
          code,
          MAX(CASE WHEN rn = 1 THEN revenue END) AS r1,
          MAX(CASE WHEN rn = 2 THEN revenue END) AS r2,
          MAX(CASE WHEN rn = 3 THEN revenue END) AS r3,
          MAX(CASE WHEN rn = 4 THEN revenue END) AS r4,
          MAX(CASE WHEN rn = 1 THEN net_profit END) AS p1,
          MAX(CASE WHEN rn = 2 THEN net_profit END) AS p2,
          MAX(CASE WHEN rn = 3 THEN net_profit END) AS p3,
          MAX(CASE WHEN rn = 4 THEN net_profit END) AS p4,
          COUNT(*) AS cnt
        FROM last4_limited
        GROUP BY code
      ) g
      WHERE cnt = 4
        AND r1 > r2 AND r2 > r3
        AND p1 > p2 AND p2 > p3
        AND r1 > r4 AND p1 > p4
    `;
  } else if (strategyKey === 'tech_competition') {
    // 中美科技竞争战略：企业解析中提及 AI、芯片、太空/航天、新能源、机器人、卡脖子 等
    codeSubquery = `
      SELECT code FROM stex.corp_analysis
      WHERE competitiveness_analysis IS NOT NULL AND competitiveness_analysis <> ''
        AND (
          competitiveness_analysis ILIKE '%AI%'
          OR competitiveness_analysis ILIKE '%芯片%'
          OR competitiveness_analysis ILIKE '%太空%'
          OR competitiveness_analysis ILIKE '%航天%'
          OR competitiveness_analysis ILIKE '%新能源%'
          OR competitiveness_analysis ILIKE '%机器人%'
          OR competitiveness_analysis ILIKE '%卡脖子%'
          OR competitiveness_analysis ILIKE '%半导体%'
          OR competitiveness_analysis ILIKE '%人工智能%'
        )
    `;
  } else if (strategyKey === 'classic_pattern') {
    // 经典形态：杯柄、上升三法（从 pattern_signal 表取最近有信号的股票）
    codeSubquery = `
      SELECT DISTINCT code FROM stex.pattern_signal
      WHERE pattern_type IN ('cup_handle', 'rising_three')
        AND ref_date >= CURRENT_DATE - INTERVAL '60 days'
    `;
  } else if (strategyKey === 'trend_following') {
    // 趋势跟踪：均线多头排列(MA5>MA10>MA20) 且 当日收盘价在 MA20 上方（同一交易日）
    codeSubquery = `
      WITH latest_dates AS (
        SELECT code, MAX(trade_date) AS trade_date
        FROM stex.technicals
        WHERE ma5 IS NOT NULL AND ma10 IS NOT NULL AND ma20 IS NOT NULL
        GROUP BY code
      ),
      tech_day AS (
        SELECT t.code, t.ma5, t.ma10, t.ma20, d.close
        FROM stex.technicals t
        JOIN latest_dates ld ON ld.code = t.code AND ld.trade_date = t.trade_date
        JOIN stex.stock_day d ON d.code = t.code AND d.trade_date = t.trade_date
      )
      SELECT code FROM tech_day
      WHERE ma5 > ma10 AND ma10 > ma20 AND close >= ma20
    `;
  } else if (strategyKey === 'all_combined') {
    // 全策略综合：前 4 种策略中至少满足 3 种的股票
    codeSubquery = `
      WITH growth_codes AS (
        WITH last4 AS (
          SELECT code, report_date, revenue, net_profit,
                 ROW_NUMBER() OVER (PARTITION BY code ORDER BY report_date DESC) AS rn
          FROM stex.financial
          WHERE revenue IS NOT NULL AND net_profit IS NOT NULL
        ),
        last4_limited AS (SELECT * FROM last4 WHERE rn <= 4)
        SELECT code FROM (
          SELECT code,
            MAX(CASE WHEN rn = 1 THEN revenue END) AS r1,
            MAX(CASE WHEN rn = 2 THEN revenue END) AS r2,
            MAX(CASE WHEN rn = 3 THEN revenue END) AS r3,
            MAX(CASE WHEN rn = 4 THEN revenue END) AS r4,
            MAX(CASE WHEN rn = 1 THEN net_profit END) AS p1,
            MAX(CASE WHEN rn = 2 THEN net_profit END) AS p2,
            MAX(CASE WHEN rn = 3 THEN net_profit END) AS p3,
            MAX(CASE WHEN rn = 4 THEN net_profit END) AS p4,
            COUNT(*) AS cnt
          FROM last4_limited GROUP BY code
        ) g
        WHERE cnt = 4 AND r1 > r2 AND r2 > r3 AND p1 > p2 AND p2 > p3 AND r1 > r4 AND p1 > p4
      ),
      tech_codes AS (
        SELECT code FROM stex.corp_analysis
        WHERE competitiveness_analysis IS NOT NULL AND competitiveness_analysis <> ''
          AND (competitiveness_analysis ILIKE '%AI%' OR competitiveness_analysis ILIKE '%芯片%'
            OR competitiveness_analysis ILIKE '%太空%' OR competitiveness_analysis ILIKE '%航天%'
            OR competitiveness_analysis ILIKE '%新能源%' OR competitiveness_analysis ILIKE '%机器人%'
            OR competitiveness_analysis ILIKE '%卡脖子%' OR competitiveness_analysis ILIKE '%半导体%'
            OR competitiveness_analysis ILIKE '%人工智能%')
      ),
      pattern_codes AS (
        SELECT DISTINCT code FROM stex.pattern_signal
        WHERE pattern_type IN ('cup_handle', 'rising_three')
          AND ref_date >= CURRENT_DATE - INTERVAL '60 days'
      ),
      trend_codes AS (
        WITH latest_dates AS (
          SELECT code, MAX(trade_date) AS trade_date
          FROM stex.technicals
          WHERE ma5 IS NOT NULL AND ma10 IS NOT NULL AND ma20 IS NOT NULL
          GROUP BY code
        ),
        tech_day AS (
          SELECT t.code, t.ma5, t.ma10, t.ma20, d.close
          FROM stex.technicals t
          JOIN latest_dates ld ON ld.code = t.code AND ld.trade_date = t.trade_date
          JOIN stex.stock_day d ON d.code = t.code AND d.trade_date = t.trade_date
        )
        SELECT code FROM tech_day
        WHERE ma5 > ma10 AND ma10 > ma20 AND close >= ma20
      ),
      combined AS (
        SELECT code, 'g' AS sid FROM growth_codes
        UNION ALL SELECT code, 't' FROM tech_codes
        UNION ALL SELECT code, 'p' FROM pattern_codes
        UNION ALL SELECT code, 'f' FROM trend_codes
      )
      SELECT code FROM combined
      GROUP BY code
      HAVING COUNT(DISTINCT sid) >= 3
    `;
  } else if (strategyKey === 'low_vol_breakout') {
    // 低量横盘放量突破：前 7～10 日低量横盘（量<0.8*20日均量、振幅<5%），当日放量≥30% 且收涨
    codeSubquery = `
      WITH base AS (
        SELECT code, trade_date, close, volume,
          LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS prev_close,
          AVG(volume) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS vol_ma20,
          AVG(volume) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 8 PRECEDING AND 2 PRECEDING) AS cons_vol_avg,
          MAX(close) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 8 PRECEDING AND 2 PRECEDING) AS cons_max,
          MIN(close) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 8 PRECEDING AND 2 PRECEDING) AS cons_min
        FROM stex.stock_day
      ),
      with_range AS (
        SELECT code, trade_date,
          cons_vol_avg,
          (cons_max - cons_min) / NULLIF((cons_max + cons_min) / 2, 0) * 100 AS range_pct,
          volume, vol_ma20, prev_close, close
        FROM base
        WHERE vol_ma20 IS NOT NULL AND vol_ma20 > 0 AND prev_close IS NOT NULL
      ),
      breakout AS (
        SELECT code, trade_date
        FROM with_range
        WHERE cons_vol_avg < 0.8 * vol_ma20
          AND range_pct < 5
          AND volume >= 1.3 * vol_ma20
          AND close > prev_close
          AND trade_date >= CURRENT_DATE - INTERVAL '60 days'
      ),
      latest_breakout AS (
        SELECT code, MAX(trade_date) AS trade_date FROM breakout GROUP BY code
      )
      SELECT code FROM latest_breakout
    `;
  }
  return codeSubquery;
}

async function runStrategyAndFetchList(codeSubquery, limitNum) {
  const sql = `
    SELECT c.code, c.name, c.market, c.industry, c.sector,
           COALESCE(c.market_cap, f.market_cap) AS market_cap,
           COALESCE(c.pe, f.pe) AS pe,
           c.pb,
           f.net_profit, f.profit_growth,
           d.latest_close
    FROM stex.corp c
    INNER JOIN (${codeSubquery}) s ON s.code = c.code
    LEFT JOIN LATERAL (
      SELECT market_cap, pe, net_profit, profit_growth
      FROM stex.fundamentals
      WHERE code = c.code
      ORDER BY report_date DESC
      LIMIT 1
    ) f ON true
    LEFT JOIN LATERAL (
      SELECT close AS latest_close
      FROM stex.stock_day
      WHERE code = c.code
      ORDER BY trade_date DESC
      LIMIT 1
    ) d ON true
    ORDER BY c.code
    LIMIT $1
  `;
  const { rows } = await db.query(sql, [limitNum]);
  return rows;
}

selectionRouter.get('/strategy', async (req, res) => {
  const { strategy, limit = 10000 } = req.query;
  const strategyKey = String(strategy || '').toLowerCase();
  const limitNum = Math.min(Number(limit) || 10000, 10000);

  if (!STRATEGY_NAMES[strategyKey]) {
    return res.status(400).json({
      error: `strategy 需为: ${VALID_STRATEGY_KEYS.filter(k => k !== 'all_combined').join(' | ')} | all_combined`,
    });
  }

  const codeSubquery = getStrategyCodeSubquery(strategyKey);
  if (!codeSubquery) {
    return res.status(400).json({ error: '不支持的策略' });
  }

  const list = await runStrategyAndFetchList(codeSubquery, limitNum);
  res.json({
    strategy: strategyKey,
    strategyName: STRATEGY_NAMES[strategyKey],
    list,
  });
});

// 多策略选股：strategies 数组，combine 为 and（交集）或 or（并集）
selectionRouter.get('/strategies', async (req, res) => {
  const { strategies: strategiesParam, combine = 'or', limit = 10000 } = req.query;
  const limitNum = Math.min(Number(limit) || 10000, 10000);
  const combineMode = String(combine || 'or').toLowerCase() === 'and' ? 'and' : 'or';

  const strategies = (typeof strategiesParam === 'string' ? strategiesParam.split(',') : [])
    .map(s => String(s).trim().toLowerCase())
    .filter(s => s && VALID_STRATEGY_KEYS.includes(s) && s !== 'all_combined');

  if (strategies.length === 0) {
    return res.status(400).json({ error: '请至少选择一个策略，如 strategies=growth,low_vol_breakout' });
  }

  const codeSets = [];
  const strategyNames = [];
  for (const key of strategies) {
    const codeSubquery = getStrategyCodeSubquery(key);
    if (!codeSubquery) continue;
    const { rows } = await db.query(`SELECT code FROM (${codeSubquery}) x`, []);
    codeSets.push({ key, name: STRATEGY_NAMES[key], codes: new Set(rows.map(r => r.code)) });
    strategyNames.push(STRATEGY_NAMES[key]);
  }

  if (codeSets.length === 0) {
    return res.json({ strategies: strategyNames, combine: combineMode, list: [] });
  }

  let combinedCodes;
  if (combineMode === 'and') {
    combinedCodes = codeSets[0].codes;
    for (let i = 1; i < codeSets.length; i++) {
      combinedCodes = new Set([...combinedCodes].filter(c => codeSets[i].codes.has(c)));
    }
  } else {
    combinedCodes = new Set();
    for (const { codes } of codeSets) {
      codes.forEach(c => combinedCodes.add(c));
    }
  }

  const codes = [...combinedCodes];
  if (codes.length === 0) {
    return res.json({ strategies: strategyNames, combine: combineMode, list: [] });
  }

  const placeholders = codes.map((_, i) => `$${i + 1}`).join(',');
  const sql = `
    SELECT c.code, c.name, c.market, c.industry, c.sector,
           COALESCE(c.market_cap, f.market_cap) AS market_cap,
           COALESCE(c.pe, f.pe) AS pe,
           c.pb,
           f.net_profit, f.profit_growth,
           d.latest_close
    FROM stex.corp c
    INNER JOIN (SELECT unnest($1::text[]) AS code) s ON s.code = c.code
    LEFT JOIN LATERAL (
      SELECT market_cap, pe, net_profit, profit_growth
      FROM stex.fundamentals
      WHERE code = c.code
      ORDER BY report_date DESC
      LIMIT 1
    ) f ON true
    LEFT JOIN LATERAL (
      SELECT close AS latest_close
      FROM stex.stock_day
      WHERE code = c.code
      ORDER BY trade_date DESC
      LIMIT 1
    ) d ON true
    ORDER BY c.code
    LIMIT $2
  `;
  const { rows } = await db.query(sql, [codes, limitNum]);
  res.json({
    strategies: strategyNames,
    combine: combineMode,
    list: rows,
  });
});
