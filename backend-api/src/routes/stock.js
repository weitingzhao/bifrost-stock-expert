import { Router } from 'express';
import { db } from '../db.js';

export const stockRouter = Router();

// 指数代码 -> 展示名（与 backend-services index_data_agent 一致）
const INDEX_NAMES = {
  '000001.SH': '上证指数',
  '399001.SZ': '深证成指',
  '399006.SZ': '创业板指',
  '000300.SH': '沪深300',
  '000905.SH': '中证500',
};

// 获取单只股票概览（基本信息 + 最新日线 + 信号 + 企业解析 + 同日大盘快照 + 投资总结）
stockRouter.get('/:code', async (req, res) => {
  const { code } = req.params;
  const [corp, day, signals, watch, corpAnalysis, investmentSummary] = await Promise.all([
    db.query('SELECT * FROM stex.corp WHERE code = $1', [code]),
    db.query(
      'SELECT * FROM stex.stock_day WHERE code = $1 ORDER BY trade_date DESC LIMIT 1',
      [code]
    ),
    db.query(
      'SELECT * FROM stex.signals WHERE code = $1 ORDER BY ref_date DESC NULLS LAST, created_at DESC LIMIT 30',
      [code]
    ),
    db.query('SELECT * FROM stex.watchlist WHERE code = $1', [code]),
    db.query(
      'SELECT code, business_intro, competitiveness_analysis, updated_at FROM stex.corp_analysis WHERE code = $1',
      [code]
    ),
    db.query(
      'SELECT code, content, updated_at FROM stex.investment_summary WHERE code = $1',
      [code]
    ),
  ]);
  const latestDay = day.rows[0] ? normTradeDate(day.rows[0]) : null;
  let indexSnapshot = null;
  if (latestDay && latestDay.trade_date) {
    const td = String(latestDay.trade_date).slice(0, 10);
    const { rows } = await db.query(
      'SELECT index_code, trade_date, close, pct_chg FROM stex.index_day WHERE trade_date = $1::date ORDER BY index_code',
      [td]
    );
    if (rows.length) {
      indexSnapshot = {
        trade_date: td,
        indices: rows.map((r) => ({
          index_code: r.index_code,
          name: INDEX_NAMES[r.index_code] || r.index_code,
          close: r.close != null ? Number(r.close) : null,
          pct_chg: r.pct_chg != null ? Number(r.pct_chg) : null,
        })),
      };
    }
  }
  const summaryRow = investmentSummary.rows[0];
  const investmentSummaryData = summaryRow
    ? {
        content: summaryRow.content,
        updated_at: summaryRow.updated_at,
      }
    : null;
  res.json({
    corp: corp.rows[0] || null,
    latestDay,
    signals: signals.rows,
    watchlist: watch.rows[0] || null,
    corpAnalysis: corpAnalysis.rows[0] || null,
    indexSnapshot,
    investmentSummary: investmentSummaryData,
  });
});

// 将 trade_date 统一为 YYYY-MM-DD 字符串，避免时区导致界面少显示一天
function normTradeDate(row) {
  if (!row.trade_date) return row;
  const d = row.trade_date;
  const str = typeof d === 'string' ? d.slice(0, 10) : (d.toISOString ? d.toISOString().slice(0, 10) : String(d).slice(0, 10));
  return { ...row, trade_date: str };
}

// K 线日线
stockRouter.get('/:code/kline', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 120, 500);
  const { rows } = await db.query(
    'SELECT trade_date, open, high, low, close, volume, amount FROM stex.stock_day WHERE code = $1 ORDER BY trade_date DESC LIMIT $2',
    [code, limit]
  );
  res.json(rows.reverse().map(normTradeDate));
});

// 技术指标
stockRouter.get('/:code/technicals', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 60, 200);
  const { rows } = await db.query(
    'SELECT * FROM stex.technicals WHERE code = $1 ORDER BY trade_date DESC LIMIT $2',
    [code, limit]
  );
  res.json(rows.reverse());
});

// 基本面
stockRouter.get('/:code/fundamentals', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 20, 50);
  const { rows } = await db.query(
    'SELECT * FROM stex.fundamentals WHERE code = $1 ORDER BY report_date DESC LIMIT $2',
    [code, limit]
  );
  res.json(rows);
});

// 每日资金流向（净流入汇总 + 小/中/大/特大单买卖额万元、买卖量手）
stockRouter.get('/:code/moneyflow', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 120, 500);
  const { rows } = await db.query(
    `SELECT trade_date, net_mf_amount, net_mf_vol,
     buy_sm_amount, sell_sm_amount, buy_sm_vol, sell_sm_vol,
     buy_md_amount, sell_md_amount, buy_md_vol, sell_md_vol,
     buy_lg_amount, sell_lg_amount, buy_lg_vol, sell_lg_vol,
     buy_elg_amount, sell_elg_amount, buy_elg_vol, sell_elg_vol
     FROM stex.moneyflow WHERE code = $1 ORDER BY trade_date DESC LIMIT $2`,
    [code, limit]
  );
  res.json(rows.reverse().map(normTradeDate));
});

// 财务指标（季度等）
stockRouter.get('/:code/financial', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 10, 50);
  const { rows } = await db.query(
    'SELECT * FROM stex.financial WHERE code = $1 ORDER BY report_date DESC LIMIT $2',
    [code, limit]
  );
  res.json(rows);
});

// 新闻
stockRouter.get('/:code/news', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 20, 100);
  const { rows } = await db.query(
    'SELECT id, title, source, publish_at, summary, url FROM stex.news WHERE code = $1 OR code IS NULL ORDER BY publish_at DESC NULLS LAST LIMIT $2',
    [code, limit]
  );
  res.json(rows);
});

// 新闻舆论拉取/分析记录（拉取日期、信号日期、信号结果、新闻摘要）
stockRouter.get('/:code/news-opinion-records', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const { rows } = await db.query(
    `SELECT id, code, fetch_date, ref_date, direction, reason, news_content, created_at
     FROM stex.news_opinion_record WHERE code = $1 ORDER BY fetch_date DESC, ref_date DESC LIMIT $2`,
    [code, limit]
  );
  res.json(rows.map((r) => ({
    id: r.id,
    code: r.code,
    fetch_date: r.fetch_date ? (typeof r.fetch_date === 'string' ? r.fetch_date.slice(0, 19) : r.fetch_date.toISOString?.().slice(0, 19) ?? String(r.fetch_date)) : null,
    ref_date: r.ref_date ? String(r.ref_date).slice(0, 10) : null,
    direction: r.direction,
    reason: r.reason,
    news_content: r.news_content,
    created_at: r.created_at,
  })));
});

// 信号列表（可选 ref_date 按交易日筛选，便于查历史信号）
stockRouter.get('/:code/signals', async (req, res) => {
  const { code } = req.params;
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const refDate = req.query.ref_date ? String(req.query.ref_date).slice(0, 10) : null;
  const { rows } = await db.query(
    refDate
      ? 'SELECT * FROM stex.signals WHERE code = $1 AND ref_date = $2::date ORDER BY signal_type'
      : 'SELECT * FROM stex.signals WHERE code = $1 ORDER BY ref_date DESC NULLS LAST, created_at DESC LIMIT $2',
    refDate ? [code, refDate] : [code, limit]
  );
  res.json(rows.map((r) => (r.ref_date ? { ...r, ref_date: String(r.ref_date).slice(0, 10) } : r)));
});

// 加入/取消收藏并开启跟踪
stockRouter.post('/:code/watch', async (req, res) => {
  const { code } = req.params;
  const { track = true } = req.body || {};
  await db.query(
    `INSERT INTO stex.watchlist (code, track) VALUES ($1, $2)
     ON CONFLICT (code) DO UPDATE SET track = $2, updated_at = NOW()`,
    [code, track]
  );
  res.json({ code, track });
});

stockRouter.delete('/:code/watch', async (req, res) => {
  const { code } = req.params;
  await db.query('DELETE FROM stex.watchlist WHERE code = $1', [code]);
  res.json({ code, removed: true });
});
