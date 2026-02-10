import { Router } from 'express';
import { db } from '../db.js';

export const indicesRouter = Router();

const INDEX_NAMES = {
  '000001.SH': '上证指数',
  '399001.SZ': '深证成指',
  '399006.SZ': '创业板指',
  '000300.SH': '沪深300',
  '000905.SH': '中证500',
};

const INDEX_CODES = Object.keys(INDEX_NAMES);

function normDate(row) {
  const d = row.trade_date;
  if (!d) return null;
  const str = typeof d === 'string' ? d.slice(0, 10) : (d.toISOString ? d.toISOString().slice(0, 10) : String(d).slice(0, 10));
  return str;
}

/** 大盘指数近期日线，按指数分组返回，供列表页小图使用 */
indicesRouter.get('/daily', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 30, 120);
  const { rows } = await db.query(
    `WITH last_dates AS (
       SELECT DISTINCT trade_date FROM stex.index_day ORDER BY trade_date DESC LIMIT $1
     )
     SELECT index_code, trade_date, close, pct_chg
     FROM stex.index_day
     WHERE trade_date IN (SELECT trade_date FROM last_dates)
       AND index_code = ANY($2::text[])
     ORDER BY index_code, trade_date`,
    [limit, INDEX_CODES]
  );
  const byCode = {};
  for (const code of INDEX_CODES) {
    byCode[code] = { index_code: code, name: INDEX_NAMES[code], data: [] };
  }
  for (const r of rows) {
    const code = r.index_code;
    byCode[code].data.push({
      trade_date: normDate(r),
      close: r.close != null ? Number(r.close) : null,
      pct_chg: r.pct_chg != null ? Number(r.pct_chg) : null,
    });
  }
  const result = INDEX_CODES.map((code) => byCode[code]);
  res.json(result);
});
