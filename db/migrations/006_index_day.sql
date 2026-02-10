-- 大盘指数日线：用于股票详情页对比展示（同日涨跌幅、是否跑赢大盘）
-- 数据来源：Tushare index_daily
CREATE TABLE IF NOT EXISTS stex.index_day (
  index_code VARCHAR(20) NOT NULL,
  trade_date  DATE NOT NULL,
  open        NUMERIC(18,4),
  high        NUMERIC(18,4),
  low         NUMERIC(18,4),
  close       NUMERIC(18,4),
  pre_close   NUMERIC(18,4),
  pct_chg     NUMERIC(10,4),
  vol         NUMERIC(20,2),
  amount      NUMERIC(24,2),
  PRIMARY KEY (index_code, trade_date)
);

COMMENT ON TABLE stex.index_day IS '大盘指数日线（上证/深证/创业板/沪深300等）';
COMMENT ON COLUMN stex.index_day.pct_chg IS '涨跌幅(%)';
