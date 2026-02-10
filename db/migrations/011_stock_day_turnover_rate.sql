-- 股票日线增加换手率（%），来源 Tushare daily_basic.turnover_rate
ALTER TABLE stex.stock_day ADD COLUMN IF NOT EXISTS turnover_rate NUMERIC(8,4);
COMMENT ON COLUMN stex.stock_day.turnover_rate IS '换手率(%)，来自 Tushare daily_basic';
