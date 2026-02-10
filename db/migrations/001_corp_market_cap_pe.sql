-- 为 stex.corp 增加市场分类、市值、市盈率、市净率（已有库执行）
ALTER TABLE stex.corp ADD COLUMN IF NOT EXISTS market_cap NUMERIC(20,2);
ALTER TABLE stex.corp ADD COLUMN IF NOT EXISTS pe NUMERIC(12,4);
ALTER TABLE stex.corp ADD COLUMN IF NOT EXISTS pb NUMERIC(12,4);
