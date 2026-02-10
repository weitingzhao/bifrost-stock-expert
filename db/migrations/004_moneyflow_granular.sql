-- 资金流向细粒度：小单/中单/大单/特大单买卖额(万元)、买卖量(手)，来源 Tushare moneyflow 同接口
ALTER TABLE stex.moneyflow
  ADD COLUMN IF NOT EXISTS buy_sm_amount  NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS sell_sm_amount NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS buy_sm_vol     BIGINT,
  ADD COLUMN IF NOT EXISTS sell_sm_vol    BIGINT,
  ADD COLUMN IF NOT EXISTS buy_md_amount  NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS sell_md_amount NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS buy_md_vol     BIGINT,
  ADD COLUMN IF NOT EXISTS sell_md_vol    BIGINT,
  ADD COLUMN IF NOT EXISTS buy_lg_amount  NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS sell_lg_amount NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS buy_lg_vol     BIGINT,
  ADD COLUMN IF NOT EXISTS sell_lg_vol    BIGINT,
  ADD COLUMN IF NOT EXISTS buy_elg_amount NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS sell_elg_amount NUMERIC(16,2),
  ADD COLUMN IF NOT EXISTS buy_elg_vol    BIGINT,
  ADD COLUMN IF NOT EXISTS sell_elg_vol   BIGINT;
