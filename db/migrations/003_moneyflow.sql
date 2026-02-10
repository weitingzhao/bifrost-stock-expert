-- 每日资金流向：净流入额（万元）、净流入量（手），来源 Tushare moneyflow
CREATE TABLE IF NOT EXISTS stex.moneyflow (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  trade_date    DATE NOT NULL,
  net_mf_amount NUMERIC(16,2),
  net_mf_vol    BIGINT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_moneyflow_code_date ON stex.moneyflow(code, trade_date DESC);
