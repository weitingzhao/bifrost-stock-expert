-- 投资信号按 (code, ref_date, signal_type) 唯一，便于按交易日覆盖写入
CREATE UNIQUE INDEX IF NOT EXISTS idx_signals_code_ref_type
  ON stex.signals (code, ref_date, signal_type)
  WHERE ref_date IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_signals_code_ref_date ON stex.signals (code, ref_date DESC);
