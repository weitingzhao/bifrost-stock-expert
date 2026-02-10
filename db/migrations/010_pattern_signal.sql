-- 经典K线形态识别结果（杯柄形态、上升三法等），供策略选股使用
CREATE TABLE IF NOT EXISTS stex.pattern_signal (
  id          BIGSERIAL PRIMARY KEY,
  code        VARCHAR(10) NOT NULL,
  pattern_type VARCHAR(50) NOT NULL,
  ref_date    DATE NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pattern_signal_code ON stex.pattern_signal(code);
CREATE INDEX IF NOT EXISTS idx_pattern_signal_type ON stex.pattern_signal(pattern_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pattern_signal_code_type_ref
  ON stex.pattern_signal(code, pattern_type, ref_date);

COMMENT ON TABLE stex.pattern_signal IS 'K线形态信号：cup_handle 杯柄形态, rising_three 上升三法';
