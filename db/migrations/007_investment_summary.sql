-- 股票投资总结：由 AI 综合信号、日线、技术指标、企业分析、大盘、财务等生成的投资建议（建仓区间、持仓时长、关注信号等）
CREATE TABLE IF NOT EXISTS stex.investment_summary (
  code          VARCHAR(10) NOT NULL PRIMARY KEY,
  content       TEXT NOT NULL,
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stex.investment_summary IS '股票投资总结（AI 生成：建仓价位、持仓时间、关注信号等）';
