-- 应用级配置（如投资总结生成用的提示词，全局生效）
CREATE TABLE IF NOT EXISTS stex.app_config (
  key   VARCHAR(100) PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE stex.app_config IS '应用配置键值，如 investment_summary_prompt';
