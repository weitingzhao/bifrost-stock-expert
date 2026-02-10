-- 企业解析结果：主营业务介绍、核心竞争力（中美科技竞争战略）分析
CREATE TABLE IF NOT EXISTS stex.corp_analysis (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL UNIQUE,
  business_intro           TEXT,
  competitiveness_analysis TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_corp_analysis_code ON stex.corp_analysis(code);
