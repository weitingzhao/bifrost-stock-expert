-- 新闻舆论拉取/分析记录：每次拉取分析的新闻内容、拉取日期、股票代码、分析后的信号结果
CREATE TABLE IF NOT EXISTS stex.news_opinion_record (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  fetch_date    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ref_date      DATE NOT NULL,
  direction     VARCHAR(20) NOT NULL,
  reason        TEXT,
  news_content  TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_opinion_record_code ON stex.news_opinion_record(code);
CREATE INDEX IF NOT EXISTS idx_news_opinion_record_fetch ON stex.news_opinion_record(fetch_date DESC);
