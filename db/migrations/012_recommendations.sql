-- 综合推荐历史记录表：按日期保存推荐分析结果
CREATE TABLE IF NOT EXISTS stex.recommendations (
    id SERIAL PRIMARY KEY,
    ref_date DATE NOT NULL,
    code VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    industry VARCHAR(100),
    latest_close NUMERIC(18, 4),
    pe NUMERIC(18, 4),
    composite_score NUMERIC(10, 2),
    signals JSONB,
    analysis TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ref_date, code)
);

CREATE INDEX IF NOT EXISTS idx_recommendations_ref_date ON stex.recommendations (ref_date DESC);
CREATE INDEX IF NOT EXISTS idx_recommendations_code ON stex.recommendations (code);
