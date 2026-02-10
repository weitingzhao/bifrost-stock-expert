-- StEx 股票专家系统 - PostgreSQL 初始化
-- 使用前请创建数据库: CREATE DATABASE stock;

CREATE SCHEMA IF NOT EXISTS stex;

-- 上市企业基本信息（市场：上证/深证/科创/创业/北证；市值/市盈率/市净率为行情快照）
CREATE TABLE IF NOT EXISTS stex.corp (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL UNIQUE,
  name          VARCHAR(100),
  market        VARCHAR(20),
  industry      VARCHAR(100),
  sector        VARCHAR(100),
  list_date     DATE,
  market_cap    NUMERIC(20,2),
  pe            NUMERIC(12,4),
  pb            NUMERIC(12,4),
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_corp_code ON stex.corp(code);
CREATE INDEX IF NOT EXISTS idx_corp_industry ON stex.corp(industry);
CREATE INDEX IF NOT EXISTS idx_corp_sector ON stex.corp(sector);

-- 股票日线
CREATE TABLE IF NOT EXISTS stex.stock_day (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  trade_date    DATE NOT NULL,
  open          NUMERIC(12,4),
  high          NUMERIC(12,4),
  low           NUMERIC(12,4),
  close         NUMERIC(12,4),
  volume        BIGINT,
  amount        NUMERIC(20,2),
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_day_code_date ON stex.stock_day(code, trade_date DESC);

-- 股票分钟线
CREATE TABLE IF NOT EXISTS stex.stock_min (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  trade_time    TIMESTAMPTZ NOT NULL,
  open          NUMERIC(12,4),
  high          NUMERIC(12,4),
  low           NUMERIC(12,4),
  close         NUMERIC(12,4),
  volume        BIGINT,
  interval_min  SMALLINT DEFAULT 15,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, trade_time, interval_min)
);

CREATE INDEX IF NOT EXISTS idx_stock_min_code_time ON stex.stock_min(code, trade_time DESC, interval_min);

-- 技术指标
CREATE TABLE IF NOT EXISTS stex.technicals (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  trade_date    DATE NOT NULL,
  ma5           NUMERIC(12,4),
  ma10          NUMERIC(12,4),
  ma20          NUMERIC(12,4),
  macd          NUMERIC(12,4),
  macd_signal   NUMERIC(12,4),
  macd_hist     NUMERIC(12,4),
  rsi           NUMERIC(8,2),
  kdj_k         NUMERIC(8,2),
  kdj_d         NUMERIC(8,2),
  kdj_j         NUMERIC(8,2),
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_technicals_code_date ON stex.technicals(code, trade_date DESC);

-- 基本面指标
CREATE TABLE IF NOT EXISTS stex.fundamentals (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  report_date   DATE NOT NULL,
  pe            NUMERIC(12,4),
  pb            NUMERIC(12,4),
  ps            NUMERIC(12,4),
  market_cap    NUMERIC(20,2),
  revenue       NUMERIC(20,2),
  net_profit    NUMERIC(20,2),
  profit_growth NUMERIC(12,2),
  roe           NUMERIC(8,2),
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, report_date)
);

CREATE INDEX IF NOT EXISTS idx_fundamentals_code_date ON stex.fundamentals(code, report_date DESC);

-- 用户录入的交易/持仓
CREATE TABLE IF NOT EXISTS stex.trade (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  direction     VARCHAR(10),
  price         NUMERIC(12,4),
  volume        INTEGER,
  trade_at      TIMESTAMPTZ,
  note          TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trade_code ON stex.trade(code);

-- 监管政策
CREATE TABLE IF NOT EXISTS stex.policy (
  id            BIGSERIAL PRIMARY KEY,
  title         VARCHAR(500),
  source        VARCHAR(200),
  publish_date  DATE,
  content       TEXT,
  summary       TEXT,
  url           VARCHAR(1000),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_policy_publish_date ON stex.policy(publish_date DESC);

-- 新闻热点
CREATE TABLE IF NOT EXISTS stex.news (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10),
  title         VARCHAR(500),
  source        VARCHAR(200),
  publish_at    TIMESTAMPTZ,
  content       TEXT,
  summary       TEXT,
  url           VARCHAR(1000),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_code ON stex.news(code);
CREATE INDEX IF NOT EXISTS idx_news_publish_at ON stex.news(publish_at DESC);

-- 企业财务披露
CREATE TABLE IF NOT EXISTS stex.financial (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  report_date   DATE NOT NULL,
  report_type   VARCHAR(50),
  revenue       NUMERIC(20,2),
  net_profit    NUMERIC(20,2),
  total_assets  NUMERIC(20,2),
  raw_content   TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(code, report_date, report_type)
);

CREATE INDEX IF NOT EXISTS idx_financial_code_date ON stex.financial(code, report_date DESC);

-- 分析信号（看涨/看跌、来源）
CREATE TABLE IF NOT EXISTS stex.signals (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL,
  signal_type   VARCHAR(20) NOT NULL,
  direction     VARCHAR(10) NOT NULL,
  strength      NUMERIC(4,2),
  source        VARCHAR(200),
  reason        TEXT,
  ref_date      DATE,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_signals_code ON stex.signals(code);
CREATE INDEX IF NOT EXISTS idx_signals_created ON stex.signals(created_at DESC);

-- 收藏/跟踪股票
CREATE TABLE IF NOT EXISTS stex.watchlist (
  id            BIGSERIAL PRIMARY KEY,
  code          VARCHAR(10) NOT NULL UNIQUE,
  track         BOOLEAN DEFAULT TRUE,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_watchlist_code ON stex.watchlist(code);

-- 工作流/Agent 任务日志（扩展用）
CREATE TABLE IF NOT EXISTS stex.workflow_log (
  id            BIGSERIAL PRIMARY KEY,
  workflow_id   VARCHAR(100),
  agent_id      VARCHAR(100),
  task          VARCHAR(200),
  status        VARCHAR(20),
  input_snapshot JSONB,
  output_snapshot JSONB,
  started_at    TIMESTAMPTZ,
  finished_at   TIMESTAMPTZ,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_log_workflow ON stex.workflow_log(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_log_started ON stex.workflow_log(started_at DESC);
