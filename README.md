# 中国市场股票分析专家系统
简称：股票专家 StEx = Stock Expert

## 系统概述
1. 本系统采用多Agent相互配合，由工作流编排，采集分析中国市场A股交易数据、企业基本面技术面分析数据、相关企业经营数据、监管政策文件、相关行业市场新闻热点等数据
2. 对所有数据进行总结归纳后最终形成：股票推荐（分短线盈利潜力和长线投资潜力）、以及对指定股票代码的看涨和看跌信号呈现给用户
3. 持续采集、跟踪已经持仓的股票交易数据（筹码分布、主力资金动向、技术指标等），行程风险提示信号、止盈止损建议信号

## 系统模块架构
0. 模块概览
- 选股模块：支持多条件筛选、推荐和收藏股票
- 股票模块：展示指定的股票代码交易数据、看涨看跌信号等
- 工作流模块：管理不同Agent生命周期、任务执行等
- 备注：本系统搭建目的主要为自用的本机运行环境，所以前期不需要用户模块，免除登录鉴权等功能，开发时保持扩展空间即可

1. 选股模块：
- 功能：对中国市场A股不同板块的股票代码进行筛选、推荐和收藏
- 举例：找出市值<100亿，连续两年净利润增长>25%的企业股票代码
 - 进一步筛选结果中属于航天、芯片、新能源板块的股票
 - 进一步筛选结果中最近1个月资金净流入大于净流出的股票
 - 进一步筛选结果中能鉴别主力资金动向的股票
 - 对筛选结果中指定股票进行收藏保存，并开启跟踪检测

2. 股票模块：
- 功能：对已经交易或者收藏的股票数据进行跟踪采集、策略分析、信号展示
- 举例：收藏了股票代码688795‌，摩尔线程
  - 展示该股票的K线图，基本面、技术面指标、企业基本信息
  - 展示该股票的筹码分布、主力资金分析
  - 展示该股票所有的看涨、看跌信号（包括每个信号结论的来源）
  - 展示该股票的相关市场热点新闻、最新相关国家政策的解读

3. 工作流模块
- 功能：支持Agent工作流编排、对不同任务的Agent进行参数更新、管理Agent生命周期以及任务执行日志
- 举例：管理负责采集股票市场交易数据的Agent
  - 可按交易代码范围指定采集股票对象
  - 可设定采集样本频率（日线数据、15分钟线数据等）
  - 可切换免费、付费等不同股票交易数据源

## 系统技术架构
1. 开发语言
- 前端：React/Vite
- 后端：NodeJS
- AI分析、数据采集任务：Python
- 数据库：Postgres（本机）
- 工作流框架：CrewAI或LangGraph
- LLM模型：Moonshot API(提供API KEY)

2. 数据库主要核心表：
  - stex.corp：上市企业的基本信息
  - stex.stock_day：指定代码的股票市场日线交易数据
  - stex.stock_min: 指定代码的股票市场分钟线交易数据
  - stex.technicals: 股票代码的技术指标数据
  - stex.fundamentals：股票代码的基本面指标数据
  - stex.trade: 对指定股票录入的交易数据
  - stex.policy: 采集到的监管政策数据
  - stex.news: 采集到的相关股票代码的新闻热点数据
  - stex.financial: 股票企业披露的财务数据
  - stex.signals: 对股票分析之后形成的信号数据
  - stex.corp_analysis: 企业解析结果（主营业务介绍、核心竞争力/中美科技竞争战略分析）
  - stex.index_day: 大盘指数日线（上证/深证/创业板/沪深300/中证500），用于股票详情页同日涨跌幅对比与跑赢大盘展示
  - stex.investment_summary: 股票投资总结（AI 生成：建仓价位区间、持仓时间、应关注的波动与交易信号）

3. 项目工程结构：
- 前端展示工程（展示界面、图标、数据）
- 后端API工程（扮演中间件，对接后端服务的接口输出或者数据库数据拉取）
- 后端服务工程（AI大模型调用、数据采集、数据分析 -> 结果入库或者接口输出）

## 效果期望
通过本系统，期望可以达到目的：
1. 通过股票历史数据的筛选，可以得到具有投资价值的股票代码
2. 选择指定股票代码之后，可以开启对多支股票的持续跟踪，派发任务给Agent
3. 工作流配置多Agent各自独立干活，持续采集指定股票代码多个维度的数据（市场交易、新闻热点、资金动向等）
4. 有专门的Agent总结归纳所有采集到的数据进行分析，形成指定股票代码投资建议和涨跌信号

---

## 本地运行（本机已安装 Python / Node.js / PostgreSQL 17）

### 1. 数据库

```bash
# 创建数据库
createdb stock
# 初始化 schema 与表
psql -d stock -f db/init.sql
# 可选：执行增量迁移（企业解析、资金流向、信号唯一、大盘指数日线）
psql -d stock -f db/migrations/002_corp_analysis.sql
psql -d stock -f db/migrations/003_moneyflow.sql
psql -d stock -f db/migrations/004_moneyflow_granular.sql
psql -d stock -f db/migrations/005_signals_unique_ref.sql
psql -d stock -f db/migrations/006_index_day.sql
psql -d stock -f db/migrations/007_investment_summary.sql
```

### 2. 后端 API（Node.js）

```bash
cd backend-api
cp .env.example .env   # 按需填写 PG_* 等
npm install
npm run dev            # 默认 http://localhost:3000
```
若报 `EMFILE: too many open files`，可改用 `npm start`（无热重载）。

### 3. 后端服务（Python，数据采集 + AI 分析）

若终端没有 `python`/`pip` 命令，请用 `python3`/`pip3`（macOS/Linux 常见）。

```bash
cd backend-services
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip3 install -r requirements.txt
cp .env.example .env        # 填写 MOONSHOT_API_KEY、PG_* 等
uvicorn src.main:app --reload --port 8000
```

在 `backend-api/.env` 中设置 `PYTHON_SERVICE_URL=http://localhost:8000` 后，前端触发的工作流会转发到 Python 服务。

**新闻舆论 Agent 信息源与配置**

| 信息源 | 采集方式 | 是否需要注册/配置 |
|--------|----------|-------------------|
| 巨潮资讯网 | RSSHub 路由 `cninfo/announcement` | 否。需配置 `RSSHUB_BASE_URL` 后才会拉取 |
| 财联社电报 | RSSHub 路由 `cls/telegraph` | 否。同上 |
| 证券时报 | RSSHub 路由 `stcn/article/list` | 否。同上 |
| 中国证券报 | RSSHub 路由 `cs/news` | 否。同上 |
| 雪球 | RSSHub 路由 `xueqiu/stock_info/:symbol/news` | 否。同上；自建 RSSHub 可选配雪球 Cookie 以提升稳定性 |
| 东方财富股吧 | DuckDuckGo 站内搜索 `site:guba.eastmoney.com` | 否。无需 API 或注册，依赖现有 DDG 搜索 |

- **RSSHUB_BASE_URL**（`backend-services/.env`）：可选。不配置时，新闻 Agent 仅使用 DuckDuckGo 综合新闻与站内搜索；配置后从 RSSHub 拉取上述财联社、证券时报、中国证券报、雪球、巨潮资讯等 RSS。可填公网实例如 `https://rsshub.app`（可能限流），或自建 [RSSHub](https://github.com/DIYgod/RSSHub) 后填自建地址。
- 以上信息源均**不需要**单独 API Key 或站点注册；若自建 RSSHub，部分路由（如雪球、巨潮）可在 RSSHub 文档中选配 Cookie 等，非必须。

### 4. 前端（React/Vite）

```bash
cd frontend
npm install
npm run dev               # 默认 http://localhost:5173，/api 代理到 3000
```

### 目录说明

| 目录 | 说明 |
|------|------|
| `frontend/` | React+Vite 展示与选股/股票/工作流页面 |
| `backend-api/` | Node.js 中间件，提供 REST API，读写 PostgreSQL |
| `backend-services/` | Python 数据采集、Moonshot 分析、工作流（可扩展 CrewAI/LangGraph） |
| `db/` | PostgreSQL 初始化脚本 `init.sql` |
