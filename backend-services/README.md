# StEx 后端服务（Python）

- 数据采集：A 股上市公司基础数据、日线/分钟线等（支持 **Tushare Pro** / akshare）
- AI 分析：调用 Moonshot API 做总结与信号生成
- 工作流：Agent 编排（可扩展 CrewAI / LangGraph）

## 运行

若终端没有 `python`/`pip`，请用 `python3`/`pip3`。

```bash
cd backend-services
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip3 install -r requirements.txt
cp .env.example .env        # 填写 MOONSHOT_API_KEY、TUSHARE_TOKEN 等
uvicorn src.main:app --reload --port 8000
```

## 数据源（corp_agent 采集上市公司基础数据）

- **Tushare Pro**（推荐）：在 `.env` 中设置 `DATA_SOURCE=tushare`、`TUSHARE_TOKEN=你的token`（https://tushare.pro 注册获取）。表结构已兼容，直接写入 `stex.corp`。
- **akshare**：默认 `DATA_SOURCE=akshare`，无需 token，依赖东方财富接口（易遇限流/断连）。

命令行单独跑采集：`python3 run_corp_agent.py`（会按 `DATA_SOURCE` 选 Tushare 或 akshare）。

## API

- `GET /health` 健康检查
- `POST /api/trigger` 触发采集/分析（由 Node API 转发调用）
