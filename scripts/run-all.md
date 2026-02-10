# StEx 本地运行步骤

在项目根目录打开 **3 个终端**，依次执行：

---

## 终端 1：数据库（首次运行执行一次）

```bash
# 若尚未创建数据库
createdb stock

# 初始化表结构
psql -d stock -f db/init.sql
```

---

## 终端 2：后端 API（Node.js）

```bash
cd backend-api
npm install
npm run dev
```

看到 `StEx API listening on http://localhost:3000` 即成功。

---

## 终端 3：Python 服务（数据采集 + AI）

```bash
cd backend-services
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip3 install -r requirements.txt
uvicorn src.main:app --reload --port 8000
```

看到 `Uvicorn running on http://0.0.0.0:8000` 即成功。

在 `backend-api/.env` 中设置 `PYTHON_SERVICE_URL=http://localhost:8000`，选股页的「采集基础数据」才会生效。

---

## 终端 4：前端（React/Vite）

```bash
cd frontend
npm install
npm run dev
```

看到 `Local: http://localhost:5173/` 即成功。

若提示 Vite 需要 Node 20.19+ 或 22.12+，请升级 Node（如 `nvm install 22 && nvm use 22`）。

---

## 访问

- 前端（选股/股票/工作流）： http://localhost:5173
- 后端 API： http://localhost:3000
- Python 服务： http://localhost:8000
- Python 健康检查： http://localhost:8000/health

---

## 若 Node 报 EMFILE（too many open files）

可不用监视模式启动后端 API：

```bash
cd backend-api
npm start
```

修改代码后需手动重启。

---

## 如何运行 corp_agent（采集 A 股板块上市公司基础数据）

### 方式一：前端选股页（推荐）

1. 确保 **后端 API** 和 **Python 服务** 已启动，且 `backend-api/.env` 中设置了 `PYTHON_SERVICE_URL=http://localhost:8000`。
2. 打开前端 http://localhost:5173 ，进入 **选股** 页。
3. 点击 **「采集 A 股板块上市公司基础数据」**，等待完成即可。

### 方式二：curl 调用接口

**通过 Node API（需 Node + Python 都跑着）：**

```bash
curl -X POST http://localhost:3000/api/workflow/trigger \
  -H "Content-Type: application/json" \
  -d '{"action":"collect_corp"}'
```

**直接调 Python 服务（只需 Python 跑着）：**

```bash
curl -X POST http://localhost:8000/api/trigger \
  -H "Content-Type: application/json" \
  -d '{"action":"collect_corp"}'
```

### 方式三：命令行直接跑（不依赖 Node/前端）

只需 **数据库已建表** 且 **backend-services 依赖已装**（含 akshare）：

```bash
cd backend-services
source .venv/bin/activate   # 若用虚拟环境
python3 run_corp_agent.py
```

成功会打印 `成功: 行业数 xx 入库条数 xx`，数据写入 `stex.corp`。之后在选股页筛选或下拉行业即可看到。
