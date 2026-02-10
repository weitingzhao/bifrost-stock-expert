import os
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "stock")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

MOONSHOT_API_KEY = os.getenv("MOONSHOT_API_KEY", "")
MOONSHOT_BASE_URL = os.getenv("MOONSHOT_BASE_URL", "https://api.moonshot.cn/v1")

# 数据源：tushare | akshare。corp_agent 采集时按此切换
DATA_SOURCE = os.getenv("DATA_SOURCE", "akshare").strip().lower()
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "").strip()

PORT = int(os.getenv("PORT", "8000"))

# 新闻舆论 agent：RSSHub 实例 base URL（可选）。配置后将从 财联社/证券时报/中证网/雪球 等 RSS 路由拉取
RSSHUB_BASE_URL = (os.getenv("RSSHUB_BASE_URL") or "").strip().rstrip("/")
