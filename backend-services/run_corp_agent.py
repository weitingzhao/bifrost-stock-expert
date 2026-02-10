#!/usr/bin/env python3
"""
命令行直接运行 corp_agent：采集 A 股上市公司基础数据并写入 stex.corp。
数据源由 .env 的 DATA_SOURCE 决定：tushare（Tushare Pro）或 akshare（东方财富）。
仅需：数据库已建表 + 本目录已安装依赖（pip install -r requirements.txt）。
用法：在 backend-services 目录下执行
  python3 run_corp_agent.py
或先激活虚拟环境： source .venv/bin/activate && python3 run_corp_agent.py
使用 Tushare 时请设置 .env：DATA_SOURCE=tushare，TUSHARE_TOKEN=你的token
"""
import os
import sys

# 确保能 import src
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import DATA_SOURCE
from src.agents.corp_agent import run_corp_agent
from src.agents.tushare_corp_agent import run_tushare_corp_agent

if __name__ == "__main__":
    runner = run_tushare_corp_agent if DATA_SOURCE == "tushare" else run_corp_agent
    print("corp_agent 开始采集… (数据源:", DATA_SOURCE, ")")
    result = runner()
    print("结果:", result)
    if result.get("ok"):
        print("成功: 入库条数", result.get("total_upserted", 0), result.get("industries") and f"行业数 {result['industries']}" or "")
    else:
        print("失败:", result.get("error", "未知错误"))
        sys.exit(1)
