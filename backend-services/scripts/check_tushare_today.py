#!/usr/bin/env python3
"""
测试 Tushare 今日/最近交易日数据是否已更新。
用法：在 backend-services 目录下执行
  python scripts/check_tushare_today.py
或
  cd backend-services && python scripts/check_tushare_today.py
"""
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 保证能加载到项目 config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    # 加载 backend-services/.env
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.isfile(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        print("未设置 TUSHARE_TOKEN，请在 backend-services/.env 中配置")
        return 1

    try:
        import tushare as ts
    except ImportError:
        print("请安装 tushare: pip install tushare")
        return 1

    pro = ts.pro_api(token)
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    today_str = today.strftime("%Y%m%d")
    print(f"当前中国时区日期: {today} ({today_str})")
    print()

    # 1) 交易日历：含今日在内的最近开盘日
    last_open = None
    start_cal = (today - timedelta(days=14)).strftime("%Y%m%d")
    end_cal = today.strftime("%Y%m%d")
    try:
        cal = pro.trade_cal(exchange="SSE", start_date=start_cal, end_date=end_cal, is_open="1")
        if cal is not None and not cal.empty and "cal_date" in cal.columns:
            last_open = str(cal["cal_date"].max() or "")[:8]
            print(f"交易日历(含今日)最近开盘日: {last_open}")
        else:
            print("交易日历无数据")
    except Exception as e:
        print(f"交易日历请求失败: {e}")
        return 1

    # 2) 查今日日线是否有数据（用全市场 daily 接口）
    print()
    try:
        df = pro.daily(trade_date=today_str)
        if df is not None and not df.empty:
            n = len(df)
            print(f"今日({today_str})日线数据: 已有，共 {n} 条")
            print(df.head(3).to_string())
        else:
            print(f"今日({today_str})日线数据: 暂无（可能尚未收盘或数据未更新）")
    except Exception as e:
        print(f"今日日线请求失败: {e}")

    # 3) 用「最近开盘日」再查一次，确认能拿到的最新日期
    if last_open and last_open != today_str:
        print()
        try:
            df2 = pro.daily(trade_date=last_open)
            if df2 is not None and not df2.empty:
                print(f"最近开盘日({last_open})日线: 共 {len(df2)} 条（可正常拉取）")
            else:
                print(f"最近开盘日({last_open})日线: 无数据")
        except Exception as e2:
            print(f"最近开盘日日线请求失败: {e2}")

    print()
    print("测试结束")
    return 0

if __name__ == "__main__":
    sys.exit(main())
