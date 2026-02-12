#!/usr/bin/env python3
"""
分析指定股票在指定区间的日线数据，识别「持续低量横盘后放量上涨」形态。
用法（在 backend-services 目录下）：
  python scripts/analyze_low_vol_breakout.py 002440 2026-01-10 2026-01-25
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载 .env
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.isfile(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def main():
    code = (sys.argv[1] or "002440").strip()
    start = (sys.argv[2] or "2026-01-10").strip()[:10]
    end = (sys.argv[3] or "2026-01-25").strip()[:10]

    from src.db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, open, high, low, close, volume, turnover_rate
                FROM stex.stock_day
                WHERE code = %s AND trade_date >= %s::date - INTERVAL '30 days' AND trade_date <= %s::date
                ORDER BY trade_date
                """,
                (code, start, end),
            )
            rows = cur.fetchall()

    if not rows:
        print(f"无数据：{code} 在 {start} 至 {end} 区间")
        return 1

    # 转为 list[dict]，日期升序
    days = []
    for r in rows:
        td = r[0]
        td_str = td.isoformat()[:10] if hasattr(td, "isoformat") else str(td)[:10]
        days.append({
            "trade_date": td_str,
            "open": float(r[1]) if r[1] is not None else None,
            "high": float(r[2]) if r[2] is not None else None,
            "low": float(r[3]) if r[3] is not None else None,
            "close": float(r[4]) if r[4] is not None else None,
            "volume": float(r[5]) or 0 if r[5] is not None else 0,
            "turnover_rate": float(r[6]) if r[6] is not None else None,
        })

    # 筛选目标区间
    in_range = [d for d in days if start <= d["trade_date"] <= end]
    if not in_range:
        print(f"区间 {start}～{end} 内无交易日数据")
        return 1

    print(f"股票 {code}：{start} ～ {end} 共 {len(in_range)} 个交易日")
    print("-" * 72)
    print(f"{'日期':<12} {'收盘':>8} {'成交量':>12} {'换手%':>6} {'涨跌幅':>8}")
    print("-" * 72)

    for i, d in enumerate(in_range):
        close = d["close"]
        vol = d["volume"]
        tr = d["turnover_rate"]
        pct = ""
        if i > 0:
            prev = in_range[i - 1]["close"]
            if prev and prev != 0 and close is not None:
                pct = f"{(close - prev) / prev * 100:+.2f}%"
        vol_str = f"{int(vol):,}" if vol else "-"
        tr_str = f"{tr:.2f}" if tr is not None else "-"
        print(f"{d['trade_date']:<12} {close or '-':>8} {vol_str:>12} {tr_str:>6} {pct:>8}")

    # 分析：持续低量横盘 + 放量上涨
    # 1) 区间内 20 日量均（用 days 全量）
    vol_list = [d["volume"] for d in days if d["volume"]]
    avg_vol_20 = sum(vol_list[-20:]) / len(vol_list[-20:]) if len(vol_list) >= 20 else (sum(vol_list) / len(vol_list) if vol_list else 0)
    # 2) 区间内前几日是否「低量横盘」：取前 5 日（若不足则前 N 日），量 < 0.8*avg_vol_20，收盘振幅 (max-min)/mid < 5%
    # 3) 某日「放量上涨」：量 > 1.5*avg_vol_20 且 涨幅 > 0

    print()
    print("形态简析（持续低量横盘 + 放量上涨）：")
    print(f"  区间及前段 20 日平均成交量: {int(avg_vol_20):,}" if avg_vol_20 else "  无成交量数据")
    look_back = 5
    for i in range(look_back, len(in_range)):
        window = in_range[i - look_back:i]
        closes = [d["close"] for d in window if d["close"] is not None]
        vols = [d["volume"] for d in window if d["volume"]]
        if not closes or not vols:
            continue
        mid = (min(closes) + max(closes)) / 2
        range_pct = (max(closes) - min(closes)) / mid * 100 if mid else 0
        avg_win = sum(vols) / len(vols)
        low_vol_sideways = avg_win < 0.8 * avg_vol_20 and range_pct < 5.0

        today = in_range[i]
        today_vol = today["volume"] or 0
        prev_close = in_range[i - 1]["close"] if i else None
        today_close = today["close"]
        pct_up = (today_close - prev_close) / prev_close * 100 if prev_close and prev_close != 0 and today_close else 0
        volume_surge = today_vol >= 1.5 * avg_vol_20 if avg_vol_20 else False

        if low_vol_sideways and volume_surge and pct_up > 0:
            print(f"  → {today['trade_date']}: 前{look_back}日低量横盘(均量{int(avg_win):,}<0.8*均20, 振幅{range_pct:.1f}%<5%)，当日放量({int(today_vol):,}≥1.5*均20)且上涨{pct_up:.2f}%")

    print()
    print("说明：样本数较少时仅为示意；实盘信号需在 signal_agent 中按全市场逐日计算。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
