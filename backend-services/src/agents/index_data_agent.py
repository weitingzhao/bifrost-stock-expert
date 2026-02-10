"""
Agent：采集大盘指数日线（上证、深证、创业板、沪深300 等），写入 stex.index_day。
用于股票详情页展示同日大盘涨跌幅，并对比股票是否跑赢大盘。
数据来源：Tushare index_daily。
"""
import logging
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from ..config import TUSHARE_TOKEN
from ..db import get_conn

logger = logging.getLogger(__name__)
DELAY = 0.3

# Tushare 指数代码 -> 展示名（前端用）
INDEX_NAMES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
}

# 采集的指数列表（与 INDEX_NAMES 一致）
INDEX_CODES = list(INDEX_NAMES.keys())


def _safe_num(v) -> Optional[Decimal]:
    if v is None or (isinstance(v, float) and (v != v)):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _upsert_index_day(
    conn,
    index_code: str,
    trade_date: str,
    open_,
    high,
    low,
    close,
    pre_close,
    pct_chg,
    vol,
    amount,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.index_day (index_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, amount)
            VALUES (%s, %s::date, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (index_code, trade_date) DO UPDATE SET
              open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
              close = EXCLUDED.close, pre_close = EXCLUDED.pre_close,
              pct_chg = EXCLUDED.pct_chg, vol = EXCLUDED.vol, amount = EXCLUDED.amount
            """,
            (
                index_code,
                trade_date,
                _safe_num(open_),
                _safe_num(high),
                _safe_num(low),
                _safe_num(close),
                _safe_num(pre_close),
                _safe_num(pct_chg),
                _safe_num(vol),
                _safe_num(amount),
            ),
        )


def run_index_data_agent() -> dict[str, Any]:
    """
    拉取大盘指数日线（上证/深证/创业板/沪深300/中证500），写入 stex.index_day。
    日期范围：最近约 2 年，与 watchlist 日线一致。
    """
    if not TUSHARE_TOKEN:
        return {"ok": False, "error": "请设置 TUSHARE_TOKEN", "days_updated": 0}

    try:
        import tushare as ts
    except ImportError:
        return {"ok": False, "error": "请安装 tushare: pip install tushare", "days_updated": 0}

    pro = ts.pro_api(TUSHARE_TOKEN)
    end = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    start = end - timedelta(days=365 * 2)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    total_days = 0
    with get_conn() as conn:
        for index_code in INDEX_CODES:
            time.sleep(DELAY)
            try:
                df = pro.index_daily(
                    ts_code=index_code,
                    start_date=start_str,
                    end_date=end_str,
                )
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    td = str(row.get("trade_date", ""))
                    if len(td) < 8:
                        continue
                    _upsert_index_day(
                        conn,
                        index_code,
                        td,
                        row.get("open"),
                        row.get("high"),
                        row.get("low"),
                        row.get("close"),
                        row.get("pre_close"),
                        row.get("pct_chg"),
                        row.get("vol"),
                        row.get("amount"),
                    )
                    total_days += 1
            except Exception as e:
                logger.warning("index_daily %s: %s", index_code, e)
        conn.commit()

    return {
        "ok": True,
        "indices": INDEX_CODES,
        "days_updated": total_days,
        "request_date_range": {"start": start_str, "end": end_str},
    }
