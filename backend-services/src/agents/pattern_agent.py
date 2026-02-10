"""
Agent：K 线形态识别（杯柄形态、上升三法），结果写入 stex.pattern_signal。
经典形态策略选股依赖本表；需先拉取日线数据，再执行本任务。
"""
import logging
from datetime import date, timedelta
from typing import Any, Optional

from ..db import get_conn

logger = logging.getLogger(__name__)

# 回溯天数（需至少约 40 日做形态判断）
LOOKBACK_DAYS = 65


def _float(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _get_codes_with_data(conn, codes: Optional[list[str]], limit: int = 8000) -> list[str]:
    """有日线数据的股票；若传入 codes 则在其范围内筛选。"""
    if codes:
        codes = [str(c).strip() for c in codes if c and str(c).strip()]
    with conn.cursor() as cur:
        if codes:
            placeholders = ",".join(["%s"] * len(codes))
            cur.execute(
                f"""
                SELECT DISTINCT code FROM stex.stock_day
                WHERE code IN ({placeholders})
                GROUP BY code HAVING COUNT(*) >= 40
                ORDER BY code
                LIMIT %s
                """,
                (*codes, limit),
            )
        else:
            cur.execute(
                """
                SELECT code FROM (
                    SELECT code, COUNT(*) AS cnt FROM stex.stock_day GROUP BY code HAVING COUNT(*) >= 40
                ) t ORDER BY code LIMIT %s
                """,
                (limit,),
            )
        return [str(r[0]) for r in cur.fetchall()]


def _fetch_days(conn, code: str, limit: int = LOOKBACK_DAYS) -> list[dict]:
    """近 limit 日 日线，按日期升序。"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, open, high, low, close, volume
            FROM stex.stock_day WHERE code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (code, limit),
        )
        rows = cur.fetchall()
    if not rows:
        return []
    by_date = {}
    for r in rows:
        td = r[0]
        td_str = td.isoformat()[:10] if hasattr(td, "isoformat") else str(td)[:10]
        by_date[td_str] = {
            "trade_date": td_str,
            "open": _float(r[1]),
            "high": _float(r[2]),
            "low": _float(r[3]),
            "close": _float(r[4]),
            "volume": _float(r[5]) or 0,
        }
    dates_sorted = sorted(by_date.keys())
    return [by_date[d] for d in dates_sorted]


def _detect_cup_handle(days: list[dict]) -> Optional[str]:
    """
    简化杯柄：约 40 日内先有回落（杯），再回升并出现小幅回踩后收高（柄）。
    条件：区间内最低价出现在中段；最后几日有“柄”状回踩且最后一根收在近期高位附近。
    """
    if len(days) < 40:
        return None
    window = days[-40:]
    highs = [w["high"] for w in window if w["high"] is not None]
    lows = [w["low"] for w in window if w["low"] is not None]
    closes = [w["close"] for w in window if w["close"] is not None]
    if not highs or not lows or not closes:
        return None
    cup_low = min(lows)
    cup_high_start = max(highs[:8]) if len(highs) >= 8 else highs[0]
    cup_high_end = max(highs[-8:]) if len(highs) >= 8 else highs[-1]
    low_idx = min(range(len(lows)), key=lambda i: lows[i] if lows[i] is not None else 1e18)
    # 最低点落在中段（约 1/4 ～ 3/4）
    if low_idx < 8 or low_idx > 32:
        return None
    # 后期回升：后半段高点接近或超过前半段
    if cup_high_end < cup_high_start * 0.92:
        return None
    # 最后 5 根：有回踩（某根最低 < 前几根最高）且最后一根收高
    last5 = window[-5:]
    last5_highs = [x["high"] for x in last5 if x["high"] is not None]
    last5_lows = [x["low"] for x in last5 if x["low"] is not None]
    last_close = closes[-1]
    if not last5_highs or not last5_lows:
        return None
    handle_high = max(last5_highs)
    handle_low = min(last5_lows)
    # 柄：回踩幅度约 2%～15%，且最后一根收在柄区间上半部分
    pullback = (handle_high - handle_low) / handle_high if handle_high and handle_high > 0 else 0
    if pullback < 0.01 or pullback > 0.2:
        return None
    if last_close < handle_low + (handle_high - handle_low) * 0.5:
        return None
    return window[-1]["trade_date"]


def _detect_rising_three(days: list[dict]) -> Optional[str]:
    """
    简化上升三法：一根明显阳线后，三根小阴/小阳（实体在首日范围内），再一根放量阳线突破首日高点。
    """
    if len(days) < 5:
        return None
    d0, d1, d2, d3, d4 = days[-5], days[-4], days[-3], days[-2], days[-1]
    for d in (d0, d1, d2, d3, d4):
        if d.get("open") is None or d.get("close") is None or d.get("high") is None or d.get("low") is None:
            return None
    o0, c0, h0, l0 = d0["open"], d0["close"], d0["high"], d0["low"]
    o4, c4, h4 = d4["open"], d4["close"], d4["high"]
    if o0 <= 0:
        return None
    # 首日阳线：涨幅 >= 1.5%
    if c0 <= o0 or (c0 - o0) / o0 < 0.015:
        return None
    # 第四日（最后一根）阳线且收盘突破首日高点
    if c4 <= o4 or c4 <= h0:
        return None
    # 中间三根：高点不超过首日高点太多，低点不低于首日低点太多（允许小幅震荡）
    for d in (d1, d2, d3):
        h, l_ = d["high"], d["low"]
        if h > h0 * 1.02 or l_ < l0 * 0.98:
            return None
    return d4["trade_date"]


def run_pattern_agent(codes: Optional[list[str]] = None, limit_codes: int = 8000) -> dict[str, Any]:
    """
    对指定或全量有日线数据的股票做形态识别，写入 stex.pattern_signal。
    codes 为空时：对有足够日线的股票全量扫描（受 limit_codes 限制）。
    """
    try:
        with get_conn() as conn:
            code_list = _get_codes_with_data(conn, codes, limit=limit_codes)
        if not code_list:
            return {"ok": True, "codes_processed": 0, "cup_handle": 0, "rising_three": 0, "message": "无足够日线数据的股票"}

        cup_handle_count = 0
        rising_three_count = 0
        inserted = []

        with get_conn() as conn:
            for code in code_list:
                days = _fetch_days(conn, code, limit=LOOKBACK_DAYS)
                if len(days) < 40:
                    continue
                ref_cup = _detect_cup_handle(days)
                ref_rise = _detect_rising_three(days)
                with conn.cursor() as cur:
                    if ref_cup:
                        cur.execute(
                            """
                            INSERT INTO stex.pattern_signal (code, pattern_type, ref_date)
                            VALUES (%s, 'cup_handle', %s::date)
                            ON CONFLICT (code, pattern_type, ref_date) DO NOTHING
                            """,
                            (code, ref_cup),
                        )
                        if cur.rowcount and cur.rowcount > 0:
                            cup_handle_count += 1
                    if ref_rise:
                        cur.execute(
                            """
                            INSERT INTO stex.pattern_signal (code, pattern_type, ref_date)
                            VALUES (%s, 'rising_three', %s::date)
                            ON CONFLICT (code, pattern_type, ref_date) DO NOTHING
                            """,
                            (code, ref_rise),
                        )
                        if cur.rowcount and cur.rowcount > 0:
                            rising_three_count += 1
            conn.commit()

        # 近 60 日形态信号总数（含历史运行写入的）
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pattern_type, COUNT(*) FROM stex.pattern_signal WHERE ref_date >= CURRENT_DATE - INTERVAL '60 days' GROUP BY pattern_type"
                )
                counts = dict(cur.fetchall())
        cup_handle_total = counts.get("cup_handle", 0)
        rising_three_total = counts.get("rising_three", 0)

        return {
            "ok": True,
            "codes_processed": len(code_list),
            "cup_handle": cup_handle_total,
            "rising_three": rising_three_total,
            "message": f"扫描 {len(code_list)} 只，近60日杯柄 {cup_handle_total} 条、上升三法 {rising_three_total} 条",
        }
    except Exception as e:
        logger.exception("pattern_agent")
        return {"ok": False, "error": str(e), "codes_processed": 0, "cup_handle": 0, "rising_three": 0}
