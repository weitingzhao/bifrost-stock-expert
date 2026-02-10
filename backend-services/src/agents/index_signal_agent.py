"""
Agent：计算大盘指数投资信号（仅用日线，无资金流数据）。
依赖：stex.index_day。
结果写入 stex.signals，code 存指数代码。信号类型：成交量MA20、成交量涨跌幅、均线金叉死叉、均线多空排列、支撑阻力位、量价背离、波动率突破。
"""
import logging
from typing import Any, Optional

from ..db import get_conn

logger = logging.getLogger(__name__)

# 与 index_data_agent 一致
INDEX_NAMES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
}
INDEX_CODES = list(INDEX_NAMES.keys())

DIR_BULL = "看涨"
DIR_BEAR = "看跌"
DIR_NEUTRAL = "中性"
DIR_NONE = "无信号"

SIG_VOL_MA20 = "成交量MA20"  # 放量 + 与 MA20 位置（无资金参考）
SIG_VOL_PCT = "成交量涨跌幅"
SIG_MA_CROSS = "均线金叉死叉"
SIG_MA_ALIGN = "均线多空排列"  # MA5/10/20 多头或空头排列
SIG_SUPPORT_RESIST = "支撑阻力位"
SIG_VOL_PRICE_DIV = "量价背离"  # 价创新高/新低且量缩
SIG_VOLATILITY_BREAK = "波动率突破"  # 突破前N日高低点或波动率放大


def _float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fetch_index_days(conn, index_code: str, limit: int = 65) -> list[dict]:
    """index_day 日线：trade_date, open, high, low, close, volume（升序日期），volume 用 vol 字段"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, open, high, low, close, vol
            FROM stex.index_day WHERE index_code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (index_code, limit),
        )
        rows = cur.fetchall()
    if not rows:
        return []
    by_date = {}
    for r in rows:
        td = r[0]
        if hasattr(td, "isoformat"):
            td = td.isoformat()[:10]
        else:
            td = str(td)[:10]
        by_date[td] = {
            "trade_date": td,
            "open": _float(r[1]),
            "high": _float(r[2]),
            "low": _float(r[3]),
            "close": _float(r[4]),
            "volume": _float(r[5]) or 0,
        }
    dates_sorted = sorted(by_date.keys())
    return [by_date[d] for d in dates_sorted]


def _compute_ma(conn, index_code: str, limit: int = 65) -> dict[str, dict]:
    """从 index_day.close 计算 MA5/MA10/MA20，返回 trade_date -> { ma5, ma10, ma20 }"""
    days = _fetch_index_days(conn, index_code, limit=limit)
    if len(days) < 20:
        return {}
    closes = [d["close"] for d in days]
    out = {}
    for i in range(19, len(days)):
        td = days[i]["trade_date"]
        c = closes[i]
        if c is None:
            continue
        ma5 = sum(closes[i - 4 : i + 1]) / 5 if all(closes[i - 4 : i + 1]) else None
        ma10 = sum(closes[i - 9 : i + 1]) / 10 if i >= 9 and all(closes[i - 9 : i + 1]) else None
        ma20 = sum(closes[i - 19 : i + 1]) / 20 if all(closes[i - 19 : i + 1]) else None
        out[td] = {"ma5": _float(ma5), "ma10": _float(ma10), "ma20": _float(ma20)}
    return out


def _signal_vol_pct(
    day: dict,
    days: list[dict],
    avg_vol_days: int = 5,
    volume_ratio_th: float = 1.2,
) -> tuple[str, str]:
    """成交量+涨跌幅：放量上涨→看涨；缩量下跌→看跌；放量下跌→中性；缩量上涨→中性"""
    td = day["trade_date"]
    close = day.get("close")
    vol = day.get("volume") or 0
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or idx < 1 or idx < avg_vol_days:
        return DIR_NONE, "历史日数不足"
    prev = days[idx - 1]
    prev_close = prev.get("close")
    if close is None or prev_close is None or prev_close == 0:
        return DIR_NONE, "缺收盘价"
    pct = (close - prev_close) / prev_close
    avg_vol = sum((days[j].get("volume") or 0) for j in range(idx - avg_vol_days, idx)) / avg_vol_days
    if avg_vol <= 0:
        return DIR_NEUTRAL, "均量无效"
    is_volume_up = vol >= volume_ratio_th * avg_vol
    is_up = pct > 0
    if is_volume_up and is_up:
        return DIR_BULL, "放量上涨"
    if not is_volume_up and not is_up:
        return DIR_BEAR, "缩量下跌"
    if is_volume_up and not is_up:
        return DIR_NEUTRAL, "放量下跌"
    return DIR_NEUTRAL, "缩量上涨"


def _signal_vol_ma20(
    day: dict,
    days: list[dict],
    tech: dict,
    avg_vol_days: int = 5,
    volume_ratio_th: float = 1.2,
) -> tuple[str, str]:
    """成交量 + MA20 位置（无资金）：低位放量→看涨，高位放量→中性，缩量→中性"""
    td = day["trade_date"]
    close = day.get("close")
    vol = day.get("volume") or 0
    ma20 = tech.get(td, {}).get("ma20")
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or idx < avg_vol_days or close is None:
        return DIR_NONE, "历史日数不足或缺收盘价"
    avg_vol = sum((days[j].get("volume") or 0) for j in range(idx - avg_vol_days, idx)) / avg_vol_days
    if avg_vol <= 0:
        return DIR_NEUTRAL, "均量无效"
    is_volume_up = vol >= volume_ratio_th * avg_vol
    if not is_volume_up:
        return DIR_NEUTRAL, "未放量"
    if ma20 is None:
        return DIR_NEUTRAL, "无MA20"
    if close < ma20:
        return DIR_BULL, "低位放量"
    return DIR_NEUTRAL, "高位放量"


def _signal_ma_cross(
    td: str,
    tech: dict,
    dates_sorted: list[str],
) -> tuple[str, str]:
    """MA5/MA10/MA20 金叉/死叉"""
    if td not in dates_sorted:
        return DIR_NONE, "无该日技术指标"
    idx = dates_sorted.index(td)
    if idx < 1:
        return DIR_NEUTRAL, "无前一日数据"
    prev_td = dates_sorted[idx - 1]
    t = tech.get(td, {})
    p = tech.get(prev_td, {})
    ma5, ma10, ma20 = t.get("ma5"), t.get("ma10"), t.get("ma20")
    p5, p10, p20 = p.get("ma5"), p.get("ma10"), p.get("ma20")
    if None in (ma5, ma20, p5, p20):
        return DIR_NEUTRAL, "均线数据不全"
    golden = (p5 <= p20 and ma5 > ma20) or (ma10 is not None and p10 is not None and p10 <= p20 and ma10 > ma20)
    death = (p5 >= p20 and ma5 < ma20) or (ma10 is not None and p10 is not None and p10 >= p20 and ma10 < ma20)
    if golden:
        return DIR_BULL, "均线金叉"
    if death:
        return DIR_BEAR, "均线死叉"
    return DIR_NEUTRAL, "无金叉死叉"


def _signal_ma_align(td: str, tech: dict) -> tuple[str, str]:
    """均线多空排列：MA5>MA10>MA20→看涨，MA5<MA10<MA20→看跌，否则中性"""
    t = tech.get(td, {})
    ma5, ma10, ma20 = t.get("ma5"), t.get("ma10"), t.get("ma20")
    if None in (ma5, ma10, ma20):
        return DIR_NEUTRAL, "均线数据不全"
    if ma5 > ma10 > ma20:
        return DIR_BULL, "多头排列"
    if ma5 < ma10 < ma20:
        return DIR_BEAR, "空头排列"
    return DIR_NEUTRAL, "均线纠缠"


def _signal_support_resist(
    day: dict,
    days: list[dict],
    look: int = 20,
    near_pct: float = 0.02,
) -> tuple[str, str]:
    """K线接近支撑/阻力"""
    td = day["trade_date"]
    close = day.get("close")
    low = day.get("low")
    high = day.get("high")
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or close is None or idx < look:
        return DIR_NONE, "历史K线不足"
    window = days[max(0, idx - look) : idx]
    if not window:
        return DIR_NEUTRAL, "无前段区间"
    support = min(d.get("low") or close for d in window)
    resistance = max(d.get("high") or close for d in window)
    if support is None or resistance is None:
        return DIR_NEUTRAL, "无有效高低点"
    thr = near_pct * (resistance - support) if resistance != support else 0.01 * close
    if thr <= 0:
        return DIR_NEUTRAL, "区间无波动"
    near_support = (low is not None and low <= support + thr) or (close <= support + thr)
    near_resist = (high is not None and high >= resistance - thr) or (close >= resistance - thr)
    if near_support and not near_resist:
        return DIR_BULL, "触及或接近关键支撑位"
    if near_resist and not near_support:
        return DIR_BEAR, "触及或接近关键阻力位"
    if near_support and near_resist:
        return DIR_NEUTRAL, "同时接近支撑与阻力"
    return DIR_NEUTRAL, "未触及关键支撑/阻力位"


def _signal_vol_price_divergence(
    day: dict,
    days: list[dict],
    look: int = 10,
    avg_vol_days: int = 5,
) -> tuple[str, str]:
    """量价背离：价创新高且量缩→看跌（顶背离），价创新低且量缩→看涨（底背离），否则中性"""
    td = day["trade_date"]
    close = day.get("close")
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or close is None or idx < look or idx < avg_vol_days * 2:
        return DIR_NONE, "历史日数不足"
    window = days[idx - look : idx + 1]
    if len(window) < look:
        return DIR_NEUTRAL, "区间不足"
    closes = [d.get("close") for d in window if d.get("close") is not None]
    vols = [(d.get("volume") or 0) for d in window]
    if not closes or len(vols) < avg_vol_days * 2:
        return DIR_NEUTRAL, "数据不足"
    recent_vol = sum(vols[-avg_vol_days:]) / avg_vol_days if vols[-avg_vol_days:] else 0
    prev_vol = sum(vols[-avg_vol_days * 2 : -avg_vol_days]) / avg_vol_days if len(vols) >= avg_vol_days * 2 else 0
    if prev_vol <= 0:
        return DIR_NEUTRAL, "前段均量无效"
    is_volume_shrink = recent_vol < prev_vol
    max_c = max(closes)
    min_c = min(closes)
    if close >= max_c and is_volume_shrink:
        return DIR_BEAR, "顶背离(价创新高量缩)"
    if close <= min_c and is_volume_shrink:
        return DIR_BULL, "底背离(价创新低量缩)"
    return DIR_NEUTRAL, "无显著量价背离"


def _signal_volatility_breakout(
    day: dict,
    days: list[dict],
    look: int = 20,
    vol_expand_ratio: float = 1.2,
) -> tuple[str, str]:
    """波动率突破：收盘突破前N日高点→看涨，跌破前N日低点→看跌；若无突破但波动率明显放大→中性"""
    td = day["trade_date"]
    close = day.get("close")
    high = day.get("high")
    low = day.get("low")
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or close is None or idx < look:
        return DIR_NONE, "历史日数不足"
    prev_window = days[max(0, idx - look) : idx]
    if not prev_window:
        return DIR_NEUTRAL, "无前段区间"
    highs = [d.get("high") if d.get("high") is not None else d.get("close") for d in prev_window]
    lows = [d.get("low") if d.get("low") is not None else d.get("close") for d in prev_window]
    resistance = max(v for v in highs if v is not None) if highs else 0
    support = min(v for v in lows if v is not None) if lows else 0
    if resistance <= 0 or support <= 0:
        return DIR_NEUTRAL, "无有效高低点"
    if close > resistance:
        return DIR_BULL, "突破前段高点"
    if close < support:
        return DIR_BEAR, "跌破前段低点"
    # 波动率放大：近5日振幅均值 > 前10日振幅均值的 vol_expand_ratio 倍
    if idx >= 15:
        recent_5 = days[idx - 5 : idx + 1]
        prev_10 = days[idx - 15 : idx - 5]
        def avg_range(win):
            r = []
            for d in win:
                h, l_, c = d.get("high"), d.get("low"), d.get("close") or 0
                if (h is not None and l_ is not None) and (h - l_) > 0 and c and c > 0:
                    r.append((h - l_) / c)
            return sum(r) / len(r) if r else 0
        ar5 = avg_range(recent_5)
        ar10 = avg_range(prev_10)
        if ar10 > 0 and ar5 >= vol_expand_ratio * ar10:
            return DIR_NEUTRAL, "波动率放大"
    return DIR_NEUTRAL, "未突破"


def _upsert_signal(
    conn,
    code: str,
    ref_date: str,
    signal_type: str,
    direction: str,
    reason: str,
    source: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.signals (code, signal_type, direction, ref_date, reason, source)
            VALUES (%s, %s, %s, %s::date, %s, %s)
            ON CONFLICT (code, ref_date, signal_type) WHERE ref_date IS NOT NULL
            DO UPDATE SET direction = EXCLUDED.direction, reason = EXCLUDED.reason, source = EXCLUDED.source
            """,
            (code, signal_type, direction, ref_date, reason, source or "index_signal_agent"),
        )


def run_index_signal_agent(days_per_code: int = 30) -> dict[str, Any]:
    """
    对大盘指数（INDEX_CODES）计算 成交量MA20、成交量涨跌幅、均线金叉死叉、均线多空排列、支撑阻力位、量价背离、波动率突破 并入库。
    无资金流数据，仅用日线；写入 stex.signals（code=指数代码）。
    返回：{ ok, indices_processed, signals_written, error }
    """
    try:
        with get_conn() as conn:
            total_signals = 0
            for index_code in INDEX_CODES:
                days = _fetch_index_days(conn, index_code, limit=65)
                if len(days) < 25:
                    logger.warning("index_signal_agent: %s 日线不足", index_code)
                    continue
                tech = _compute_ma(conn, index_code, limit=65)
                dates_sorted = [d["trade_date"] for d in days]
                for d in days[-days_per_code:]:
                    td = d["trade_date"]
                    dir0, reason0 = _signal_vol_ma20(d, days, tech)
                    _upsert_signal(conn, index_code, td, SIG_VOL_MA20, dir0, reason0)
                    total_signals += 1
                    dir1, reason1 = _signal_vol_pct(d, days)
                    _upsert_signal(conn, index_code, td, SIG_VOL_PCT, dir1, reason1)
                    total_signals += 1
                    dir2, reason2 = _signal_ma_cross(td, tech, dates_sorted)
                    _upsert_signal(conn, index_code, td, SIG_MA_CROSS, dir2, reason2)
                    total_signals += 1
                    dir_align, reason_align = _signal_ma_align(td, tech)
                    _upsert_signal(conn, index_code, td, SIG_MA_ALIGN, dir_align, reason_align)
                    total_signals += 1
                    dir3, reason3 = _signal_support_resist(d, days, look=20)
                    _upsert_signal(conn, index_code, td, SIG_SUPPORT_RESIST, dir3, reason3)
                    total_signals += 1
                    dir_div, reason_div = _signal_vol_price_divergence(d, days, look=10)
                    _upsert_signal(conn, index_code, td, SIG_VOL_PRICE_DIV, dir_div, reason_div)
                    total_signals += 1
                    dir_break, reason_break = _signal_volatility_breakout(d, days, look=20)
                    _upsert_signal(conn, index_code, td, SIG_VOLATILITY_BREAK, dir_break, reason_break)
                    total_signals += 1
                conn.commit()
        return {
            "ok": True,
            "indices_processed": len(INDEX_CODES),
            "signals_written": total_signals,
        }
    except Exception as e:
        logger.exception("index_signal_agent failed")
        return {"ok": False, "error": str(e), "indices_processed": 0, "signals_written": 0}
