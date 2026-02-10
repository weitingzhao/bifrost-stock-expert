"""
Agent：计算股票多维度投资信号，每个信号取值为 看涨 / 看跌 / 中性 / 无信号。
依赖：stex.stock_day、stex.technicals、stex.moneyflow（含细粒度特大单）。
结果写入 stex.signals，按 (code, ref_date, signal_type) 覆盖。
"""
import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Optional

from ..db import get_conn

logger = logging.getLogger(__name__)

DIR_BULL = "看涨"
DIR_BEAR = "看跌"
DIR_NEUTRAL = "中性"
DIR_NONE = "无信号"

# 信号类型（与 spec 一致）
SIG_VOL_MF_MA20 = "成交量资金MA20"
SIG_VOL_PCT = "成交量涨跌幅"
SIG_SUSTAINED_MF = "持续资金流向"
SIG_MA_CROSS = "均线金叉死叉"
SIG_MAIN_FORCE = "主力资金"
SIG_SUPPORT_RESIST = "支撑阻力位"
SIG_TURNOVER = "换手率"  # 交投清淡(0-3%)/正常活跃(3-10%)/异常活跃(>10%)


def _ensure_conn():
    from ..db import get_conn
    return get_conn()


def _float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _get_codes(conn, codes: Optional[list[str]]) -> list[str]:
    if codes:
        return [str(c).strip() for c in codes if c and str(c).strip()]
    with conn.cursor() as cur:
        cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
        return [str(r[0]) for r in cur.fetchall()]


def _fetch_days(conn, code: str, limit: int = 65) -> list[dict]:
    """近 limit 日 日线：trade_date, open, high, low, close, volume, turnover_rate（升序日期）"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, open, high, low, close, volume, turnover_rate
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
            "turnover_rate": _float(r[6]) if len(r) > 6 else None,
        }
    dates_sorted = sorted(by_date.keys())
    return [by_date[d] for d in dates_sorted]


def _fetch_technicals(conn, code: str, limit: int = 65) -> dict[str, dict]:
    """trade_date -> { ma5, ma10, ma20 }"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, ma5, ma10, ma20
            FROM stex.technicals WHERE code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (code, limit),
        )
        rows = cur.fetchall()
    out = {}
    for r in rows:
        td = r[0]
        if hasattr(td, "isoformat"):
            td = td.isoformat()[:10]
        else:
            td = str(td)[:10]
        out[td] = {"ma5": _float(r[1]), "ma10": _float(r[2]), "ma20": _float(r[3])}
    return out


def _fetch_moneyflow(conn, code: str, limit: int = 65) -> dict[str, dict]:
    """trade_date -> { net_mf_amount, buy_elg_amount, sell_elg_amount }"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, net_mf_amount, buy_elg_amount, sell_elg_amount
            FROM stex.moneyflow WHERE code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (code, limit),
        )
        rows = cur.fetchall()
    out = {}
    for r in rows:
        td = r[0]
        if hasattr(td, "isoformat"):
            td = td.isoformat()[:10]
        else:
            td = str(td)[:10]
        net = _float(r[1])
        buy_elg = _float(r[2])
        sell_elg = _float(r[3])
        out[td] = {
            "net_mf_amount": net,
            "buy_elg_amount": buy_elg,
            "sell_elg_amount": sell_elg,
            "net_elg": (buy_elg - sell_elg) if (buy_elg is not None and sell_elg is not None) else None,
        }
    return out


def _signal_vol_mf_ma20(
    day: dict,
    days: list[dict],
    tech: dict,
    mf: dict,
    avg_vol_days: int = 5,
    volume_ratio_th: float = 1.2,
) -> tuple[str, str]:
    """成交量+资金+MA20：高位放量净流入→中性；低位放量净流入→看涨；低位放量净流出→看跌；否则中性/无信号"""
    td = day["trade_date"]
    close = day.get("close")
    vol = day.get("volume") or 0
    net_mf = mf.get(td, {}).get("net_mf_amount")
    ma20 = tech.get(td, {}).get("ma20")
    if close is None or vol <= 0:
        return DIR_NONE, "缺日线或成交量"
    idx = next((i for i, d in enumerate(days) if d["trade_date"] == td), None)
    if idx is None or idx < avg_vol_days:
        return DIR_NONE, "历史日数不足"
    avg_vol = sum((days[i].get("volume") or 0) for i in range(idx - avg_vol_days, idx)) / avg_vol_days
    if avg_vol <= 0:
        return DIR_NONE, "均量无效"
    is_volume_up = vol >= volume_ratio_th * avg_vol
    if not is_volume_up:
        return DIR_NEUTRAL, "未放量"
    if net_mf is None:
        return DIR_NEUTRAL, "无资金流向数据"
    is_inflow = net_mf > 0
    high_pos = (ma20 is not None and close > ma20) or (ma20 is None)
    low_pos = ma20 is not None and close < ma20
    if high_pos and is_inflow:
        return DIR_NEUTRAL, "高位放量净流入"
    if low_pos and is_inflow:
        return DIR_BULL, "低位放量净流入"
    if high_pos and not is_inflow:
        return DIR_NEUTRAL, "高位放量净流出"
    if low_pos and not is_inflow:
        return DIR_BEAR, "低位放量净流出"
    return DIR_NEUTRAL, "放量"


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
    avg_vol = sum((days[i].get("volume") or 0) for i in range(idx - avg_vol_days, idx)) / avg_vol_days
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


def _signal_sustained_mf(
    td: str,
    mf: dict,
    look_days: int = 5,
) -> tuple[str, str]:
    """3-5日持续净流入/净流出：持续净流入→看涨；持续净流出→看跌；否则中性"""
    dates_sorted = sorted(mf.keys(), reverse=True)
    if td not in dates_sorted:
        return DIR_NONE, "无该日资金数据"
    pos = dates_sorted.index(td)
    window = dates_sorted[pos : pos + look_days]
    if len(window) < look_days:
        return DIR_NEUTRAL, "不足连续天数"
    amounts = [mf.get(d, {}).get("net_mf_amount") for d in window]
    if any(x is None for x in amounts):
        return DIR_NEUTRAL, "部分日无净流入额"
    if all(x > 0 for x in amounts):
        return DIR_BULL, f"连续{look_days}日净流入"
    if all(x < 0 for x in amounts):
        return DIR_BEAR, f"连续{look_days}日净流出"
    return DIR_NEUTRAL, "无持续单向净流入/净流出"


def _signal_ma_cross(
    td: str,
    tech: dict,
    dates_sorted: list[str],
) -> tuple[str, str]:
    """MA5/MA10/MA20 金叉/死叉：金叉→看涨；死叉→看跌；否则中性"""
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
    # 金叉：短期上穿长期（MA5 上穿 MA20 或 MA10 上穿 MA20）
    golden = (p5 <= p20 and ma5 > ma20) or (ma10 is not None and p10 is not None and p10 <= p20 and ma10 > ma20)
    death = (p5 >= p20 and ma5 < ma20) or (ma10 is not None and p10 is not None and p10 >= p20 and ma10 < ma20)
    if golden:
        return DIR_BULL, "均线金叉"
    if death:
        return DIR_BEAR, "均线死叉"
    return DIR_NEUTRAL, "无金叉死叉"


def _signal_main_force(td: str, mf: dict) -> tuple[str, str]:
    """主力(特大单)净流入/净流出：净流入→看涨；净流出→看跌；否则中性"""
    row = mf.get(td, {})
    net_elg = row.get("net_elg")
    if net_elg is None:
        return DIR_NEUTRAL, "无特大单数据"
    if net_elg > 0:
        return DIR_BULL, "主力净流入"
    if net_elg < 0:
        return DIR_BEAR, "主力净流出"
    return DIR_NEUTRAL, "主力无净流入流出"


def _signal_support_resist(
    day: dict,
    days: list[dict],
    look: int = 20,
    near_pct: float = 0.02,
) -> tuple[str, str]:
    """K线接近支撑/阻力：接近支撑→看涨；接近阻力→看跌；否则中性"""
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


def _signal_turnover(day: dict) -> tuple[str, str]:
    """换手率信号：交投清淡(0-3%)→减分，正常活跃(3-10%)→加分，异常活跃(>10%)→小幅加分。direction 存 交投清淡/正常活跃/异常活跃。"""
    rate = day.get("turnover_rate")
    if rate is None:
        return DIR_NONE, "无换手率数据"
    try:
        r = float(rate)
    except (TypeError, ValueError):
        return DIR_NONE, "换手率无效"
    if r < 0:
        return DIR_NONE, "换手率为负"
    if r < 3:
        return "交投清淡", f"换手率{r:.2f}%"
    if r <= 10:
        return "正常活跃", f"换手率{r:.2f}%"
    return "异常活跃", f"换手率{r:.2f}%"


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
            (code, signal_type, direction, ref_date, reason, source or "signal_agent"),
        )


def run_signal_agent(codes: Optional[list[str]] = None, days_per_code: int = 30) -> dict[str, Any]:
    """
    对指定股票（或 watchlist 全部）计算 6 类投资信号并入库。
    每只股票按最近 days_per_code 个交易日逐日计算，写入 stex.signals（按 code+ref_date+signal_type 覆盖）。
    返回：{ ok, codes_processed, signals_written, error }
    """
    try:
        with get_conn() as conn:
            code_list = _get_codes(conn, codes)
            if not code_list:
                return {"ok": True, "codes_processed": 0, "signals_written": 0, "message": "暂无股票"}

            total_signals = 0
            for code in code_list:
                days = _fetch_days(conn, code, limit=65)
                tech = _fetch_technicals(conn, code, limit=65)
                mf = _fetch_moneyflow(conn, code, limit=65)
                dates_sorted = [d["trade_date"] for d in days]
                # 只对最近 days_per_code 个交易日计算（避免重复历史）
                for d in days[-days_per_code:]:
                    td = d["trade_date"]
                    dir1, reason1 = _signal_vol_mf_ma20(d, days, tech, mf)
                    _upsert_signal(conn, code, td, SIG_VOL_MF_MA20, dir1, reason1)
                    total_signals += 1
                    dir2, reason2 = _signal_vol_pct(d, days)
                    _upsert_signal(conn, code, td, SIG_VOL_PCT, dir2, reason2)
                    total_signals += 1
                    dir3, reason3 = _signal_sustained_mf(td, mf, look_days=5)
                    _upsert_signal(conn, code, td, SIG_SUSTAINED_MF, dir3, reason3)
                    total_signals += 1
                    dir4, reason4 = _signal_ma_cross(td, tech, dates_sorted)
                    _upsert_signal(conn, code, td, SIG_MA_CROSS, dir4, reason4)
                    total_signals += 1
                    dir5, reason5 = _signal_main_force(td, mf)
                    _upsert_signal(conn, code, td, SIG_MAIN_FORCE, dir5, reason5)
                    total_signals += 1
                    dir6, reason6 = _signal_support_resist(d, days, look=20)
                    _upsert_signal(conn, code, td, SIG_SUPPORT_RESIST, dir6, reason6)
                    total_signals += 1
                    dir7, reason7 = _signal_turnover(d)
                    _upsert_signal(conn, code, td, SIG_TURNOVER, dir7, reason7)
                    total_signals += 1
                conn.commit()
        return {
            "ok": True,
            "codes_processed": len(code_list),
            "signals_written": total_signals,
        }
    except Exception as e:
        logger.exception("signal_agent failed")
        return {"ok": False, "error": str(e), "codes_processed": 0, "signals_written": 0}
