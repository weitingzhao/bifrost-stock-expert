"""
Agent：增量拉取全市场「最近一个交易日」（含今日）的日线行情与每日指标，用于快速更新最新一天数据。
收盘后执行可拉到当日数据；未收盘或数据源未更新时则为上一交易日。
逻辑对齐详情页「更新数据」：日线 + daily_basic + 自算 MA5/10/20 写 technicals + 对跟踪列表补当日资金流向。
写入 stex.stock_day、stex.fundamentals、stex.technicals；对 watchlist 写入 stex.moneyflow。
"""
import logging
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, Tuple

from ..db import get_conn

logger = logging.getLogger(__name__)
DELAY = 0.3  # 与 watchlist_data_agent 一致，请求间隔防限流


def _safe_num(v) -> Optional[Decimal]:
    if v is None or (isinstance(v, float) and (v != v)):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _ts_code_to_code(ts_code: str) -> str:
    """Tushare ts_code (000001.SZ) -> 6 位代码"""
    s = (ts_code or "").strip()
    return s.split(".")[0] if s else ""


def _upsert_stock_day(conn, code: str, trade_date: str, open_, high, low, close, vol, amount) -> None:
    """Tushare daily: vol=成交量(手), amount=成交额(千元) -> 存 volume=vol, amount=amount*1000"""
    amt = _safe_num(amount)
    if amt is not None:
        amt = amt * 1000
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.stock_day (code, trade_date, open, high, low, close, volume, amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, trade_date) DO UPDATE SET
              open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
              close = EXCLUDED.close, volume = EXCLUDED.volume, amount = EXCLUDED.amount
            """,
            (code, trade_date, _safe_num(open_), _safe_num(high), _safe_num(low), _safe_num(close), _safe_num(vol), amt),
        )


def _update_stock_day_turnover(conn, code: str, trade_date: str, turnover_rate) -> None:
    """将 daily_basic 的换手率写回 stock_day（按 code+trade_date 更新）。"""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE stex.stock_day SET turnover_rate = %s WHERE code = %s AND trade_date = %s::date",
            (_safe_num(turnover_rate), code, trade_date),
        )


def _upsert_fundamentals_row(conn, code: str, report_date: str, pe, pb, ps, market_cap) -> None:
    """写入当日 fundamentals 一条（仅 PE/PB/PS/市值）"""
    cap = _safe_num(market_cap)
    if cap is not None and cap < 1e10:
        cap = cap * 10000
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.fundamentals (code, report_date, pe, pb, ps, market_cap, revenue, net_profit, profit_growth, roe)
            VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL)
            ON CONFLICT (code, report_date) DO UPDATE SET
              pe = COALESCE(EXCLUDED.pe, stex.fundamentals.pe),
              pb = COALESCE(EXCLUDED.pb, stex.fundamentals.pb),
              ps = COALESCE(EXCLUDED.ps, stex.fundamentals.ps),
              market_cap = COALESCE(EXCLUDED.market_cap, stex.fundamentals.market_cap)
            """,
            (code, report_date, _safe_num(pe), _safe_num(pb), _safe_num(ps), cap),
        )


def _upsert_technicals_row(conn, code: str, trade_date: str, ma5, ma10, ma20) -> None:
    """写入当日 technicals 一条（仅 MA5/10/20，与详情页 pro_bar 一致）"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.technicals (code, trade_date, ma5, ma10, ma20, macd, macd_signal, macd_hist, rsi, kdj_k, kdj_d, kdj_j)
            VALUES (%s, %s::date, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            ON CONFLICT (code, trade_date) DO UPDATE SET
              ma5 = COALESCE(EXCLUDED.ma5, stex.technicals.ma5),
              ma10 = COALESCE(EXCLUDED.ma10, stex.technicals.ma10),
              ma20 = COALESCE(EXCLUDED.ma20, stex.technicals.ma20)
            """,
            (code, trade_date, _safe_num(ma5), _safe_num(ma10), _safe_num(ma20)),
        )


def _compute_ma_for_date(conn, code: str, trade_date: str, limit: int = 65) -> Tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
    """根据 stock_day 计算该 code 在 trade_date 的 MA5/10/20，与详情页 pro_bar(ma=[5,10,20]) 一致"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, close
            FROM stex.stock_day
            WHERE code = %s AND trade_date <= %s::date
            ORDER BY trade_date DESC
            LIMIT %s
            """,
            (code, trade_date, limit),
        )
        rows = cur.fetchall()
    if not rows:
        return (None, None, None)
    # 按日期升序，最后一天应为 trade_date
    by_date = {}
    for r in rows:
        td = r[0]
        if hasattr(td, "isoformat"):
            td = td.isoformat()[:10].replace("-", "")
        else:
            td = str(td)[:10].replace("-", "")
        by_date[td] = float(r[1]) if r[1] is not None else None
    sorted_dates = sorted(by_date.keys())
    if not sorted_dates or sorted_dates[-1] != trade_date:
        return (None, None, None)
    closes = [by_date[d] for d in sorted_dates if by_date[d] is not None]
    if not closes:
        return (None, None, None)
    n = len(closes)
    ma5 = sum(closes[-5:]) / min(5, n) if n >= 1 else None
    ma10 = sum(closes[-10:]) / min(10, n) if n >= 1 else None
    ma20 = sum(closes[-20:]) / min(20, n) if n >= 1 else None
    return (
        Decimal(str(round(ma5, 4))) if ma5 is not None else None,
        Decimal(str(round(ma10, 4))) if ma10 is not None else None,
        Decimal(str(round(ma20, 4))) if ma20 is not None else None,
    )


def _get_last_trade_date(pro) -> Optional[str]:
    """获取「含今天在内」的最近一个交易日，格式 YYYYMMDD。收盘后执行可拉到当日数据。"""
    from zoneinfo import ZoneInfo
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    end = today.strftime("%Y%m%d")  # 包含今天，收盘后 Tushare 有当日数据时可更新到今日
    start = (today - timedelta(days=14)).strftime("%Y%m%d")
    try:
        cal = pro.trade_cal(exchange="SSE", start_date=start, end_date=end, is_open="1")
        if cal is not None and not cal.empty and "cal_date" in cal.columns:
            return str(cal["cal_date"].max() or "")[:8]
    except Exception as e:
        logger.warning("trade_cal failed: %s", e)
    return (today - timedelta(days=1)).strftime("%Y%m%d")


def run_incremental_daily_agent(trade_date: Optional[str] = None) -> dict[str, Any]:
    """
    增量拉取全市场指定交易日的日线与每日指标；不传 trade_date 时使用「最近一个交易日（含今日）」。
    收盘后执行可更新到今日数据，与「更新指定股票数据」行为一致。
    使用 Tushare daily(trade_date) 与 daily_basic(trade_date) 各一次请求，全量写入该日数据。
    返回：ok, trade_date, rows_stock_day, rows_fundamentals, error?
    """
    try:
        from ..config import TUSHARE_TOKEN
        if not TUSHARE_TOKEN:
            return {"ok": False, "error": "请设置 TUSHARE_TOKEN", "rows_stock_day": 0, "rows_fundamentals": 0}
    except Exception:
        return {"ok": False, "error": "配置不可用", "rows_stock_day": 0, "rows_fundamentals": 0}

    try:
        import tushare as ts
    except ImportError:
        return {"ok": False, "error": "请安装 tushare: pip install tushare", "rows_stock_day": 0, "rows_fundamentals": 0}

    pro = ts.pro_api(TUSHARE_TOKEN)
    if not trade_date or len(str(trade_date).strip()) != 8:
        trade_date = _get_last_trade_date(pro)
    else:
        trade_date = str(trade_date).strip()[:8]

    if not trade_date:
        return {"ok": False, "error": "无法获取交易日", "rows_stock_day": 0, "rows_fundamentals": 0}

    rows_stock_day = 0
    rows_fundamentals = 0
    codes_with_day = set()  # 当日有日线的 code，用于后续补 technicals

    # 1) 全市场当日日线：一次请求（与详情页「更新数据」同源逻辑，保证数据一致）
    try:
        df = pro.daily(trade_date=trade_date)
    except Exception as e:
        logger.warning("daily(trade_date=%s) failed: %s", trade_date, e)
        return {
            "ok": False,
            "error": str(e),
            "trade_date": trade_date,
            "rows_stock_day": 0,
            "rows_fundamentals": 0,
        }

    with get_conn() as conn:
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                ts_code = str(row.get("ts_code", ""))
                code = _ts_code_to_code(ts_code)
                if not code:
                    continue
                td = str(row.get("trade_date", ""))[:10].replace("-", "") or trade_date
                if len(td) != 8:
                    td = trade_date
                o, h, l, c = row.get("open"), row.get("high"), row.get("low"), row.get("close")
                vol, amt = row.get("vol"), row.get("amount")
                _upsert_stock_day(conn, code, td, o, h, l, c, vol, amt)
                rows_stock_day += 1
                codes_with_day.add(code)
        conn.commit()

    # 2) 全市场当日每日指标（PE/PB/市值/换手率）：一次请求
    try:
        df_basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,trade_date,pe,pb,ps,total_mv,turnover_rate")
    except Exception as e:
        logger.warning("daily_basic(trade_date=%s) failed: %s", trade_date, e)
        return {
            "ok": True,
            "trade_date": trade_date,
            "rows_stock_day": rows_stock_day,
            "rows_fundamentals": rows_fundamentals,
            "message": f"日线已写入 {rows_stock_day} 条，daily_basic 失败: {e}",
        }

    with get_conn() as conn:
        if df_basic is not None and not df_basic.empty:
            for _, row in df_basic.iterrows():
                ts_code = str(row.get("ts_code", ""))
                code = _ts_code_to_code(ts_code)
                if not code:
                    continue
                td = str(row.get("trade_date", ""))[:10].replace("-", "") or trade_date
                if len(td) != 8:
                    td = trade_date
                _upsert_fundamentals_row(
                    conn,
                    code,
                    td,
                    row.get("pe"),
                    row.get("pb"),
                    row.get("ps"),
                    row.get("total_mv"),
                )
                rows_fundamentals += 1
                if row.get("turnover_rate") is not None:
                    _update_stock_day_turnover(conn, code, td, row.get("turnover_rate"))
        conn.commit()

    # 3) 按 stock_day 自算当日 MA5/10/20 写入 technicals（与详情页 pro_bar ma=[5,10,20] 一致，供信号计算）
    rows_technicals = 0
    if codes_with_day:
        with get_conn() as conn:
            for code in codes_with_day:
                ma5, ma10, ma20 = _compute_ma_for_date(conn, code, trade_date)
                if ma5 is not None or ma10 is not None or ma20 is not None:
                    _upsert_technicals_row(conn, code, trade_date, ma5, ma10, ma20)
                    rows_technicals += 1
            conn.commit()
        logger.info("incremental_daily: technicals(MA) written %s rows for %s", rows_technicals, trade_date)

    # 4) 对跟踪列表补当日资金流向（与详情页「更新数据」一致，需 Tushare 2000+ 积分）
    from ..agents.watchlist_data_agent import _code_to_ts_code, _upsert_moneyflow
    watchlist_codes = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
            watchlist_codes = [str(r[0]) for r in cur.fetchall()]
    rows_moneyflow = 0
    if watchlist_codes:
        with get_conn() as conn:
            for code in watchlist_codes:
                time.sleep(DELAY)
                ts_code = _code_to_ts_code(code)
                if not ts_code:
                    continue
                try:
                    mf = pro.moneyflow(ts_code=ts_code, start_date=trade_date, end_date=trade_date)
                    if mf is not None and not mf.empty:
                        for _, r in mf.iterrows():
                            td = str(r.get("trade_date", ""))
                            if td:
                                _upsert_moneyflow(
                                    conn,
                                    code,
                                    td,
                                    r.get("net_mf_amount"),
                                    r.get("net_mf_vol"),
                                    buy_sm_amount=r.get("buy_sm_amount"),
                                    sell_sm_amount=r.get("sell_sm_amount"),
                                    buy_sm_vol=r.get("buy_sm_vol"),
                                    sell_sm_vol=r.get("sell_sm_vol"),
                                    buy_md_amount=r.get("buy_md_amount"),
                                    sell_md_amount=r.get("sell_md_amount"),
                                    buy_md_vol=r.get("buy_md_vol"),
                                    sell_md_vol=r.get("sell_md_vol"),
                                    buy_lg_amount=r.get("buy_lg_amount"),
                                    sell_lg_amount=r.get("sell_lg_amount"),
                                    buy_lg_vol=r.get("buy_lg_vol"),
                                    sell_lg_vol=r.get("sell_lg_vol"),
                                    buy_elg_amount=r.get("buy_elg_amount"),
                                    sell_elg_amount=r.get("sell_elg_amount"),
                                    buy_elg_vol=r.get("buy_elg_vol"),
                                    sell_elg_vol=r.get("sell_elg_vol"),
                                )
                                rows_moneyflow += 1
                except Exception as e:
                    logger.warning("moneyflow %s for %s: %s", code, trade_date, e)
            conn.commit()
        logger.info("incremental_daily: moneyflow written %s rows (watchlist) for %s", rows_moneyflow, trade_date)

    return {
        "ok": True,
        "trade_date": trade_date,
        "rows_stock_day": rows_stock_day,
        "rows_fundamentals": rows_fundamentals,
        "rows_technicals": rows_technicals,
        "rows_moneyflow": rows_moneyflow,
        "message": f"增量更新 {trade_date}：日线 {rows_stock_day}，每日指标 {rows_fundamentals}，技术指标(MA) {rows_technicals}，资金流向(跟踪) {rows_moneyflow}",
    }
