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


def _get_trade_dates_between(pro, start_date: str, end_date: str) -> list[str]:
    """获取 [start_date, end_date] 区间内（含两端）的所有交易日，格式 YYYYMMDD，升序。"""
    start_str = str(start_date).strip().replace("-", "")[:8]
    end_str = str(end_date).strip().replace("-", "")[:8]
    if not start_str or not end_str or start_str > end_str:
        return []
    try:
        cal = pro.trade_cal(exchange="SSE", start_date=start_str, end_date=end_str, is_open="1")
        if cal is not None and not cal.empty and "cal_date" in cal.columns:
            dates = sorted([str(d)[:8] for d in cal["cal_date"].tolist()])
            return dates
    except Exception as e:
        logger.warning("trade_cal between %s %s failed: %s", start_str, end_str, e)
    return []


def _normalize_date(s: Optional[str]) -> Optional[str]:
    """统一为 YYYYMMDD 8 位，支持 YYYY-MM-DD / YYYYMMDD。"""
    if not s or not str(s).strip():
        return None
    s = str(s).strip().replace("-", "")[:8]
    return s if len(s) == 8 else None


def run_incremental_daily_agent(
    trade_date: Optional[str] = None,
    start_date: Optional[str] = None,
) -> dict[str, Any]:
    """
    增量拉取全市场日线与每日指标。
    - 若传 start_date（YYYYMMDD 或 YYYY-MM-DD）：拉取从 start_date（含）到最近交易日之间所有交易日的数据，避免节假日误判。
    - 若只传 trade_date：仅拉取该交易日。
    - 都不传：使用「最近一个交易日（含今日）」拉取单日。
    使用 Tushare daily(trade_date) 与 daily_basic(trade_date) 各一次请求/日，全量写入。
    返回：ok, trade_date(s), rows_stock_day, rows_fundamentals, dates_updated?, error?
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

    # 确定要处理的交易日列表
    if _normalize_date(start_date):
        # 起始日期模式：从 start_date 到最近交易日（含）之间的所有交易日
        end_d = _get_last_trade_date(pro)
        if not end_d:
            return {"ok": False, "error": "无法获取最近交易日", "rows_stock_day": 0, "rows_fundamentals": 0}
        dates_to_process = _get_trade_dates_between(pro, start_date, end_d)
        if not dates_to_process:
            return {"ok": False, "error": f"区间内无交易日: {start_date} ~ {end_d}", "rows_stock_day": 0, "rows_fundamentals": 0}
        logger.info("incremental_daily: start_date=%s -> %d days from %s to %s", start_date, len(dates_to_process), dates_to_process[0], dates_to_process[-1])
    elif trade_date and _normalize_date(trade_date):
        dates_to_process = [_normalize_date(trade_date)]
    else:
        single = _get_last_trade_date(pro)
        if not single:
            return {"ok": False, "error": "无法获取交易日", "rows_stock_day": 0, "rows_fundamentals": 0}
        dates_to_process = [single]

    total_stock_day = 0
    total_fundamentals = 0
    total_technicals = 0
    total_moneyflow = 0
    from ..agents.watchlist_data_agent import _code_to_ts_code, _upsert_moneyflow, _upsert_financial
    watchlist_codes = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
            watchlist_codes = [str(r[0]) for r in cur.fetchall()]

    for trade_date in dates_to_process:
        rows_stock_day = 0
        rows_fundamentals = 0
        codes_with_day = set()

        # 1) 全市场当日日线
        try:
            df = pro.daily(trade_date=trade_date)
        except Exception as e:
            logger.warning("daily(trade_date=%s) failed: %s", trade_date, e)
            return {
                "ok": False,
                "error": str(e),
                "trade_date": trade_date,
                "dates_updated": dates_to_process[: dates_to_process.index(trade_date)] if trade_date in dates_to_process else [],
                "rows_stock_day": total_stock_day,
                "rows_fundamentals": total_fundamentals,
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

        # 2) 全市场当日每日指标
        try:
            df_basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,trade_date,pe,pb,ps,total_mv,turnover_rate")
        except Exception as e:
            logger.warning("daily_basic(trade_date=%s) failed: %s", trade_date, e)
            total_stock_day += rows_stock_day
            total_fundamentals += rows_fundamentals
            continue

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

        # 3) 当日 MA 写入 technicals
        rows_technicals = 0
        if codes_with_day:
            with get_conn() as conn:
                for code in codes_with_day:
                    ma5, ma10, ma20 = _compute_ma_for_date(conn, code, trade_date)
                    if ma5 is not None or ma10 is not None or ma20 is not None:
                        _upsert_technicals_row(conn, code, trade_date, ma5, ma10, ma20)
                        rows_technicals += 1
                conn.commit()
        total_technicals += rows_technicals

        # 4) 跟踪列表当日资金流向
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
        total_moneyflow += rows_moneyflow

        total_stock_day += rows_stock_day
        total_fundamentals += rows_fundamentals
        logger.info("incremental_daily: %s -> 日线 %s 指标 %s MA %s 资金(跟踪) %s", trade_date, rows_stock_day, rows_fundamentals, rows_technicals, rows_moneyflow)

    # 5) 对 watchlist 补财务指标（季报：利润表+资产负债表），与 watchlist_data_agent 对齐
    total_financial = 0
    if watchlist_codes:
        from zoneinfo import ZoneInfo
        end = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = end - timedelta(days=365 * 2)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        with get_conn() as conn:
            for code in watchlist_codes:
                ts_code = _code_to_ts_code(code)
                if not ts_code:
                    continue
                time.sleep(DELAY)
                try:
                    inc = pro.income(ts_code=ts_code, start_date=start_str[:4] + "0101", end_date=end_str, report_type="1", fields="end_date,revenue,n_income")
                    time.sleep(DELAY)
                    bal = pro.balancesheet(ts_code=ts_code, start_date=start_str[:4] + "0101", end_date=end_str, report_type="1", fields="end_date,total_assets")
                    by_ed: dict[str, dict] = {}
                    if inc is not None and not inc.empty:
                        for _, r in inc.iterrows():
                            ed = str(r.get("end_date", ""))[:8]
                            if len(ed) >= 8:
                                by_ed.setdefault(ed, {"revenue": None, "net_profit": None, "total_assets": None})
                                by_ed[ed]["revenue"] = r.get("revenue")
                                by_ed[ed]["net_profit"] = r.get("n_income")
                    if bal is not None and not bal.empty:
                        for _, r in bal.iterrows():
                            ed = str(r.get("end_date", ""))[:8]
                            if len(ed) >= 8:
                                by_ed.setdefault(ed, {"revenue": None, "net_profit": None, "total_assets": None})
                                by_ed[ed]["total_assets"] = r.get("total_assets")
                    for ed, v in by_ed.items():
                        _upsert_financial(conn, code, ed, "季度", v.get("revenue"), v.get("net_profit"), v.get("total_assets"))
                        total_financial += 1
                except Exception as e:
                    logger.warning("income/balancesheet %s: %s", code, e)
            conn.commit()
        logger.info("incremental_daily: 财务指标(watchlist) written %s rows", total_financial)

    last_date = dates_to_process[-1] if dates_to_process else None
    return {
        "ok": True,
        "trade_date": last_date,
        "dates_updated": dates_to_process if len(dates_to_process) != 1 else None,
        "rows_stock_day": total_stock_day,
        "rows_fundamentals": total_fundamentals,
        "rows_technicals": total_technicals,
        "rows_moneyflow": total_moneyflow,
        "rows_financial": total_financial,
        "message": f"增量更新 {len(dates_to_process)} 天（{dates_to_process[0]}～{last_date}）：日线 {total_stock_day}，每日指标 {total_fundamentals}，技术指标(MA) {total_technicals}，资金流向(跟踪) {total_moneyflow}，财务指标(跟踪) {total_financial}",
    }
