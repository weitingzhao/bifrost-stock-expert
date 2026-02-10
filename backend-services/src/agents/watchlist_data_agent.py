"""
Agent：针对已收藏跟踪的股票，用 Tushare 拉取日线、每日指标(基本面)、财务指标，
写入 stex.stock_day / stex.fundamentals / stex.technicals / stex.financial。
日线及 MA 使用通用行情接口 pro_bar（https://tushare.pro/document/2?doc_id=109）一次拉取。
"""
import logging
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional
from zoneinfo import ZoneInfo

from ..db import get_conn

logger = logging.getLogger(__name__)
DELAY = 0.3  # 请求间隔，避免限流


def _code_to_ts_code(code: str) -> str:
    """6 位代码 -> Tushare ts_code"""
    code = str(code).strip()
    if not code or len(code) < 4:
        return ""
    if code.startswith("6") or code.startswith("68"):
        return f"{code}.SH"
    if code.startswith("4") or code.startswith("8"):
        return f"{code}.BJ"
    return f"{code}.SZ"


def _safe_num(v) -> Optional[Decimal]:
    if v is None or (isinstance(v, float) and (v != v)):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _ensure_conn():
    from ..db import get_conn
    return get_conn()


def _get_watchlist_codes() -> list[str]:
    with _ensure_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
            return [str(r[0]) for r in cur.fetchall()]


def _upsert_stock_day(conn, code: str, trade_date: str, open_, high, low, close, vol, amount) -> None:
    """Tushare daily: vol=成交量(手), amount=成交额(千元) -> 存 volume=vol, amount=amount*1000"""
    amt = _safe_num(amount)
    if amt is not None:
        amt = amt * 1000  # 千元 -> 元
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
    """将 daily_basic 的换手率写回 stock_day。"""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE stex.stock_day SET turnover_rate = %s WHERE code = %s AND trade_date = %s::date",
            (_safe_num(turnover_rate), code, trade_date),
        )


def _upsert_fundamentals(conn, code: str, report_date: str, pe, pb, ps, market_cap, revenue, net_profit, profit_growth, roe) -> None:
    cap = _safe_num(market_cap)
    if cap is not None and cap < 1e10:  # Tushare 总市值可能是万元
        cap = cap * 10000  # 万元->元，用 int 避免 Decimal * float 报错
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.fundamentals (code, report_date, pe, pb, ps, market_cap, revenue, net_profit, profit_growth, roe)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, report_date) DO UPDATE SET
              pe = COALESCE(EXCLUDED.pe, stex.fundamentals.pe), pb = COALESCE(EXCLUDED.pb, stex.fundamentals.pb),
              ps = COALESCE(EXCLUDED.ps, stex.fundamentals.ps), market_cap = COALESCE(EXCLUDED.market_cap, stex.fundamentals.market_cap),
              revenue = COALESCE(EXCLUDED.revenue, stex.fundamentals.revenue), net_profit = COALESCE(EXCLUDED.net_profit, stex.fundamentals.net_profit),
              profit_growth = COALESCE(EXCLUDED.profit_growth, stex.fundamentals.profit_growth), roe = COALESCE(EXCLUDED.roe, stex.fundamentals.roe)
            """,
            (code, report_date, _safe_num(pe), _safe_num(pb), _safe_num(ps), cap, _safe_num(revenue), _safe_num(net_profit), _safe_num(profit_growth), _safe_num(roe)),
        )


def _upsert_technicals(conn, code: str, trade_date: str, ma5, ma10, ma20, macd, macd_signal, macd_hist, rsi, kdj_k, kdj_d, kdj_j) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.technicals (code, trade_date, ma5, ma10, ma20, macd, macd_signal, macd_hist, rsi, kdj_k, kdj_d, kdj_j)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, trade_date) DO UPDATE SET
              ma5=EXCLUDED.ma5, ma10=EXCLUDED.ma10, ma20=EXCLUDED.ma20,
              macd=EXCLUDED.macd, macd_signal=EXCLUDED.macd_signal, macd_hist=EXCLUDED.macd_hist,
              rsi=EXCLUDED.rsi, kdj_k=EXCLUDED.kdj_k, kdj_d=EXCLUDED.kdj_d, kdj_j=EXCLUDED.kdj_j
            """,
            (code, trade_date, _safe_num(ma5), _safe_num(ma10), _safe_num(ma20),
             _safe_num(macd), _safe_num(macd_signal), _safe_num(macd_hist),
             _safe_num(rsi), _safe_num(kdj_k), _safe_num(kdj_d), _safe_num(kdj_j)),
        )


def _upsert_financial(conn, code: str, report_date: str, report_type: str, revenue, net_profit, total_assets) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.financial (code, report_date, report_type, revenue, net_profit, total_assets)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (code, report_date, report_type) DO UPDATE SET
              revenue = EXCLUDED.revenue, net_profit = EXCLUDED.net_profit, total_assets = EXCLUDED.total_assets
            """,
            (code, report_date, report_type or "季度", _safe_num(revenue), _safe_num(net_profit), _safe_num(total_assets)),
        )


def _upsert_moneyflow(
    conn,
    code: str,
    trade_date: str,
    net_mf_amount,
    net_mf_vol,
    *,
    buy_sm_amount=None,
    sell_sm_amount=None,
    buy_sm_vol=None,
    sell_sm_vol=None,
    buy_md_amount=None,
    sell_md_amount=None,
    buy_md_vol=None,
    sell_md_vol=None,
    buy_lg_amount=None,
    sell_lg_amount=None,
    buy_lg_vol=None,
    sell_lg_vol=None,
    buy_elg_amount=None,
    sell_elg_amount=None,
    buy_elg_vol=None,
    sell_elg_vol=None,
) -> None:
    """Tushare moneyflow: 净流入汇总 + 小/中/大/特大单买卖额(万元)、买卖量(手)"""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.moneyflow (
              code, trade_date, net_mf_amount, net_mf_vol,
              buy_sm_amount, sell_sm_amount, buy_sm_vol, sell_sm_vol,
              buy_md_amount, sell_md_amount, buy_md_vol, sell_md_vol,
              buy_lg_amount, sell_lg_amount, buy_lg_vol, sell_lg_vol,
              buy_elg_amount, sell_elg_amount, buy_elg_vol, sell_elg_vol
            ) VALUES (
              %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (code, trade_date) DO UPDATE SET
              net_mf_amount = EXCLUDED.net_mf_amount,
              net_mf_vol = EXCLUDED.net_mf_vol,
              buy_sm_amount = EXCLUDED.buy_sm_amount,
              sell_sm_amount = EXCLUDED.sell_sm_amount,
              buy_sm_vol = EXCLUDED.buy_sm_vol,
              sell_sm_vol = EXCLUDED.sell_sm_vol,
              buy_md_amount = EXCLUDED.buy_md_amount,
              sell_md_amount = EXCLUDED.sell_md_amount,
              buy_md_vol = EXCLUDED.buy_md_vol,
              sell_md_vol = EXCLUDED.sell_md_vol,
              buy_lg_amount = EXCLUDED.buy_lg_amount,
              sell_lg_amount = EXCLUDED.sell_lg_amount,
              buy_lg_vol = EXCLUDED.buy_lg_vol,
              sell_lg_vol = EXCLUDED.sell_lg_vol,
              buy_elg_amount = EXCLUDED.buy_elg_amount,
              sell_elg_amount = EXCLUDED.sell_elg_amount,
              buy_elg_vol = EXCLUDED.buy_elg_vol,
              sell_elg_vol = EXCLUDED.sell_elg_vol
            """,
            (
                code,
                trade_date,
                _safe_num(net_mf_amount),
                _safe_num(net_mf_vol),
                _safe_num(buy_sm_amount),
                _safe_num(sell_sm_amount),
                _safe_num(buy_sm_vol),
                _safe_num(sell_sm_vol),
                _safe_num(buy_md_amount),
                _safe_num(sell_md_amount),
                _safe_num(buy_md_vol),
                _safe_num(sell_md_vol),
                _safe_num(buy_lg_amount),
                _safe_num(sell_lg_amount),
                _safe_num(buy_lg_vol),
                _safe_num(sell_lg_vol),
                _safe_num(buy_elg_amount),
                _safe_num(sell_elg_amount),
                _safe_num(buy_elg_vol),
                _safe_num(sell_elg_vol),
            ),
        )


def run_watchlist_data_agent(codes: Optional[list[str]] = None) -> dict[str, Any]:
    """
    拉取指定股票（或 watchlist 全部）的日线（含 MA5/10/20）、每日指标、财务指标，写入对应表。
    日线与 MA 使用 Tushare 通用行情接口 pro_bar 一次拉取。
    codes 为空时拉取 watchlist 全部；否则只处理 codes 中的代码（可为单只）。
    返回：{ ok, codes_processed, days_updated, error }
    """
    from ..config import TUSHARE_TOKEN
    if not TUSHARE_TOKEN:
        return {"ok": False, "error": "请设置 TUSHARE_TOKEN", "codes_processed": 0, "days_updated": 0}

    try:
        import tushare as ts
    except ImportError:
        return {"ok": False, "error": "请安装 tushare: pip install tushare", "codes_processed": 0, "days_updated": 0}

    if codes is not None:
        codes = [str(c).strip() for c in codes if c and str(c).strip()]
    if not codes:
        codes = _get_watchlist_codes()
    if not codes:
        return {"ok": True, "codes_processed": 0, "days_updated": 0, "message": "暂无收藏跟踪股票"}

    pro = ts.pro_api(TUSHARE_TOKEN)
    # 使用中国时区“今天”作为 end_date，避免服务器在 UTC 等时区时少拉一天
    end = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    start = end - timedelta(days=365 * 2)  # 最近约 2 年
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")
    print(f"[StEx] 请求日线数据截止到 {end.month}月{end.day}日（中国时区今天）", flush=True)
    total_days = 0
    latest_trade_date_seen = None  # 实际从 Tushare 拿到的最大 trade_date
    latest_trade_date_code = None  # 哪只股票对应该最新日期

    with _ensure_conn() as conn:
        for code in codes:
            ts_code = _code_to_ts_code(code)
            if not ts_code:
                continue
            time.sleep(DELAY)
            try:
                # 日线 + MA5/10/20：使用通用行情接口 pro_bar，一次拉取
                df = ts.pro_bar(
                    ts_code=ts_code,
                    start_date=start_str,
                    end_date=end_str,
                    asset="E",
                    freq="D",
                    ma=[5, 10, 20],
                    api=pro,
                )
                if df is not None and not df.empty:
                    df = df.sort_values("trade_date").reset_index(drop=True)
                    max_td = str(df["trade_date"].max()) if "trade_date" in df.columns else None
                    if max_td and (latest_trade_date_seen is None or max_td > latest_trade_date_seen):
                        latest_trade_date_seen = max_td
                        latest_trade_date_code = code
                    for _, row in df.iterrows():
                        td = str(row.get("trade_date", ""))
                        o, h, l, c = row.get("open"), row.get("high"), row.get("low"), row.get("close")
                        vol, amt = row.get("vol"), row.get("amount")
                        _upsert_stock_day(conn, code, td, o, h, l, c, vol, amt)
                        total_days += 1
                        ma5 = row.get("ma5") if "ma5" in row else None
                        ma10 = row.get("ma10") if "ma10" in row else None
                        ma20 = row.get("ma20") if "ma20" in row else None
                        _upsert_technicals(conn, code, td, ma5, ma10, ma20, None, None, None, None, None, None, None)
            except Exception as e:
                logger.warning("pro_bar(daily) %s: %s", code, e)
            time.sleep(DELAY)
            try:
                # 每日指标 -> fundamentals（按交易日）
                dbasic = pro.daily_basic(ts_code=ts_code, start_date=start_str, end_date=end_str, fields="trade_date,pe,pb,ps,total_mv,turnover_rate")
                if dbasic is not None and not dbasic.empty:
                    for _, r in dbasic.iterrows():
                        td = str(r.get("trade_date", ""))
                        mv = r.get("total_mv")
                        if mv is not None and float(mv) < 1e10:
                            mv = float(mv) * 1e4  # 万元 -> 元
                        _upsert_fundamentals(conn, code, td, r.get("pe"), r.get("pb"), r.get("ps"), mv, None, None, None, None)
                        if r.get("turnover_rate") is not None:
                            _update_stock_day_turnover(conn, code, td, r.get("turnover_rate"))
            except Exception as e:
                logger.warning("daily_basic %s: %s", code, e)
            time.sleep(DELAY)
            try:
                # 财务指标（季度）：利润表 -> 营收、净利润；资产负债表 -> 总资产
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
            except Exception as e:
                logger.warning("income/balancesheet %s: %s", code, e)
            time.sleep(DELAY)
            try:
                # 每日资金流向：净流入汇总 + 小/中/大/特大单买卖额(万元)、买卖量(手)，需 Tushare 2000+ 积分
                mf = pro.moneyflow(ts_code=ts_code, start_date=start_str, end_date=end_str)
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
            except Exception as e:
                logger.warning("moneyflow %s: %s", code, e)
            logger.info("采集完成: %s", code)
        conn.commit()

    if latest_trade_date_seen and len(latest_trade_date_seen) >= 8:
        y, m, d = latest_trade_date_seen[:4], int(latest_trade_date_seen[4:6]), int(latest_trade_date_seen[6:8])
        code_hint = f"（来自股票 {latest_trade_date_code}）" if latest_trade_date_code else ""
        print(f"[StEx] Tushare 实际返回的最新交易日：{y}年{m}月{d}日{code_hint}", flush=True)
    out = {
        "ok": True,
        "codes_processed": len(codes),
        "days_updated": total_days,
        "request_date_range": {"start": start_str, "end": end_str},
    }
    if latest_trade_date_seen:
        out["latest_trade_date_from_api"] = latest_trade_date_seen
    return out