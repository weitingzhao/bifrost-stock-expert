"""
Agent：采集 A 股各板块上市公司基础数据，写入 stex.corp。
- 市场分类：上证、深证、科创、创业、北证（按代码规则）
- 行业/板块：东方财富行业板块 + 成分股
- 市值、市盈率、市净率：来自成分股接口或 A 股实时行情补全

网络说明：需能访问东方财富等数据源，若报连接/超时错误请检查网络、代理或稍后重试。
"""
import logging
import time
from typing import Any, Callable, Optional, TypeVar

import pandas as pd

from ..collectors.corp import upsert_corp
from ..db import get_conn

logger = logging.getLogger(__name__)

# 请求间隔（秒），避免触发限流/RemoteDisconnected
REQUEST_DELAY = 0.6

T = TypeVar("T")

# RemoteDisconnected / 连接被远端关闭时重试
try:
    from http.client import RemoteDisconnected
except ImportError:
    RemoteDisconnected = Exception  # noqa
try:
    from urllib3.exceptions import ProtocolError
except ImportError:
    ProtocolError = Exception  # noqa


def _retry_request(
    fn: Callable[[], T],
    max_attempts: int = 3,
    delay: float = 2.0,
) -> T:
    """对无参可调用 fn 做重试；RemoteDisconnected/ProtocolError 时等待 delay 秒再试。"""
    last_err = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except (RemoteDisconnected, ProtocolError, ConnectionError, OSError) as e:
            last_err = e
            if attempt < max_attempts - 1:
                logger.warning("attempt %s failed (connection): %s, retry in %.1fs", attempt + 1, e, delay)
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            last_err = e
            if attempt < max_attempts - 1:
                logger.warning("attempt %s failed: %s, retry in %.1fs", attempt + 1, e, delay)
                time.sleep(delay)
            else:
                raise
    raise last_err

# 市场分类：按股票代码前缀（券商常用）
MARKET_RULES = [
    ("688", "科创"),   # 科创板
    ("60", "上证"),   # 沪市主板
    ("00", "深证"),   # 深市主板
    ("30", "创业"),   # 创业板
    ("4", "北证"),    # 北交所 4xxxxx
    ("8", "北证"),    # 北交所 8xxxxx
]


def _market_from_code(code: str) -> str:
    """按代码推导市场：上证、深证、科创、创业、北证。"""
    code = str(code).strip()
    if not code:
        return ""
    for prefix, market in MARKET_RULES:
        if code.startswith(prefix):
            return market
    return ""


def _col(df: pd.DataFrame, *names: str) -> Optional[str]:
    """取 DataFrame 列名（支持中英文）"""
    for c in df.columns:
        c_str = str(c).strip()
        for n in names:
            if c_str == n or c_str == str(n).strip():
                return c
    return None


def _safe_float(row: pd.Series, col: Optional[str]) -> Optional[float]:
    if col is None or col not in row:
        return None
    try:
        v = row[col]
        if v is None or (isinstance(v, float) and (v != v or v == float("inf"))):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def run_corp_agent() -> dict[str, Any]:
    """
    拉取东方财富行业板块及成分股，写入 stex.corp。
    市场：按代码分类（上证/深证/科创/创业/北证）；行业/板块为板块名称；
    市值、市盈率、市净率：从成分股或 A 股实时行情获取。
    返回：{ "ok", "industries", "total_upserted", "error" }
    """
    try:
        import akshare as ak
    except ImportError:
        return {"ok": False, "error": "请安装 akshare: pip install akshare", "industries": 0, "total_upserted": 0}

    try:
        # 不再调用 stock_zh_a_spot_em()：该接口一次拉全市场，易触发 RemoteDisconnected。
        # 总市值仅从行业成分股接口取（若该接口有 总市值/市值 列则写入，否则为 null）。
        spot_market_cap: dict[str, float] = {}

        # 行业板块列表
        name_df = _retry_request(lambda: ak.stock_board_industry_name_em(), max_attempts=3, delay=2.0)
        if name_df is None or name_df.empty:
            return {"ok": True, "industries": 0, "total_upserted": 0, "message": "无行业板块数据"}

        name_col = _col(name_df, "板块名称", "name", "行业") or (name_df.columns[0] if len(name_df.columns) > 0 else None)
        if name_col is None:
            return {"ok": True, "industries": 0, "total_upserted": 0, "message": "无板块名称列"}
        industry_names = name_df[name_col].dropna().unique().tolist()

        total = 0
        for industry_name in industry_names:
            industry_name = str(industry_name).strip()
            if not industry_name:
                continue
            time.sleep(REQUEST_DELAY)
            try:
                cons_df = _retry_request(
                    lambda s=industry_name: ak.stock_board_industry_cons_em(symbol=s),
                    max_attempts=3,
                    delay=2.0,
                )
                if cons_df is None or cons_df.empty:
                    continue
                code_col = _col(cons_df, "代码", "code", "股票代码")
                name_col_cons = _col(cons_df, "名称", "name", "股票名称")
                pe_col = _col(cons_df, "市盈率-动态", "市盈率", "pe")
                pb_col = _col(cons_df, "市净率", "pb")
                cap_col = _col(cons_df, "总市值", "市值", "market_cap")
                if not code_col:
                    continue
                for _, row in cons_df.iterrows():
                    code = str(row.get(code_col, "")).strip()
                    if not code or len(code) > 10:
                        continue
                    name = str(row.get(name_col_cons, "")).strip() if name_col_cons else None
                    market = _market_from_code(code)
                    pe = _safe_float(row, pe_col)
                    pb = _safe_float(row, pb_col)
                    market_cap = _safe_float(row, cap_col)
                    if market_cap is None and code in spot_market_cap:
                        market_cap = spot_market_cap[code]
                    upsert_corp(
                        code=code,
                        name=name or None,
                        market=market or None,
                        industry=industry_name,
                        sector=industry_name,
                        list_date=None,
                        market_cap=market_cap,
                        pe=pe,
                        pb=pb,
                    )
                    total += 1
            except Exception as e:
                logger.warning("industry %s: %s", industry_name, e)
                continue

        return {"ok": True, "industries": len(industry_names), "total_upserted": total}
    except Exception as e:
        logger.exception("corp_agent")
        err_msg = str(e)
        if "connect" in err_msg.lower() or "timeout" in err_msg.lower() or "refused" in err_msg.lower():
            err_msg = f"{err_msg}（请检查网络、代理或稍后重试）"
        return {"ok": False, "error": err_msg, "industries": 0, "total_upserted": 0}
