"""
上市公司基础数据入库：与 stex.corp 表结构一致，支持重复采集（upsert）。
含市场分类、市值、市盈率、市净率。
"""
from datetime import date
from decimal import Decimal
from typing import Optional, Union

from ..db import get_conn


def _parse_date(v) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v).split("T")[0])
    except Exception:
        return None


def _num(v) -> Optional[Decimal]:
    if v is None or v == "" or (isinstance(v, float) and (v != v or v == float("inf"))):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def upsert_corp(
    code: str,
    name: Optional[str] = None,
    market: Optional[str] = None,
    industry: Optional[str] = None,
    sector: Optional[str] = None,
    list_date=None,
    market_cap: Optional[Union[int, float, Decimal]] = None,
    pe: Optional[Union[float, Decimal]] = None,
    pb: Optional[Union[float, Decimal]] = None,
) -> None:
    """
    写入或更新一条 stex.corp 记录。
    code 必填；market 建议为 上证/深证/科创/创业/北证；市值/市盈率/市净率为行情快照。
    """
    list_d = _parse_date(list_date)
    cap = _num(market_cap)
    pe_val = _num(pe)
    pb_val = _num(pb)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stex.corp (code, name, market, industry, sector, list_date, market_cap, pe, pb)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code) DO UPDATE SET
                  name = COALESCE(EXCLUDED.name, stex.corp.name),
                  market = COALESCE(EXCLUDED.market, stex.corp.market),
                  industry = COALESCE(EXCLUDED.industry, stex.corp.industry),
                  sector = COALESCE(EXCLUDED.sector, stex.corp.sector),
                  list_date = COALESCE(EXCLUDED.list_date, stex.corp.list_date),
                  market_cap = COALESCE(EXCLUDED.market_cap, stex.corp.market_cap),
                  pe = COALESCE(EXCLUDED.pe, stex.corp.pe),
                  pb = COALESCE(EXCLUDED.pb, stex.corp.pb),
                  updated_at = NOW()
                """,
                (code, name, market, industry, sector, list_d, cap, pe_val, pb_val),
            )
        conn.commit()
