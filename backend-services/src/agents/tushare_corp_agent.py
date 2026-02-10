"""
Agent：使用 Tushare Pro 采集 A 股上市公司基础数据，写入 stex.corp。
- 市场：主板/创业板/科创板/北交所 映射为 上证/深证/科创/创业/北证
- 市值、市盈率、市净率：来自 daily_basic（总市值单位：万元，入库为元）
"""
import logging
from datetime import date, datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from ..collectors.corp import upsert_corp

logger = logging.getLogger(__name__)


def _ts_code_to_code(ts_code: str) -> str:
    """600519.SH -> 600519"""
    if not ts_code:
        return ""
    return str(ts_code).split(".")[0].strip()


def _market_stex(ts_code: str, market_tushare: Optional[str]) -> str:
    """Tushare market + ts_code 映射为 上证/深证/科创/创业/北证"""
    ts_code = str(ts_code or "").upper()
    m = (market_tushare or "").strip()
    if ts_code.endswith(".BJ"):
        return "北证"
    if ts_code.endswith(".SH"):
        if "688" in ts_code[:6]:
            return "科创"
        return "上证"
    if ts_code.endswith(".SZ"):
        if ts_code.startswith("30"):
            return "创业"
        return "深证"
    if "科创" in m or "科创板" in m:
        return "科创"
    if "创业" in m or "创业板" in m:
        return "创业"
    if "北交" in m or "北交所" in m:
        return "北证"
    if "主板" in m:
        return "上证" if ".SH" in ts_code else "深证"
    if "CDR" in (m or ""):
        return "上证"
    return ""


def run_tushare_corp_agent() -> dict[str, Any]:
    """
    拉取 Tushare Pro stock_basic + daily_basic，写入 stex.corp。
    返回：{ "ok", "total_upserted", "error" }
    """
    from ..config import TUSHARE_TOKEN
    token = TUSHARE_TOKEN or ""
    try:
        import tushare as ts
    except ImportError:
        return {"ok": False, "error": "请安装 tushare: pip install tushare", "total_upserted": 0}

    if not token:
        return {"ok": False, "error": "请设置 TUSHARE_TOKEN（.env 或环境变量）", "total_upserted": 0}

    try:
        pro = ts.pro_api(token)
    except Exception as e:
        return {"ok": False, "error": f"Tushare 初始化失败: {e}", "total_upserted": 0}

    try:
        # 股票列表（上市状态）
        basic_df = pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,list_date",
        )
        if basic_df is None or basic_df.empty:
            return {"ok": True, "total_upserted": 0, "message": "无股票列表"}

        # 最新交易日（用于取 daily_basic），使用中国时区“今天”
        end = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = end - timedelta(days=30)
        cal_df = pro.trade_cal(
            exchange="SSE",
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            is_open="1",
        )
        trade_date = ""
        if cal_df is not None and not cal_df.empty and "cal_date" in cal_df.columns:
            trade_date = str(cal_df["cal_date"].max() or "")
        if not trade_date:
            trade_date = end.strftime("%Y%m%d")

        # 全市场当日指标（PE/PB/总市值），总市值单位：万元
        daily_map: dict[str, dict] = {}
        try:
            daily_df = pro.daily_basic(
                trade_date=trade_date,
                fields="ts_code,trade_date,pe,pb,total_mv",
            )
            if daily_df is not None and not daily_df.empty:
                for _, r in daily_df.iterrows():
                    tc = str(r.get("ts_code", "")).strip()
                    if tc:
                        daily_map[tc] = {
                            "pe": r.get("pe"),
                            "pb": r.get("pb"),
                            "total_mv": r.get("total_mv"),
                        }
        except Exception as e:
            logger.warning("daily_basic 拉取失败，仅写入基础信息: %s", e)

        total = 0
        for _, row in basic_df.iterrows():
            ts_code = str(row.get("ts_code", "")).strip()
            code = _ts_code_to_code(ts_code)
            if not code or len(code) > 10:
                continue
            name = str(row.get("name", "")).strip() or None
            industry = str(row.get("industry", "")).strip() or None
            market_tushare = str(row.get("market", "")).strip() or None
            market = _market_stex(ts_code, market_tushare) or None
            list_date = row.get("list_date")
            if hasattr(list_date, "item"):
                list_date = list_date.item() if list_date is not None else None
            pe, pb, market_cap = None, None, None
            if ts_code in daily_map:
                d = daily_map[ts_code]
                pe = d.get("pe")
                pb = d.get("pb")
                # total_mv 万元 -> 元
                mv = d.get("total_mv")
                if mv is not None and mv == mv:
                    try:
                        market_cap = float(mv) * 1e4
                    except (TypeError, ValueError):
                        pass
            upsert_corp(
                code=code,
                name=name,
                market=market,
                industry=industry,
                sector=industry,
                list_date=list_date,
                market_cap=market_cap,
                pe=pe,
                pb=pb,
            )
            total += 1

        return {"ok": True, "total_upserted": total, "trade_date": trade_date}
    except Exception as e:
        logger.exception("tushare_corp_agent")
        err = str(e)
        if "权限" in err or "integral" in err.lower() or "积分" in err:
            err = f"{err}（请检查 Tushare 积分与接口权限）"
        return {"ok": False, "error": err, "total_upserted": 0}
