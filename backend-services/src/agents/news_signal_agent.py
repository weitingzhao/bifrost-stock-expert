"""
Agent：新闻舆论信号 —— 指定信息源采集 + AI 大模型
信息源：巨潮资讯网、财联社电报、证券时报、中国证券报（RSS/RSSHub）、雪球个股新闻、东方财富股吧（DDG 站内），
以及 DDG 综合新闻与多站点搜索兜底。由 LLM 判断利好/利空，输出 看涨/看跌/中性/无信号，写入 stex.signals。
"""
import logging
from datetime import datetime
from typing import Any, Optional

from openai import OpenAI

from ..config import MOONSHOT_API_KEY, MOONSHOT_BASE_URL
from ..db import get_conn
from .parse_corp_agent import _get_corp_name, _llm, _web_search
from .news_sources import gather_from_specified_sources

logger = logging.getLogger(__name__)

SIGNAL_TYPE_NEWS = "新闻舆论"
DIR_BULL = "看涨"
DIR_BEAR = "看跌"
DIR_NEUTRAL = "中性"
DIR_NONE = "无信号"

# 兜底：多站点 DDG 搜索（当未配置 RSSHub 或需补充时）
NEWS_SITE_QUERIES = [
    "site:eastmoney.com",
    "site:cls.cn",
    "site:stcn.com",
    "site:cs.com.cn",
    "site:cninfo.com.cn",
    "site:xueqiu.com",
    "site:finance.sina.com.cn",
    "site:money.163.com",
]


def _get_latest_trade_date(code: str) -> Optional[str]:
    """该股票在 stock_day 中的最新交易日，返回 YYYY-MM-DD。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date FROM stex.stock_day WHERE code = %s ORDER BY trade_date DESC LIMIT 1",
                (code,),
            )
            row = cur.fetchone()
    if not row:
        return None
    td = row[0]
    if hasattr(td, "isoformat"):
        return td.isoformat()[:10]
    return str(td)[:10]


def _get_recent_trade_dates(code: str, limit: int = 10) -> list[str]:
    """该股票最近 limit 个交易日，降序，返回 YYYY-MM-DD 列表。"""
    rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date FROM stex.stock_day WHERE code = %s ORDER BY trade_date DESC LIMIT %s",
                (code, limit),
            )
            rows = cur.fetchall()
    out = []
    for row in rows:
        td = row[0]
        s = td.isoformat()[:10] if hasattr(td, "isoformat") else str(td)[:10]
        out.append(s)
    return out


def _news_date_to_ref_date(news_date_str: str, trade_dates_desc: list[str]) -> Optional[str]:
    """将新闻日期（YYYY-MM-DD）映射到最近的交易日（<= 该日）。"""
    if not trade_dates_desc:
        return None
    try:
        d = news_date_str[:10]
        for td in trade_dates_desc:
            if td <= d:
                return td
        return trade_dates_desc[-1]
    except Exception:
        return trade_dates_desc[0] if trade_dates_desc else None


def _news_search(keywords: str, max_results: int = 10, timelimit: str = "w") -> list[dict]:
    """使用 DDGS.news() 搜索新闻，返回带 date 的列表。"""
    try:
        from duckduckgo_search import DDGS

        with DDGS() as ddgs:
            raw = ddgs.news(
                keywords=keywords,
                region="wt-wt",
                safesearch="off",
                timelimit=timelimit,
                max_results=max_results,
            )
        items = []
        for r in raw or []:
            date_str = (r.get("date") or "")[:10]
            if not date_str and r.get("date"):
                try:
                    dt = datetime.fromisoformat((r.get("date") or "").replace("Z", "+00:00"))
                    date_str = dt.strftime("%Y-%m-%d")
                except Exception:
                    date_str = ""
            title = (r.get("title") or "").strip()
            body = (r.get("body") or "").strip()
            if title or body:
                items.append({"date": date_str, "title": title, "body": body, "snippet": f"{title} {body}".strip()})
        return items
    except Exception as e:
        logger.warning("news_search failed: %s", e)
        return []


def _text_search_multi_sites(corp_name: str, code: str, per_site_max: int = 2) -> list[str]:
    """多站点 + 多关键词文本搜索，扩大覆盖面。无日期，仅返回摘要列表。"""
    combined = []
    base_terms = [f"{corp_name} {code} 股票", corp_name or code] if corp_name else [f"{code} 股票"]
    for site in NEWS_SITE_QUERIES[:6]:
        for term in base_terms[:2]:
            q = f"{site} {term} 新闻 利好 利空"
            for s in _web_search(q, max_results=per_site_max):
                if s and s.strip() and s.strip() not in combined:
                    combined.append(s.strip())
    # 通用查询（无 site）增加条数
    for q in [f"{corp_name} {code} 股票 新闻 热点", f"{corp_name} 政策 业绩 公告"] if corp_name else [f"{code} 股票 新闻"]:
        for s in _web_search(q, max_results=4):
            if s and s.strip() and s.strip() not in combined:
                combined.append(s.strip())
    return combined


def _upsert_news_signal(
    code: str,
    ref_date: str,
    direction: str,
    reason: str,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stex.signals (code, signal_type, direction, ref_date, reason, source)
                VALUES (%s, %s, %s, %s::date, %s, %s)
                ON CONFLICT (code, ref_date, signal_type) WHERE ref_date IS NOT NULL
                DO UPDATE SET direction = EXCLUDED.direction, reason = EXCLUDED.reason, source = EXCLUDED.source
                """,
                (code, SIGNAL_TYPE_NEWS, direction, ref_date, reason or "新闻舆论", "news_signal_agent"),
            )
        conn.commit()


def _insert_news_opinion_record(
    code: str,
    fetch_date: datetime,
    ref_date: str,
    direction: str,
    reason: str,
    news_content: str,
) -> None:
    """将本次拉取分析的新闻内容、拉取日期、信号结果写入 stex.news_opinion_record。失败仅打日志，不阻断信号写入。"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.news_opinion_record (code, fetch_date, ref_date, direction, reason, news_content)
                    VALUES (%s, %s, %s::date, %s, %s, %s)
                    """,
                    (code, fetch_date, ref_date, direction or "无信号", (reason or "")[:2000], (news_content or "")[:50000]),
                )
            conn.commit()
    except Exception as e:
        logger.warning("insert news_opinion_record failed (code=%s): %s", code, e)


def _parse_direction_from_llm(text: str) -> tuple[str, str]:
    """从 LLM 回复中解析方向与理由。返回 (direction, reason)。"""
    text = (text or "").strip()
    if not text:
        return DIR_NONE, "无新闻"
    # 优先匹配第一行或明显关键词
    for kw in [DIR_BULL, DIR_BEAR, DIR_NEUTRAL, DIR_NONE]:
        if kw in text:
            # 取包含该关键词的一句或一段作为 reason
            reason = text[:200].replace("\n", " ").strip()
            if len(text) > 200:
                reason = text[:200].rsplit("。", 1)[0] + "。" if "。" in text[:200] else text[:200]
            return kw, reason or "新闻舆论"
    # 默认中性
    return DIR_NEUTRAL, text[:200] if len(text) > 200 else text


def run_news_signal_agent(codes: Optional[list[str]] = None) -> dict[str, Any]:
    """
    对指定股票（或单只）执行「新闻舆论」信号：多源搜索近期新闻 + LLM 判断利好/利空，
    输出 看涨/看跌/中性/无信号，写入 stex.signals（signal_type=新闻舆论）。
    新闻日期归属到信号所属交易日（ref_date）；无新闻或无法判断时记为 无信号。
    """
    if not MOONSHOT_API_KEY:
        return {"ok": False, "error": "MOONSHOT_API_KEY 未配置", "codes_processed": 0}

    norm_codes = [str(c).strip() for c in (codes or []) if c and str(c).strip()]
    if not norm_codes:
        # 批量模式：优先选取尚未采集或最早采集过新闻舆论的跟踪股票（最多 20 只），多次运行可覆盖全列表
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT w.code
                    FROM stex.watchlist w
                    LEFT JOIN (
                        SELECT code, MAX(ref_date) AS last_ref
                        FROM stex.signals
                        WHERE signal_type = %s AND ref_date IS NOT NULL
                        GROUP BY code
                    ) s ON s.code = w.code
                    ORDER BY s.last_ref ASC NULLS FIRST, w.code
                    LIMIT 20
                    """,
                    (SIGNAL_TYPE_NEWS,),
                )
                norm_codes = [str(r[0]) for r in cur.fetchall()]
        logger.info("news_signal: batch mode, selected %s codes (no/oldest news first)", len(norm_codes))
    if not norm_codes:
        logger.warning("news_signal: no codes to process (watchlist empty or none given)")
        return {"ok": True, "codes_processed": 0, "message": "暂无股票"}

    client = OpenAI(api_key=MOONSHOT_API_KEY, base_url=MOONSHOT_BASE_URL)
    results = []
    for code in norm_codes[:20]:
        try:
            fetch_ts = datetime.now()
            corp_name = _get_corp_name(code)
            latest_ref = _get_latest_trade_date(code)
            if not latest_ref:
                latest_ref = datetime.now().strftime("%Y-%m-%d")
            trade_dates = _get_recent_trade_dates(code, limit=10)
            if not trade_dates:
                trade_dates = [latest_ref]

            # 1) 指定信息源：巨潮/财联社/证券时报/中国证券报/雪球/东财股吧 + DDG 新闻与站内搜索（异常时仅打日志，保证后续仍可写入 无新闻）
            try:
                news_items = gather_from_specified_sources(
                    code,
                    corp_name,
                    web_search_fn=_web_search,
                    include_ddg_news=True,
                    ddg_news_fn=_news_search,
                    ddg_site_fn=lambda q, max_results=3: _web_search(q, max_results=max_results),
                )
            except Exception as e:
                logger.warning("gather_from_specified_sources failed (code=%s): %s", code, e)
                news_items = []

            by_ref: dict[str, list[str]] = {}
            for item in news_items:
                ref_date = _news_date_to_ref_date(item.get("date") or "", trade_dates) or latest_ref
                sn = ((item.get("title") or "") + " " + (item.get("body") or "")).strip()
                if sn and ref_date:
                    by_ref.setdefault(ref_date, []).append(sn)

            # 2) 多站点文本搜索兜底（无日期）：并入最新交易日（异常时不阻断）
            try:
                text_snippets = _text_search_multi_sites(corp_name, code, per_site_max=2)
                if text_snippets:
                    by_ref.setdefault(latest_ref, []).extend(text_snippets[:12])
            except Exception as e:
                logger.warning("_text_search_multi_sites failed (code=%s): %s", code, e)

            # 3) 每个有内容的 ref_date 跑一次 LLM 并写入
            written_refs = set()
            for ref_date in trade_dates:
                if ref_date not in by_ref or not by_ref[ref_date]:
                    continue
                context = "\n\n".join(by_ref[ref_date][:15])[:5000]
                if not context.strip():
                    continue
                prompt = f"""你是一位 A 股舆情分析助手。根据以下与该公司/股票相关的新闻或政策摘要，判断对该公司股价的影响是 利好 还是 利空 或 中性；若无有效信息则判断为 无信号。

股票代码：{code}
公司名称：{corp_name or '未知'}
信号日期：{ref_date}

新闻/政策摘要：
{context}

请严格按以下格式回答（只输出一行结论，不要多余解释）：
第一行：仅输出四个词之一 —— 看涨、看跌、中性、无信号
第二行起（可选）：用一句话说明理由。"""
                raw = _llm(client, prompt, max_tokens=400)
                direction, reason = _parse_direction_from_llm(raw)
                _upsert_news_signal(code, ref_date, direction, reason)
                _insert_news_opinion_record(code, fetch_ts, ref_date, direction, reason, context)
                written_refs.add(ref_date)
                results.append({"code": code, "ref_date": ref_date, "direction": direction, "reason": reason[:100]})

            # 4) 若最新交易日未写入过，补一条（无新闻则 无信号）并写入拉取记录，确保每只股票至少有一条新闻舆论信号
            if latest_ref not in written_refs:
                _upsert_news_signal(code, latest_ref, DIR_NONE, "无新闻")
                _insert_news_opinion_record(code, fetch_ts, latest_ref, DIR_NONE, "无新闻", "")
                results.append({"code": code, "ref_date": latest_ref, "direction": DIR_NONE, "reason": "无新闻"})
                logger.info("news_signal %s: no news content, wrote 无新闻 for ref_date=%s", code, latest_ref)
        except Exception as e:
            logger.exception("news_signal %s: %s", code, e)
            # 确保至少写入一条 无新闻 信号，避免前端完全无 新闻舆论 列数据
            try:
                fetch_ts = datetime.now()
                latest_ref = _get_latest_trade_date(code) or datetime.now().strftime("%Y-%m-%d")
                _upsert_news_signal(code, latest_ref, DIR_NONE, "无新闻或拉取异常")
                _insert_news_opinion_record(code, fetch_ts, latest_ref, DIR_NONE, "无新闻或拉取异常", "")
            except Exception as e2:
                logger.warning("news_signal %s: fallback 无新闻 write failed: %s", code, e2)
            results.append({"code": code, "error": str(e)})

    return {
        "ok": True,
        "codes_processed": len(results),
        "results": results,
    }
