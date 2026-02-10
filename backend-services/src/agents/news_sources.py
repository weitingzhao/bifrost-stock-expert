"""
指定信息源采集新闻：巨潮资讯网、财联社电报、证券时报、中国证券报、雪球、东方财富股吧。
支持方式：RSS 直连、RSSHub 路由、DDG 站内搜索（兜底）。
若配置 RSSHUB_BASE_URL，则优先从 RSSHub 拉取财联社/证券时报/中证网/雪球等；未配置则仅用 DDG 与直连 RSS。
"""
import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote

import httpx

from ..config import RSSHUB_BASE_URL

logger = logging.getLogger(__name__)

# 每条新闻统一格式
NEWS_ITEM = dict[str, Any]  # {"date": "YYYY-MM-DD", "title": "", "body": "", "source": ""}


def _norm_date(s: str) -> str:
    """从 RSS 日期字符串提取 YYYY-MM-DD。"""
    if not s:
        return ""
    s = s.strip()[:25]
    for fmt in ("%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(s.replace("Z", "+00:00").replace("+0000", "+00:00")[:22], fmt[:22])
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    if re.match(r"\d{4}-\d{2}-\d{2}", s):
        return s[:10]
    return ""


def fetch_rss(url: str, source_label: str = "", timeout: float = 15.0) -> list[NEWS_ITEM]:
    """拉取任意 RSS/Atom URL，返回统一格式列表。"""
    try:
        import feedparser
    except ImportError:
        logger.warning("feedparser not installed, skip RSS %s", url[:60])
        return []
    out: list[NEWS_ITEM] = []
    try:
        resp = httpx.get(url, timeout=timeout, follow_redirects=True)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        for e in getattr(feed, "entries", [])[:30]:
            title = (e.get("title") or "").strip()
            desc = (e.get("summary") or e.get("description") or "").strip()
            if isinstance(desc, dict) and desc.get("value"):
                desc = desc["value"]
            published = e.get("published") or e.get("updated") or ""
            date_str = _norm_date(published)
            if title or desc:
                out.append({"date": date_str, "title": title, "body": desc[:1000], "source": source_label or url})
    except Exception as e:
        logger.warning("fetch_rss %s: %s", url[:50], e)
    return out


def fetch_rsshub(path: str, source_label: str, timeout: float = 15.0) -> list[NEWS_ITEM]:
    """请求 RSSHub 某路由，解析 RSS 返回。path 如 cls/telegraph/watch。"""
    if not RSSHUB_BASE_URL:
        return []
    url = f"{RSSHUB_BASE_URL}/{path.lstrip('/')}"
    return fetch_rss(url, source_label=source_label, timeout=timeout)


def code_to_xueqiu_symbol(code: str) -> str:
    """A 股代码转雪球 symbol：上海 60xxxx -> SH60xxxx，深圳 00/30xxxx -> SZ00xxxx。"""
    code = (code or "").strip()
    if len(code) >= 6:
        c6 = code[-6:].lstrip("0") or "0"
        prefix = code[:1] if len(code) > 6 else (code[0] if code else "")
        if prefix in ("6", "5") or (len(code) == 6 and code[0] in "56"):
            return "SH" + code[-6:].zfill(6)
        return "SZ" + code[-6:].zfill(6)
    if code.startswith("6") or code.startswith("5"):
        return "SH" + code.zfill(6)
    return "SZ" + code.zfill(6)


def gather_cls() -> list[NEWS_ITEM]:
    """财联社电报（RSSHub：看盘/公司等）。"""
    items: list[NEWS_ITEM] = []
    for sub in ("watch", "announcement"):
        items.extend(fetch_rsshub(f"cls/telegraph/{sub}", source_label="财联社", timeout=12.0))
    return items[:25]


def gather_stcn() -> list[NEWS_ITEM]:
    """证券时报（RSSHub：要闻/列表）。"""
    items: list[NEWS_ITEM] = []
    for channel in ("yw", "gs", "company"):
        items.extend(fetch_rsshub(f"stcn/article/list/{channel}", source_label="证券时报", timeout=12.0))
    return items[:25]


def gather_cs() -> list[NEWS_ITEM]:
    """中国证券报/中证网（RSSHub：栏目）。"""
    items: list[NEWS_ITEM] = []
    for channel in ("xwzx", "ssgs", "gppd"):
        items.extend(fetch_rsshub(f"cs/news/{channel}", source_label="中国证券报", timeout=12.0))
    return items[:25]


def gather_xueqiu_stock(code: str) -> list[NEWS_ITEM]:
    """雪球单只股票新闻（RSSHub：股票信息/新闻）。无需 Cookie；若自建 RSSHub 可配 XUEQIU 相关。"""
    if not code or not RSSHUB_BASE_URL:
        return []
    symbol = code_to_xueqiu_symbol(code)
    return fetch_rsshub(f"xueqiu/stock_info/{quote(symbol)}/news", source_label="雪球", timeout=12.0)


def gather_cninfo(code: str) -> list[NEWS_ITEM]:
    """巨潮资讯：RSSHub 曾有 /cninfo/announcement，按股票筛选需公告接口；此处拉全量公告流（若有路由）。"""
    if not RSSHUB_BASE_URL:
        return []
    # 部分 RSSHub 实例提供 /cninfo/announcement/:code 或 /cninfo/announcement/all
    items = fetch_rsshub("cninfo/announcement/all", source_label="巨潮资讯", timeout=12.0)
    if code and items:
        # 简单过滤：标题或正文含该代码
        code_short = code[-6:] if len(code) >= 6 else code
        items = [i for i in items if code_short in (i.get("title") or "") or code_short in (i.get("body") or "")]
    return items[:20]


def gather_eastmoney_guba_ddg(corp_name: str, code: str, web_search_fn) -> list[str]:
    """东方财富股吧：无公开 RSS，用 DDG 站内搜索返回摘要列表（与现有 _text_search_multi_sites 一致）。"""
    if not web_search_fn:
        return []
    snippets: list[str] = []
    query = f"site:guba.eastmoney.com {code} {corp_name or ''} 股吧"
    try:
        for s in web_search_fn(query, max_results=5):
            if s and s.strip():
                snippets.append(s.strip())
    except Exception as e:
        logger.warning("eastmoney guba search: %s", e)
    return snippets


def gather_from_specified_sources(
    code: str,
    corp_name: str,
    *,
    web_search_fn=None,
    include_ddg_news: bool = True,
    ddg_news_fn=None,
    ddg_site_fn=None,
) -> list[NEWS_ITEM]:
    """
    从指定信息源汇总新闻：
    - 巨潮资讯（RSSHub cninfo，若配置）
    - 财联社电报（RSSHub cls）
    - 证券时报（RSSHub stcn）
    - 中国证券报（RSSHub cs）
    - 雪球（RSSHub xueqiu 个股新闻）
    - 东方财富股吧（DDG site 搜索）
    并可选保留原有 DDG 综合新闻与站内搜索。
    返回带 date/title/body/source 的列表，供按日期归属与 LLM 分析。
    """
    all_items: list[NEWS_ITEM] = []
    if RSSHUB_BASE_URL:
        all_items.extend(gather_cls())
        all_items.extend(gather_stcn())
        all_items.extend(gather_cs())
        all_items.extend(gather_xueqiu_stock(code))
        all_items.extend(gather_cninfo(code))

    if web_search_fn:
        guba_snippets = gather_eastmoney_guba_ddg(corp_name, code, web_search_fn)
        today = datetime.now().strftime("%Y-%m-%d")
        for sn in guba_snippets[:10]:
            all_items.append({"date": today, "title": "", "body": sn, "source": "东方财富股吧"})

    if include_ddg_news and ddg_news_fn:
        try:
            q = f"{corp_name} {code} 股票" if corp_name else f"{code} 股票"
            for item in ddg_news_fn(q, max_results=8, timelimit="w"):
                all_items.append({
                    "date": item.get("date") or "",
                    "title": (item.get("title") or "").strip(),
                    "body": (item.get("body") or "").strip(),
                    "source": item.get("source") or "搜索",
                })
        except Exception as e:
            logger.warning("ddg news in gather: %s", e)

    if ddg_site_fn and (corp_name or code):
        try:
            for site in ("site:cninfo.com.cn", "site:cls.cn", "site:stcn.com", "site:cs.com.cn", "site:xueqiu.com", "site:eastmoney.com"):
                q = f"{site} {corp_name or code} 新闻 公告"
                for s in ddg_site_fn(q, max_results=2):
                    if s and s.strip():
                        all_items.append({"date": "", "title": "", "body": s.strip(), "source": "搜索"})
        except Exception as e:
            logger.warning("ddg site in gather: %s", e)

    return all_items
