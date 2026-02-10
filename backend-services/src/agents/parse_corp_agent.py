"""
Agent：解析企业 —— 互联网搜索 + AI 大模型
1. 搜索该股票企业的主营业务介绍，经 LLM 整理后入库；
2. 对该企业主营业务进行核心竞争力分析，重点为是否利于中美科技竞争战略
   （太空经济、航天制造、AI、芯片制造、新能源、机器人制造、前沿稀缺材料），结果入库。
"""
import logging
from typing import Any, Optional

from openai import OpenAI

from ..config import MOONSHOT_API_KEY, MOONSHOT_BASE_URL
from ..db import get_conn

logger = logging.getLogger(__name__)

# 互联网搜索：优先 duckduckgo-search，无则跳过搜索仅用 LLM
def _web_search(query: str, max_results: int = 6) -> list[str]:
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        return [r.get("body") or r.get("title", "") for r in results if r.get("body") or r.get("title")]
    except Exception as e:
        logger.warning("web_search failed: %s", e)
        return []


def _llm(client: OpenAI, prompt: str, max_tokens: int = 2000) -> str:
    try:
        resp = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        logger.exception("llm call failed: %s", e)
        raise


def _get_corp_name(code: str) -> str:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM stex.corp WHERE code = %s", (code,))
            row = cur.fetchone()
    return (row[0] or "").strip() if row else ""


def _upsert_corp_analysis(
    code: str,
    business_intro: Optional[str] = None,
    competitiveness_analysis: Optional[str] = None,
) -> None:
    """仅更新传入的非 None 字段，未传入的保留库内原值。"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stex.corp_analysis (code, business_intro, competitiveness_analysis, updated_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (code) DO UPDATE SET
                  business_intro = COALESCE(EXCLUDED.business_intro, stex.corp_analysis.business_intro),
                  competitiveness_analysis = COALESCE(EXCLUDED.competitiveness_analysis, stex.corp_analysis.competitiveness_analysis),
                  updated_at = NOW()
                """,
                (code, business_intro, competitiveness_analysis),
            )
        conn.commit()


def run_parse_corp_agent(codes: Optional[list[str]] = None) -> dict[str, Any]:
    """
    对指定股票代码执行「解析企业」：搜索主营业务 → LLM 整理入库；再 LLM 分析核心竞争力（中美科技竞争战略）入库。
    支持批量 codes：逐只解析并入库，返回汇总结果。
    """
    if not MOONSHOT_API_KEY:
        return {"ok": False, "error": "MOONSHOT_API_KEY 未配置"}

    if not codes or len(codes) == 0:
        return {"ok": False, "error": "请提供至少一只股票代码"}

    client = OpenAI(api_key=MOONSHOT_API_KEY, base_url=MOONSHOT_BASE_URL)
    # 去重 + 清洗
    norm_codes: list[str] = []
    seen = set()
    for c in codes:
        s = str(c).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        norm_codes.append(s)
    if not norm_codes:
        return {"ok": False, "error": "股票代码为空"}

    # 避免一次任务过久：限制批量数量（可按需调大）
    max_batch = 30
    targets = norm_codes[:max_batch]

    def _parse_one(code: str) -> dict[str, Any]:
        corp_name = _get_corp_name(code)
        search_term = f"{code} {corp_name} 主营业务 公司介绍" if corp_name else f"{code} 股票 主营业务 公司介绍"

        # Step 1: 互联网搜索 + LLM 整理主营业务
        snippets = _web_search(search_term)
        context = "\n\n".join(snippets[:8]) if snippets else "（未获取到搜索结果，请根据你的知识简要介绍。）"

        prompt_intro = f"""你是一位证券研究助手。根据以下搜索摘要（或你的知识），用 2～5 段话整理该 A 股上市公司的主营业务介绍以及主要的竞争对手名字，要求客观、简洁、突出主业与核心产品/服务。
股票代码：{code}
公司名称：{corp_name or '未知'}

搜索/参考内容：
{context[:6000]}

只列列举主要竞争对手名字，不要列列举竞争对手的详细介绍
请直接输出「主营业务介绍」正文，不要加标题或前缀。"""

        business_intro = _llm(client, prompt_intro, max_tokens=1500)
        if not business_intro:
            business_intro = "（未能生成主营业务介绍）"
        _upsert_corp_analysis(code, business_intro=business_intro)

        # Step 2: 核心竞争力与中美科技竞争战略分析
        prompt_comp = f"""你是一位战略投资与产业分析专家。基于以下该公司主营业务介绍，分析其核心竞争力，并重点回答：该企业在中国本土或者全球市场的地位，该企业的核心业务是否有利于中美科技竞争战略？
需结合以下领域至少一项或多项进行判断：太空经济、航天制造、人工智能（AI）、芯片/半导体制造、新能源、机器人/自动化制造、前沿稀缺材料。
若与上述领域关联较弱或无关，请如实说明。

主营业务介绍：
{business_intro[:4000]}

请用 3～8 段话输出分析结果，结构建议：
1. 核心竞争力简述；
2. 在本土或者全球市场的地位；
3. 与战略领域关联性（若有）；
4. 中美科技竞争格局作用（若有）；
5. 结论与局限。"""

        competitiveness_analysis = _llm(client, prompt_comp, max_tokens=2000)
        if not competitiveness_analysis:
            competitiveness_analysis = "（未能生成竞争力分析）"
        _upsert_corp_analysis(code, competitiveness_analysis=competitiveness_analysis)

        return {
            "ok": True,
            "code": code,
            "business_intro_len": len(business_intro or ""),
            "competitiveness_analysis_len": len(competitiveness_analysis or ""),
        }

    results: list[dict[str, Any]] = []
    ok_count = 0
    fail_count = 0
    for c in targets:
        try:
            r = _parse_one(c)
            results.append(r)
            ok_count += 1
        except Exception as e:
            fail_count += 1
            results.append({"ok": False, "code": c, "error": str(e)})

    return {
        "ok": ok_count > 0 and fail_count == 0,
        "codes_requested": len(norm_codes),
        "codes_processed": len(targets),
        "codes_ok": ok_count,
        "codes_failed": fail_count,
        "results": results,
        "note": (None if len(norm_codes) <= max_batch else f"仅处理前 {max_batch} 只，剩余 {len(norm_codes) - max_batch} 只未处理"),
    }
