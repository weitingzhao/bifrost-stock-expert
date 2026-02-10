"""
Agent：股票投资总结
读取系统计算信号、近 30 日日线、技术指标、企业核心竞争力分析、大盘指数表现、企业财务数据，
调用 AI 大模型对指定股票进行分析，输出投资建议（建仓价位区间、持仓时间、应关注的波动与交易信号等），
写入 stex.investment_summary。
"""
import logging
from typing import Any, Optional

from openai import OpenAI

from ..config import MOONSHOT_API_KEY, MOONSHOT_BASE_URL
from ..db import get_conn

logger = logging.getLogger(__name__)

LIMIT_DAYS = 30


def _float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _str_date(d) -> str:
    if d is None:
        return ""
    if hasattr(d, "isoformat"):
        return d.isoformat()[:10]
    return str(d)[:10]


def _llm(client: OpenAI, prompt: str, max_tokens: int = 4000) -> str:
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


def _gather_context(conn, code: str, corp_name: str) -> str:
    """从数据库汇总该股票近 30 日相关数据，拼成供 LLM 使用的文本。"""
    parts = []

    # 1) 日线（近 30 日，升序）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, open, high, low, close, volume, amount
            FROM stex.stock_day WHERE code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (code, LIMIT_DAYS),
        )
        day_rows = cur.fetchall()
    if day_rows:
        dates_sorted = sorted(set(_str_date(r[0]) for r in day_rows))
        day_by_date = {_str_date(r[0]): r for r in day_rows}
        lines = []
        for d in dates_sorted:
            r = day_by_date.get(d)
            if not r:
                continue
            o, h, l, c, vol, amt = _float(r[1]), _float(r[2]), _float(r[3]), _float(r[4]), _float(r[5]), _float(r[6])
            lines.append(f"{d} O:{o} H:{h} L:{l} C:{c} 量:{vol} 额:{amt}")
        parts.append("【近30日日线】\n" + "\n".join(lines[-LIMIT_DAYS:]))

    # 2) 技术指标（ma5/10/20, macd, rsi, kdj）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT trade_date, ma5, ma10, ma20, macd, macd_signal, macd_hist, rsi, kdj_k, kdj_d, kdj_j
            FROM stex.technicals WHERE code = %s ORDER BY trade_date DESC LIMIT %s
            """,
            (code, LIMIT_DAYS),
        )
        tech_rows = cur.fetchall()
    if tech_rows:
        tech_by_date = {_str_date(r[0]): r for r in tech_rows}
        dates_tech = sorted(tech_by_date.keys())
        lines = []
        for d in dates_tech[-LIMIT_DAYS:]:
            r = tech_by_date[d]
            ma5, ma10, ma20 = _float(r[1]), _float(r[2]), _float(r[3])
            macd, sig, hist = _float(r[4]), _float(r[5]), _float(r[6])
            rsi, k, d_, j = _float(r[7]), _float(r[8]), _float(r[9]), _float(r[10])
            line = f"{d} MA5:{ma5} MA10:{ma10} MA20:{ma20}"
            if macd is not None or rsi is not None:
                line += f" MACD:{macd} signal:{sig} hist:{hist} RSI:{rsi} KDJ(K:{k} D:{d_} J:{j})"
            lines.append(line)
        parts.append("【技术指标】\n" + "\n".join(lines))

    # 3) 系统计算信号（近 30 条，按 ref_date）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT signal_type, direction, reason, ref_date
            FROM stex.signals WHERE code = %s ORDER BY ref_date DESC NULLS LAST, created_at DESC LIMIT %s
            """,
            (code, LIMIT_DAYS),
        )
        sig_rows = cur.fetchall()
    if sig_rows:
        lines = []
        for r in sig_rows:
            stype, direction, reason, ref = r[0], r[1], (r[2] or "")[:200], _str_date(r[3])
            lines.append(f"{ref} [{stype}] {direction} {reason}")
        parts.append("【系统信号】\n" + "\n".join(lines))

    # 4) 企业核心竞争力分析
    with conn.cursor() as cur:
        cur.execute(
            "SELECT business_intro, competitiveness_analysis FROM stex.corp_analysis WHERE code = %s",
            (code,),
        )
        row = cur.fetchone()
    if row and (row[0] or row[1]):
        intro = (row[0] or "")[:2000]
        comp = (row[1] or "")[:3000]
        parts.append("【企业分析】\n主营业务摘要：" + intro + "\n核心竞争力分析：" + comp)

    # 5) 大盘指数（与日线同期的交易日：取该股近 30 日交易日对应的指数涨跌幅）
    if day_rows:
        trade_dates = list({_str_date(r[0]) for r in day_rows})
        if trade_dates:
            placeholders = ",".join(["%s::date"] * len(trade_dates))
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT trade_date, index_code, close, pct_chg
                    FROM stex.index_day
                    WHERE trade_date IN ({placeholders})
                    AND index_code IN ('000001.SH','399006.SZ')
                    ORDER BY trade_date DESC, index_code
                    """,
                    trade_dates,
                )
                idx_rows = cur.fetchall()
            if idx_rows:
                by_d = {}
                for r in idx_rows:
                    td = _str_date(r[0])
                    if td not in by_d:
                        by_d[td] = []
                    by_d[td].append(f"{r[1]}:收{r[2]} 涨跌{r[3]}%")
                lines = [f"{d} " + " | ".join(by_d[d]) for d in sorted(by_d.keys(), reverse=True)[:LIMIT_DAYS]]
                parts.append("【大盘同期表现】\n" + "\n".join(lines))

    # 6) 基本面/估值（fundamentals 最近几条）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT report_date, pe, pb, ps, market_cap, revenue, net_profit, profit_growth, roe
            FROM stex.fundamentals WHERE code = %s ORDER BY report_date DESC LIMIT 8
            """,
            (code,),
        )
        fund_rows = cur.fetchall()
    if fund_rows:
        lines = []
        for r in fund_rows:
            d = _str_date(r[0])
            pe, pb, ps = _float(r[1]), _float(r[2]), _float(r[3])
            cap, rev, profit, growth, roe = _float(r[4]), _float(r[5]), _float(r[6]), _float(r[7]), _float(r[8])
            lines.append(f"{d} PE:{pe} PB:{pb} PS:{ps} 市值:{cap} 营收:{rev} 净利润:{profit} 利润增速:{growth}% ROE:{roe}%")
        parts.append("【基本面/估值】\n" + "\n".join(lines))

    # 7) 企业财务披露（financial）
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT report_date, report_type, revenue, net_profit, total_assets
            FROM stex.financial WHERE code = %s ORDER BY report_date DESC LIMIT 8
            """,
            (code,),
        )
        fin_rows = cur.fetchall()
    if fin_rows:
        lines = []
        for r in fin_rows:
            d, rtype = _str_date(r[0]), r[1] or ""
            rev, profit, assets = _float(r[2]), _float(r[3]), _float(r[4])
            lines.append(f"{d} {rtype} 营收:{rev} 净利润:{profit} 总资产:{assets}")
        parts.append("【财务披露】\n" + "\n".join(lines))

    header = f"股票代码：{code}\n公司名称：{corp_name or '未知'}\n\n"
    if not parts:
        return header.rstrip()  # 无任何数据时仅返回头部，调用方据此判断跳过 LLM
    return header + "\n\n".join(parts)


def _upsert_summary(conn, code: str, content: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stex.investment_summary (code, content, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (code) DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()
            """,
            (code, content),
        )
    conn.commit()


def run_investment_summary_agent(codes: Optional[list[str]] = None) -> dict[str, Any]:
    """
    对指定股票执行「股票投资总结」：汇总信号、日线、技术指标、企业分析、大盘、财务，
    调用 LLM 生成投资建议（建仓价位、持仓时间、关注信号等），写入 stex.investment_summary。
    """
    if not MOONSHOT_API_KEY:
        return {"ok": False, "error": "MOONSHOT_API_KEY 未配置", "results": []}

    norm_codes: list[str] = []
    seen = set()
    for c in (codes or []):
        s = str(c).strip()
        if s and s not in seen:
            seen.add(s)
            norm_codes.append(s)
    if not norm_codes:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
                norm_codes = [str(r[0]) for r in cur.fetchall()]
    if not norm_codes:
        return {"ok": False, "error": "请提供至少一只股票代码或先添加收藏", "results": []}

    max_batch = 10
    targets = norm_codes[:max_batch]
    client = OpenAI(api_key=MOONSHOT_API_KEY, base_url=MOONSHOT_BASE_URL)

    default_sys_prompt = """你是一位 A 股投资顾问。请根据下方提供的系统数据（日线、技术指标、系统计算信号、企业竞争力分析、大盘表现、财务数据），
对该只股票撰写一份简洁的「投资总结」与操作建议。内容必须包含且分点写清：

1. **建仓价位区间**：结合近期高低点与均线，给出一个可考虑的建仓价格区间（例如「XX 元～XX 元」），并简要说明理由。
2. **建议持仓时间**：短线/波段/中长线的大致持有周期（如 1～2 周、1～3 个月等），及对应逻辑。
3. **应重点关注的波动与交易信号**：例如放量突破、均线金叉/死叉、RSI 超买超卖、主力资金异动、系统信号中的看涨/看跌触发等，列出 3～5 条并说明如何利用。

全文用中文，语气专业但简洁。文末注明：以上内容仅供参考，不构成投资建议。"""

    sys_prompt = default_sys_prompt
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM stex.app_config WHERE key = %s",
                ("investment_summary_prompt",),
            )
            row = cur.fetchone()
            if row and row[0] and (row[0] or "").strip():
                sys_prompt = (row[0] or "").strip()

    results: list[dict[str, Any]] = []
    ok_count = 0
    with get_conn() as conn:
        for code in targets:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT name FROM stex.corp WHERE code = %s", (code,))
                    row = cur.fetchone()
                corp_name = (row[0] or "").strip() if row else ""

                context = _gather_context(conn, code, corp_name)
                header_only = f"股票代码：{code}\n公司名称：{corp_name or '未知'}"
                if not context.strip() or context.strip() == header_only:
                    results.append({"code": code, "ok": False, "error": "无日线等数据，无法生成总结"})
                    continue

                user_content = context + "\n\n请按上述要求输出投资总结（建仓区间、持仓时间、关注信号）。"
                prompt = sys_prompt + "\n\n---\n\n" + user_content

                content = _llm(client, prompt, max_tokens=4000)
                if not content:
                    content = "（生成失败或为空）"
                _upsert_summary(conn, code, content)
                results.append({"code": code, "ok": True, "content_len": len(content)})
                ok_count += 1
            except Exception as e:
                logger.exception("investment_summary %s: %s", code, e)
                results.append({"code": code, "ok": False, "error": str(e)})

    return {
        "ok": ok_count > 0,
        "codes_requested": len(norm_codes),
        "codes_processed": len(targets),
        "codes_ok": ok_count,
        "results": results,
        "note": (None if len(norm_codes) <= max_batch else f"仅处理前 {max_batch} 只"),
    }
