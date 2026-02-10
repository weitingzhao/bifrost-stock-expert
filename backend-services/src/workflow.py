"""
工作流：从数据库拉取指定代码的数据，调用 Moonshot 做简要分析并写入 signals。
可后续替换为 CrewAI / LangGraph 多 Agent 编排。
"""
import json
from openai import OpenAI
from .config import MOONSHOT_API_KEY, MOONSHOT_BASE_URL
from .db import get_conn


async def run_workflow(codes: list[str]) -> dict:
    if not MOONSHOT_API_KEY:
        return {"error": "MOONSHOT_API_KEY not set"}

    client = OpenAI(api_key=MOONSHOT_API_KEY, base_url=MOONSHOT_BASE_URL)
    summary_parts = []

    with get_conn() as conn:
        for code in codes[:5]:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, open, high, low, close, volume FROM stex.stock_day WHERE code = %s ORDER BY trade_date DESC LIMIT 30",
                    (code,),
                )
                rows = cur.fetchall()
            if not rows:
                summary_parts.append(f"{code}: 无日线数据")
                continue
            # 简单文本摘要供 LLM 参考
            lines = [f"日期{r[0]} O{r[1]} H{r[2]} L{r[3]} C{r[4]} V{r[5]}" for r in reversed(rows)]
            text = f"股票{code} 最近30日: " + " | ".join(lines[-10:])
            summary_parts.append(text)

    prompt = "以下是中国A股部分股票的近期行情摘要，请用一两句话给出简要看法（偏多、偏空或中性）并说明理由。\n\n" + "\n\n".join(summary_parts)
    try:
        resp = client.chat.completions.create(
            model="moonshot-v1-8k",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
        )
        content = resp.choices[0].message.content if resp.choices else ""
    except Exception as e:
        return {"error": str(e), "summary": ""}

    # 将结论写入 signals（简化：按第一只代码写入一条）
    if codes and content:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.signals (code, signal_type, direction, source, reason)
                    VALUES (%s, 'summary', 'neutral', 'moonshot', %s)
                    """,
                    (codes[0], content[:2000]),
                )
            conn.commit()

    return {"summary": content, "codes": codes}
