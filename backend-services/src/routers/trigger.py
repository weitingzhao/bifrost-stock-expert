import json
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from ..config import MOONSHOT_API_KEY, DATA_SOURCE
from ..db import get_conn
from ..workflow import run_workflow
from ..agents.corp_agent import run_corp_agent
from ..agents.tushare_corp_agent import run_tushare_corp_agent
from ..agents.watchlist_data_agent import run_watchlist_data_agent
from ..agents.parse_corp_agent import run_parse_corp_agent
from ..agents.signal_agent import run_signal_agent
from ..agents.news_signal_agent import run_news_signal_agent
from ..agents.index_data_agent import run_index_data_agent
from ..agents.index_signal_agent import run_index_signal_agent
from ..agents.investment_summary_agent import run_investment_summary_agent
from ..agents.pattern_agent import run_pattern_agent
from ..agents.incremental_daily_agent import run_incremental_daily_agent

logger = logging.getLogger(__name__)
router = APIRouter()


class TriggerBody(BaseModel):
    action: str = "collect"  # collect | analyze | collect_corp | collect_watchlist | collect_full_market | parse_corp_batch | compute_signals | compute_index_signals | news_signal | ...
    codes: Optional[list[str]] = None
    interval_min: Optional[int] = None  # 日线=0, 15分钟=15 等
    batch_size: Optional[int] = None  # collect_full_market 每批数量，默认 80
    industry: Optional[str] = None  # parse_corp_batch 时可选：行业名，逗号分隔，不传则用默认科技/制造行业
    batches: Optional[int] = None  # collect_full_market / parse_corp_batch 时：连续批次数，默认 1
    start_date: Optional[str] = None  # incremental_daily 时可选：起始日期 YYYY-MM-DD 或 YYYYMMDD，拉取该日（含）之后到最近交易日


@router.post("/trigger")
async def trigger(body: TriggerBody):
    # Agent：采集 A 股各板块上市公司基础数据（按 DATA_SOURCE 选 akshare 或 tushare）
    if body.action == "collect_corp":
        log_id = None
        agent_id = "tushare_corp_agent" if DATA_SOURCE == "tushare" else "corp_agent"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("collect_corp", agent_id, "采集A股上市公司基础数据", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_tushare_corp_agent() if DATA_SOURCE == "tushare" else run_corp_agent()
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s
                    """,
                    (status, json.dumps(result, default=str), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "collect_corp", "result": result}

    # Agent：拉取已收藏跟踪股票的日线/技术/基本面/财务（可指定 codes 仅更新单只或若干只）
    if body.action == "collect_watchlist" or body.action == "collect_stock":
        log_id = None
        codes_arg = body.codes if body.codes else None
        task_label = f"更新单只 {codes_arg[0]}" if (codes_arg and len(codes_arg) == 1) else "拉取收藏股票日线/技术/基本面/财务"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("collect_watchlist", "watchlist_data_agent", task_label, "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_watchlist_data_agent(codes=codes_arg)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": body.action, "result": result}

    # Agent：全市场数据采集（支持批次数，后台异步串行执行，防止前端超时）
    if body.action == "collect_full_market":
        import threading
        import time

        batch_size = min((body.batch_size or 80), 200)
        batches = max(1, min(body.batches or 1, 20))  # 一次触发最多串行 20 批
        delay_sec = 60  # 批次间隔，防限流，可按需调小

        # 先探测是否有待更新股票
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.code FROM stex.corp c
                    LEFT JOIN (
                        SELECT code, MAX(trade_date) AS last_date FROM stex.stock_day GROUP BY code
                    ) d ON d.code = c.code
                    ORDER BY d.last_date NULLS FIRST, d.last_date ASC NULLS FIRST
                    LIMIT %s
                    """,
                    (batch_size,),
                )
                codes_arg = [str(r[0]) for r in cur.fetchall()]
        if not codes_arg:
            return {
                "ok": True,
                "action": "collect_full_market",
                "result": {"message": "无待更新股票，全市场已有日线数据", "codes_processed": 0},
            }

        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("collect_full_market", "watchlist_data_agent", f"全市场数据采集(计划 {batches} 批，每批约 {batch_size} 只)", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()

        def _run_batches():
            per_batch = []
            total_processed = 0
            ok_all = True
            try:
                for b in range(batches):
                    # 每批重新选择“最缺日线”的股票
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                SELECT c.code FROM stex.corp c
                                LEFT JOIN (
                                    SELECT code, MAX(trade_date) AS last_date FROM stex.stock_day GROUP BY code
                                ) d ON d.code = c.code
                                ORDER BY d.last_date NULLS FIRST, d.last_date ASC NULLS FIRST
                                LIMIT %s
                                """,
                                (batch_size,),
                            )
                            codes_batch = [str(r[0]) for r in cur.fetchall()]
                    if not codes_batch:
                        per_batch.append({"batch": b + 1, "codes": 0, "ok": True, "message": "无待更新股票，提前结束"})
                        break

                    result = run_watchlist_data_agent(codes=codes_batch)
                    result["batch_size"] = len(codes_batch)
                    result["batch_index"] = b + 1
                    total_processed += result.get("codes_processed", 0)
                    ok_all = ok_all and result.get("ok", False)
                    per_batch.append(result)
                    logger.info("全市场数据采集 批次 %s 完成，共 %s 只", b + 1, result.get("codes_processed", 0))

                    if b < batches - 1:
                        time.sleep(delay_sec)

                summary = {
                    "ok": ok_all,
                    "batches_requested": batches,
                    "batches_run": len(per_batch),
                    "total_processed": total_processed,
                    "per_batch": per_batch,
                }
                status = "success" if ok_all else "failed"
            except Exception as e:
                logger.exception("collect_full_market async")
                summary = {"ok": False, "error": str(e), "per_batch": per_batch}
                status = "failed"

            # 写回日志
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                            (status, json.dumps(summary, default=str), log_id),
                        )
                    conn.commit()
            except Exception:
                logger.exception("collect_full_market log update failed")

        threading.Thread(target=_run_batches, daemon=True).start()

        return {
            "ok": True,
            "action": "collect_full_market",
            "result": {
                "message": f"已后台启动，全市场数据采集计划 {batches} 批，每批约 {batch_size} 只，间隔 ~{delay_sec}s；请稍后在执行日志查看进度",
                "batches": batches,
                "batch_size": batch_size,
                "log_id": log_id,
            },
        }

    if body.action == "collect" and body.codes:
        with get_conn() as conn:
            for code in body.codes[:20]:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        """,
                        ("collect", "collector", f"collect_{code}", "running"),
                    )
            conn.commit()
        return {"ok": True, "action": "collect", "codes": body.codes, "message": "Task queued (placeholder)"}

    if body.action == "analyze" and body.codes:
        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        result = await run_workflow(body.codes)
        return {"ok": True, "action": "analyze", "result": result}

    # Agent：解析企业（互联网搜索 + LLM 主营业务介绍与核心竞争力/中美科技竞争战略分析，入库）
    if body.action == "parse_corp" and body.codes:
        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        log_id = None
        code_arg = body.codes[0] if body.codes else None
        task_label = (
            f"解析企业 批量({len(body.codes)})"
            if body.codes and len(body.codes) > 1
            else f"解析企业 {code_arg or ''}"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("parse_corp", "parse_corp_agent", task_label, "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_parse_corp_agent(codes=body.codes)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "parse_corp", "result": result}

    # 批量解析企业（按行业选未解析的股票，供「中美科技竞争战略」选股用）— 异步后台执行
    if body.action == "parse_corp_batch":
        import threading
        import time

        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        default_industries = ["电子", "计算机", "国防军工", "电气设备", "通信", "传媒", "汽车", "机械设备"]
        if body.industry and str(body.industry).strip():
            industries = [s.strip() for s in str(body.industry).split(",") if s.strip()]
        else:
            industries = default_industries
        batch_limit = min((body.batch_size or 50), 60)  # 单批最多 60，默认 50
        batches = max(1, min(body.batches or 1, 10))   # 最多串行 10 批
        delay_sec = 5  # 批次间隔，适度错峰，避免长时间占用

        # 先探测是否有未解析的股票
        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(industries))
                cur.execute(
                    f"""
                    SELECT c.code FROM stex.corp c
                    LEFT JOIN stex.corp_analysis a ON a.code = c.code
                    WHERE a.code IS NULL AND c.industry IN ({placeholders})
                    ORDER BY c.code
                    LIMIT %s
                    """,
                    (*industries, batch_limit),
                )
                codes_probe = [str(r[0]) for r in cur.fetchall()]
        if not codes_probe:
            return {
                "ok": True,
                "action": "parse_corp_batch",
                "result": {"message": "当前行业下暂无未解析股票", "codes_processed": 0, "industries": industries},
            }

        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("parse_corp_batch", "parse_corp_agent", f"批量解析企业 计划 {batches} 批，每批约 {batch_limit} 只", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()

        def _run_parse_batches():
            per_batch = []
            total_ok = 0
            total_fail = 0
            try:
                for b in range(batches):
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            placeholders = ",".join(["%s"] * len(industries))
                            cur.execute(
                                f"""
                                SELECT c.code FROM stex.corp c
                                LEFT JOIN stex.corp_analysis a ON a.code = c.code
                                WHERE a.code IS NULL AND c.industry IN ({placeholders})
                                ORDER BY c.code
                                LIMIT %s
                                """,
                                (*industries, batch_limit),
                            )
                            codes_batch = [str(r[0]) for r in cur.fetchall()]
                    if not codes_batch:
                        per_batch.append({"batch": b + 1, "codes": 0, "ok": True, "message": "无未解析股票，提前结束"})
                        break

                    chunk_size = 30
                    ok_all = True
                    ok_cnt = 0
                    fail_cnt = 0
                    for i in range(0, len(codes_batch), chunk_size):
                        chunk = codes_batch[i : i + chunk_size]
                        result = run_parse_corp_agent(codes=chunk)
                        if not result.get("ok"):
                            ok_all = False
                        ok_cnt += result.get("codes_ok", 0)
                        fail_cnt += result.get("codes_failed", 0)
                    total_ok += ok_cnt
                    total_fail += fail_cnt
                    per_batch.append(
                        {
                            "batch": b + 1,
                            "codes": len(codes_batch),
                            "ok": ok_all,
                            "codes_ok": ok_cnt,
                            "codes_failed": fail_cnt,
                        }
                    )
                    logger.info("批量解析企业 批次 %s 完成，共 %s 只，成功 %s，失败 %s", b + 1, len(codes_batch), ok_cnt, fail_cnt)

                    if b < batches - 1:
                        time.sleep(delay_sec)

                summary = {
                    "ok": all(p.get("ok", False) for p in per_batch) if per_batch else True,
                    "batches_requested": batches,
                    "batches_run": len(per_batch),
                    "total_ok": total_ok,
                    "total_failed": total_fail,
                    "per_batch": per_batch,
                }
                status = "success" if summary["ok"] else "failed"
            except Exception as e:
                logger.exception("parse_corp_batch async")
                summary = {"ok": False, "error": str(e), "per_batch": per_batch}
                status = "failed"

            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                            (status, json.dumps(summary, default=str, ensure_ascii=False), log_id),
                        )
                    conn.commit()
            except Exception:
                logger.exception("parse_corp_batch log update failed")

        threading.Thread(target=_run_parse_batches, daemon=True).start()

        return {
            "ok": True,
            "action": "parse_corp_batch",
            "result": {
                "message": f"已后台启动，计划 {batches} 批，每批约 {batch_limit} 只（未解析的 {', '.join(industries)} 行业股票）；可稍后在执行日志查看进度",
                "batches": batches,
                "batch_size": batch_limit,
                "log_id": log_id,
            },
        }

    # Agent：计算投资信号（成交量+资金+MA、金叉死叉、主力资金、支撑阻力等 6 类），按交易日入库
    if body.action == "compute_signals":
        codes_arg = body.codes if body.codes else None
        task_label = f"计算信号 批量({len(codes_arg)})" if codes_arg and len(codes_arg) > 1 else f"计算信号 {codes_arg[0] if codes_arg else ''}"
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("compute_signals", "signal_agent", task_label, "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_signal_agent(codes=codes_arg)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "compute_signals", "result": result}

    # Agent：计算大盘指数投资信号（成交量涨跌幅、均线金叉死叉、支撑阻力位），写入 stex.signals（code=指数代码）
    if body.action == "compute_index_signals":
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("compute_index_signals", "index_signal_agent", "计算大盘信号", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_index_signal_agent()
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "compute_index_signals", "result": result}

    # Agent：新闻舆论信号（搜索互联网新闻 + LLM 判断利好/利空 → 看涨/看跌/中性/无信号，写入投资信号表）
    if body.action == "news_signal":
        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        codes_arg = body.codes if body.codes else None
        task_label = f"新闻舆论信号 批量({len(codes_arg)})" if codes_arg and len(codes_arg) > 1 else f"新闻舆论信号 {codes_arg[0] if codes_arg else ''}"
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("news_signal", "news_signal_agent", task_label, "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_news_signal_agent(codes=codes_arg)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "news_signal", "result": result}

    # Agent：批量采集跟踪股票新闻舆论（不传 codes，使用 watchlist 最多 20 只）
    if body.action == "news_signal_batch":
        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("news_signal_batch", "news_signal_agent", "批量采集跟踪股票新闻舆论", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_news_signal_agent(codes=None)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "news_signal_batch", "result": result}

    # Agent：采集大盘指数日线（上证/深证/创业板/沪深300等），写入 stex.index_day
    if body.action == "collect_index":
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("collect_index", "index_data_agent", "采集大盘指数日线", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_index_data_agent()
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "collect_index", "result": result}

    # Agent：增量拉取全市场最新交易日日线与每日指标（含今日，收盘后可更新到当日；Tushare daily + daily_basic 各一次请求）
    if body.action == "incremental_daily":
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("incremental_daily", "incremental_daily_agent", "增量日线(最新) 全市场", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_incremental_daily_agent(start_date=body.start_date)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "incremental_daily", "result": result}

    # Agent：股票投资总结（信号+日线+技术+企业分析+大盘+财务 → LLM 输出建仓区间、持仓时间、关注信号，写入 stex.investment_summary）
    if body.action == "investment_summary":
        if not MOONSHOT_API_KEY:
            raise HTTPException(503, "MOONSHOT_API_KEY not configured")
        codes_arg = body.codes if body.codes else None
        task_label = (
            f"投资总结 批量({len(codes_arg)})"
            if codes_arg and len(codes_arg) > 1
            else f"投资总结 {codes_arg[0] if codes_arg else ''}"
        )
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("investment_summary", "investment_summary_agent", task_label, "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        result = run_investment_summary_agent(codes=codes_arg)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "investment_summary", "result": result}

    # 形态识别：杯柄、上升三法，写入 stex.pattern_signal，供选股「经典形态策略」使用
    if body.action == "detect_pattern":
        log_id = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    RETURNING id
                    """,
                    ("detect_pattern", "pattern_agent", "K线形态识别(杯柄/上升三法)", "running"),
                )
                row = cur.fetchone()
                log_id = row[0] if row else None
            conn.commit()
        codes_arg = body.codes if body.codes else None
        result = run_pattern_agent(codes=codes_arg)
        status = "success" if result.get("ok") else "failed"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE stex.workflow_log SET status = %s, finished_at = NOW(), output_snapshot = %s WHERE id = %s",
                    (status, json.dumps(result, default=str, ensure_ascii=False), log_id),
                )
            conn.commit()
        return {"ok": result.get("ok", False), "action": "detect_pattern", "result": result}

    # 每日编排：一次执行当日所需全部任务（增量日线(最新) → 大盘 → 解析新跟踪企业 → 大盘信号 → 跟踪股信号 → 新闻舆论，新闻放最后便于前面结果先展示）
    if body.action == "daily_tasks":
        steps_summary = []
        overall_ok = True

        def _log_step(workflow_id: str, agent_id: str, task: str, status: str, result_snapshot: dict):
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO stex.workflow_log (workflow_id, agent_id, task, status, started_at, finished_at, output_snapshot)
                        VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
                        """,
                        (workflow_id, agent_id, task, status, json.dumps(result_snapshot, default=str, ensure_ascii=False)),
                    )
                conn.commit()

        # 1. 增量日线（最新交易日，含今日）
        try:
            result = run_incremental_daily_agent(start_date=body.start_date)
            ok = result.get("ok", False)
            steps_summary.append({"step": "incremental_daily", "ok": ok, "result": result})
            if not ok:
                overall_ok = False
            _log_step("daily_tasks", "incremental_daily_agent", "1. 增量日线(最新) 全市场", "success" if ok else "failed", result)
        except Exception as e:
            logger.exception("daily_tasks step incremental_daily failed: %s", e)
            steps_summary.append({"step": "incremental_daily", "ok": False, "error": str(e)})
            _log_step("daily_tasks", "incremental_daily_agent", "1. 增量日线(最新) 全市场", "failed", {"error": str(e)})
            overall_ok = False

        # 2. 采集大盘指数日线
        try:
            result = run_index_data_agent()
            ok = result.get("ok", False)
            steps_summary.append({"step": "collect_index", "ok": ok, "result": result})
            if not ok:
                overall_ok = False
            _log_step("daily_tasks", "index_data_agent", "2. 采集大盘指数日线", "success" if ok else "failed", result)
        except Exception as e:
            logger.exception("daily_tasks step collect_index failed: %s", e)
            steps_summary.append({"step": "collect_index", "ok": False, "error": str(e)})
            _log_step("daily_tasks", "index_data_agent", "2. 采集大盘指数日线", "failed", {"error": str(e)})
            overall_ok = False

        # 3. 解析「跟踪列表中尚未解析」的企业
        unparsed_codes = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT w.code FROM stex.watchlist w
                    LEFT JOIN stex.corp_analysis a ON a.code = w.code
                    WHERE a.code IS NULL
                    ORDER BY w.code
                    """
                )
                unparsed_codes = [str(r[0]) for r in cur.fetchall()]
        if unparsed_codes:
            try:
                result = run_parse_corp_agent(codes=unparsed_codes)
                ok = result.get("ok", False)
                steps_summary.append({"step": "parse_new_watchlist", "ok": ok, "codes_count": len(unparsed_codes), "result": result})
                if not ok:
                    overall_ok = False
                _log_step("daily_tasks", "parse_corp_agent", f"3. 解析新跟踪企业({len(unparsed_codes)}只)", "success" if ok else "failed", result)
            except Exception as e:
                logger.exception("daily_tasks step parse_new_watchlist failed: %s", e)
                steps_summary.append({"step": "parse_new_watchlist", "ok": False, "error": str(e)})
                _log_step("daily_tasks", "parse_corp_agent", f"3. 解析新跟踪企业({len(unparsed_codes)}只)", "failed", {"error": str(e)})
                overall_ok = False
        else:
            steps_summary.append({"step": "parse_new_watchlist", "ok": True, "codes_count": 0, "message": "无未解析跟踪股，跳过"})
            _log_step("daily_tasks", "parse_corp_agent", "3. 解析新跟踪企业(0只，跳过)", "success", {"message": "无未解析跟踪股"})

        # 4. 计算大盘信号
        try:
            result = run_index_signal_agent()
            ok = result.get("ok", False)
            steps_summary.append({"step": "compute_index_signals", "ok": ok, "result": result})
            if not ok:
                overall_ok = False
            _log_step("daily_tasks", "index_signal_agent", "4. 计算大盘信号", "success" if ok else "failed", result)
        except Exception as e:
            logger.exception("daily_tasks step compute_index_signals failed: %s", e)
            steps_summary.append({"step": "compute_index_signals", "ok": False, "error": str(e)})
            _log_step("daily_tasks", "index_signal_agent", "4. 计算大盘信号", "failed", {"error": str(e)})
            overall_ok = False

        # 5. 计算跟踪股票信号（使用 watchlist 全部）
        watchlist_codes = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT code FROM stex.watchlist ORDER BY code")
                watchlist_codes = [str(r[0]) for r in cur.fetchall()]
        if watchlist_codes:
            try:
                result = run_signal_agent(codes=watchlist_codes)
                ok = result.get("ok", False)
                steps_summary.append({"step": "compute_signals", "ok": ok, "codes_count": len(watchlist_codes), "result": result})
                if not ok:
                    overall_ok = False
                _log_step("daily_tasks", "signal_agent", f"5. 计算跟踪股信号({len(watchlist_codes)}只)", "success" if ok else "failed", result)
            except Exception as e:
                logger.exception("daily_tasks step compute_signals failed: %s", e)
                steps_summary.append({"step": "compute_signals", "ok": False, "error": str(e)})
                _log_step("daily_tasks", "signal_agent", f"5. 计算跟踪股信号({len(watchlist_codes)}只)", "failed", {"error": str(e)})
                overall_ok = False
        else:
            steps_summary.append({"step": "compute_signals", "ok": True, "codes_count": 0, "message": "跟踪列表为空，跳过"})
            _log_step("daily_tasks", "signal_agent", "5. 计算跟踪股信号(0只，跳过)", "success", {"message": "跟踪列表为空"})

        # 6. 批量采集跟踪股票新闻舆论（放最后，最耗时，前面步骤完成后可先展示）
        try:
            result = run_news_signal_agent(codes=None)
            ok = result.get("ok", False)
            steps_summary.append({"step": "news_signal_batch", "ok": ok, "result": result})
            if not ok:
                overall_ok = False
            _log_step("daily_tasks", "news_signal_agent", "6. 批量采集新闻舆论", "success" if ok else "failed", result)
        except Exception as e:
            logger.exception("daily_tasks step news_signal_batch failed: %s", e)
            steps_summary.append({"step": "news_signal_batch", "ok": False, "error": str(e)})
            _log_step("daily_tasks", "news_signal_agent", "6. 批量采集新闻舆论", "failed", {"error": str(e)})
            overall_ok = False

        return {
            "ok": overall_ok,
            "action": "daily_tasks",
            "steps": steps_summary,
            "result": {"steps": steps_summary},
        }

    raise HTTPException(400, "Need action (collect_corp | collect_watchlist | collect_stock | collect_full_market | incremental_daily | collect | analyze | parse_corp | parse_corp_batch | compute_signals | compute_index_signals | news_signal | news_signal_batch | collect_index | investment_summary | detect_pattern | daily_tasks). collect_stock / parse_corp / compute_signals / news_signal / investment_summary 需 codes（news_signal_batch 使用 watchlist）。")
