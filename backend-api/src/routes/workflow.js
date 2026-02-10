import { Router } from 'express';
import { db } from '../db.js';

export const workflowRouter = Router();

// 工作流执行日志列表
workflowRouter.get('/logs', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 50, 200);
  const { rows } = await db.query(
    `SELECT id, workflow_id, agent_id, task, status, started_at, finished_at, created_at
     FROM stex.workflow_log
     ORDER BY started_at DESC NULLS LAST, created_at DESC
     LIMIT $1`,
    [limit]
  );
  res.json(rows);
});

// 单条日志详情
workflowRouter.get('/logs/:id', async (req, res) => {
  const { id } = req.params;
  const { rows } = await db.query(
    'SELECT * FROM stex.workflow_log WHERE id = $1',
    [id]
  );
  if (!rows[0]) return res.status(404).json({ error: 'Not found' });
  res.json(rows[0]);
});

// 触发采集/分析（转发到 Python 服务，若未配置则返回提示）
workflowRouter.post('/trigger', async (req, res) => {
  const baseUrl = process.env.PYTHON_SERVICE_URL;
  if (!baseUrl) {
    return res.status(503).json({
      error: 'Python service not configured',
      hint: 'Set PYTHON_SERVICE_URL in backend-api .env',
    });
  }
  try {
    const r = await fetch(`${baseUrl}/api/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body || {}),
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) {
      return res.status(r.status).json(data || { error: 'Trigger failed' });
    }
    res.json(data);
  } catch (e) {
    res.status(502).json({ error: e.message || 'Python service unreachable' });
  }
});
