import { Router } from 'express';
import { db } from '../db.js';

export const configRouter = Router();

const INVESTMENT_SUMMARY_PROMPT_KEY = 'investment_summary_prompt';

const DEFAULT_INVESTMENT_SUMMARY_PROMPT = `你是一位 A 股投资顾问。请根据下方提供的系统数据（日线、技术指标、系统计算信号、企业竞争力分析、大盘表现、财务数据），
对该只股票撰写一份简洁的「投资总结」与操作建议。内容必须包含且分点写清：

1. **建仓价位区间**：结合近期高低点与均线，给出一个可考虑的建仓价格区间（例如「XX 元～XX 元」），并简要说明理由。
2. **建议持仓时间**：短线/波段/中长线的大致持有周期（如 1～2 周、1～3 个月等），及对应逻辑。
3. **应重点关注的波动与交易信号**：例如放量突破、均线金叉/死叉、RSI 超买超卖、主力资金异动、系统信号中的看涨/看跌触发等，列出 3～5 条并说明如何利用。

全文用中文，语气专业但简洁。文末注明：以上内容仅供参考，不构成投资建议。`;

configRouter.get('/investment-summary-prompt', async (_req, res) => {
  const { rows } = await db.query(
    'SELECT value FROM stex.app_config WHERE key = $1',
    [INVESTMENT_SUMMARY_PROMPT_KEY]
  );
  const value = rows[0]?.value;
  res.json({
    prompt: value != null && value !== '' ? value : DEFAULT_INVESTMENT_SUMMARY_PROMPT,
  });
});

configRouter.put('/investment-summary-prompt', async (req, res) => {
  const prompt = typeof req.body?.prompt === 'string' ? req.body.prompt : '';
  await db.query(
    `INSERT INTO stex.app_config (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [INVESTMENT_SUMMARY_PROMPT_KEY, prompt]
  );
  res.json({ ok: true });
});
