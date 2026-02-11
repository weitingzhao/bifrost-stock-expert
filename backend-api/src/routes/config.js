import { Router } from 'express';
import { db } from '../db.js';

export const configRouter = Router();

const INVESTMENT_SUMMARY_PROMPT_KEY = 'investment_summary_prompt';
const SCORE_WEIGHTS_KEY = 'score_weights';

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

// 综合得分权重配置：大盘分数权重 + 个股信号权重（用于 watchlist-signals 计算 composite_score）
const DEFAULT_SCORE_WEIGHTS = {
  // 大盘分数：base + bullPerIndex * 看涨指数个数 + bearPerIndex * 看跌指数个数
  marketBase: 1,
  marketBullPerIndex: 0.1,
  marketBearPerIndex: -0.1,
  // 个股信号：除换手率外通用
  stockBull: 1,
  stockBear: -2, // 看跌减分加倍
  stockNeutralUp: 0.2,
  stockNeutralDown: -0.2,
  // 个股信号：每个 signal_type 单独权重（不含换手率）
  signalWeights: {
    '成交量资金MA20': { bull: 1, bear: -2 },
    '成交量涨跌幅': { bull: 1, bear: -2 },
    '持续资金流向': { bull: 1, bear: -2 },
    '均线金叉死叉': { bull: 1, bear: -2 },
    '主力资金': { bull: 1, bear: -2 },
    '支撑阻力位': { bull: 1, bear: -2 },
    '新闻舆论': { bull: 1, bear: -2 },
  },
  // 换手率专用权重
  turnover: {
    low: -0.5,    // 交投清淡
    normal: 0.3,  // 正常活跃
    high: 0.1,    // 异常活跃
  },
};

configRouter.get('/score-weights', async (_req, res) => {
  const { rows } = await db.query(
    'SELECT value FROM stex.app_config WHERE key = $1',
    [SCORE_WEIGHTS_KEY]
  );
  const raw = rows[0]?.value;
  let saved = {};
  if (raw) {
    try {
      saved = JSON.parse(raw);
    } catch {
      saved = {};
    }
  }
  const weights = {
    ...DEFAULT_SCORE_WEIGHTS,
    ...saved,
    turnover: {
      ...DEFAULT_SCORE_WEIGHTS.turnover,
      ...(saved.turnover || {}),
    },
    signalWeights: {
      ...DEFAULT_SCORE_WEIGHTS.signalWeights,
      ...(saved.signalWeights || {}),
    },
  };
  res.json({ weights });
});

configRouter.put('/score-weights', async (req, res) => {
  const weights = req.body?.weights && typeof req.body.weights === 'object'
    ? req.body.weights
    : {};
  await db.query(
    `INSERT INTO stex.app_config (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()`,
    [SCORE_WEIGHTS_KEY, JSON.stringify(weights)]
  );
  res.json({ ok: true });
});
