import { useState, useEffect } from 'react';
import { api } from '../api';

// 当前支持触发的任务类型（与后端 trigger 的 action 一致）
const TASK_TYPES = [
  {
    id: 'collect_corp',
    label: '采集全市场基础数据',
    taskType: '采集',
    needCodes: false,
    description: '全市场上市公司列表与市值等（数据源：Tushare / AkShare）',
  },
  {
    id: 'collect_watchlist',
    label: '采集跟踪股票数据',
    taskType: '采集',
    needCodes: false,
    description: '拉取所有跟踪收藏股票的日线、技术、基本面、财务指标数据',
  },
  {
    id: 'collect_full_market',
    label: '全市场数据采集',
    taskType: '采集',
    needCodes: false,
    description: '每批拉取约 80 只「尚未有日线或数据最旧」股票，可多次执行',
  },
  {
    id: 'incremental_daily',
    label: '增量日线(最新)',
    taskType: '采集',
    needCodes: false,
    description: '按「最近一个交易日（含今日）」快速更新全市场日线与指标',
  },
  {
    id: 'collect_index',
    label: '采集大盘指数',
    taskType: '采集',
    needCodes: false,
    description: '拉取上证/深证/创业板/沪深300/中证500 日线，用于对比大盘',
  },
  {
    id: 'collect_stock',
    label: '更新指定股票数据',
    taskType: '采集',
    needCodes: true,
    description: '仅更新指定股票代码的日线交易数据 / 技术指标 /基本面 / 财务指标 / 波动率等数据',
  },
  {
    id: 'investment_summary',
    label: '股票投资总结',
    taskType: '分析',
    needCodes: true,
    description: '综合系统信号、日线、技术指标、企业竞争力、大盘表现、财务数据，由 AI 给出投资建议',
  },
  {
    id: 'parse_corp',
    label: '解析股票企业',
    taskType: '分析',
    needCodes: true,
    description: '互联网搜索 + AI 生成主营业务介绍与核心竞争力/中美科技竞争战略分析，入库后在股票详情页展示',
  },
  {
    id: 'parse_corp_batch',
    label: '批量解析股票企业',
    taskType: '分析',
    needCodes: false,
    description: '筛选「尚未解析」的股票（默认：电子、计算机、国防军工、电气设备、通信、传媒、汽车、机械设备）',
  },
  {
    id: 'compute_signals',
    label: '计算投资信号',
    taskType: '分析',
    needCodes: true,
    description: '计算 7 类信号：成交量资金MA20、涨跌幅、持续资金、金叉死叉、主力资金、支撑阻力、换手率',
  },
  {
    id: 'compute_index_signals',
    label: '计算大盘信号',
    taskType: '分析',
    needCodes: false,
    description: '对大盘指数计算成交量MA20、涨跌幅、金叉死叉、多空排列、支撑阻力、量价背离、波动率突破',
  },
  {
    id: 'detect_pattern',
    label: 'K线形态识别',
    taskType: '分析',
    needCodes: false,
    description: '对有日线数据的股票识别杯柄形态、上升三法，写入形态信号表；选股「经典形态策略」依赖任务结果',
  },
  {
    id: 'news_signal',
    label: '新闻舆论信号',
    taskType: '分析',
    needCodes: true,
    description: '主流财经媒体搜索该股票企业近期新闻/热点/政策，解读判断利好/利空，输出 看涨 / 看跌 / 中性 / 无信号',
  },
  {
    id: 'news_signal_batch',
    label: '批量采集新闻舆论',
    taskType: '分析',
    needCodes: false,
    description: '对收藏列表中的跟踪股票（最多 20 只）批量拉取新闻并判断利好/利空，写入新闻舆论信号',
  },
  {
    id: 'daily_tasks',
    label: '每日一键执行',
    taskType: '编排',
    needCodes: false,
    description: '一键按顺序执行：增量日线(最新) → 大盘指数 → 解析新跟踪企业 → 大盘信号 → 跟踪股信号 → 批量新闻舆论',
  }
];

function getTaskTypeFromWorkflow(workflowId) {
  if (!workflowId) return '-';
  if (String(workflowId).startsWith('daily_tasks')) return '编排';
  if (String(workflowId).startsWith('investment_summary')) return '分析';
  if (String(workflowId).startsWith('analyze')) return '分析';
  if (String(workflowId).startsWith('parse_corp')) return '分析';
  if (String(workflowId).startsWith('parse_corp_batch')) return '分析';
  if (String(workflowId).startsWith('compute_signals')) return '分析';
  if (String(workflowId).startsWith('compute_index_signals')) return '分析';
  if (String(workflowId).startsWith('detect_pattern')) return '分析';
  if (String(workflowId).startsWith('news_signal')) return '分析';
  if (String(workflowId).startsWith('collect_index')) return '采集';
  if (String(workflowId).startsWith('incremental_daily')) return '采集';
  if (String(workflowId).startsWith('collect_full_market')) return '采集';
  if (String(workflowId).startsWith('collect')) return '采集';
  return '-';
}

function getStatusClass(status) {
  const s = (status || '').toLowerCase();
  if (s === 'success') return 'log-status-success';
  if (s === 'failed' || s === 'error') return 'log-status-failed';
  if (s === 'running' || s === 'pending') return 'log-status-running';
  return '';
}

const PAGE_SIZE = 30;

export function Workflow() {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [triggering, setTriggering] = useState(null); // 正在执行的任务 id
  const [message, setMessage] = useState('');
  const [codesByTask, setCodesByTask] = useState({}); // collect_stock / analyze 的代码输入
  const [batchesByTask, setBatchesByTask] = useState({}); // 批次数：collect_full_market / parse_corp_batch
  const [page, setPage] = useState(1);

  const fetchLogs = () => {
    setLoading(true);
    api.workflow.logs(200).then(setLogs).catch(() => setLogs([])).finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchLogs();
  }, []);

  // 执行结果 5 秒后自动隐藏
  useEffect(() => {
    if (!message) return;
    const t = setTimeout(() => setMessage(''), 5000);
    return () => clearTimeout(t);
  }, [message]);

  const total = logs.length;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const currentPage = Math.min(Math.max(1, page), totalPages);
  const start = (currentPage - 1) * PAGE_SIZE;
  const pageLogs = logs.slice(start, start + PAGE_SIZE);

  const runTrigger = (task) => {
    const needCodes = task.needCodes;
    const codes = needCodes
      ? (codesByTask[task.id] || '').split(/[\s,，]+/).map((s) => s.trim()).filter(Boolean)
      : undefined;
    const batches = (task.id === 'collect_full_market' || task.id === 'parse_corp_batch')
      ? Number(batchesByTask[task.id] || 1)
      : undefined;
    if (needCodes && (!codes || !codes.length)) {
      setMessage(`任务「${task.label}」需填写至少一个股票代码`);
      return;
    }
    setTriggering(task.id);
    setMessage('');
    api.workflow
      .trigger({ action: task.id, codes, batches })
      .then((res) => {
        setMessage(JSON.stringify(res, null, 2));
        fetchLogs();
      })
      .catch((err) => setMessage('触发失败: ' + (err.message || err)))
      .finally(() => setTriggering(null));
  };

  return (
    <div className="page workflow">
      <section className="section">
        <h2>可触发的任务</h2>
        <p className="muted">选择下方任务立即执行；需代码的任务请先填写股票代码（逗号分隔）。</p>
        <div className="workflow-task-cards">
          {TASK_TYPES.map((task) => (
            <div key={task.id} className={`workflow-task-card workflow-task-card--${task.taskType === '编排' ? 'orchestrate' : task.taskType === '采集' ? 'collect' : 'analyze'}`}>
              <div className="workflow-task-card-head">
                <span className={`workflow-task-type workflow-task-type--${task.taskType === '编排' ? 'orchestrate' : task.taskType === '采集' ? 'collect' : 'analyze'}`}>
                  {task.taskType}
                </span>
                <h3>{task.label}</h3>
              </div>
              <p className="workflow-task-desc">{task.description}</p>
              <div className="workflow-task-actions">
                {task.needCodes && (
                  <input
                    type="text"
                    className="workflow-task-codes"
                    placeholder="股票代码，如 688795,600519"
                    value={codesByTask[task.id] || ''}
                    onChange={(e) => setCodesByTask((prev) => ({ ...prev, [task.id]: e.target.value }))}
                  />
                )}
                {(task.id === 'collect_full_market' || task.id === 'parse_corp_batch') && (
                  <input
                    type="number"
                    className="workflow-task-batches"
                    min={1}
                    max={task.id === 'collect_full_market' ? 20 : 10}
                    placeholder="批次数，如 10"
                    value={batchesByTask[task.id] || ''}
                    onChange={(e) => setBatchesByTask((prev) => ({ ...prev, [task.id]: e.target.value }))}
                    title={task.id === 'collect_full_market'
                      ? '一次触发串行跑多批，每批约 80 只，批次间隔约 60 秒'
                      : '批量解析企业：串行多批，每批约 50 只，批次间隔约 5 秒'}
                  />
                )}
                <button
                  type="button"
                  className="workflow-task-run"
                  onClick={() => runTrigger(task)}
                  disabled={triggering !== null}
                >
                  {triggering === task.id ? '执行中…' : '立即执行'}
                </button>
              </div>
            </div>
          ))}
        </div>
        {message && (
          <div className="workflow-message-wrap">
            <pre className="workflow-message">{message}</pre>
            <button type="button" className="workflow-message-close" onClick={() => setMessage('')} title="关闭" aria-label="关闭">×</button>
          </div>
        )}
        <p className="muted">采集/分析由 Python 服务执行，需配置 PYTHON_SERVICE_URL；分析需 MOONSHOT_API_KEY。</p>
      </section>

      <section className="section">
        <div className="section-head-with-action">
          <h2>执行日志</h2>
          <button type="button" className="workflow-logs-refresh" onClick={fetchLogs} disabled={loading}>
            {loading ? '加载中…' : '刷新'}
          </button>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>任务类型</th>
                <th>工作流</th>
                <th>Agent</th>
                <th>任务</th>
                <th>状态</th>
                <th>开始时间</th>
                <th>结束时间</th>
              </tr>
            </thead>
            <tbody>
              {pageLogs.map((row) => (
                <tr key={row.id} className={getStatusClass(row.status)}>
                  <td>{row.id}</td>
                  <td>{getTaskTypeFromWorkflow(row.workflow_id)}</td>
                  <td>{row.workflow_id}</td>
                  <td>{row.agent_id}</td>
                  <td>{row.task}</td>
                  <td>
                    <span className={`log-status-badge log-status-badge--${(row.status || '').toLowerCase()}`}>
                      {row.status || '-'}
                    </span>
                  </td>
                  <td>{row.started_at ? new Date(row.started_at).toLocaleString() : '-'}</td>
                  <td>{row.finished_at ? new Date(row.finished_at).toLocaleString() : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {logs.length === 0 && !loading && <p className="muted">暂无日志</p>}
          {logs.length > 0 && (
            <div className="pagination">
              <button
                type="button"
                disabled={currentPage <= 1}
                onClick={() => setPage((p) => Math.max(1, p - 1))}
              >
                上一页
              </button>
              <span className="page-info">
                第 {start + 1}-{Math.min(start + PAGE_SIZE, total)} 条，共 {total} 条
                {totalPages > 1 && ` · 第 ${currentPage} / ${totalPages} 页`}
              </span>
              <button
                type="button"
                disabled={currentPage >= totalPages}
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              >
                下一页
              </button>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
