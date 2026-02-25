import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../api';

export function Home() {
  const [loading, setLoading] = useState(false);
  const [recommendations, setRecommendations] = useState(null);
  const [currentRefDate, setCurrentRefDate] = useState(null);
  const [error, setError] = useState('');

  // 历史推荐
  const [historyDates, setHistoryDates] = useState([]);
  const [selectedDate, setSelectedDate] = useState('');
  const [historyData, setHistoryData] = useState(null);
  const [historyLoading, setHistoryLoading] = useState(false);

  // 加载历史日期列表
  useEffect(() => {
    api.selection.recommendationDates().then(res => {
      setHistoryDates(res.dates || []);
    }).catch(() => {});
  }, [recommendations]);

  // 选择日期后加载历史数据
  const handleLoadHistory = async (date) => {
    if (!date) return;
    setSelectedDate(date);
    setHistoryLoading(true);
    try {
      const res = await api.selection.recommendations(date);
      if (res.ok) {
        setHistoryData(res);
      } else {
        setHistoryData(null);
      }
    } catch {
      setHistoryData(null);
    } finally {
      setHistoryLoading(false);
    }
  };

  const handleRecommend = async () => {
    setLoading(true);
    setError('');
    setRecommendations(null);
    setCurrentRefDate(null);
    try {
      const res = await api.selection.topRecommended(10);
      if (res.ok) {
        setRecommendations(res.stocks);
        setCurrentRefDate(res.ref_date || null);
      } else {
        setError(res.error || '获取推荐失败');
      }
    } catch (e) {
      setError(e.message || '请求失败');
    } finally {
      setLoading(false);
    }
  };

  const [expandedCards, setExpandedCards] = useState({});

  const toggleExpand = (code) => {
    setExpandedCards(prev => ({ ...prev, [code]: !prev[code] }));
  };

  const renderStockCard = (stock) => {
    const isExpanded = expandedCards[stock.code];
    const signalEntries = Object.entries(stock.signals || {});
    const bullCount = signalEntries.filter(([, v]) => v === '看涨').length;
    const bearCount = signalEntries.filter(([, v]) => v === '看跌').length;

    return (
      <div key={stock.code} className={`recommend-card-compact ${isExpanded ? 'expanded' : ''}`}>
        <div className="rcc-header">
          <a href={`/stock/${stock.code}`} target="_blank" rel="noopener noreferrer" className="rcc-code">
            {stock.code}
          </a>
          <span className="rcc-name">{stock.name}</span>
          <span className="rcc-score">{stock.composite_score}</span>
        </div>
        <div className="rcc-meta">
          <span>{stock.industry || '-'}</span>
          <span>¥{stock.latest_close != null && !isNaN(Number(stock.latest_close)) ? Number(stock.latest_close).toFixed(2) : '-'}</span>
          <span>PE {stock.pe != null && !isNaN(Number(stock.pe)) ? Number(stock.pe).toFixed(1) : '-'}</span>
          <span className="rcc-signal-summary">
            {bullCount > 0 && <em className="bull">↑{bullCount}</em>}
            {bearCount > 0 && <em className="bear">↓{bearCount}</em>}
          </span>
        </div>
        <div className="rcc-analysis" onClick={() => toggleExpand(stock.code)}>
          {isExpanded ? stock.analysis : (stock.analysis || '').slice(0, 80) + ((stock.analysis || '').length > 80 ? '...' : '')}
        </div>
        {isExpanded && (
          <div className="rcc-signals">
            {signalEntries.map(([k, v]) => (
              <span key={k} className={`signal-tag ${v === '看涨' ? 'bull' : v === '看跌' ? 'bear' : ''}`}>
                {k}: {v}
              </span>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="page home">
      <h1>StEx 股票专家</h1>
      <p className="lead">中国市场股票分析专家系统：选股、跟踪、信号与工作流。</p>
      <div className="cards">
        <Link to="/selection" className="card-link">
          <h3>选股</h3>
          <p>多条件筛选、收藏跟踪 A 股</p>
        </Link>
        <Link to="/stock" className="card-link">
          <h3>股票</h3>
          <p>收藏跟踪、K 线、信号与数据更新</p>
        </Link>
        <Link to="/workflow" className="card-link">
          <h3>工作流</h3>
          <p>Agent 任务与执行日志</p>
        </Link>
      </div>

      <div className="recommend-section">
        <button
          className="recommend-btn"
          onClick={handleRecommend}
          disabled={loading}
        >
          {loading ? '正在分析...' : '综合推荐'}
        </button>
        <span className="recommend-hint">基于跟踪股票信号得分，AI 分析建仓策略</span>
      </div>

      {error && <div className="recommend-error">{error}</div>}

      {recommendations && recommendations.length > 0 && (
        <div className="recommend-results">
          <h2>综合推荐 Top {recommendations.length} {currentRefDate && <span className="recommend-date">（{currentRefDate}）</span>}</h2>
          <div className="recommend-grid">
            {recommendations.map(renderStockCard)}
          </div>
        </div>
      )}

      {recommendations && recommendations.length === 0 && (
        <div className="recommend-empty">暂无符合条件的推荐股票</div>
      )}

      {/* 历史推荐查询 */}
      <div className="history-section">
        <h2>历史推荐记录</h2>
        <div className="history-controls">
          <select
            value={selectedDate}
            onChange={(e) => handleLoadHistory(e.target.value)}
            disabled={historyLoading}
          >
            <option value="">选择日期查看</option>
            {historyDates.map(d => (
              <option key={d} value={d}>{d}</option>
            ))}
          </select>
          {historyLoading && <span className="history-loading">加载中...</span>}
        </div>

        {historyData && historyData.stocks && historyData.stocks.length > 0 && (
          <div className="history-results">
            <h3>{historyData.ref_date} 推荐 ({historyData.count} 只)</h3>
            <div className="recommend-grid">
              {historyData.stocks.map(renderStockCard)}
            </div>
          </div>
        )}

        {historyData && historyData.stocks && historyData.stocks.length === 0 && (
          <div className="history-empty">该日期暂无推荐记录</div>
        )}

        {historyDates.length === 0 && !historyLoading && (
          <div className="history-empty">暂无历史推荐记录，请先点击「综合推荐」生成</div>
        )}
      </div>
    </div>
  );
}
