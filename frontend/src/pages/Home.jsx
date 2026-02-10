import { Link } from 'react-router-dom';

export function Home() {
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
    </div>
  );
}
