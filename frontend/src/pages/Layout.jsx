import { Link, Outlet, useLocation } from 'react-router-dom';

export function Layout() {
  const loc = useLocation();
  const nav = [
    { path: '/', label: '首页' },
    { path: '/selection', label: '选股' },
    { path: '/stock', label: '股票' },
    { path: '/workflow', label: '工作流' },
  ];
  return (
    <div className="layout">
      <header className="header">
        <span className="brand">StEx 股票专家</span>
        <nav>
          {nav.map(({ path, label }) => (
            <Link key={path} to={path} className={loc.pathname === path ? 'active' : ''}>{label}</Link>
          ))}
        </nav>
      </header>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}
