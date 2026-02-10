import { Routes, Route } from 'react-router-dom';
import { Layout } from './pages/Layout';
import { Home } from './pages/Home';
import { Selection } from './pages/Selection';
import { StockList } from './pages/StockList';
import { Stock } from './pages/Stock';
import { Workflow } from './pages/Workflow';
import './App.css';

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Home />} />
        <Route path="selection" element={<Selection />} />
        <Route path="stock" element={<StockList />} />
        <Route path="stock/:code" element={<Stock />} />
        <Route path="workflow" element={<Workflow />} />
      </Route>
    </Routes>
  );
}

export default App;
