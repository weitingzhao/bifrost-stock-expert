import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { db } from './db.js';
import { stockRouter } from './routes/stock.js';
import { selectionRouter } from './routes/selection.js';
import { workflowRouter } from './routes/workflow.js';
import { indicesRouter } from './routes/indices.js';
import { configRouter } from './routes/config.js';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

app.get('/health', (_, res) => res.json({ ok: true, service: 'stex-api' }));

app.use('/api/stock', stockRouter);
app.use('/api/selection', selectionRouter);
app.use('/api/workflow', workflowRouter);
app.use('/api/indices', indicesRouter);
app.use('/api/config', configRouter);

app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ error: err.message || 'Internal Server Error' });
});

async function start() {
  try {
    await db.query('SELECT 1');
    console.log('PostgreSQL connected');
  } catch (e) {
    console.error('PostgreSQL connect failed:', e.message);
  }
  app.listen(PORT, () => console.log(`StEx API listening on http://localhost:${PORT}`));
}

start();
