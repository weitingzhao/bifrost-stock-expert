import pg from 'pg';

const { Pool } = pg;

// PostgreSQL DATE (OID 1082) 按字符串返回，避免 node-pg 转成 JS Date 后因时区导致少一天
pg.types.setTypeParser(1082, (val) => (val == null ? null : String(val).slice(0, 10)));

export const db = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: Number(process.env.PG_PORT) || 5432,
  database: process.env.PG_DATABASE || 'stock',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || '',
});
