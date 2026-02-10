# 数据库

PostgreSQL 17，schema: `stex`。

- 执行建表：`psql -d stock -f db/init.sql`
- 若表已存在需增加 corp 市值/市盈率/市净率列：`psql -d stock -f db/migrations/001_corp_market_cap_pe.sql`
