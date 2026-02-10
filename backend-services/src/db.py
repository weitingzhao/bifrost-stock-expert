import psycopg
from contextlib import contextmanager
from .config import PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD

_conninfo = f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} user={PG_USER} password={PG_PASSWORD}"


@contextmanager
def get_conn():
    with psycopg.connect(_conninfo) as conn:
        yield conn
