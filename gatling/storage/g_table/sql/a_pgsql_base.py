from psycopg_pool import ConnectionPool
from sqlalchemy.dialects import postgresql as pg_dialect

from gatling.storage.g_table.sql.base_sql_table import compile_stmt as _compile_stmt

_PG_DIALECT = pg_dialect.dialect()


# ===================== Pool =====================

def create_pool(conninfo: str, max_size: int = 10, **kwargs) -> ConnectionPool:
    pool = ConnectionPool(conninfo=conninfo, max_size=max_size, open=True, **kwargs)
    return pool


# ===================== Compile =====================

def compile_stmt(stmt) -> tuple[str, dict]:
    """Compile a SQLAlchemy statement to (sql_string, params_dict) for psycopg3."""
    return _compile_stmt(stmt, _PG_DIALECT)


# ===================== Table Ops =====================

def exist_table(pool: ConnectionPool, table_name: str, schema: str = None) -> bool:
    with pool.connection() as conn:
        cur = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s)",
            (schema or 'public', table_name),
        )
        row = cur.fetchone()
        return row[0]
