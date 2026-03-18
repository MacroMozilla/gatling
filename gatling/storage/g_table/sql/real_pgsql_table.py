from sqlalchemy import (
    MetaData,
    select, update as sa_update, delete as sa_delete,
    func,
)
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.dialects.postgresql import insert as pg_insert, JSONB, JSON
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from gatling.storage.g_table.sql.base_sql_table import BaseSQLTable, build_where
from gatling.storage.g_table.sql.a_pgsql_base import compile_stmt, exist_table, _PG_DIALECT


class PGSQLTable(BaseSQLTable):

    def __init__(self, table_name: str, pool: ConnectionPool):
        super().__init__()
        self.table_name = table_name
        self.pool = pool
        self._conn = None
        self.tabledefine = None
        self._table = None
        self._all_keys = None
        self._primary_keys = None
        self._json_cols = set()

    # ===================== Schema & Init =====================

    def create(self, tabledefine) -> 'PGSQLTable':
        self.tabledefine = tabledefine
        self._all_keys = tabledefine.keys()
        self._primary_keys = [m.name for m in tabledefine if m.primary]
        src = tabledefine.get_sql_table()
        self._table = src.to_metadata(MetaData(), name=self.table_name)
        self._json_cols = {c.name for c in self._table.columns if isinstance(c.type, (JSONB, JSON))}
        sql = str(CreateTable(self._table, if_not_exists=True).compile(dialect=_PG_DIALECT))
        self._exec(sql)
        return self

    def exists(self) -> bool:
        return exist_table(self.pool, self.table_name)

    def truncate(self) -> 'PGSQLTable':
        self._exec(f'TRUNCATE TABLE "{self.table_name}"')
        return self

    def drop(self) -> 'PGSQLTable':
        if self._table is not None:
            sql = str(DropTable(self._table, if_exists=True).compile(dialect=_PG_DIALECT))
        else:
            sql = f'DROP TABLE IF EXISTS "{self.table_name}"'
        self._exec(sql)
        return self

    def _adapt_json(self, row: dict) -> dict:
        if not self._json_cols:
            return row
        out = {}
        for k, v in row.items():
            if k in self._json_cols and isinstance(v, (dict, list)):
                out[k] = Jsonb(v)
            else:
                out[k] = v
        return out

    # ===================== Context Manager =====================

    def __enter__(self):
        self._conn = self.pool.getconn()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is not None:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
            self.pool.putconn(self._conn)
            self._conn = None

    def _exec(self, sql, params=None) -> int:
        if self._conn is not None:
            cur = self._conn.execute(sql, params)
            return cur.rowcount
        else:
            with self.pool.connection() as conn:
                cur = conn.execute(sql, params)
                conn.commit()
                return cur.rowcount

    def _query(self, sql, params=None) -> list[dict]:
        if self._conn is not None:
            cur = self._conn.execute(sql, params)
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
            return [{k: v for k, v in zip(cols, row)} for row in rows]
        else:
            with self.pool.connection() as conn:
                cur = conn.execute(sql, params)
                cols = [d.name for d in cur.description]
                rows = cur.fetchall()
                return [{k: v for k, v in zip(cols, row)} for row in rows]

    def _scalar(self, sql, params=None):
        if self._conn is not None:
            cur = self._conn.execute(sql, params)
            row = cur.fetchone()
            return row[0]
        else:
            with self.pool.connection() as conn:
                cur = conn.execute(sql, params)
                row = cur.fetchone()
                return row[0]

    # ===================== Insert =====================

    def insert(self, *rows: dict, replace: bool = False, batch_size: int = 1000) -> 'PGSQLTable':
        if not rows:
            return self
        if replace:
            stmt = pg_insert(self._table)
            non_pk = [k for k in rows[0] if k not in self._primary_keys]
            if non_pk:
                stmt = stmt.on_conflict_do_update(
                    index_elements=self._primary_keys,
                    set_={k: stmt.excluded[k] for k in non_pk},
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=self._primary_keys)
            sql, _ = compile_stmt(stmt)
        else:
            sql, _ = compile_stmt(self._table.insert())

        if len(rows) == 1:
            self._exec(sql, self._adapt_json(rows[0]))
        else:
            adapted = [self._adapt_json(r) for r in rows]
            if self._conn is not None:
                cur = self._conn.cursor()
                for i in range(0, len(adapted), batch_size):
                    cur.executemany(sql, adapted[i:i + batch_size])
            else:
                with self.pool.connection() as conn:
                    cur = conn.cursor()
                    for i in range(0, len(adapted), batch_size):
                        cur.executemany(sql, adapted[i:i + batch_size])
                    conn.commit()
        return self

    # ===================== Fetch =====================

    def fetch(self, where: dict = None, keys: list[str] = None,
              order_by: dict[str, bool] = None, limit: int = None, offset: int = None) -> list[dict]:
        if keys:
            stmt = select(*[self._table.c[k] for k in keys])
        else:
            stmt = select(self._table)
        w = build_where(self._table, where)
        if w is not None:
            stmt = stmt.where(w)
        if order_by:
            for col, desc in order_by.items():
                c = self._table.c[col]
                stmt = stmt.order_by(c.desc() if desc else c.asc())
        if limit is not None:
            stmt = stmt.limit(limit)
        if offset is not None:
            stmt = stmt.offset(offset)
        sql, params = compile_stmt(stmt)
        return self._query(sql, params or None)

    def count(self, where: dict = None) -> int:
        stmt = select(func.count()).select_from(self._table)
        w = build_where(self._table, where)
        if w is not None:
            stmt = stmt.where(w)
        sql, params = compile_stmt(stmt)
        return self._scalar(sql, params or None)

    # ===================== Update =====================

    def update(self, updates: dict, where: dict) -> int:
        stmt = sa_update(self._table).values(self._adapt_json(updates))
        w = build_where(self._table, where)
        if w is not None:
            stmt = stmt.where(w)
        sql, params = compile_stmt(stmt)
        return self._exec(sql, params)

    # ===================== Delete =====================

    def delete(self, where: dict) -> int:
        if not where:
            raise ValueError("WHERE required for delete(). Use truncate() to clear all rows.")
        stmt = sa_delete(self._table)
        w = build_where(self._table, where)
        if w is not None:
            stmt = stmt.where(w)
        sql, params = compile_stmt(stmt)
        return self._exec(sql, params)

    # ===================== Meta =====================

    def keys(self) -> list[str]:
        if self._all_keys:
            return list(self._all_keys)
        return [c.name for c in self._table.columns]

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(table={self.table_name!r})>"
