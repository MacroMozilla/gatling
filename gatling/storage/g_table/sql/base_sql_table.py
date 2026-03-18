from abc import abstractmethod

from sqlalchemy import Table, and_


# ===================== Shared Utilities =====================

def compile_stmt(stmt, dialect) -> tuple[str, dict]:
    """Compile a SQLAlchemy statement to (sql_string, params_dict) for a given dialect."""
    compiled = stmt.compile(dialect=dialect)
    return str(compiled), dict(compiled.params)


def build_where(table: Table, where: dict):
    if not where:
        return None
    conds = []
    for col, val in where.items():
        if val is None:
            conds.append(table.c[col].is_(None))
        else:
            conds.append(table.c[col] == val)
    return and_(*conds) if len(conds) > 1 else conds[0]


# ===================== Abstract Base =====================

class BaseSQLTable:

    def __init__(self):
        super().__init__()

    @abstractmethod
    def create(self, tabledefine):
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def truncate(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    # ===================== Context Manager =====================

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    # ===================== CRUD =====================

    @abstractmethod
    def insert(self, *rows: dict, replace: bool = False, batch_size: int = 1000):
        pass

    @abstractmethod
    def fetch(self, where: dict = None, keys: list[str] = None,
              order_by: dict[str, bool] = None, limit: int = None, offset: int = None) -> list[dict]:
        pass

    @abstractmethod
    def count(self, where: dict = None) -> int:
        pass

    @abstractmethod
    def update(self, updates: dict, where: dict) -> int:
        pass

    @abstractmethod
    def delete(self, where: dict) -> int:
        pass

    # ===================== Meta =====================

    @abstractmethod
    def keys(self) -> list[str]:
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass
