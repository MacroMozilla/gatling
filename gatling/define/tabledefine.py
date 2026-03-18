import datetime
from typing import Any, Callable, Optional

import ciso8601

from sqlalchemy import (
    # --- Column ---
    Column, ForeignKey, text,
    # --- Table ---
    MetaData, Table,
    # --- Numeric ---
    SmallInteger, Integer, BigInteger, Float, Numeric, Boolean,
    # --- String ---
    String, Text, LargeBinary,
    # --- Date/Time ---
    Date, Time, DateTime, Interval,
)
from sqlalchemy.schema import CreateTable, DropTable
from sqlalchemy.dialects import postgresql as pg_dialect
from sqlalchemy.dialects.postgresql import (
    DOUBLE_PRECISION, MONEY,
    JSONB, JSON, UUID, BYTEA,
    ARRAY, BIT, TSVECTOR, ENUM,
    INET, CIDR, MACADDR, MACADDR8,
    TIMESTAMP, TIME,
)

from gatling.define.basedefine import BaseDefine



# ===================== Type Maps =====================

# Python type -> SQL type (simple mode)
_PY_TO_SQL = {
    int:                Integer,
    float:              Float,
    str:                Text,
    bool:               Boolean,
    bytes:              LargeBinary,
    datetime.date:      Date,
    datetime.time:      Time,
    datetime.datetime:  DateTime,
    datetime.timedelta: Interval,
}

# SQL type -> Python type (reverse lookup, used by tostr/fmstr)
_SQL_TO_PY = {
    SmallInteger:     int,
    Integer:          int,
    BigInteger:       int,
    Float:            float,
    DOUBLE_PRECISION: float,
    Numeric:          float,
    MONEY:            float,
    Boolean:          bool,
    String:           str,
    Text:             str,
    LargeBinary:      bytes,
    BYTEA:            bytes,
    Date:             datetime.date,
    Time:             datetime.time,
    TIME:             datetime.time,
    DateTime:         datetime.datetime,
    TIMESTAMP:        datetime.datetime,
    Interval:         datetime.timedelta,
    JSON:             dict,
    JSONB:            dict,
    UUID:             str,
    INET:             str,
    CIDR:             str,
    MACADDR:          str,
    MACADDR8:         str,
    ARRAY:            list,
    BIT:              str,
    TSVECTOR:         str,
    ENUM:             str,
}

_AUTO_TOSTR = {
    str:                str,
    int:                str,
    float:              str,
    bool:               lambda x: str(int(x)),
    datetime.date:      datetime.date.isoformat,
    datetime.time:      datetime.time.isoformat,
    datetime.datetime:  datetime.datetime.isoformat,
}

_AUTO_FMSTR = {
    str:                str,
    int:                int,
    float:              float,
    bool:               lambda x: x != '0',
    datetime.date:      lambda x: ciso8601.parse_datetime(x).date(),
    datetime.time:      lambda x: datetime.time.fromisoformat(x),
    datetime.datetime:  ciso8601.parse_datetime,
}

_PY_DEFAULTS = {
    str:                "",
    int:                0,
    float:              0.0,
    bool:               False,
    datetime.date:      None,
    datetime.time:      None,
    datetime.datetime:  None,
    datetime.timedelta: None,
    bytes:              b"",
    dict:               {},
    list:               [],
}


# ===================== Field =====================

class Field:

    def __init__(self, dtype: Any = str, default: Any = None, primary: bool = False, nullable: bool = True,
                 tostr: Optional[Callable] = None, fmstr: Optional[Callable] = None,
                 unique: bool = False, index: bool = False, comment: Optional[str] = None,
                 autoincrement: bool = False, server_default: Optional[str] = None,
                 foreign_key: Optional[str] = None):

        self.primary: bool = primary
        self.nullable: bool = nullable
        self.unique: bool = unique
        self.index: bool = index
        self.comment: Optional[str] = comment
        self.autoincrement: bool = autoincrement
        self.server_default: Optional[str] = server_default
        self.foreign_key: Optional[str] = foreign_key

        if dtype in _PY_TO_SQL:
            self.mode: str = "py"
            self.dtype: type = dtype
            self._sql_type = _PY_TO_SQL[dtype]
        else:
            self.mode: str = "sql"
            sql_class = dtype if isinstance(dtype, type) else type(dtype)
            self.dtype: type = _SQL_TO_PY.get(sql_class, str)
            self._sql_type = dtype

        self.default: Any = default if default is not None else _PY_DEFAULTS.get(self.dtype)
        self.tostr: Optional[Callable] = tostr or _AUTO_TOSTR.get(self.dtype)
        self.fmstr: Optional[Callable] = fmstr or _AUTO_FMSTR.get(self.dtype)
        self.column: Optional[Column] = None

    def get_column(self, name: str) -> Column:
        args = [name, self._sql_type]
        if self.foreign_key:
            args.append(ForeignKey(self.foreign_key))

        kwargs = {
            "primary_key": self.primary,
            "nullable": self.nullable,
            "unique": self.unique,
            "index": self.index,
        }
        if self.comment:
            kwargs["comment"] = self.comment
        if self.autoincrement:
            kwargs["autoincrement"] = True
        if self.default is not None and not self.primary:
            kwargs["default"] = self.default
        if self.server_default:
            kwargs["server_default"] = text(self.server_default)

        self.column = Column(*args, **kwargs)
        return self.column

    def __repr__(self) -> str:
        return f"Field(dtype={self.dtype.__name__}, default={self.default!r}, primary={self.primary})"


# ===================== TableDefine =====================

class TableDefine(BaseDefine):
    """Field-based enum — table schema with typed columns."""

    def __init__(self, value):
        if not isinstance(value, Field):
            raise TypeError(f"{self.__class__.__name__} members must be Field, got {type(value).__name__}")

    # --- SQL helpers ---

    @classmethod
    def get_name2dtype(cls):
        return {m.name: m.value.dtype for m in cls}

    @classmethod
    def get_sql_table(cls) -> Table:
        if not hasattr(cls, '_sql_table'):
            meta = MetaData()
            cols = [m.value.get_column(m.name) for m in cls]
            cls._sql_table = Table(cls.__name__, meta, *cols)
        return cls._sql_table

    @classmethod
    def get_sql_create(cls) -> str:
        table = cls.get_sql_table()
        return str(CreateTable(table, if_not_exists=True).compile(dialect=pg_dialect.dialect()))

    @classmethod
    def get_sql_drop(cls) -> str:
        table = cls.get_sql_table()
        return str(DropTable(table, if_exists=True).compile(dialect=pg_dialect.dialect()))



# ===================== Example =====================

if __name__ == "__main__":

    class UserTable(TableDefine):
        Id        = Field(BigInteger, primary=True, autoincrement=True)
        Name      = Field(String(64), nullable=False)
        Score     = Field(Float, default=0.0)
        CreatedAt = Field(TIMESTAMP(timezone=True), server_default="now()")

    print("=== TableDefine ===")
    for m in UserTable:
        print(f"  {m.name:<14} dtype={m.value.dtype.__name__:<12} default={m.value.default!r}")
    print()

    # access — .value is a Field object
    print(f"  {UserTable.Id.value = }")
    print(f"  {UserTable.Id.value.dtype = }")
    print(f"  {UserTable.Id.value.primary = }")
    print(f"  {UserTable.Name.value.nullable = }")
    print(f"  {UserTable.Score.value.tostr(9.5) = }")
    print(f"  {UserTable.Score.value.fmstr('9.5') = }")
    print()

    # lookup — [], get() return Field directly
    print(f"  {'Id' in UserTable = }")
    print(f"  {'nope' in UserTable = }")
    print(f"  {UserTable['Id'] = }")
    print(f"  {UserTable.get('Id') = }")
    print(f"  {UserTable.get('nope') = }")
    print()

    # collection
    print(f"  {UserTable.keys() = }")
    print(f"  {UserTable.items() = }")
    print(f"  {dict(UserTable) = }")
    print(f"  {UserTable.get_name2dtype() = }")
    print(f"  {len(UserTable) = }")
    print()

    # SQL
    print(UserTable.get_sql_create())
    print(UserTable.get_sql_drop())
