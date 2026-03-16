import datetime
from enum import Enum
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



# ===================== Type Maps =====================

# Python type -> SQL type (简单模式)
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

# SQL type -> Python type (反查, 用于 tostr/fmstr)
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


# ===================== SchemaBase =====================

class SchemaBase(Enum):

    @property
    def default(self) -> Any:
        return self.value.default if isinstance(self.value, Field) else self.value

    @property
    def dtype(self) -> type:
        return self.value.dtype if isinstance(self.value, Field) else type(self.value)

    @property
    def primary(self) -> bool:
        return self.value.primary if isinstance(self.value, Field) else False

    @property
    def nullable(self) -> bool:
        return self.value.nullable if isinstance(self.value, Field) else True

    @property
    def tostr(self) -> Optional[Callable]:
        return self.value.tostr if isinstance(self.value, Field) else None

    @property
    def fmstr(self) -> Optional[Callable]:
        return self.value.fmstr if isinstance(self.value, Field) else None

    @property
    def column(self) -> Optional[Column]:
        f = self.value
        if isinstance(f, Field):
            if f.column is None:
                f.get_column(self.name)
            return f.column
        return None

    @classmethod
    def keys(cls) -> list[str]:
        return [m.name for m in cls]

    @classmethod
    def items(cls) -> dict[str, Any]:
        return {m.name: m.default for m in cls}

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls.__members__

    @classmethod
    def get(cls, name: str, default: Any = None):
        if cls.has(name):
            return cls[name]
        return default

    @classmethod
    def get_key2type(cls):
        return {m.name: m.dtype for m in cls}

    @classmethod
    def get_sql_table(cls) -> Table:
        if not hasattr(cls, '_sql_table'):
            meta = MetaData()
            cols = [m.column for m in cls if m.column is not None]
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




# ===================== 示例 =====================

if __name__ == "__main__":

    # --- py 模式: 覆盖所有 Python type ---
    class ConstKey(SchemaBase):
        AppName   = Field(str, default="my_app")
        Port      = Field(int, default=8080)
        Lr        = Field(float, default=0.001)
        Debug     = Field(bool, default=False)
        Secret    = Field(bytes, default=b"key123")
        StartDate = Field(datetime.date, default=datetime.date(2025, 1, 1))
        AlarmTime = Field(datetime.time, default=datetime.time(8, 0))
        CreatedAt = Field(datetime.datetime, default=datetime.datetime(2025, 1, 1, 12, 0))
        Timeout   = Field(datetime.timedelta, default=datetime.timedelta(seconds=30))

    print("=== py 模式 (所有 Python type) ===")
    for m in ConstKey:
        print(f"  {m.name:<14} dtype={m.dtype.__name__:<12} default={m.default!r}")
    print()
    print(f".keys()  = {ConstKey.keys()}")
    print(f".items() = {ConstKey.items()}")
    print(f".has()   = {ConstKey.has('Port')}")
    print(f".get()   = {ConstKey.get('Port')}")
    print(f"['Port'] = {ConstKey['Port']}")
    print(f"len()    = {len(ConstKey)}")
    print()

    # tostr / fmstr
    print(f"tostr(date)  = {ConstKey.StartDate.tostr(ConstKey.StartDate.default)}")
    print(f"fmstr(date)  = {ConstKey.StartDate.fmstr('2025-06-15')}")
    print(f"tostr(bool)  = {ConstKey.Debug.tostr(True)}")
    print(f"fmstr(bool)  = {ConstKey.Debug.fmstr('0')}")
    print()

    # --- sql 模式: 覆盖 PostgreSQL 常用类型 ---
    class TempTable(SchemaBase):
        # 数字
        Id        = Field(BigInteger, primary=True, autoincrement=True, comment="主键")
        Age       = Field(SmallInteger, default=0)
        ViewCount = Field(Integer, default=0)
        Price     = Field(Numeric(10, 2), default=0.0)
        Rating    = Field(Float, default=0.0)
        Balance   = Field(DOUBLE_PRECISION, default=0.0)
        # 字符串
        Name      = Field(String(64), nullable=False, comment="用户名")
        Bio       = Field(Text)
        # 布尔
        IsActive  = Field(Boolean, default=True)
        # 日期时间
        Birthday  = Field(Date)
        Alarm     = Field(Time)
        CreatedAt = Field(TIMESTAMP(timezone=True), server_default="now()")
        Duration  = Field(Interval)
        # JSON
        Settings  = Field(JSONB)
        RawData   = Field(JSON)
        # 二进制
        Avatar    = Field(BYTEA)
        # UUID
        Token     = Field(UUID)
        # 网络
        LoginIp   = Field(INET)
        Network   = Field(CIDR)
        DeviceMac = Field(MACADDR)

    print("=== sql 模式 (PostgreSQL 常用类型) ===")
    for m in TempTable:
        print(f"  {m.name:<14} dtype={m.dtype.__name__:<12} default={m.default!r}")
    print()
    print(TempTable.get_sql_create())
    print(TempTable.get_sql_drop())
