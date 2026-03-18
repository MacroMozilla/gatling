# Gatling

A unified I/O management toolkit for Python. Orchestrate processes, threads, and coroutines in one pipeline. Read and write files, databases, and queues through a consistent API.

```bash
pip install gatling
```

Requires Python 3.11+

---

## Modules

| Module | Description |
|---|---|
| **Runtime** | Multi-stage pipeline: process + thread + coroutine workers |
| **Storage — Table** | Append-only TSV tables, PostgreSQL, SQLite |
| **Storage — Queue** | Thread-safe in-memory queue |
| **Storage — Dict** | In-memory dictionary with batch ops |
| **Storage — SFS** | Virtual file system with path routing |
| **Define** | ConstDefine for constants/keys, TableDefine for table schemas |
| **HTTP** | Async/sync HTTP client (GET/POST/PUT/DELETE) |
| **File I/O** | JSON, JSONL, Pickle, TOML, text, bytes, zstd compression |
| **Watch** | Stopwatch, function timing decorator |

---

## TaskFlow Pipeline

Run CPU-bound, I/O-bound, and network-bound tasks in a single pipeline with automatic queue management.

```python
from gatling.runtime.taskflow_manager import TaskFlowManager
from gatling.storage import MemoryQueue

if __name__ == '__main__':
    q_wait = MemoryQueue()
    for i in range(100):
        q_wait.put(i)

    tfm = TaskFlowManager(q_wait, retry_on_error=False)

    with tfm.execute(log_interval=1):
        tfm.register_process(cpu_task, worker=4)       # multiprocessing
        tfm.register_coroutine(net_task, worker=10)     # asyncio
        tfm.register_thread(disk_task, worker=4)        # threading

    results = list(tfm.get_qdone())
```

Workers can be **functions** (one-in-one-out) or **generators** (one-in-many-out). Stages chain automatically.

---

## Define — ConstDefine / TableDefine

Two `Enum`-based classes for defining constants. Both inherit from `BaseDefine` and share:

| Method | Description |
|---|---|
| `str(m)` | Returns member name |
| `"name" in Cls` | Check if member exists by name |
| `Cls.keys()` | List of all member names |
| `Cls.items()` | List of `[(name, value), ...]` tuples (use `dict(Cls.items())` for dict) |
| `Cls.get(name, default)` | Safe lookup, returns `.value` (`default` if not found) |
| `Cls["name"]` | Access `.value` by name (raises `KeyError` if missing) |
| `dict(Cls)` | Convert to `{name: value}` dict |
| `Cls.Name` | Access member (enum object with `.name` and `.value`) |
| `len(Cls)` | Number of members |
| `for m in Cls` | Iterate over members |

### ConstDefine — constants and keys

```python
from enum import auto
from gatling.define.constdefine import ConstDefine

class Config(ConstDefine):
    # direct values — any Python literal
    Port      = 8080
    Debug     = False
    Rate      = 0.001
    Name      = "my_app"
    # auto() — member name becomes the string value
    username  = auto()          # .value = "username"
    email     = auto()          # .value = "email"
    # None — single sentinel (only one None allowed per class)
    Secret    = None
```

Access:

```python
Config.Port.value               # 8080
Config.Port.name                # "Port"
str(Config.Port)                # "Port"
Config.username.value           # "username"
Config.Secret.value             # None
```

Lookup:

```python
"Port" in Config                # True
"nope" in Config                # False
Config.get("Port")              # 8080
Config.get("nope")              # None
Config.get("nope", "fallback") # "fallback"
Config["Port"]                  # 8080
dict(Config)                    # {'Port': 8080, 'Debug': False, ..., 'Secret': None}
```

Collection:

```python
Config.keys()                   # ['Port', 'Debug', 'Rate', 'Name', 'username', 'email', 'Secret']
Config.items()                  # [('Port', 8080), ('Debug', False), ..., ('Secret', None)]
dict(Config.items())            # {'Port': 8080, 'Debug': False, ..., 'Secret': None}
len(Config)                     # 7
[m.name for m in Config]        # ['Port', 'Debug', 'Rate', 'Name', 'username', 'email', 'Secret']
```

### TableDefine — typed table schema

All members must be `Field` (non-Field values raise `TypeError`).

```python
import datetime
from gatling.define.tabledefine import TableDefine, Field

class Users(TableDefine):
    id        = Field(int, primary=True)
    name      = Field(str, nullable=False)
    score     = Field(float, default=0.0)
    active    = Field(bool, default=True)
    birthday  = Field(datetime.date)
    created   = Field(datetime.datetime)
```

Field attributes (via `.value`):

```python
Users.name.value.dtype          # str
Users.name.value.default        # ""
Users.name.value.primary        # False
Users.name.value.nullable       # False
Users.score.value.default       # 0.0
Users.id.value.primary          # True
```

Serialize / deserialize:

```python
Users.score.value.tostr(9.5)                            # "9.5"
Users.score.value.fmstr("9.5")                          # 9.5
Users.active.value.tostr(True)                           # "1"
Users.active.value.fmstr("0")                            # False
Users.birthday.value.tostr(datetime.date(2025, 6, 15))  # "2025-06-15"
Users.birthday.value.fmstr("2025-06-15")                 # datetime.date(2025, 6, 15)
```

Lookup and collection (same as ConstDefine):

```python
"name" in Users                 # True
Users.get("name")               # Field(dtype=str, ...)  (returns Field directly)
Users.keys()                    # ['id', 'name', 'score', 'active', 'birthday', 'created']
Users.items()                   # [('id', Field(...)), ('name', Field(...)), ...]
Users.get_name2dtype()          # {'id': int, 'name': str, 'score': float, ...}
```

SQL generation (PostgreSQL dialect):

```python
from sqlalchemy import BigInteger, String, Float
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB

class Posts(TableDefine):
    id        = Field(BigInteger, primary=True, autoincrement=True, comment="primary key")
    title     = Field(String(256), nullable=False)
    score     = Field(Float, default=0.0, index=True)
    tags      = Field(JSONB)
    created   = Field(TIMESTAMP(timezone=True), server_default="now()")

Posts.get_sql_create()          # CREATE TABLE IF NOT EXISTS "Posts" (id BIGSERIAL ... )
Posts.get_sql_drop()            # DROP TABLE IF EXISTS "Posts"
```

Field options:

| Option | Type | Description |
|---|---|---|
| `dtype` | type | Python type (`int`, `str`, ...) or SQLAlchemy type (`BigInteger`, `String(64)`, ...) |
| `default` | any | Default value (auto-inferred from dtype if omitted) |
| `primary` | bool | Primary key |
| `nullable` | bool | Allow NULL (default `True`) |
| `unique` | bool | Unique constraint |
| `index` | bool | Create index |
| `autoincrement` | bool | Auto-increment |
| `server_default` | str | SQL server-side default expression (e.g. `"now()"`) |
| `foreign_key` | str | Foreign key reference (e.g. `"users.id"`) |
| `comment` | str | Column comment |
| `tostr` | callable | Custom serializer (auto-inferred from dtype if omitted) |
| `fmstr` | callable | Custom deserializer (auto-inferred from dtype if omitted) |

---

## Tables

All table types share the same schema. Only creation differs.

### Create

```python
# SQLite
from gatling.storage.g_table.sql.real_sqlite_table import SQLiteTable
ft = SQLiteTable("users", "app.db")
ft.create(schema)

# PostgreSQL
from gatling.storage.g_table.sql.a_pgsql_base import create_pool
from gatling.storage.g_table.sql.real_pgsql_table import PGSQLTable
pool = create_pool("postgresql://user:pass@localhost:5432/db")
ft = PGSQLTable("users", pool)
ft.create(schema)

# Append-only TSV file
from gatling.storage.g_table.append_only.real_tsv_table import TSVTable
ft = TSVTable("users.tsv")
ft.create(schema)
```

### Usage (same for all SQL tables)

```python
# Insert — single or batch
ft.insert({"id": 1, "name": "Alice", "score": 9.5})
ft.insert(
    {"id": 2, "name": "Bob",   "score": 8.0},
    {"id": 3, "name": "Carol", "score": 7.5},
)

# Query
ft.fetch(where={"name": "Alice"})
ft.fetch(order_by={"score": True}, limit=10)
ft.count(where={"score": 9.5})

# Update / Delete
ft.update({"score": 10.0}, where={"id": 1})
ft.delete(where={"id": 3})

# Transaction (rollback on exception)
with ft:
    ft.insert({"id": 4, "name": "Dan", "score": 6.0})
    ft.update({"score": 0.0}, where={"id": 2})
```

TSV tables use `append` / `extend` and support indexing:

```python
ft.append({"ts": "2025-01-01", "level": "INFO", "msg": "started"})
ft.extend([...])

with ft:
    print(len(ft))      # row count
    print(ft[0])         # first row
    print(ft[-1])        # last row
    print(ft[2:5])       # slice
```

---

## HTTP Client

```python
from gatling.utility.http_fetch_fctns import sync_fetch_http, async_fetch_http, fwrap

# Sync
data, status, size = sync_fetch_http("https://httpbin.org/get", rtype="json")

# Async
data, status, size = await fwrap(async_fetch_http, target_url="https://httpbin.org/get", rtype="json")
```

---

## File I/O

```python
from gatling.utility.io_fctns import (
    save_json, read_json,
    save_jsonl, read_jsonl,
    save_text, read_text,
    save_pickle, read_pickle,
    save_bytes, read_bytes,
    read_toml, remove_file,
)

save_json({"key": "value"}, "data.json")
save_jsonl([{"a": 1}, {"a": 2}], "data.jsonl")
save_text("hello", "msg.txt")
```

---

## Watch

```python
from gatling.utility.watch import Watch, watch_time

@watch_time
def slow():
    time.sleep(1)

w = Watch()
# ... work ...
print(w.see_seconds())    # interval since last check
print(w.total_seconds())  # total elapsed
```

---

## License

MIT
