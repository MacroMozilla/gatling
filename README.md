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
| **Schema** | Type-safe field definitions — table schemas, constants, config |
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

## Schema (TableDefine)

`TableDefine` is an extended `Enum`. It can define table schemas, but also work as a typed constants/config manager.

### As table schema

```python
from gatling.define.schema import TableDefine, Field

schema = TableDefine('Users', {
    'id':    Field(int, primary=True),
    'name':  Field(str),
    'score': Field(float),
})
```

### As constants / config

```python
class Config(TableDefine):
    AppName   = Field(str, default="my_app")
    Port      = Field(int, default=8080)
    Debug     = Field(bool, default=False)
    StartDate = Field(datetime.date, default=datetime.date(2025, 1, 1))

Config.keys()                # ['AppName', 'Port', 'Debug', 'StartDate']
Config.items()               # {'AppName': 'my_app', 'Port': 8080, ...}
Config['Port'].default       # 8080
Config.Port.dtype            # int
Config.has('Port')           # True
Config.get('Missing', None)  # None

# Serialize / deserialize
Config.Port.tostr(8080)              # "8080"
Config.Port.fmstr("8080")            # 8080
Config.StartDate.tostr(date.today()) # "2025-03-17"
Config.StartDate.fmstr("2025-03-17") # datetime.date(2025, 3, 17)
```

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
