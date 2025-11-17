
# 🧩 Gatling Utility Library

**Gatling** is a lightweight asynchronous utility library built on `aiohttp`, `asyncio`, and `threading`.
It provides concurrent HTTP requests, coroutine-thread orchestration, data pipelines, and handy file utilities.

---

## 📦 Installation

```bash
pip install gatling
```

---

## 📁 Module Overview

| Module                     | Description                                |
|----------------------------|--------------------------------------------|
| `http_client.py`           | Async/sync HTTP request handling           |
| `coroutine_thread_mana.py` | Thread + coroutine concurrent task manager |
| `file_utils.py`            | Common file read/write helpers             |
| `taskflow_manager.py`      | Multi-stage task pipeline system           |
| `watch.py`                 | Stopwatch and timing tools                 |

---

## 🌐 1. HTTP Client Module

**File:** `gatling/utility/http_client.py`

Provides unified async/sync HTTP request helpers supporting `GET`, `POST`, `PUT`, and `DELETE`.

### Example

```python
from gatling.utility.http_fetch_fctns import sync_fetch_http, async_fetch_http, fwrap
import asyncio, aiohttp

# --- Synchronous request ---
result, status, size = sync_fetch_http("https://httpbin.org/get")
print(status, size, result[:80])


# --- Asynchronous request ---
async def main():
    async with aiohttp.ClientSession() as session:
        res, status, size = await async_fetch_http(
            "https://httpbin.org/ip", session=session, rtype="json"
        )
        print(res)


asyncio.run(main())
```

**Main functions**

* `async_fetch_http(...)`: Generic async HTTP fetcher
* `fwrap(...)`: Safely manages aiohttp session lifecycle
* `sync_fetch_http(...)`: Simple synchronous wrapper (for scripts)

---

## 🧵 2. Coroutine & Thread Manager

**File:** `gatling/utility/coroutine_thread_mana.py`

A hybrid **thread + coroutine** manager that can run both sync and async tasks concurrently.

### Example

```python
from gatling.utility.coroutine_thread_mana import CoroutineThreadManager
import asyncio, time


# --- Async task ---
async def async_job(name, delay=0.5):
    print(f"{name} running")
    await asyncio.sleep(delay)


# --- Sync task ---
def sync_job(name, delay=0.5):
    print(f"{name} running")
    time.sleep(delay)


# Async mode
m = CoroutineThreadManager(async_job, args=("async-A",), kwargs={"delay": 0.3})
m.start(thread_worker=2, coroutine_worker=2)
time.sleep(2)
m.stop()

# Sync mode
m = CoroutineThreadManager(sync_job, args=("sync-B",), kwargs={"delay": 0.2})
m.start(thread_worker=2)
time.sleep(2)
m.stop()
```

**Main methods**

* `.start(thread_worker, coroutine_worker)`: Starts the workers
* `.stop()`: Stops all threads safely

---

## 💾 3. File Utility Module

**File:** `gatling/utility/file_utils.py`

Convenient helpers for reading and writing JSON, JSONL, Pickle, TOML, text, and byte files.

### Example

```python
from gatling.utility.io_fctns import *

save_json({"a": 1}, "data.json")
print(read_json("data.json"))

save_jsonl([{"x": 1}, {"x": 2}], "data.jsonl")
print(read_jsonl("data.jsonl"))

save_text("Hello world", "msg.txt")
print(read_text("msg.txt"))
```

**Main functions**

* `save_json / read_json`
* `save_jsonl / read_jsonl`
* `save_text / read_text`
* `save_pickle / read_pickle`
* `save_bytes / read_bytes`
* `read_toml`
* `remove_file`

---

## 🔄 4. Task Flow Manager

**File:** `gatling/utility/taskflow_manager.py`

Builds a **multi-stage processing pipeline** — combining threads, coroutines, and queues.
Each stage can be synchronous or asynchronous.

### Example

```python
from gatling.utility.task_flow_manager import TaskFlowManager
from queue import Queue
import asyncio, time


def sync_square(x):
    time.sleep(0.2)
    return x * x


async def async_double(x):
    await asyncio.sleep(0.3)
    return x * 2


def sync_to_str(x):
    return f"Result: {x}"


q_wait = Queue()
q_done = Queue()

tfm = TaskFlowManager(q_wait, q_done, retry_on_error=False)
tfm.append_stagefctn(sync_square)
tfm.append_stagefctn(async_double)
tfm.append_stagefctn(sync_to_str)

for i in range(5):
    q_wait.put(i)

tfm.start()
tfm.await_print(interval=1)
tfm.stop()

print(list(q_done.queue))
```

**Main classes**

* `TaskFlowManager`: Coordinates multi-stage parallel workflows
* `TaskQueueTracker`: Monitors queue states, errors, and speed metrics

---

## ⏱️ 5. Stopwatch Utility

**File:** `gatling/utility/watch.py`

A simple stopwatch for timing operations, plus a decorator for measuring function execution time.

### Example

```python
from gatling.utility.watch import Watch, watch_time
import time


@watch_time
def slow_func():
    time.sleep(1)


slow_func()

w = Watch()
time.sleep(0.5)
print("Δt:", w.see_seconds(), "Total:", w.total_seconds())
```

**Main items**

* `Watch`: Manual stopwatch class for measuring intervals
* `watch_time`: Decorator that prints function execution time

---
