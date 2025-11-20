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

target_url = "https://httpbin.org/get"
# --- Synchronous request ---
result, status, size = sync_fetch_http(target_url)
print(status, size, result[:80])


# --- Asynchronous request ---
async def main():
    res, status, size = await fwrap(async_fetch_http, target_url=target_url, rtype="json")
    print(res)


asyncio.run(main())

```

**Main functions**

* `async_fetch_http(...)`: Generic async HTTP fetcher
* `fwrap(...)`: Safely manages aiohttp session lifecycle
* `sync_fetch_http(...)`: Simple synchronous wrapper (for scripts)

---

## 🧵 2. Coroutine & Thread Manager

**File:** `gatling/runtime/runtime_task_manager_thread.py`

**File:** `gatling/runtime/runtime_task_manager_coroutine.py`

A hybrid **thread + coroutine** manager that can run both sync and async tasks concurrently.

### Example

```python
from gatling.runtime.runtime_task_manager_thread import RuntimeTaskManagerThread
import time


# --- Async task ---
def async_fctn(name, delay=0.1):
    print(f"{name} running")
    time.sleep(delay)


def async_iter(name, delay=0.1):
    for i in range(5):
        sent = f"{name}-{i}"
        yield sent
        time.sleep(delay)  # simulate async source


# Async mode
print("--- Function ---")
m = RuntimeTaskManagerThread(async_fctn, args=("fctn",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Function with Context Manager ---")
m = RuntimeTaskManagerThread(async_fctn, args=("fctn",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)

print("--- Iterator ---")
m = RuntimeTaskManagerThread(async_iter, args=("iter",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Iterator with Context Manager ---")
m = RuntimeTaskManagerThread(async_iter, args=("iter",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)

```

```python
from gatling.runtime.runtime_task_manager_coroutine import RuntimeTaskManagerCoroutine
from gatling.runtime.runtime_task_manager_thread import RuntimeTaskManagerThread
import asyncio, time


# --- Async task ---

async def async_fctn(name, delay=0.5):
    print(f"{name} running")
    await asyncio.sleep(delay)


async def async_iter(name, delay=0.1):
    for i in range(10):
        sent = f"{name}-{i}"
        print(sent)
        yield sent
        await asyncio.sleep(delay)  # simulate async source


# Async mode
print("--- Async Function ---")
m = RuntimeTaskManagerCoroutine(async_fctn, args=("async_fctn",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Async Function with Context Manager ---")
m = RuntimeTaskManagerCoroutine(async_fctn, args=("async_fctn",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)

print("--- Async Iterator ---")
m = RuntimeTaskManagerCoroutine(async_iter, args=("async_iter",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Async Iterator with Context Manager ---")
m = RuntimeTaskManagerCoroutine(async_iter, args=("async_iter",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)

```

**Main methods**

* `.start(worker:int)`: Starts the workers
* `.stop()`: Stops all threads safely

---

## 💾 3. File Utility Module

**File:** `gatling/utility/io_fctns.py`

Convenient helpers for reading and writing JSON, JSONL, Pickle, TOML, text, and byte files.

### Example

```python
from gatling.utility.io_fctns import *

save_json({"a": 1}, "data.json")
print(read_json("data.json"))
remove_file("data.json")

save_jsonl([{"x": 1}, {"x": 2}], "data.jsonl")
print(read_jsonl("data.jsonl"))

remove_file("data.jsonl")

save_text("Hello world", "msg.txt")
print(read_text("msg.txt"))

remove_file("msg.txt")

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
from gatling.runtime.taskflow_manager import TaskFlowManager
from gatling.storage.queue.memory_queue import MemoryQueue
import asyncio, time


def sync_square(x):
    time.sleep(0.2)
    return x * x


async def async_double(x):
    await asyncio.sleep(0.3)
    return x * 2


def sync_to_str(x):
    return f"Result<{x}>"


q_wait = MemoryQueue()
q_done = MemoryQueue()

# Queue must have items before starting.
for i in range(5):
    q_wait.put(i)

tfm = TaskFlowManager(q_wait, q_done, retry_on_error=False)
with tfm.execute(log_interval=1):
    tfm.register_thread(sync_square, worker=2)
    tfm.register_coroutine(async_double, worker=5)
    tfm.register_thread(sync_to_str, worker=2)

print(list(q_done))

```

**Main classes**

* `TaskFlowManager`: Coordinates multi-stage parallel workflows
* `TaskQueueTracker`: Monitors queue states, errors, and speed metrics

---

## ⏱️ 5. Watch Utility

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
