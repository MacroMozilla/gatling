from gatling.runtime.runtime_task_manager_coroutine import RuntimeTaskManagerCoroutine
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
