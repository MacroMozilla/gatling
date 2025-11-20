from gatling.runtime.taskflow_manager import TaskFlowManager
from gatling.storage.queue.memory_queue import MemoryQueue
import asyncio
import time


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
