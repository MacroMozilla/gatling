import random
import time

from gatling.utility.batch_tools_gatling import batch_execute_gatling
from gatling.utility.const import K_args


def heavy_cpu_task(a: float, b: float = 0.0) -> float:
    N = 1024 ** 2
    total = 0.0
    for i in range(N):
        total += a * b
    return int(round(total))


def heavy_io_task(a: float, b: float = 0.0) -> float:
    time.sleep(0.1)
    return int(round(a + b))

random.seed(42)
task_num = 1000
cpu_tasks = [{K_args: [round(random.uniform(1, 10), 4),
                       round(random.uniform(1, 10), 4)]
              } for _ in range(task_num)]

res = batch_execute_gatling(
    func=heavy_cpu_task,
    args_kwargs_s=cpu_tasks, reset_results=False, clean_result=False)
