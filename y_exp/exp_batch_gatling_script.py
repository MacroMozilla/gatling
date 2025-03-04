import argparse
import os
import random
import time

from gatling.databasetool.redis_base import get_redis_master
from gatling.databasetool.redis_z_taskqueuemanager import RedisTaskQueueManager
from gatling.utility.const import K_args
from gatling.utility.exec_tools import run_python_script, get_pids_by_cmd, kill_process


def heavy_cpu_task(a: float, b: float = 0.0) -> float:
    N = 1024 ** 2
    total = 0.0
    for i in range(N):
        total += a * b
    return int(round(total))


def heavy_io_task(a: float, b: float = 0.0) -> float:
    time.sleep(0.1)
    return int(round(a + b))


rtqm_update_task = RedisTaskQueueManager(fctn=heavy_cpu_task, redis_master=get_redis_master('127.0.0.1', 6379))


def batch_push_task():
    task_num = 10000
    cpu_tasks = [{K_args: [round(random.uniform(1, 10), 4),
                           round(random.uniform(1, 10), 4)]
                  } for _ in range(task_num)]
    rtqm_update_task.reset_params()
    rtqm_update_task.push_waiting(cpu_tasks)


def batch_exec_task():
    rtqm_update_task.batch_execute()


def check_done_task():
    rtqm_update_task.check_done_block(5)


def fetch_result_task():
    return rtqm_update_task.fetch_results()


if __name__ == '__main__':
    pass
    fctns = [batch_exec_task]

    def get_sys_kwargs():
        # Set up the argument parser
        parser = argparse.ArgumentParser(description="Select mode for the program.")

        choices = [fctn.__name__ for fctn in fctns]
        # 添加 --mode 参数，允许用户选择模式
        parser.add_argument(
            '--fctn',
            type=str,
            default=None,
            choices=choices,
            help=f"choose fctn fm {fctns}"
        )

        kwargs = parser.parse_args()
        return kwargs


    kwargs = get_sys_kwargs()
    print(kwargs)

    name2fctn = {fctn.__name__: fctn for fctn in fctns}
    if kwargs.fctn is not None:
        name2fctn[kwargs.fctn]()
    else:
        pass

        batch_push_task()

        worker_num = os.cpu_count() - 2
        for i in range(worker_num):
            run_python_script(os.path.realpath(__file__), f"--fctn {batch_exec_task.__name__}", block=False)
        check_done_task()

        pids = get_pids_by_cmd(os.path.realpath(__file__), f"--fctn {batch_exec_task.__name__}")
        for pid in pids:
            kill_process(pid)

        res = fetch_result_task()
