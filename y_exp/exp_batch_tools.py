import random
import time

from gatling.utility.batch_tools import batch_execute_forloop, batch_execute_process, batch_execute_thread
from gatling.utility.batch_tools_gatling import batch_execute_gatling
from gatling.utility.const import K_args
from gatling.utility.watch import Watch

from a_plot_tools import *


def heavy_cpu_task(a: float, b: float = 0.0) -> float:
    N = 1024 ** 2
    total = 0.0
    for i in range(N):
        total += a * b
    return int(round(total))


def heavy_io_task(a: float, b: float = 0.0) -> float:
    time.sleep(0.1)
    return int(round(a + b))


if __name__ == '__main__':
    pass
    # 定义任务数量
    task_numbers = [128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768][:]
    executors = {
        "forloop": batch_execute_forloop,
        "process": batch_execute_process,
        "thread": batch_execute_thread,
        "gatling": batch_execute_gatling,
    }

    # 记录运行时间
    cpu_task_times = {name: [] for name in executors}
    io_task_times = {name: [] for name in executors}

    for task_num in task_numbers:
        cpu_tasks = [{K_args: (random.uniform(1, 10), random.uniform(1, 10))} for _ in range(task_num)]
        io_tasks = [{K_args: (random.uniform(1, 10), random.uniform(1, 10))} for _ in range(task_num)]

        # 运行 CPU 任务
        for name, exec_func in executors.items():
            watch = Watch()
            results = exec_func(heavy_cpu_task, cpu_tasks)
            cost = watch.see_timedelta()

            cpu_task_times[name].append(cost.total_seconds())

        # 运行 IO 任务
        for name, exec_func in executors.items():
            watch = Watch()
            results = exec_func(heavy_io_task, io_tasks)
            cost = watch.see_timedelta()
            io_task_times[name].append(cost.total_seconds())

        fig, axs = plt.subplots(2, 1, figsize=(10, 10))

        # CPU-bound task execution time (log scale)
        axs[0].set_yscale("log")
        for name, times in cpu_task_times.items():
            axs[0].plot(task_numbers[:len(times)], times, marker='o', label=name)
        axs[0].set_xlabel("Number of Tasks")
        axs[0].set_ylabel("Execution Time (seconds)")
        axs[0].set_title("CPU-bound Task Execution Time")
        axs[0].legend()
        axs[0].grid(True)

        # IO-bound task execution time (log scale)
        axs[1].set_yscale("log")
        for name, times in io_task_times.items():
            axs[1].plot(task_numbers[:len(times)], times, marker='o', label=name)
        axs[1].set_xlabel("Number of Tasks")
        axs[1].set_ylabel("Execution Time (seconds)")
        axs[1].set_title("IO-bound Task Execution Time")
        axs[1].legend()
        axs[1].grid(True)

        plt.tight_layout()
        # plt_show()
        fpath_fig = "../x_figs/evaluation_speed_for_batch_tools.png"
        plt.savefig(fpath_fig)
        print(f"SAVE FIG {fpath_fig}")
        plt.close()
