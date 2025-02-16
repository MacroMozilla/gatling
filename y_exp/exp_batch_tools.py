import random
import time

from gatling.utility.batch_tools import batch_execute_forloop, batch_execute_process, batch_execute_thread
from gatling.utility.batch_tools_gatling import batch_execute_gatling
from gatling.utility.const import K_args
from gatling.utility.watch import Watch, watch_time

from a_plot_tools import *


# @watch_time
def heavy_cpu_task(a: float, b: float = 0.0) -> float:
    N = 2 ** 20
    total = 0.0
    for i in range(N):
        total += a * b
    return int(round(total))


# @watch_time
def heavy_io_task(a: float, b: float = 0.0) -> float:
    sleep_total_interval = 0.05
    time.sleep(sleep_total_interval)
    return int(round(a + b))


# @watch_time
def heavy_cpuio_task(a: float, b: float = 0.0) -> float:
    N = 2 ** 20 // 2
    sleep_total_interval = 0.05 // 2
    sleep_times = 10
    sleep_interval = sleep_total_interval / sleep_times
    invN = sleep_times / N
    total = 0.0
    for i in range(N):
        total = a * b
        if random.random() < invN:
            time.sleep(sleep_interval)

    return int(round(total))


def find_cpu_io_params(interval_per_call=0.05):
    select_N = 0
    select_x = 0
    for select_x in range(25):
        w = Watch()
        select_N = 2 ** select_x
        a = random.random() * 10
        b = random.random() * 10
        total = 0.0
        for i in range(select_N):
            total += a * b
        cost = w.see_seconds()
        print(f"2^{select_x} float_mul cost {cost}")
        if cost > interval_per_call:
            break


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

    taskname2fctn = {'cpu': heavy_cpu_task,
                     'io': heavy_io_task,
                     'cpu&io': heavy_cpuio_task}

    taskname2execname2costs = {taskname: {name: [] for name in executors} for taskname in taskname2fctn.keys()}

    for task_num in task_numbers:
        argskwargss = [{K_args: (random.uniform(1, 10), random.uniform(1, 10))} for _ in range(task_num)]

        for taskname, fctn in taskname2fctn.items():
            for execname, exec_func in executors.items():
                watch = Watch()
                results = exec_func(fctn, argskwargss)
                cost = watch.see_timedelta()
                taskname2execname2costs[taskname][execname].append(cost.total_seconds())

        nrows = len(taskname2fctn)
        fig, axs = plt.subplots(nrows, 1, figsize=(10, 5 * nrows))

        for i, (taskname, execname2cost) in enumerate(taskname2execname2costs.items()):

            axs[i].set_yscale("log")
            axs[i].set_xscale("log")
            for execname, costs in execname2cost.items():
                axs[i].plot(task_numbers[:len(costs)], costs, marker='o', label=execname, alpha=0.5)
            axs[i].set_xlabel("Number of Tasks")
            axs[i].set_ylabel("Execution Time (seconds)")
            axs[i].set_title(f"{taskname.upper()}-bound Task Execution Time")
            axs[i].legend()
            axs[i].grid(True)

        plt.tight_layout()
        fpath_fig = "../x_figs/evaluation_speed_for_batch_tools.png"
        plt.savefig(fpath_fig)
        print(f"SAVE FIG {fpath_fig}")
        plt.close()
