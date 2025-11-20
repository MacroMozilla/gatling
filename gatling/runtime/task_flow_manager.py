import asyncio
import inspect
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from typing import Callable, List, Any, Optional

from gatling.runtime.runtime_task_manager_base import RuntimeTaskManager
from gatling.runtime.runtime_task_manager_coroutine import RuntimeTaskManagerCoroutine
from gatling.runtime.runtime_task_manager_thread import RuntimeTaskManagerThread
from gatling.storage.queue.base_queue import BaseQueue
from gatling.storage.queue.countonly_queue import CountOnlyQueue
from gatling.storage.queue.memory_queue import MemoryQueue

from gatling.utility.watch import Watch


def format_timedelta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def status2sent(status):
    sent = ""
    for waitinfo in status[K_wait]:
        sent += f"{waitinfo[K_name]}[{waitinfo[K_size]}]"

    for workinfo, errrinfo, doneinfo in zip(status[K_work], status[K_errr], status[K_done]):
        sent += f" > {workinfo[K_name]}({workinfo[K_size]}|{errrinfo[K_size]})[{doneinfo[K_size]}]"
    return sent


class Stage:
    """Represents one stage in the task pipeline."""

    def __init__(self, fctn, name: Optional[str] = None):
        self.fctn = fctn
        self.name = name or self.fctn.__name__

        # Core tracking sets / queues
        self.q_work_info: CountOnlyQueue[Any] = CountOnlyQueue()
        self.q_wait_info: MemoryQueue[Any] = MemoryQueue()
        self.q_done_info: MemoryQueue[Any] = MemoryQueue()
        self.q_errr_info: MemoryQueue[Any] = MemoryQueue()

    # ---- Queue setters ----
    def set_queue_wait(self, q: BaseQueue[Any]):
        """Assigns the wait queue (for initial stage)."""
        self.q_wait_info = q
        return self  # allow chaining

    def set_queue_done(self, q: BaseQueue[Any]):
        """Assigns the done queue (output of this stage)."""
        self.q_done_info = q
        return self

    def set_queue_errr(self, q: BaseQueue[Any]):
        """Assigns the error queue (for failed tasks)."""
        self.q_errr_info = q
        return self

    # ---- Queue getters ----
    def get_queue_wait(self) -> Optional[BaseQueue[Any]]:
        """Returns the wait queue."""
        return self.q_wait_info

    def get_queue_done(self) -> Optional[BaseQueue[Any]]:
        """Returns the done queue."""
        return self.q_done_info

    def get_queue_errr(self) -> Optional[BaseQueue[Any]]:
        """Returns the error queue."""
        return self.q_errr_info

    def get_queue_work(self) -> Optional[BaseQueue[Any]]:
        return self.q_work_info

    # ---- Representation ----
    def __str__(self):
        n_work = len(self.q_work_info) if self.q_work_info else 0
        n_done = len(self.q_done_info) if self.q_done_info else 0
        n_errr = len(self.q_errr_info) if self.q_errr_info else 0
        n_wait = len(self.q_wait_info) if self.q_wait_info else 0
        return (
            f"<Stage {self.name}: "
            f"wait={n_wait}, work={n_work}, done={n_done}, errr={n_errr}>"
        )


K_cost = 'cost'
K_speed = 'speed'
K_srate = 'srate'
K_remain = 'remain'

K_wait = 'wait'
K_work = 'work'
K_done = 'done'
K_errr = 'errr'

K_name = 'name'
K_size = 'size'
K_id = 'id'


class TaskQueueTracker:
    """
    Build a continuous processing pipeline with states tracked:
    - first queue = wait
    - each stage tracks work/done/errr
    - last stage results are stored in reslist
    """

    def __init__(self, wait_queue: BaseQueue[Any], done_queue: BaseQueue[Any], retry_on_error=True):

        # Build stages
        self.stages: List[Stage] = []
        self.wait_queue: BaseQueue[Any] = wait_queue
        self.done_queue: BaseQueue[Any] = done_queue
        self.retry_on_error = retry_on_error
        self.SENTINEL = object()

    def append_stagefctn(self, stage: Stage):
        current_stage = stage
        if len(self.stages) == 0:
            current_stage.set_queue_wait(self.wait_queue)
        else:
            previous_stage = self.stages[-1]
            intermediate_queue = MemoryQueue()
            previous_stage.set_queue_done(intermediate_queue)
            current_stage.set_queue_wait(previous_stage.get_queue_done())

        current_stage.set_queue_done(self.done_queue)

        if self.retry_on_error:
            current_stage.set_queue_errr(current_stage.get_queue_wait())
        else:
            error_queue = MemoryQueue()
            current_stage.set_queue_errr(error_queue)

        self.stages.append(current_stage)

    def register_stagefctns(self) -> list[Callable]:
        """
        Create and return a list of wrapper functions for all stages.
        Supports sync/async/generator/async-generator stage functions.
        """

        wrappers = []

        # ---- Sync normal function ----
        def make_sync_wrapper(stg: Stage):
            def sync_wrapper():
                args_kwargs = stg.q_wait_info.get()
                if args_kwargs is self.SENTINEL:
                    return self.SENTINEL
                stg.q_work_info.put(args_kwargs)
                try:
                    result = stg.fctn(args_kwargs)
                    stg.q_done_info.put(result)
                except Exception as e:
                    print(traceback.format_exc())
                    stg.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:
                    stg.q_work_info.get()

            sync_wrapper.__name__ = stg.fctn.__name__
            return sync_wrapper

        # ---- Async normal coroutine ----
        def make_async_wrapper(stg: Stage):
            async def async_wrapper():
                loop = asyncio.get_event_loop()
                args_kwargs = await loop.run_in_executor(None, stg.q_wait_info.get)
                if args_kwargs is self.SENTINEL:
                    return self.SENTINEL
                stg.q_work_info.put(args_kwargs)
                try:
                    result = await stg.fctn(args_kwargs)
                    stg.q_done_info.put(result)
                except Exception as e:
                    print(traceback.format_exc())
                    stg.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:

                    await loop.run_in_executor(None, stg.q_work_info.get)

            async_wrapper.__name__ = stg.fctn.__name__
            return async_wrapper

        # ---- Sync generator (iterator) ----
        def make_gen_wrapper(stg: Stage):
            def gen_wrapper():
                args_kwargs = stg.q_wait_info.get()
                if args_kwargs is self.SENTINEL:
                    return self.SENTINEL
                stg.q_work_info.put(args_kwargs)
                try:
                    for val in stg.fctn(args_kwargs):
                        stg.q_done_info.put(val)
                except Exception as e:
                    print(traceback.format_exc())
                    stg.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:
                    stg.q_work_info.get()

            gen_wrapper.__name__ = stg.fctn.__name__
            return gen_wrapper

        # ---- Async generator ----
        def make_asyncgen_wrapper(stg: Stage):
            async def asyncgen_wrapper():
                loop = asyncio.get_event_loop()
                args_kwargs = await loop.run_in_executor(None, stg.q_wait_info.get)
                if args_kwargs is self.SENTINEL:
                    return self.SENTINEL
                stg.q_work_info.put(args_kwargs)
                try:
                    agen = stg.fctn(args_kwargs)
                    async for val in agen:
                        stg.q_done_info.put(val)
                except Exception as e:
                    print(traceback.format_exc())
                    stg.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:
                    await loop.run_in_executor(None, stg.q_work_info.get)

            asyncgen_wrapper.__name__ = stg.fctn.__name__
            return asyncgen_wrapper

        # ---- Build all wrappers ----
        for stage in self.stages:
            if inspect.isasyncgenfunction(stage.fctn):
                wrappers.append(make_asyncgen_wrapper(stage))
            elif inspect.isgeneratorfunction(stage.fctn):
                wrappers.append(make_gen_wrapper(stage))
            elif inspect.iscoroutinefunction(stage.fctn):
                wrappers.append(make_async_wrapper(stage))
            else:
                wrappers.append(make_sync_wrapper(stage))

        return wrappers

    def check_done(self) -> bool:
        conds = []
        for stage in self.stages:
            qx_work = stage.get_queue_work()
            qx_wait = stage.get_queue_wait()
            if qx_work is not None:
                conds.append(len(qx_work) == 0)
            if qx_wait is not None:
                conds.append(len(qx_wait) == 0)
        # print(f"[check_done] conds={conds}")
        return all(conds)

    def get_status(self) -> dict:

        waitinfos = []
        workinfos = []
        errorinfos = []
        doneinfos = []

        waitinfos.append({K_name: "wait", K_size: len(self.wait_queue), K_id: id(self.wait_queue)})

        for stage in self.stages:
            workinfos.append({K_name: stage.name, K_size: len(stage.q_work_info) if stage.q_work_info else 0, K_id: id(stage.q_work_info)})
            errorinfos.append({K_name: stage.name, K_size: len(stage.q_errr_info) if stage.q_errr_info else 0, K_id: id(stage.q_errr_info)})
            doneinfos.append({K_name: stage.name, K_size: len(stage.q_done_info) if stage.q_done_info else 0, K_id: id(stage.q_done_info)})

        status = {K_wait: waitinfos, K_work: workinfos, K_errr: errorinfos, K_done: doneinfos}
        return status

    def get_gen_speedinfo(self):
        N_already_done = len(self.done_queue)
        w = Watch()

        def get_speedinfo():
            N_done = len(self.done_queue)

            N_wait = len(self.wait_queue)
            N_cur_done = N_done - N_already_done
            N_error = sum(len(stage.q_errr_info) for stage in self.stages if stage.q_errr_info is not None)

            w.see_timedelta()
            cost_td = w.total_timedelta()
            cost_sec = cost_td.total_seconds()

            srate = N_cur_done / (N_cur_done + N_error) if (N_cur_done + N_error) > 0 else 0

            speed = N_cur_done / cost_sec if cost_sec > 0 else 0

            cost = format_timedelta(cost_td)

            remain = format_timedelta(timedelta(seconds=(N_wait / speed)) if speed > 0 else timedelta.max)
            speedinfo = {}
            speedinfo[K_cost] = cost
            speedinfo[K_speed] = speed
            speedinfo[K_srate] = srate
            speedinfo[K_wait] = N_wait
            speedinfo[K_remain] = remain
            return speedinfo

        while not self.check_done():
            yield get_speedinfo()
        yield get_speedinfo()

    def await_print(self, interval=1.0, logfctn=print):

        gen_speedinfo = self.get_gen_speedinfo()
        for sinfo in gen_speedinfo:
            cost = sinfo[K_cost]
            speed = sinfo[K_speed]
            srate = sinfo[K_srate]
            remain = sinfo[K_remain]

            status = self.get_status()
            status_sent = status2sent(status)

            sent = f"[{cost}] remain={remain} {speed:.1f} iter/sec {srate=:.2f} {status_sent}"
            logfctn(sent)
            time.sleep(interval)

        logfctn("DONE !!!")


class TaskFlowManager:

    def __init__(self, wait_queue: BaseQueue[Any], done_queue: BaseQueue[Any], retry_on_error=True, interval=1):
        self.tqt = TaskQueueTracker(wait_queue, done_queue, retry_on_error)
        self.rtm_cls_s = []
        self.rtms: List[RuntimeTaskManager] = []
        self.workers: List[int] = []
        self.log_interval = interval

    def register_thread(self, fctn: Callable, worker: int = 1, name: Optional[str] = None):
        stage = Stage(fctn, name=name)
        self.tqt.append_stagefctn(stage)
        self.workers.append(worker)
        self.rtm_cls_s.append(RuntimeTaskManagerThread)

    def register_coroutine(self, fctn: Callable, worker: int = 10, name: Optional[str] = None):
        stage = Stage(fctn, name=name)
        self.tqt.append_stagefctn(stage)
        self.workers.append(worker)
        self.rtm_cls_s.append(RuntimeTaskManagerCoroutine)

    def start(self):
        for wrap_fctn, rtm_cls in zip(self.tqt.register_stagefctns(), self.rtm_cls_s):
            self.rtms.append(rtm_cls(wrap_fctn, interval=0))  # run as fast as possible

        for tcm, stage, worker in zip(self.rtms, self.tqt.stages, self.workers):
            tcm.start(worker=worker)

    def stop(self):
        # Start a dedicated sentinel broadcast thread.

        stop_flag = threading.Event()

        def broadcast_sentinel():
            """Continuously send SENTINELs until the stop_flag is set."""
            while not stop_flag.is_set():
                for stage in self.tqt.stages:
                    # Send several stop signals into the wait queue of each stage.
                    stage.q_wait_info.put(self.tqt.SENTINEL)
                time.sleep(0.001)  # Avoid CPU spinning.

        with ThreadPoolExecutor(max_workers=1) as tpe:
            future = tpe.submit(broadcast_sentinel)
            # RuntimeTaskManager stopped the worker thread
            for rtm in self.rtms:
                rtm.stop()
            # Wait for all RuntimeTaskManager instances to fully exit.
            for rtm in self.rtms:
                if hasattr(rtm, "join"):
                    rtm.join(timeout=2)
            # Signal the broadcast thread to terminate.
            stop_flag.set()
            future.result()

        # clean remaining SENTINEL in wait info queues
        for stage in self.tqt.stages:
            stage.q_wait_info.clear()

        print("[TaskFlowManager] all runtimes stopped cleanly.")

    def await_print(self, interval=None):
        if interval is not None:
            self.log_interval = interval
        self.tqt.await_print(self.log_interval)

    @contextmanager
    def execute(self, log_interval=1):
        yield self
        self.start()
        self.await_print(interval=log_interval)
        self.stop()


if __name__ == '__main__':
    pass
    from gatling.utility.sample_tasks import async_fake_fctn_io, wrap_async_iter, fake_fctn_io, wrap_iter, fake_fctn_cpu, real_fctn_cpu, mix64
    # === async ===
    task_async_fake_fctn = async_fake_fctn_io
    task_async_fake_iter = wrap_async_iter(n=2)(async_fake_fctn_io)

    # === sync ===
    task_fake_fctn_io = fake_fctn_io
    task_fake_fctn_cpu = fake_fctn_cpu
    task_real_fctn_cpu = real_fctn_cpu
    task_fake_iter_io = wrap_iter(n=2)(fake_fctn_io)
    task_fake_iter_cpu = wrap_iter(n=2)(fake_fctn_cpu)
    task_real_iter_cpu = wrap_iter(n=2)(real_fctn_cpu)

    # ---------- Build and run the pipeline ----------
    if True:
        q_wait = MemoryQueue()
        q_done = MemoryQueue()

        for i in range(10):
            q_wait.put(i + 1)

        tfm = TaskFlowManager(q_wait, q_done, retry_on_error=False)

        tfm.register_coroutine(task_async_fake_fctn, worker=5)
        tfm.register_thread(task_fake_fctn_io, worker=2)
        tfm.register_thread(task_fake_fctn_cpu, worker=2)
        tfm.register_thread(task_real_fctn_cpu, worker=2)

        tfm.register_coroutine(task_async_fake_iter, worker=5)
        tfm.register_thread(task_fake_iter_io, worker=2)
        tfm.register_thread(task_fake_iter_cpu, worker=2)
        tfm.register_thread(task_real_iter_cpu, worker=2)

        tfm.start()
        tfm.await_print(interval=0.5)
        tfm.stop()

        results = list(q_done)
        print(f"\n=== Final Results ({len(results)})===")

    if False:

        q_wait = MemoryQueue()
        q_done = MemoryQueue()
        for i in range(10):
            q_wait.put(i + 1)

        tfm = TaskFlowManager(q_wait, q_done, retry_on_error=False)

        with tfm.execute(log_interval=0.1):
            tfm.register_coroutine(task_async_fake_fctn, worker=5)
            tfm.register_thread(task_fake_fctn_io, worker=2)
            tfm.register_thread(task_fake_fctn_cpu, worker=2)
            tfm.register_thread(task_real_fctn_cpu, worker=2)
            tfm.register_coroutine(task_async_fake_iter, worker=5)
            tfm.register_thread(task_fake_iter_io, worker=2)
            tfm.register_thread(task_fake_iter_cpu, worker=2)
            tfm.register_thread(task_real_iter_cpu, worker=2)

        results = list(q_done)
        print(f"\n=== Final Results ({len(results)})===")
