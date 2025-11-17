import asyncio
import inspect
import time
import traceback
from datetime import timedelta
from queue import Queue
from typing import Callable, List, Any, Optional

from src.utility.coroutine_thread_mana import CoroutineThreadManager
from src.utility.watch import Watch


class FakeQueue(Queue):
    """
    A lightweight fake queue that only counts how many items are active.
    Behaves like Queue but doesn't actually store items.
    """

    def __init__(self):
        super().__init__()
        self._count = 0

    def put(self, item=None, block=True, timeout=None):
        """Increment count instead of storing items."""
        with self.mutex:
            self._count += 1
            # Trigger the not_empty notification to let waiting threads know that "a task is available."
            self.not_empty.notify()

    def get(self, block=True, timeout=None):
        """Decrement count instead of retrieving real item."""
        with self.mutex:
            if self._count > 0:
                self._count -= 1
            else:
                # Simulate the native Queue’s waiting logic (simplified version).
                self.not_empty.wait(timeout=timeout)
        return None  # No real object

    def qsize(self):
        return self._count

    def empty(self):
        return self._count == 0

    def full(self):
        return False  # Never full

    def __len__(self):
        return self._count

    def __repr__(self):
        return f"<FakeQueue count={self._count}>"


class Stage:
    """Represents one stage in the task pipeline."""

    def __init__(self, fctn: Callable, name: Optional[str] = None, thread_worker: int = None, coroutine_worker: int = None):
        self.fctn = fctn
        self.name = name or fctn.__name__

        if thread_worker is None:
            self.thread_worker = 1
        else:
            self.thread_worker = thread_worker

        if coroutine_worker is None:
            if inspect.iscoroutinefunction(self.fctn):
                self.coroutine_worker = 1
            else:
                self.coroutine_worker = 0
        else:
            self.coroutine_worker = coroutine_worker

        # Core tracking sets / queues
        self.fq_work_info: Optional[Queue[Any]] = FakeQueue()
        self.q_wait_info: Optional[Queue[Any]] = None
        self.q_done_info: Optional[Queue[Any]] = None
        self.q_errr_info: Optional[Queue[Any]] = None

    # ---- Queue setters ----
    def set_queue_wait(self, q: Queue[Any]):
        """Assigns the wait queue (for initial stage)."""
        self.q_wait_info = q
        return self  # allow chaining

    def set_queue_done(self, q: Queue[Any]):
        """Assigns the done queue (output of this stage)."""
        self.q_done_info = q
        return self

    def set_queue_errr(self, q: Queue[Any]):
        """Assigns the error queue (for failed tasks)."""
        self.q_errr_info = q
        return self

    # ---- Queue getters ----
    def get_queue_wait(self) -> Optional[Queue[Any]]:
        """Returns the wait queue."""
        return self.q_wait_info

    def get_queue_done(self) -> Optional[Queue[Any]]:
        """Returns the done queue."""
        return self.q_done_info

    def get_queue_errr(self) -> Optional[Queue[Any]]:
        """Returns the error queue."""
        return self.q_errr_info

    def get_set_work(self) -> Optional[Queue[Any]]:
        return self.fq_work_info

    # ---- Representation ----
    def __repr__(self):
        n_work = self.q_done_info.qsize() if self.q_done_info else 0
        n_done = self.q_done_info.qsize() if self.q_done_info else 0
        n_errr = self.q_errr_info.qsize() if self.q_errr_info else 0
        n_wait = self.q_wait_info.qsize() if self.q_wait_info else 0
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


class TaskQueueTracker:
    """
    Build a continuous processing pipeline with states tracked:
    - first queue = wait
    - each stage tracks work/done/errr
    - last stage results are stored in reslist
    """

    def __init__(self, wait_queue: Queue, done_queue: Queue, retry_on_error=True):

        # Build stages
        self.stages: List[Stage] = []
        self.wait_queue: Queue = wait_queue
        self.done_queue: Queue = done_queue
        self.retry_on_error = retry_on_error
        self.SENTINEL = object()

    def append_stagefctn(self, fctn: Callable, name: Optional[str] = None, thread_worker: int = None, coroutine_worker: int = None):

        current_stage = Stage(fctn, name, thread_worker=thread_worker, coroutine_worker=coroutine_worker)
        if len(self.stages) == 0:
            current_stage.set_queue_wait(self.wait_queue)

        else:
            previous_stage = self.stages[-1]
            intermediate_queue = Queue()
            previous_stage.set_queue_done(intermediate_queue)
            current_stage.set_queue_wait(previous_stage.get_queue_done())

        current_stage.set_queue_done(self.done_queue)

        if self.retry_on_error:
            current_stage.set_queue_errr(current_stage.get_queue_wait())
        else:
            error_queue = Queue()
            current_stage.set_queue_errr(error_queue)

        self.stages.append(current_stage)

    def register_stagefctns(self) -> list[Callable]:
        """
        Create and return a list of wrapper functions for all stages.
        Each wrapper is bound to its own Stage instance.
        These wrappers can be directly passed to AsyncThreadManager.
        """
        wrappers = []

        # ---- Sync stage wrapper factory ----
        def make_sync_wrapper(stage: Stage):
            """
            Create a synchronous wrapper function bound to the given stage.
            """

            def sync_wrapper():
                # Take input arguments from the stage's waiting queue
                args_kwargs = stage.q_wait_info.get()
                stage.q_wait_info.task_done()

                if args_kwargs is self.SENTINEL:
                    return

                stage.fq_work_info.put(args_kwargs)
                try:
                    # Execute the stage function
                    result = stage.fctn(args_kwargs)
                    # Push the result to the next (done) queue
                    stage.q_done_info.put(result)
                except Exception as e:
                    # On failure, push to the error queue
                    print(traceback.format_exc())
                    stage.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:
                    # Mark this work item as finished
                    stage.fq_work_info.get()
                    # stage.fq_work_info.task_done()

            sync_wrapper.__name__ = stage.name
            return sync_wrapper

        # ---- Async stage wrapper factory ----
        def make_async_wrapper(stage: Stage):
            """
            Create an asynchronous wrapper function bound to the given stage.
            """

            async def async_wrapper():
                # Take input arguments from the stage's waiting queue
                loop = asyncio.get_event_loop()
                args_kwargs = await loop.run_in_executor(None, stage.q_wait_info.get)
                stage.q_wait_info.task_done()

                if args_kwargs is self.SENTINEL:
                    return
                stage.fq_work_info.put(args_kwargs)

                try:
                    # Await the stage coroutine
                    result = await stage.fctn(args_kwargs)
                    # Push the result to the next (done) queue
                    stage.q_done_info.put(result)
                except Exception as e:
                    # On failure, push to the error queue
                    print(traceback.format_exc())
                    stage.q_errr_info.put({"args_kwargs": args_kwargs, "error": e})
                finally:
                    await loop.run_in_executor(None, stage.fq_work_info.get)
                    # stage.fq_work_info.task_done()

            async_wrapper.__name__ = stage.name
            return async_wrapper

        # ---- Build all wrappers ----
        for stage in self.stages:
            if inspect.iscoroutinefunction(stage.fctn):
                # Async stage → async wrapper
                wrappers.append(make_async_wrapper(stage))
            else:
                # Sync stage → sync wrapper
                wrappers.append(make_sync_wrapper(stage))

        return wrappers

    def check_done(self) -> bool:
        conds = []
        for stage in self.stages:
            q_work = stage.get_set_work()
            q_wait = stage.get_queue_wait()
            if q_work is not None:
                conds.append(q_work.empty())
            if q_wait is not None:
                conds.append(q_wait.empty())
        # print(f"[check_done] conds={conds}")
        return all(conds)

    def get_status(self) -> dict:

        waitinfos = []
        workinfos = []
        errorinfos = []
        doneinfos = []

        waitinfos.append({K_name: "wait", K_size: self.wait_queue.qsize(), K_id: id(self.wait_queue)})

        for stage in self.stages:
            workinfos.append({K_name: stage.name, K_size: stage.fq_work_info.qsize() if stage.fq_work_info else 0, K_id: id(stage.fq_work_info)})
            errorinfos.append({K_name: stage.name, K_size: stage.q_errr_info.qsize() if stage.q_errr_info else 0, K_id: id(stage.q_errr_info)})
            doneinfos.append({K_name: stage.name, K_size: stage.q_done_info.qsize() if stage.q_done_info else 0, K_id: id(stage.q_done_info)})

        status = {K_wait: waitinfos, K_work: workinfos, K_errr: errorinfos, K_done: doneinfos}
        return status

    def get_gen_speedinfo(self):
        N_already_done = self.done_queue.qsize()
        w = Watch()

        def get_speedinfo():
            N_done = self.done_queue.qsize()

            N_wait = self.wait_queue.qsize()
            N_cur_done = N_done - N_already_done
            N_error = sum(stage.q_errr_info.qsize() for stage in self.stages if stage.q_errr_info is not None)

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

    def __init__(self, wait_queue: Queue, done_queue: Queue, retry_on_error=True):
        self.tqt = TaskQueueTracker(wait_queue, done_queue, retry_on_error)
        self.tcms: List[CoroutineThreadManager] = []

    def append_stagefctn(self, fctn: Callable, name: Optional[str] = None, thread_worker: int = None, coroutine_worker: int = None):
        self.tqt.append_stagefctn(fctn, name, thread_worker, coroutine_worker)

    def start(self):
        for wrap_fctn in self.tqt.register_stagefctns():
            self.tcms.append(CoroutineThreadManager(wrap_fctn))

        for tcm, stage in zip(self.tcms, self.tqt.stages):
            tcm.start(thread_worker=stage.thread_worker, coroutine_worker=stage.coroutine_worker)

    def stop(self):
        for stage in self.tqt.stages:
            for _ in range(stage.thread_worker):
                stage.q_wait_info.put(self.tqt.SENTINEL)

        for tcm, stage in zip(self.tcms, self.tqt.stages):
            tcm.stop()

    def await_print(self, interval=1.0):
        self.tqt.await_print(interval)


if __name__ == '__main__':
    pass


    def sync_square(x):
        # print(f"[sync_square] computing {x}^2 ...")
        time.sleep(0.2)
        return x * x


    async def async_double(x):
        # print(f"[async_double] doubling {x} ...")
        await asyncio.sleep(0.3)
        return x * 2


    def sync_to_str(x):
        # print(f"[sync_to_str] formatting {x} ...")
        return f"Result: {x}"


    q_wait = Queue()
    q_done = Queue()

    tfm = TaskFlowManager(q_wait, q_done, retry_on_error=False)

    tfm.append_stagefctn(sync_square)
    tfm.append_stagefctn(async_double, thread_worker=1, coroutine_worker=1)
    tfm.append_stagefctn(sync_to_str)

    for i in range(10):
        q_wait.put(i)

    tfm.start()
    tfm.await_print()
    tfm.stop()

    for res in q_done.queue:
        print(res)
