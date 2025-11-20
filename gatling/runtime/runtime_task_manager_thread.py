import inspect
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Optional

from gatling.runtime.runtime_task_manager_base import RuntimeTaskManager


class RuntimeTaskManagerThread(RuntimeTaskManager):
    """
    Run synchronous functions or generators in threads with stop control.
    """

    def __init__(self, fctn: Callable, args: tuple = (), kwargs: Optional[dict] = None, interval=0.001,logfctn=print):
        super().__init__(fctn, args, kwargs)
        self.interval = interval
        self.thread_stop_event: threading.Event = threading.Event()  # False
        self.thread_running_executor: Optional[ThreadPoolExecutor] = None
        self.thread_running_futures: list[Future] = []
        self.logfctn=logfctn
        

    def __len__(self):
        return len(self.thread_running_futures)

    def start(self, worker=1):
        if self.thread_running_executor is not None:
            raise RuntimeError(f"{str(self)} already started")

        is_iter = inspect.isgeneratorfunction(self.fctn)

        def thread_fctn():

            def worker_task(tid):
                try:
                    self.fctn(*self.args, **self.kwargs)
                except Exception:
                    self.logfctn(f"{str(self)} fctn {tid=} exception:")
                    self.logfctn(traceback.format_exc())

            tid = threading.get_ident()
            if self.interval > 0:
                while not self.thread_stop_event.is_set():
                    worker_task(tid)
                    time.sleep(self.interval)
            else:
                while not self.thread_stop_event.is_set():
                    worker_task(tid)

        def thread_iter():

            def worker_task(tid):
                try:
                    gen = self.fctn(*self.args, **self.kwargs)
                    for val in gen:
                        pass
                except Exception:
                    self.logfctn(f"{str(self)} iter {tid=} exception:")
                    self.logfctn(traceback.format_exc())

            tid = threading.get_ident()
            if self.interval > 0:
                while not self.thread_stop_event.is_set():
                    worker_task(tid)
                    time.sleep(self.interval)
            else:
                while not self.thread_stop_event.is_set():
                    worker_task(tid)

        thread_target = thread_iter if is_iter else thread_fctn

        self.logfctn(f"{self} start triggered ... ")
        self.thread_running_executor = ThreadPoolExecutor(max_workers=worker)

        for _ in range(worker):
            rf = self.thread_running_executor.submit(thread_target)
            self.thread_running_futures.append(rf)

        self.logfctn(f"{str(self)} started >>>")

        return True

    def stop(self):
        if self.thread_running_executor is None:
            return False
        if self.thread_stop_event.is_set():
            return False

        self.logfctn(f"{self} stop triggered ... ")
        self.thread_stop_event.set()
        for rf in self.thread_running_futures:
            try:
                rf.result()
            except Exception:
                self.logfctn(traceback.format_exc())

        self.thread_running_executor.shutdown(wait=True)
        self.thread_running_executor = None
        self.thread_running_futures.clear()
        self.thread_stop_event.clear()

        self.logfctn(f"{str(self)} stopped !!!")
        return True


if __name__ == "__main__":
    from gatling.utility.sample_tasks import fake_fctn_io, wrap_iter

    task_fctn = fake_fctn_io
    task_iter = wrap_iter()(fake_fctn_io)


    sleep_time = 0.1
    print("\n=== Test: async task function ===")
    rtm = RuntimeTaskManagerThread(task_fctn, args=(), kwargs={}, interval=0)
    rtm.start(worker=2)
    time.sleep(sleep_time)
    rtm.stop()

    print("\n=== Test: async generator function ===")
    rtm2 = RuntimeTaskManagerThread(task_iter, args=(), kwargs={}, interval=0)
    rtm2.start(worker=2)
    time.sleep(sleep_time)
    rtm2.stop()

    print("\n=== Test: async generator function context manager ===")
    rtm3 = RuntimeTaskManagerThread(task_fctn, args=(), kwargs={}, interval=0)
    with rtm3.execute(worker=2):
        time.sleep(sleep_time)

    print("\n=== Test: async generator iterator context manager ===")
    rtm4 = RuntimeTaskManagerThread(task_iter, args=(), kwargs={}, interval=0)
    with rtm4.execute(worker=2):
        time.sleep(sleep_time)
