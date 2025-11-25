import queue
import threading
import time
import traceback
from concurrent.futures import Future
from typing import Callable, Any

from gatling.runtime.task_manager.runtime_task_manager_base import RuntimeTaskManager
from gatling.storage.queue.base_queue import BaseQueue
from gatling.storage.queue.memory_queue import MemoryQueue
from gatling.utility.xprint import print_flush


def producer_iter_loop(fctn, qwait, qwork, qerrr, qdone, stop_event, interval, logfctn):
    while True:
        try:
            arg = qwait.get(block=False)
            qwork.put(arg)
            try:
                gen = fctn(arg)
                for item in gen:
                    qdone.put(item)
            except Exception:
                logfctn(traceback.format_exc())
                qerrr.put(arg)
            finally:
                qwork.get(block=True)
        except queue.Empty:
            if stop_event.is_set():
                break
            else:
                time.sleep(interval)


class RuntimeTaskManagerThreadIterator(RuntimeTaskManager):

    def __init__(self, fctn: Callable,
                 qwait: BaseQueue[Any],
                 qwork: BaseQueue[Future],
                 qerrr: BaseQueue[Any],
                 qdone: BaseQueue[Any],
                 worker: int = 1,
                 interval=0.001,
                 errlogfctn=print_flush):
        super().__init__(fctn, qwait, qwork, qerrr, qdone, worker=worker)
        self.interval = interval

        self.thread_stop_event: threading.Event = threading.Event()  # False
        self.thread_running_executor_worker: int = 0
        self.errlogfctn = errlogfctn

        self.producers = []
        self.consumers = []

    def __len__(self):
        return self.thread_running_executor_worker

    def __str__(self):
        return "ThIt" + super().__str__()

    def start(self, worker):
        if self.thread_running_executor_worker > 0:
            raise RuntimeError(f"{str(self)} already started")
        if self.thread_stop_event.is_set():
            raise RuntimeError(f"{str(self)} is stopping")

        self.errlogfctn(f"{self} start triggered ... ")
        self.thread_running_executor_worker = worker

        for i in range(worker):
            producer_thread = threading.Thread(target=producer_iter_loop, args=(self.fctn, self.qwait, self.qwork, self.qerrr, self.qdone, self.thread_stop_event, self.interval, self.errlogfctn), daemon=True)
            producer_thread.start()
            self.producers.append(producer_thread)

        self.errlogfctn(f"{str(self)} started >>>")

    def stop(self):
        if self.thread_running_executor_worker == 0:
            return False
        if self.thread_stop_event.is_set():
            return False

        self.errlogfctn(f"{self} stop triggered ... ")
        self.thread_stop_event.set()

        for producer_thread in self.producers:
            producer_thread.join()
        self.producers.clear()

        for consumer_thread in self.consumers:
            consumer_thread.join()
        self.consumers.clear()

        self.thread_running_executor_worker = 0

        self.thread_stop_event.clear()

        self.errlogfctn(f"{str(self)} stopped !!!")
        return True


if __name__ == '__main__':
    pass
    from gatling.vtasks.sample_tasks import fake_iter_disk

    rt = RuntimeTaskManagerThreadIterator(fake_iter_disk, qwait=MemoryQueue(), qwork=MemoryQueue(), qerrr=MemoryQueue(), qdone=MemoryQueue())

    with rt.execute(worker=5, log_interval=1, logfctn=print_flush):
        for i in range(10):
            rt.qwait.put(i)

    print(f"[{len(rt.qdone)}] : {list(rt.qdone)}")
