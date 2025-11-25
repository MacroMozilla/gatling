import queue
import threading
import time
import traceback

import multiprocess as mp
from concurrent.futures import Future

from typing import Callable, Optional, Any

from gatling.runtime.task_manager.runtime_task_manager_base import RuntimeTaskManager
from gatling.storage.queue.base_queue import BaseQueue
from gatling.storage.queue.memory_queue import MemoryQueue
from gatling.utility.xprint import print_flush, check_picklable


def producer_fctn_loop(fctn, qwait, qwork, qerrr, qdone, running_executor, thread_stop_event, interval, logfctn):
    while True:
        try:
            arg = qwait.get(block=False)
            fut = running_executor.apply_async(fctn, (arg,))
            qwork.put(fut)
        except queue.Empty:
            if thread_stop_event.is_set():
                break
            else:
                time.sleep(interval)


def consumer_fctn_loop(fctn, qwait, qwork, qerrr, qdone, running_executor, thread_stop_event, interval, logfctn):
    while True:
        try:
            fut = qwork.get(block=False)
            try:
                res = fut.get()
                qdone.put(res)
            except Exception:
                logfctn(traceback.format_exc())
                qerrr.put(fut)
            finally:
                pass
        except queue.Empty:
            if thread_stop_event.is_set():
                break
            else:
                time.sleep(interval)


class RuntimeTaskManagerProcessFunction(RuntimeTaskManager):

    def __init__(self, fctn: Callable,
                 qwait: BaseQueue[Any],
                 qwork: BaseQueue[Future],
                 qerrr: BaseQueue[Any],
                 qdone: BaseQueue[Any],
                 worker: int = 1,
                 interval=0.001, errlogfctn=print_flush):
        super().__init__(fctn, qwait, qwork, qerrr, qdone, worker=worker)
        self.interval = interval

        self.thread_stop_event: threading.Event = threading.Event()  # False
        self.process_running_executor: Optional[mp.Pool] = None
        self.process_running_executor_worker: int = 0
        self.errlogfctn = errlogfctn

        self.producers = []
        self.consumers = []

        for fctn in [self.fctn, self.errlogfctn]:
            check_picklable(fctn)

    def __len__(self):
        return self.process_running_executor_worker

    def __str__(self):
        return "PrFn" + super().__str__()

    def start(self, worker):

        if self.process_running_executor is not None:
            raise RuntimeError(f"{str(self)} already started")
        if self.thread_stop_event.is_set():
            raise RuntimeError(f"{str(self)} is stopping")

        self.errlogfctn(f"{self} start triggered ... ")
        self.process_running_executor = mp.Pool(processes=worker)
        self.process_running_executor_worker = worker

        # process function logic begin
        producer_thread = threading.Thread(target=producer_fctn_loop, args=(self.fctn, self.qwait, self.qwork, self.qerrr, self.qdone, self.process_running_executor, self.thread_stop_event, self.interval, self.errlogfctn), daemon=True)
        producer_thread.start()
        self.producers.append(producer_thread)

        consumer_thread = threading.Thread(target=consumer_fctn_loop, args=(self.fctn, self.qwait, self.qwork, self.qerrr, self.qdone, self.process_running_executor, self.thread_stop_event, self.interval, self.errlogfctn), daemon=True)
        consumer_thread.start()
        self.consumers.append(consumer_thread)
        # process function logic end

        self.errlogfctn(f"{str(self)} started >>>")

    def stop(self):
        if self.process_running_executor is None:
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

        self.process_running_executor.close()
        self.process_running_executor.join()
        self.process_running_executor = None
        self.process_running_executor_worker = 0
        self.thread_stop_event.clear()

        self.errlogfctn(f"{str(self)} stopped !!!")
        return True


if __name__ == '__main__':
    pass

    from gatling.vtasks.sample_tasks import fake_fctn_cpu,fake_errr,prepend

    errr_fake_fctn_cpu = prepend(fake_errr)(fake_fctn_cpu)
    rt = RuntimeTaskManagerProcessFunction(fake_fctn_cpu, qwait=MemoryQueue(), qwork=MemoryQueue(), qerrr=MemoryQueue(), qdone=MemoryQueue())

    with rt.execute(worker=5, log_interval=1, logfctn=print_flush):
        for i in range(100):
            rt.qwait.put(i)

    print(f"[{len(rt.qdone)}] : {list(rt.qdone)}")
