import queue
import threading
import traceback
from concurrent.futures import Future
from typing import Callable, Any

import multiprocess as mp

from gatling.runtime.task_manager.runtime_task_manager_base import RuntimeTaskManager
from gatling.storage.g_queue.base_queue import BaseQueue
from gatling.storage.g_queue.memory_queue import MemoryQueue
from gatling.utility.xprint import xprint_flush, check_picklable


def producer_iter_loop(fctn, qwait, qwork, qerrr, qdone, stop_event, retry_on_error, retry_empty_interval, errlogfctn):
    _timeout = retry_empty_interval or 0.1
    while True:
        try:
            arg = qwait.get(block=True, timeout=_timeout)
        except queue.Empty:
            if stop_event.is_set():
                break
            continue
        while True:
            try:
                qwork.put(arg, block=True, timeout=_timeout)
                break
            except queue.Full:
                if stop_event.is_set():
                    return
        try:
            gen = fctn(arg)
            for x in gen:
                qdone.put(x)
        except Exception:
            errlogfctn(traceback.format_exc())
            qerrr.put(arg)
            if retry_on_error:
                qwait.put(arg)
        finally:
            try:
                qwork.get(block=True, timeout=_timeout)
            except queue.Empty:
                if stop_event.is_set():
                    break


def bridge(qfm, qto, qwork, qwork_action, stop_event, retry_empty_interval, errlogfctn):
    _timeout = retry_empty_interval or 0.1
    while True:
        try:
            arg = qfm.get(block=True, timeout=_timeout)
        except queue.Empty:
            if stop_event.is_set():
                break
            continue
        qto.put(arg)

        if qwork is not None:
            if qwork_action == 'put':
                while True:
                    try:
                        qwork.put(arg, block=True, timeout=_timeout)
                        break
                    except queue.Full:
                        if stop_event.is_set():
                            return
            elif qwork_action == 'get':
                try:
                    qwork.get(block=True, timeout=_timeout)
                except queue.Empty:
                    if stop_event.is_set():
                        break


class RuntimeTaskManagerProcessIterator(RuntimeTaskManager):

    def __init__(self, fctn: Callable,
                 qwait: BaseQueue[Any],
                 qwork: BaseQueue[Future],
                 qerrr: BaseQueue[Any],
                 qdone: BaseQueue[Any],
                 worker: int = 1,
                 retry_on_error: bool = False,
                 retry_empty_interval=0.001,
                 errlogfctn=xprint_flush,
                 max_work_size: int = 0):
        super().__init__(fctn, qwait, qwork, qerrr, qdone, worker=worker, retry_on_error=retry_on_error, retry_empty_interval=retry_empty_interval, max_work_size=max_work_size)

        self.process_stop_event: mp.Event = mp.Event()  # False
        self.thread_stop_event: threading.Event = threading.Event()
        self.process_running_executor_worker: int = 0
        self.errlogfctn = errlogfctn

        self.manager = mp.Manager()
        self.process_qwork = self.manager.Queue()
        self.process_qwait = self.manager.Queue()
        self.process_qerrr = self.manager.Queue()
        self.process_qdone = self.manager.Queue()

        self.producers_thread = []
        self.producers_process = []

        for f in [self.fctn, self.errlogfctn]:
            check_picklable(f)

    def __len__(self):
        return self.process_running_executor_worker

    def __str__(self):
        return "PrIt" + super().__str__()

    def start(self, worker):

        if self.process_running_executor_worker > 0:
            raise RuntimeError(f"{str(self)} already started")
        if self.process_stop_event.is_set() or self.thread_stop_event.is_set():
            raise RuntimeError(f"{str(self)} is stopping")

        self.errlogfctn(f"{self} start triggered ... ")
        self.process_running_executor_worker = worker

        # bridge thread queue to process queue                                                         track qwork in
        bridge_t2p_wait_thread = threading.Thread(target=bridge, args=(self.qwait, self.process_qwait, self.qwork, 'put', self.thread_stop_event, self.retry_empty_interval, self.errlogfctn), daemon=True)
        bridge_t2p_wait_thread.start()
        self.producers_thread.append(bridge_t2p_wait_thread)

        for _ in range(worker):
            # start N worker for process
            producer_process: mp.Process = mp.Process(target=producer_iter_loop, args=(self.fctn, self.process_qwait, self.process_qwork, self.process_qerrr, self.process_qdone, self.process_stop_event, self.retry_on_error, self.retry_empty_interval, self.errlogfctn))
            producer_process.start()
            self.producers_process.append(producer_process)

        # bridge process queue to thread queue
        bridge_p2t_thread_errr = threading.Thread(target=bridge, args=(self.process_qerrr, self.qerrr, None, None, self.thread_stop_event, self.retry_empty_interval, self.errlogfctn), daemon=True)
        bridge_p2t_thread_errr.start()
        self.producers_thread.append(bridge_p2t_thread_errr)
        #                                                                                              track qwork out
        bridge_p2t_thread_done = threading.Thread(target=bridge, args=(self.process_qdone, self.qdone, self.qwork, 'get', self.thread_stop_event, self.retry_empty_interval, self.errlogfctn), daemon=True)
        bridge_p2t_thread_done.start()
        self.producers_thread.append(bridge_p2t_thread_done)

        self.errlogfctn(f"{str(self)} started >>>")

    def stop(self):
        if self.process_running_executor_worker == 0:
            return False
        if self.process_stop_event.is_set() or self.thread_stop_event.is_set():
            return False

        self.errlogfctn(f"{self} stop triggered ... ")
        self.process_stop_event.set()
        self.thread_stop_event.set()

        for producer_process in self.producers_process:
            producer_process.join()
        self.producers_process.clear()

        for producer_thread in self.producers_thread:
            producer_thread.join()
        self.producers_thread.clear()

        self.process_running_executor_worker = 0

        self.process_stop_event.clear()
        self.thread_stop_event.clear()

        self.errlogfctn(f"{str(self)} stopped !!!")
        return True


if __name__ == '__main__':
    pass
    from gatling.vtasks.sample_tasks import fake_iter_cpu

    rt = RuntimeTaskManagerProcessIterator(fake_iter_cpu, qwait=MemoryQueue(), qwork=MemoryQueue(), qerrr=MemoryQueue(), qdone=MemoryQueue())

    with rt.execute(worker=5, log_interval=1, logfctn=xprint_flush):
        for i in range(10):
            rt.qwait.put(i)

    print(f"[{len(rt.qdone)}] : {list(rt.qdone)}")
