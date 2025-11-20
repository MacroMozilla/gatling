import time
import unittest
from gatling.runtime.runtime_task_manager_coroutine import RuntimeTaskManagerCoroutine
from gatling.utility.sample_tasks import async_fake_fctn_io, wrap_async_iter
from helper.dynamic_testcase import DynamicTestCase


# DynamicTestCase.set_name('TestRuntimeTaskManagerCoroutine')
class TestRuntimeTaskManagerCoroutine(DynamicTestCase):
    pass


# Define Test Case Function
def testcase_fctn(cls, fctn, worker=1, use_ctx=False, interval=0):
    rtm = cls(fctn, args=(), kwargs={}, interval=interval, logfctn=print)
    sleep_time = 0.01
    if not use_ctx:
        rtm.start(worker=worker)
        time.sleep(sleep_time)
        rtm.stop()
    else:
        with rtm.execute(worker=worker):
            time.sleep(sleep_time)


# === Dynamic Register Test Case ===
for fname, fctn in [("async_fake_fctn", async_fake_fctn_io),
                    ("async_fake_iter", wrap_async_iter(n=2)(async_fake_fctn_io))]:
    for worker in [1, 5]:
        for use_ctx in [True, False]:
            for interval in [0, 0.001]:
                testcase_name = f"test_{fname}_{worker=}_{use_ctx=}_{interval=:.0e}"
                TestRuntimeTaskManagerCoroutine.append_testcase(testcase_name, testcase_fctn, RuntimeTaskManagerCoroutine, fctn, worker, use_ctx, interval)

if __name__ == "__main__":
    unittest.main(verbosity=2)
