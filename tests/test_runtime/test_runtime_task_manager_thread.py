import time
import unittest
from gatling.runtime.runtime_task_manager_thread import RuntimeTaskManagerThread
from gatling.utility.sample_tasks import async_fake_fctn_io, wrap_async_iter, fake_fctn_io, wrap_iter, fake_fctn_cpu, real_fctn_cpu
from test_helper.dynamic_testcase import DynamicTestCase


# DynamicTestCase.set_name('TestRuntimeTaskManagerCoroutine')
class TestRuntimeTaskManagerThread(DynamicTestCase): pass


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
for fname, fctn in [("fake_fctn_io", fake_fctn_io),
                    ("fake_iter_io", wrap_iter(n=2)(fake_fctn_io)),
                    ("fake_fctn_cpu", fake_fctn_cpu),
                    ("fake_iter_cpu", wrap_iter(n=2)(fake_fctn_cpu)),
                    ("real_fctn_cpu", real_fctn_cpu),
                    ("real_iter_cpu", wrap_iter(n=2)(real_fctn_cpu))

                    ]:
    for worker in [1, 5]:
        for use_ctx in [True, False]:
            for interval in [0, 0.001]:
                testcase_name = f"test_{fname}_{worker=}_{use_ctx=}_{interval=:.0e}"
                TestRuntimeTaskManagerThread.append_testcase(testcase_name, testcase_fctn, RuntimeTaskManagerThread, fctn, worker, use_ctx, interval)

if __name__ == "__main__":
    unittest.main(verbosity=2)
