from gatling.runtime.runtime_task_manager_thread import RuntimeTaskManagerThread
import time


# --- Async task ---
def async_fctn(name, delay=0.1):
    print(f"{name} running")
    time.sleep(delay)


def async_iter(name, delay=0.1):
    for i in range(5):
        sent = f"{name}-{i}"
        yield sent
        time.sleep(delay)  # simulate async source


# Async mode
print("--- Function ---")
m = RuntimeTaskManagerThread(async_fctn, args=("fctn",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Function with Context Manager ---")
m = RuntimeTaskManagerThread(async_fctn, args=("fctn",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)

print("--- Iterator ---")
m = RuntimeTaskManagerThread(async_iter, args=("iter",), kwargs={"delay": 0.1})
m.start(worker=2)
time.sleep(0.5)
m.stop()

print("--- Iterator with Context Manager ---")
m = RuntimeTaskManagerThread(async_iter, args=("iter",), kwargs={"delay": 0.1})
with m.execute(worker=2):
    time.sleep(0.5)
