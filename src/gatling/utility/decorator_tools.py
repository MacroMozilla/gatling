import os
import threading
import functools


def show_process_and_thread_id(func):
    def wrapper(*args, **kwargs):
        # 获取当前进程 ID 和线程 ID
        pid = os.getpid()
        tid = threading.get_ident()
        print(f"\nProcess ID: {pid}\tThread ID: {tid}\tBefore calling {func.__name__}(*{args}, **{kwargs})")

        result = func(*args, **kwargs)

        # 执行后再次打印
        print(f"\nProcess ID: {pid}\tThread ID: {tid}\tAfter calling {func.__name__}(*{args}, **{kwargs})")

        return result

    return wrapper


def log_function_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 获取函数名
        function_name = func.__name__

        # 打印函数调用样式
        print(f"{function_name}(args={args},kwargs={kwargs})")

        # 调用实际函数
        return func(*args, **kwargs)

    return wrapper


def sample_function(x, y):
    print(f"Executing sample_function with arguments ({x}, {y})...")
    return x + y


if __name__ == "__main__":
    # 使用该函数包装并调用
    wrapped_function = show_process_and_thread_id(sample_function)
    result = wrapped_function(10, 20)
    print(f"Result of some_function: {result}")
