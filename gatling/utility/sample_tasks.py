import asyncio
import inspect
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Optional

from gatling.utility.watch import Watch

num_1K = 1024
num_1M = 1024 ** 2
num_1G = 1024 ** 3

network_bytes_per_sec = 1 * num_1M  # bytes/s
disk_bytes_per_sec = 500 * num_1M  # bytes/s

size_1KB = num_1K
size_1MB = num_1M
size_1GB = num_1G

size_target = size_1KB * 1

flops_1K = num_1K
flops_1M = num_1M
flops_1G = num_1G
flops_target = flops_1M * 1

cpu_flops_per_sec = 2.5 * num_1G

fix_error_rate = 0.05


def with_flops(n: int):
    def decorator(f: Callable):
        f.flops: int = n
        return f

    return decorator


@with_flops(610)
def mix64(x: int) -> int:
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
    x = (x ^ (x >> 27)) * 0x94d049bb133111eb
    x = x ^ (x >> 31)
    return x & 0xFFFFFFFFFFFFFFFF


def ensure_seed(seed: Optional[int] = None):
    if seed is None:
        seed = random.getrandbits(64)
    return seed


def calc_fctn_flops(fctn: Callable = mix64, seed=None, flops_per_sec=cpu_flops_per_sec, num=1, xrange=range):
    seed = ensure_seed(seed)
    w = Watch()
    for i in xrange(num):
        seed = fctn(seed)
    total_cost_secs = w.see_seconds()
    # print(f"{total_cost_secs=}")
    percall_cost_secs = total_cost_secs / num
    # print(f"{percall_cost_secs=}")
    percall_flops = flops_per_sec * percall_cost_secs
    flops = fctn.flops
    print(f"{fctn.__name__}.{flops=:.0f}=>{percall_flops:.0f} {num=}")
    return percall_flops


def approx_calc_fctn_flops(fctn: Callable = mix64, seed=None, flops_per_sec=cpu_flops_per_sec, xrange=range, iter=0, flops=flops_target):
    seed = ensure_seed(seed)
    num = int(flops / fctn.flops) + 1
    percall_flops: int = calc_fctn_flops(fctn=fctn, seed=seed, flops_per_sec=flops_per_sec, num=num, xrange=xrange)
    fctn = with_flops(percall_flops)(fctn)
    num = int(flops / fctn.flops) + 1
    for i in xrange(iter):
        percall_flops = calc_fctn_flops(fctn=fctn, seed=seed, flops_per_sec=flops_per_sec, num=num, xrange=xrange)
        fctn = with_flops(percall_flops)(fctn)
        num = int(flops / fctn.flops) + 1
    return percall_flops


mix64 = with_flops(approx_calc_fctn_flops(fctn=mix64, flops_per_sec=cpu_flops_per_sec, flops=flops_target))(mix64)


################################################################################################################################################

def fake_fctn_io(seed=None, size_bytes=size_target, bytes_per_sec=network_bytes_per_sec):
    seed = ensure_seed(seed)
    seconds = size_bytes / bytes_per_sec
    time.sleep(seconds)
    res = mix64(seed)
    return res


async def async_fake_fctn_io(seed=None, size_bytes=size_target, bytes_per_sec=network_bytes_per_sec):
    seed = ensure_seed(seed)
    seconds = size_bytes / bytes_per_sec
    await asyncio.sleep(seconds)
    seed = ensure_seed(seed)
    res = mix64(seed)
    return res


################################################################################################################################################
def fake_fctn_cpu(seed=None, flops=flops_target, flops_per_sec=cpu_flops_per_sec):
    seed = ensure_seed(seed)
    seconds = flops / flops_per_sec
    time.sleep(seconds)
    res = mix64(seed)
    return res


def real_fctn_cpu(seed=None, flops=flops_target, fctn=mix64, xrange=range):
    seed = ensure_seed(seed)
    temp = seed
    num = int(flops / fctn.flops) + 1
    for i in xrange(num):
        temp = fctn(temp)
    res = mix64(seed) + 0 * temp
    return res


################################################################################################################################################

def wrap_error(rate=0.1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if random.random() < rate:
                raise Exception("Fake error (sync)")
            return func(*args, **kwargs)

        return wrapper

    return decorator


def wrap_async_errr(rate=0.1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if random.random() < rate:
                raise Exception("Fake error (async)")
            return await func(*args, **kwargs)

        return wrapper

    return decorator


################################################################################################################################################
def wrap_iter(n=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(n):
                yield func(*args, **kwargs)

        return wrapper

    return decorator


def wrap_async_iter(n=2):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for i in range(n):
                yield await func(*args, **kwargs)

        return wrapper

    return decorator


################################################################################################################################################


if __name__ == '__main__':
    pass

    fctns = [fake_fctn_io, async_fake_fctn_io, fake_fctn_cpu, real_fctn_cpu]

    seed = ensure_seed()
    target = mix64(seed)
    for fctn in fctns:
        w = Watch()

        res = 0
        if inspect.iscoroutinefunction(fctn):
            res = asyncio.run(fctn(seed=seed))
        else:
            res = fctn(seed=seed)

        assert res == target, f"{fctn.__name__} {res} != {target} result is not equal to seed"
        cost = w.see_seconds()
        print(f"=== {fctn.__name__} {cost=}===")
