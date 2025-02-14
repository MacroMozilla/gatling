
import os
import time
import uuid
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

digits_process_id = 5
digits_thread_id = 5
digits_obase64_ns = 5
digits_obase64_mac = 48 // 6
digits_hex_alias = 4


def int2obase64(n: int, pad: int) -> str:
    base64_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz{}"
    if n == 0:
        return base64_chars[0]
    digits = []
    while n:
        digits.append(base64_chars[n & 0b111111])
        n >>= 6
    return ''.join(reversed(digits)).zfill(pad)


def get_process_id() -> int:
    return os.getpid()


def get_thread_id() -> int:
    return threading.get_native_id()


def get_date_time_ns_id() -> str:
    """
    返回一个时间ID，包含纳秒级精度，格式为：yyMMdd HHmmSS ns
    其中 ns 表示当前秒内的纳秒数，固定显示9位（不足时前面补0）
    时间部分使用美国东部时区 (US/Eastern)
    """
    # 获取当前的纳秒级时间戳
    ns_total = time.time_ns()  # 自 Unix epoch 起的总纳秒数
    seconds = ns_total // 1_000_000_000  # 整秒部分
    ns_part = ns_total % 1_000_000_000  # 当前秒内的纳秒部分

    # 将秒转换为美国东部时区的 datetime 对象
    dt = datetime.fromtimestamp(seconds, tz=ZoneInfo("US/Eastern"))
    # 格式化时间字符串
    time_str = dt.strftime("%y%m%d %H%M%S")
    return f"{time_str} {int2obase64(ns_part, digits_obase64_ns)}"


def get_machine_id():
    return int2obase64(uuid.getnode(), digits_obase64_mac)


def make_gen_unique_task_id():
    """
    生成唯一任务ID，格式为："{timestamp} {alias_in_hex}"

    参数:
      - max_alias: alias的最大数量（循环使用的范围为0到max_alias-1）

    alias部分会以16进制表示，宽度根据max_alias自动计算（不足部分补0）。
    """
    alias = 0
    max_alias = 16 ** digits_hex_alias

    while True:
        # get_date_time_ns_id() 函数需要自行定义，返回当前的时间戳（例如纳秒级）
        yield f"{get_date_time_ns_id()} {alias:0{digits_hex_alias}x}"
        alias = (alias + 1) % max_alias


gen_unique_task_id = make_gen_unique_task_id()
get_unique_task_id = lambda: next(gen_unique_task_id)

if __name__ == "__main__":
    pass

    # up_bound = 1_000_000_000
    # N = 10000
    # nums = [random.randint(0, up_bound) for _ in range(N)]
    #
    # for pad in [0, 5]:
    #     # 方法一：先对整数排序，再转换为 base64 字符串
    #     sorted_int_then_base = [int2obase64(n, pad) for n in sorted(nums)]
    #     # 方法二：先将整数转换为 base64 字符串，再进行字典排序
    #     base_then_sorted = sorted(int2obase64(n, pad) for n in nums)
    #
    #     print(f"测试 pad = {pad}: ", end='')
    #     if sorted_int_then_base == base_then_sorted:
    #         print("排序结果一致")
    #     else:
    #         print("排序结果不一致")
