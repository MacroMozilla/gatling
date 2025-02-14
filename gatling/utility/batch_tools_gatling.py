import inspect
import os
from typing import Callable, List, Any, Dict

from databasetool.redis_base import get_redis_master
from databasetool.redis_z_taskqueuemanager import RedisTaskQueueManager
from utility.const import K_args, TypeR, CallableJson, TypeJsonR, TypeJson
from utility.exec_tools import run_python_script, get_pids_by_cmd, kill_process
import ast

from utility.io_tools import save_text, delete_text

from pathlib import Path
from typing import Callable

from utility.watch import watch_time


def rename_fname(fpath_caller: str, rename_func: Callable[[str], str]) -> str:
    """
    修改路径中的最后一个文件名，使用传入的函数对旧文件名进行变换。

    参数：
        - fpath_caller (str): 绝对路径
        - rename_func (Callable[[str], str]): 接收旧文件名并返回新文件名的函数

    返回：
        - 修改后的新路径（字符串）
    """
    path = Path(fpath_caller)  # 转换为 Path 对象
    new_filename = rename_func(path.name)  # 对旧文件名应用变换函数
    new_path = path.with_name(new_filename)  # 替换文件名
    return str(new_path)


def extract_code_including_function(function_name: str, fpath: str) -> str:
    """
    从指定文件中查找目标函数，并返回从文件开头到该函数结束的所有代码。

    参数:
      - function_name: 目标函数名，例如 "my_function"
      - filename: 文件路径，默认为当前文件 (__file__)

    返回:
      - 包含目标函数及其之前所有代码的字符串。如果找不到目标函数，则返回空字符串。
    """
    # 读取整个文件内容
    with open(fpath, 'r', encoding='utf-8') as f:
        source = f.read()

    # 利用 ast 解析源代码
    tree = ast.parse(source, filename=fpath)
    target_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            target_func_node = node
            break

    if target_func_node is None:
        # 没有找到目标函数
        return ""

    try:
        # Python 3.8+ 中，ast 节点包含 end_lineno 属性，表示函数定义块的最后一行
        end_lineno = target_func_node.end_lineno
    except AttributeError:
        # 如果没有 end_lineno，可以采取简单的方式：只包含到函数定义行（不包括函数体）
        end_lineno = target_func_node.lineno

    # 将文件分割成行，并保留换行符
    lines = source.splitlines(keepends=True)
    # 返回从第一行到目标函数结束行的所有代码
    return "".join(lines[:end_lineno])


def get_calling_filename(offset: int = 1) -> str:
    """
    获取调用此函数的文件的绝对路径，可以通过 offset 指定层级。
    offset=1 表示直接调用 get_calling_filename 的那一层，
    offset=2 表示调用 get_calling_filename 的函数的调用者，以此类推。
    """
    stack = inspect.stack()
    # 如果堆栈深度不足，返回最底层的文件
    if len(stack) <= offset:
        return os.path.realpath(stack[-1].filename)
    caller_frame_info = stack[offset]
    return os.path.realpath(caller_frame_info.filename)


@watch_time
def batch_execute_gatling(func: CallableJson, args_kwargs_s: List[Dict[str, Any]], redis_master: object = None, workers: object = None, check_interval=2, delete_redis_result=True) -> List[TypeJson]:
    if redis_master is None:
        redis_master = get_redis_master('127.0.0.1', 6379)
    if workers is None:
        workers = os.cpu_count() - 2

    rtqm_for_task = RedisTaskQueueManager(fctn=func, redis_master=redis_master)

    rtqm_for_task.reset()
    rtqm_for_task.push_waiting(args_kwargs_s)

    fname_caller = get_calling_filename(offset=3)
    fpath_caller = os.path.realpath(fname_caller)

    # print(f"[fctn] {func.__name__}")
    # print(f"[fpath] {fpath_caller}")

    sent_code_including_function = extract_code_including_function(func.__name__, fpath_caller)
    # print("##### make gatling script #####")
    # print(sent_code_including_function)

    sent_code_full_script = f"""
{sent_code_including_function}    

import os
import redis
import argparse
from databasetool.redis_z_taskqueuemanager import RedisTaskQueueManager


redis_kwargs = {redis_master.connection_pool.connection_kwargs}
redis_master = redis.Redis(**redis_kwargs)
rtqm_for_task = RedisTaskQueueManager(fctn={func.__name__}, redis_master=redis_master)

if __name__ == '__main__':
    rtqm_for_task.batch_execute()
        
"""

    fpath_caller_gatling = rename_fname(fpath_caller, lambda fname: fname.replace('.py', '.gatling.py'))
    save_text(sent_code_full_script, fpath_caller_gatling)

    for i in range(workers):
        if rtqm_for_task.get_len_waiting():
            run_python_script(fpath_caller_gatling, block=False, show_window=False)

    get_pids_fctn = lambda: get_pids_by_cmd(fpath_caller_gatling)
    rtqm_for_task.check_done_block(check_interval, get_pids_fctn=get_pids_fctn)

    pids = get_pids_fctn()
    for pid in pids:
        kill_process(pid, print_log=False)

    delete_text(fpath_caller_gatling)

    res = rtqm_for_task.fetch_result()

    if delete_redis_result:
        res.delete()
    return res
