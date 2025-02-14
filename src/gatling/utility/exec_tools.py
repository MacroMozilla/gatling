import subprocess
import os
from typing import Optional

import psutil
import time
from utility.watch import watch_time
import sys


def execute_commands(cmds, block=True) -> Optional[subprocess.Popen]:
    """
    Executes a list of commands in a new terminal window and optionally blocks until they complete.
    Compatible with both Windows and Linux.

    Args:
        cmds (list of str): List of commands to execute.
        block (bool): If True, the function will block until the commands finish.
                      If False, the function will return immediately with the process object.

    Returns:
        subprocess.Popen: The created process.
    """
    try:
        # 拼接命令
        combined_cmds = " && ".join(cmds) if sys.platform == "win32" else " ; ".join(cmds)

        # 选择适合的 shell
        if sys.platform == "win32":
            full_command = f'start cmd /c "{combined_cmds}"'
            process = subprocess.Popen(full_command, shell=True)
        else:
            # 适用于不同 Linux 终端
            terminal_cmds = [
                ["x-terminal-emulator", "-e", "bash", "-c", combined_cmds],  # 适用于多数 Linux
                ["gnome-terminal", "--", "bash", "-c", combined_cmds],  # GNOME 终端
                ["konsole", "-e", "bash", "-c", combined_cmds],  # KDE 终端
            ]
            process = None
            for term in terminal_cmds:
                try:
                    process = subprocess.Popen(term)
                    break  # 成功启动后退出循环
                except FileNotFoundError:
                    continue  # 终端不存在，尝试下一个
            if process is None:
                raise EnvironmentError("No supported terminal emulator found on this Linux system.")

        # 处理输出
        linewidth = 16
        print(f"pid = {process.pid} , executing:")
        print("=" * linewidth)
        for cmd in cmds:
            print(cmd)
        print("=" * linewidth)

        if block:
            process.wait()  # 等待子进程完成

        return process
    except Exception as e:
        print(f"Error: {e}")
        return None


def run_python_script(script_path: str, script_args: str = "", block=True) -> Optional[subprocess.Popen]:
    """
    Executes a Python script in a new terminal window with an optional virtual environment activation.

    Args:
        script_path (str): Path to the Python script to execute.
        script_args (str): Arguments to pass to the Python script.
        block (bool): If True, the function will block until the script completes.
                      If False, the function will return immediately with the process object.

    Returns:
        subprocess.Popen: The created process.
    """
    try:
        # 从环境变量中获取虚拟环境路径（如果有）
        venv_dir = os.environ.get('VIRTUAL_ENV', '')

        # 构造激活虚拟环境和运行脚本的命令
        if venv_dir:
            if sys.platform == "win32":
                activate_cmd = f'{venv_dir}\\Scripts\\activate'
            else:
                activate_cmd = f'source {venv_dir}/bin/activate'
            cmds = [activate_cmd, f'python {script_path} {script_args}']
        else:
            cmds = [f'python {script_path} {script_args}']

        # 调用 execute_commands 执行命令
        process = execute_commands(cmds, block=block)

        return process
    except Exception as e:
        print(f"Error: {e}")
        return None


def kill_process(pid: int):
    """ 递归查找并终止所有子进程，并确保进程被完全杀死 """
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)  # 获取所有子进程

        # 先终止子进程
        for child in children:
            print(f"Killing child process: {child.pid}")
            child.terminate()

        # 确保所有子进程被杀死
        gone, alive = psutil.wait_procs(children, timeout=3)
        for child in alive:
            print(f"Force killing child process: {child.pid}")
            child.kill()  # 强制杀死未退出的子进程

        # 终止父进程
        print(f"Killing root process: {pid}")
        parent.terminate()

        # 确保父进程被杀死
        time.sleep(1)
        if psutil.pid_exists(pid):
            print(f"Force killing root process: {pid}")
            parent.kill()
    except psutil.NoSuchProcess:
        print(f"Process {pid} not found.")


@watch_time
def get_pids_by_cmd(script_name: str, argv: str = ""):
    """
    获取所有运行指定 .py 文件的进程 ID，如果提供参数，则匹配参数。

    Args:
        script_name (str): 目标 Python 脚本文件名，例如 "test.py"。
        argv (str, optional): 目标脚本的完整参数字符串，例如 "--arg1 value1 --arg2 value2"。默认不填，则匹配所有运行的脚本。

    Returns:
        list: 运行该脚本（或匹配参数）的进程 ID 列表。
    """
    # 打印查询信息
    print(f"查询进程: {script_name} {' ' + argv if argv else ''}")

    expected_args_list = argv.split() if argv else []  # 仅当 args 不为空时才拆分
    process_ids = []

    for process in psutil.process_iter(attrs=['pid', 'cmdline']):
        try:
            cmdline = process.info['cmdline']
            if cmdline and script_name in cmdline:
                script_index = cmdline.index(script_name)
                actual_args = cmdline[script_index + 1:]  # 获取脚本后的参数

                # 如果 args 为空，则匹配所有运行该脚本的进程
                if not expected_args_list or actual_args == expected_args_list:
                    process_ids.append(process.info['pid'])
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    # 打印查询结果
    print(f"匹配的进程数量: {len(process_ids)}")
    print(f"进程 ID: {process_ids}")

    return process_ids


if __name__ == '__main__':
    # 根据平台选择测试命令
    if sys.platform == "win32":
        test_cmd = "timeout /t 10"
    else:
        test_cmd = "sleep 10"
    proc = execute_commands([test_cmd], block=True)
    if proc:
        print(f"Test process PID: {proc.pid}")
