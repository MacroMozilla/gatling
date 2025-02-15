import os
import shutil
import time


def save_text(data: str, filename: str) -> None:
    """
    将字符串数据保存到文本文件中。

    :param data: 要保存的字符串数据。
    :param filename: 保存文件的名称。
    """
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(data)  # 将字符串写入文件


def read_text(filename: str) -> str:
    """
    从文本文件中读取字符串数据。

    :param filename: 文本文件的名称。
    :return: 文件内容的字符串。
    """
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()  # 读取整个文件内容并返回


def rmdir(path, retries=5, wait_time=2):
    """
    删除目录（等待文件解除占用后重试）

    :param path: 要删除的目录路径
    :param retries: 最大重试次数
    :param wait_time: 每次重试之间的等待时间（秒）
    """
    if not os.path.exists(path):
        print(f"⚠️  Directory does not exist: {path}")
        return

    for attempt in range(retries):
        try:
            shutil.rmtree(path)
            print(f"✅ Successfully deleted: {path}")
            return
        except PermissionError:
            print(f"⚠️  Delete failed (attempt {attempt + 1}/{retries}), retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print(f"❌ Failed to delete {path}, it might be locked!")


def rmdir_startswith(prefix, base_dir=None):
    """
    删除当前目录下所有以 `prefix` 开头的子目录（使用 rmdir() 处理占用问题）

    :param prefix: 要删除的目录名前缀
    :param base_dir: 目录路径（默认当前目录）
    """
    if base_dir is None:
        base_dir = os.getcwd()  # 默认使用当前目录

    for item in os.listdir(base_dir):
        dir_path = os.path.join(base_dir, item)

        if os.path.isdir(dir_path) and item.startswith(prefix):
            print(f"🔄 Attempting to delete: {dir_path}")
            rmdir(dir_path)  # 直接调用 rmdir() 处理删除逻辑

if __name__ == '__main__':
    pass

