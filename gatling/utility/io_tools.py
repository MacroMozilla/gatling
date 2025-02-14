import json
import os
import pickle
import shutil
from typing import Any, List, Union
import numpy as np
import pandas as pd


def save_jsonl(data: List[Any], filename: str) -> None:
    """
    将数据保存为JSONL格式的文件。

    :param data: 要保存的数据，列表中的每个元素都应该是可序列化为JSON的对象。
    :param filename: 保存文件的名称。
    """
    with open(filename, 'w', encoding='utf-8') as file:
        for entry in data:
            json_line = json.dumps(entry, ensure_ascii=False)  # 将对象转换为JSON字符串
            file.write(json_line + '\n')  # 写入文件，每个对象后换行


def read_jsonl(filename: str) -> List[Any]:
    """
    从JSONL格式的文件中读取数据。

    :param filename: JSONL文件的名称。
    :return: 包含文件中所有JSON对象的列表。
    """
    data = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            data.append(json.loads(line.strip()))  # 读取每行并转换为Python对象
    return data


def save_json(data: Any, filename: str) -> None:
    """
    将数据保存为JSON格式的文件。

    :param data: 要保存的数据，应该是可序列化为JSON的对象。
    :param filename: 保存文件的名称。
    """
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)  # 将对象转换为JSON并保存到文件


def read_json(filename: str) -> Any:
    """
    从JSON格式的文件中加载数据。

    :param filename: JSON文件的名称。
    :return: 文件中的JSON对象。
    """
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)  # 读取并转换整个文件的JSON内容


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


def save_numpy(data: np.ndarray, filename: str) -> None:
    """
    将NumPy数组保存到文件中（.npy格式）。

    :param data: 要保存的NumPy数组。
    :param filename: 保存文件的名称。
    """
    np.save(filename, data)


def delete_text(filename: str) -> bool:
    """
    删除指定的文本文件。
    如果文件存在，则删除文件并打印提示信息，返回 True；
    如果文件不存在，则打印提示信息，返回 False。

    :param filename: 要删除的文件名。
    :return: 删除成功返回 True，文件不存在返回 False。
    """
    try:
        os.remove(filename)
        # print(f"文件已删除: {filename}")
        return True
    except FileNotFoundError:
        print(f"文件不存在: {filename}")
        return False


def read_numpy(filename: str) -> np.ndarray:
    """
    从文件中读取NumPy数组（.npy格式）。

    :param filename: 文件的名称。
    :return: 读取的NumPy数组。
    """
    return np.load(filename)


def save_feather(data: pd.DataFrame, filename: str) -> None:
    """
    将Pandas DataFrame保存为Feather格式。

    :param data: 要保存的Pandas DataFrame。
    :param filename: 保存文件的名称。
    """
    data.to_feather(filename)


def read_feather(filename: str) -> pd.DataFrame:
    """
    从Feather文件中读取Pandas DataFrame。

    :param filename: Feather文件的名称。
    :return: 读取的Pandas DataFrame。
    """
    return pd.read_feather(filename)


def save_pickle(data: Any, filename: str) -> None:
    """
    将数据保存为Pickle格式。

    :param data: 要保存的Python对象。
    :param filename: 保存文件的名称。
    """
    with open(filename, 'wb') as file:
        pickle.dump(data, file)


def read_pickle(filename: str) -> Any:
    """
    从Pickle文件中加载数据。

    :param filename: Pickle文件的名称。
    :return: 文件中的Python对象。
    """
    with open(filename, 'rb') as file:
        return pickle.load(file)


def rmdir(fpath):
    """删除目录（如果存在）"""
    if os.path.exists(fpath):
        shutil.rmtree(fpath)


def rmfile(fname):
    """删除文件或目录"""
    if os.path.exists(fname):
        os.remove(fname)  # 删除文件
