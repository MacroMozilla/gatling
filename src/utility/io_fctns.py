import json
import os
import pickle
import tomllib
import traceback
from typing import Any, List


def save_jsonl(data: List[Any], filename: str, mode='w') -> None:
    with open(filename, mode, encoding='utf-8') as file:
        for entry in data:
            json_line = json.dumps(entry, ensure_ascii=False)
            file.write(json_line + '\n')


def read_jsonl(filename: str) -> List[Any]:
    data = []
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError:
                    print(traceback.format_exc())
                    print(f"[Error] JSONDecodeError: {[line]}")
                    print(f"[Error] filename = {[filename]}")

    return data


def save_json(data: Any, filename: str) -> None:
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def read_json(filename: str) -> Any:
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)


def save_text(data: str, filename: str, mode='w') -> None:
    with open(filename, mode, encoding='utf-8') as file:
        file.write(data)


def read_text(filename: str) -> str:
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()


def save_pickle(data: Any, filename: str) -> None:
    with open(filename, 'wb') as file:
        pickle.dump(data, file)


def read_pickle(filename: str) -> Any:
    with open(filename, 'rb') as file:
        return pickle.load(file)


def save_bytes(data: bytes, filename: str, mode: str = 'wb') -> None:
    with open(filename, mode) as file:
        file.write(data)


def read_bytes(filename: str, mode: str = 'rb') -> bytes:
    with open(filename, mode) as file:
        return file.read()


def read_toml(file_path: str) -> dict:
    with open(file_path, "rb") as f:
        return tomllib.load(f)


def remove_file(fname):
    if os.path.exists(fname):
        os.remove(fname)
