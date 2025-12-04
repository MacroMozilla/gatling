import os
import tomllib
import traceback
from typing import Any
import orjson
import dill
import zstandard as zstd


def read_jsonl(filename: str) -> list:
    try:
        with open(filename, 'rb') as f:

            data = []
            for i, line in enumerate(f, 1):
                if line:
                    try:
                        data.append(orjson.loads(line))
                    except Exception as e:
                        print(f"[Error] Line {i}: {e}")
                        print(f"[Error] Content: {line[:200]}")
                        print(f"[Error] File: {filename}")
            return data

    except Exception as e:
        print(f"[Error] Failed to read: {filename}")
        print(f"[Error] {e}")
        print(traceback.format_exc())
        return []


def save_jsonl(data: list, filename: str) -> bool:
    try:
        with open(filename, 'wb') as f:
            f.write(b'\n'.join(map(orjson.dumps, data)))
        return True
    except Exception as e:
        print(f"[Error] Failed to save: {filename}")
        print(f"[Error] {e}")
        print(f"[Error] Data length: {len(data)}")
        print(traceback.format_exc())
        return False


def read_json(filename: str) -> any:
    try:
        with open(filename, 'rb') as f:
            content = f.read()
            return orjson.loads(content)
    except orjson.JSONDecodeError as e:
        print(f"[Error] Failed to parse: {filename}")
        print(f"[Error] {e}")
        print(f"[Error] Content: {content[:500]}")
        print(traceback.format_exc())
        return None
    except Exception as e:
        print(f"[Error] Failed to read: {filename}")
        print(f"[Error] {e}")
        print(traceback.format_exc())
        return None


def save_json(data: any, filename: str, indent: bool = True) -> bool:
    try:
        with open(filename, 'wb') as f:
            option = orjson.OPT_INDENT_2 if indent else 0
            f.write(orjson.dumps(data, option=option))
        return True
    except Exception as e:
        print(f"[Error] Failed to save: {filename}")
        print(f"[Error] {e}")
        print(f"[Error] Data: {str(data)[:500]}")
        print(traceback.format_exc())
        return False


def save_text(data: str, filename: str, mode='w') -> None:
    with open(filename, mode, encoding='utf-8') as file:
        file.write(data)


def read_text(filename: str) -> str:
    with open(filename, 'r', encoding='utf-8') as file:
        return file.read()


def save_pickle(data: Any, filename: str, level: int = 3) -> None:
    """
    Save Python object with dill and Zstandard compression.

    Zstandard compression level:
        - 1–3  : Fast compression speed, lower compression ratio
        - 4–6  : Balanced performance (recommended)
        - 7–12 : Higher compression ratio, slower speed
        - >12  : Maximum compression, very slow (for archiving)
    """
    cctx = zstd.ZstdCompressor(level=level)
    with open(filename, "wb") as f:
        with cctx.stream_writer(f) as compressor:
            dill.dump(data, compressor)


def read_pickle(filename: str) -> Any:
    """Load Python object saved with dill and Zstandard compression."""
    try:
        dctx = zstd.ZstdDecompressor()
        with open(filename, "rb") as f:
            with dctx.stream_reader(f) as reader:
                return dill.load(reader)
    except Exception as e:
        raise RuntimeError(
            f"Error reading pickle file: {e}\n"
            "Please upgrade to the latest version of gatling:\n"
            "  pip install --upgrade gatling-py\n"
            "Then re-save the pickle file with save_pickle() and try again."
        ) from e

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
