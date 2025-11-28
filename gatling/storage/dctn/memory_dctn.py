import json
import re
from pprint import pformat
from typing import Literal, Self, Callable

from gatling.storage.dctn.base_dctn import BaseDctn, K, V
from gatling.utility.xstr import dumps_hard, dumps_soft


class MemoryDctn(BaseDctn):
    def __init__(self):
        super().__init__()
        self._dctn = {}

    def clear(self):
        self._dctn.clear()

    def update(self, E=None, **F) -> int:
        if E is None:
            self._dctn.update(F)
            return len(F)

        if not F:
            if not isinstance(E, dict):
                E = dict(E)
            self._dctn.update(E)
            return len(E)

        merged = dict(E, **F)
        self._dctn.update(merged)
        return len(merged)

    def get(self, key, default=None):
        return self._dctn.get(key, default)

    def pop(self, key, default=None):
        return self._dctn.pop(key, default)

    def __getitem__(self, item):
        return self._dctn[item]

    def __setitem__(self, key, value):
        self._dctn[key] = value

    def __delitem__(self, key):
        del self._dctn[key]

    def keys(self):
        return self._dctn.keys()

    def values(self):
        return self._dctn.values()

    def items(self):
        return self._dctn.items()

    def __contains__(self, item) -> bool:
        return item in self._dctn

    def __len__(self) -> int:
        return len(self._dctn)

    def __eq__(self, other) -> bool:
        return self._dctn == dict(other)

    def __ne__(self, other) -> bool:
        return self._dctn != dict(other)

    def sort(self, by: Literal["key", "value"] | Callable[[K, V], any] = "value", reverse: bool = False) -> Self:
        key_func = lambda x: x
        if by == "key":
            key_func = lambda kv: kv[0]
        elif by == "value":
            key_func = lambda kv: kv[1]
        elif callable(by):
            key_func = lambda kv: by(kv[0], kv[1])
        else:
            raise ValueError(f"Invalid sortby: {by}")

        temp_items = self._dctn.items()
        temp_items = sorted(temp_items, key=key_func, reverse=reverse)
        self._dctn = dict(temp_items)
        return self

    # soft dump defaults (reasonable for terminal/logging)
    SOFT_MAX_ITEMS = 20  # collapse if > 20 items
    SOFT_MAX_SIZE = 1024  # collapse if > 4KB
    SOFT_LEVEL = 3  # default indent depth
    DEFAULT_INDENT = 2

    def str(self, level: int = -1, indent: int = DEFAULT_INDENT) -> str:
        """
        Hard dump: complete output, no collapse. support json
        Args:
            level: -1 (full indent), 0 (single line), >0 (indent to level N)
            indent: spaces per level
        """
        return dumps_hard(self._dctn, level=level, indent=indent)

    def __str__(self) -> str:
        """
        Soft dump: safe for large data, collapses big objects to (count)[size].
        Uses reasonable defaults for terminal/logging output.
        """
        return dumps_soft(
            self._dctn,
            level=self.SOFT_LEVEL,
            indent=self.DEFAULT_INDENT,
            max_items=self.SOFT_MAX_ITEMS,
            max_size=self.SOFT_MAX_SIZE
        )


if __name__ == '__main__':
    pass
