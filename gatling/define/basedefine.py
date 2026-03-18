from enum import Enum, EnumMeta
from typing import Any


class _BaseDefineMeta(EnumMeta):

    def __contains__(cls, item):
        if isinstance(item, str):
            return item in cls.__members__
        return super().__contains__(item)

    def __getitem__(cls, name):
        return super().__getitem__(name).value


class BaseDefine(Enum, metaclass=_BaseDefineMeta):

    def __str__(self) -> str:
        return self.name

    @classmethod
    def keys(cls) -> list[str]:
        return [m.name for m in cls]

    @classmethod
    def items(cls) -> list[tuple[str, Any]]:
        return [(m.name, m.value) for m in cls]

    @classmethod
    def get(cls, name: str, default: Any = None):
        if name in cls.__members__:
            return cls[name]
        return default
