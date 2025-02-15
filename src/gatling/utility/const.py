from typing import TypeVar, Union, List, Dict, Protocol, Callable

K_args = 'args'
K_kwargs = 'kwargs'
K_uid = 'uid'
K_result = 'r'

TypeR = TypeVar('R')

TypeJson = Union[
    None,
    bool,
    int,
    float,
    str,
    List["TypeJson"],
    Dict[str, "TypeJson"]
]

TypeJsonR = TypeVar("TypeJsonR", bound=TypeJson)

CallableJson = Callable[..., TypeJson]
