import pickle
import types

from tqdm import tqdm
from icecream import ic as xprint_ice


def xprint_rows(box):
    for i, item in enumerate(box):
        print(i, item)


def xprint_k2v(k2v):
    for k, v in k2v.items():
        print(k, v)


def xprint_flush(*args, **kwargs):
    print(*args, flush=True, **kwargs)


def xprint_none(*args, **kwargs):
    return


def check_picklable(target):
    try:
        pickle.dumps(target)
        return True
    except Exception as e:
        print(f"[Pickle ERROR] {target.__name__=} {target} = {type(target)}  ->  {e}")
        return False


def check_globals_pickable():
    print("🔍 Scanning all globals for picklability...")
    problems = []

    for name, obj in tqdm(globals().items()):
        if name.startswith("__"):
            continue
        if isinstance(obj, types.ModuleType):
            continue
        if not check_picklable(obj):
            problems.append((name, type(obj).__name__))

    if problems:
        print("⚠️ Non-picklable globals found:")
        for name, typename in problems:
            print(f"  - {name}: {typename}")
    else:
        print("✅ All globals are picklable.")


if __name__ == '__main__':
    pass
    check_globals_pickable()

    x = 1
    xprint_ice.configureOutput(includeContext=True)
    xprint_ice(x)
