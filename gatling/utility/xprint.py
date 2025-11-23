import pickle
import types

from tqdm import tqdm


def print_box(box):
    for i, item in enumerate(box):
        print(i, item)


def print_flush(*args, **kwargs):
    print(*args, flush=True, **kwargs)


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
        if name.startswith("__"):  # 忽略内置
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
