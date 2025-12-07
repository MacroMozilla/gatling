import traceback

import pkg_resources
import os

packages = []
for pkg in pkg_resources.working_set:
    try:
        loc = pkg.location
        pkg_path = os.path.join(loc, pkg.key.replace('-', '_'))
        if not os.path.isdir(pkg_path):
            pkg_path = os.path.join(loc, pkg.key)
        if not os.path.isdir(pkg_path):
            pkg_path = os.path.join(loc, pkg.project_name.replace('-', '_'))
        if not os.path.isdir(pkg_path):
            pkg_path = os.path.join(loc, pkg.project_name)

        if os.path.isdir(pkg_path):
            size = sum(os.path.getsize(os.path.join(dp, f))
                       for dp, dn, fn in os.walk(pkg_path) for f in fn)
            packages.append((pkg.project_name, size))
    except Exception as e:
        print(traceback.format_exc())
        print(e)
        pass

packages.sort(key=lambda x: -x[1])
total = sum(p[1] for p in packages)

print(f"{'Package':<35} {'Size':>12} {'Percent':>10}")
print('-' * 60)
for name, size in packages:
    if size > 1024 * 1024:
        s = f"{size / 1024 / 1024:.2f} MB"
    elif size > 1024:
        s = f"{size / 1024:.2f} KB"
    else:
        s = f"{size} B"
    pct = size / total * 100 if total else 0
    print(f"{name:<35} {s:>12} {pct:>9.2f}%")

print('-' * 60)
print(f"{'TOTAL':<35} {total / 1024 / 1024:>9.2f} MB")
