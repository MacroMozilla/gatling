from setuptools import setup, find_packages
from z_setup.a_const import DESCRIPTION, PACKAGE_NAME
from z_setup.c_check_version import *
from z_setup.b_tools import read_text, rmdir, rmdir_startswith


new_version = gen_next_version(get_remote_version(PACKAGE_NAME))
print(f"🔹 New package version: {new_version}")

DESCRIPTION_LONG = read_text('README.md')

REQUIREMENTS = [line.strip() for line in read_text(f"src/{PACKAGE_NAME}/requirements.txt").split() if line.strip() and not line.startswith("#")]

rmdir('dist')

setup(
    name=PACKAGE_NAME,
    version=new_version,  # 使用最新版本号
    description=DESCRIPTION,
    long_description=DESCRIPTION_LONG,
    long_description_content_type="text/markdown",
    include_package_data=True,
    author="MacroMozilla",
    author_email="honyzeng7@gmail.com",
    license="BSD-3-Clause",
    packages=find_packages(where="src"),
    options={"egg_info": {"egg_base": "build"}},
    package_dir={"": "src"},
    install_requires=REQUIREMENTS,
    python_requires=">=3.10",
    url=f"https://github.com/MacroMozilla/{PACKAGE_NAME}",
    project_urls={
        "Homepage": f"https://github.com/MacroMozilla/{PACKAGE_NAME}",
        "Documentation": f"https://github.com/MacroMozilla/{PACKAGE_NAME}/wiki",
        "Source": f"https://github.com/MacroMozilla/{PACKAGE_NAME}",
    },
)

rmdir_startswith(f"{PACKAGE_NAME}-")
