# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
"""
"""
import os
import py_compile
import sysconfig

from Cython.Build import cythonize
from setuptools import setup, Extension, find_packages

# 要编译成 .pyc的文件（几个入口文件）
pyc_files = ['__init__.py', 'init.py', 'hijack.py', 'tuning.py', 'version.py']

sysconfig.get_config_vars()['CCSHARED'] = '-O2 -pipe -Wall -Wtrampolines -D_FORTIFY_SOURCE=2 -O2 -fPIC ' \
                                          '-finline-functions -fstack-protector-strong ' \
                                          '-s -Wl,-z,noexecstack -Wl,-z,relro,-z,now ' \
                                          '-ftrapv -fstack-check'


# 1 需要编译的文件
def find_py_files(root):
    py_files = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if not f.endswith(".py") or f == 'setup.py':
                continue
            if f in pyc_files:
                # 编译成 .pyc，并删除原 .py文件
                py_path = os.path.join(dirpath, f)
                # 编译生成.pyc文件
                py_compile.compile(py_path, cfile=py_path + 'c',
                                   invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH)
                # 删除原.py文件
                os.remove(py_path)
                continue
            py_files.append(os.path.join(dirpath, f))
    return py_files


# 配置扩展模块
extensions = []
for py_file in find_py_files("."):
    # 将路径转换为模块名，如 iterative.config_space.module1
    module_name = py_file[:-3].replace('./', '').replace(os.path.sep, ".")

    extensions.append(
        Extension(
            module_name,
            [py_file],
            extra_compile_args=[],  # 优化选项
        )
    )

# 编译配置
setup(
    name="show demo",
    # 自动发现所有包（包含 __init__.py 的目录）
    packages=find_packages(include=['.*']),
    # 包含所有JSON文件
    ext_modules=cythonize(
        extensions,
        # 使用 Python 3；并禁用类型检查，以避免不必要的报错 重要！勿删！
        compiler_directives={"language_level": "3", "annotation_typing": False},
        build_dir="build",
    ),
    script_args=["build_ext", "--inplace"],  # 编译到源码目录
)