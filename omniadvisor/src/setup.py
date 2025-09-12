#!/usr/bin/env python

import distutils.sysconfig
import os
import py_compile
import sysconfig

from Cython.Build import cythonize
from setuptools import setup, Extension, find_packages

# 1 变量定义
# 要编译成 .pyc的文件（几个入口文件）
PYC_FILES = ['__init__.py', 'init.py', 'hijack.py', 'tuning.py', 'version.py', 'crypto.py']

# 编译参数
COMMON_COMPILE_ARGS = [
    # 语言标准
    "-std=c11",
    # 基础安全 + 警告
    "-Wall",
    "-Wextra",
    "-Werror=format",
    "-Wtrampolines",
    "-Wformat=2",
    "-Wstrict-prototypes",
    "-Wdate-time",
    "-Wfloat-equal",
    "-Wswitch-default",
    "-Wshadow",
    "-Wunused",
    "-Wundef",
    "-Wconversion",
    "-Wcast-qual",
    "-Wcast-align",
    "-Wvla",

    # C++ 虚函数安全（对 Cython 部分可能无效，但无害）
    "-Wnon-virtual-dtor",
    "-Wdelete-non-virtual-dtor",
    "-Woverloaded-virtual",

    # 优化 + 加固
    "-O2",
    "-fstack-protector-strong",
    "-ftrapv",
    "-fstack-check",
    "-finline-functions",
    "-fno-strict-aliasing",
    "-fno-common",
    "-freg-struct-return",
    "-fvisibility=hidden",
    "-fstrong-eval-order",
    "-fsigned-char",

    # 链接器安全加固
    "-Wl,-z,noexecstack",
    "-Wl,-z,relro,-z,now",

    # 编译器内部行为
    "-pipe",

    # 可能会报 warning 或 error 的两个
    "-Wstack-usage=8192",  # 或更大，单位为字节
    "-Wframe-larger-than=2048",  # 同上，GCC 默认限制堆栈帧大小
]

LINK_ARGS = [
    "-Wl,-Bsymbolic",
    "-rdynamic",
    "-Wl,-z,relro,-z,now",
    "-s"
]


# 2 函数定义
def find_py_files(root):
    py_files = []
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if not f.endswith(".py") or f == 'setup.py':
                continue
            if f in PYC_FILES:
                # 编译成 .pyc，并删除原 .py文件
                py_path = os.path.join(dirpath, f)
                # 编译生成.pyc文件
                py_compile.compile(py_path, cfile=f'{py_path}c',
                                   invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH)
                # 删除原.py文件
                os.remove(py_path)
                continue
            py_files.append(os.path.join(dirpath, f))
    return py_files


# 3 执行逻辑
# 说明下这段代码，若环境是python 3.9，则按 3.9进行编译；否则不加 -Wl,--no-undefined 这个参数，避免报错
# 有 -Wl,--no-undefined 和没加这个参数，生成的二进制文件在功能和行为上是一样的，只起到编译时校验依赖的作用，实际上编译后的代码会跑一下，
# 所以没区别。保证其他环境可用，减轻其他环境修改代码工作量。
if 'libpython3.9' in sysconfig.get_config_var('LDLIBRARY'):
    libdir_var = sysconfig.get_config_var('LIBDIR')
    LINK_ARGS.append("-Wl,--no-undefined")
    LINK_ARGS.append(f"-L{libdir_var}")
    LINK_ARGS.append("-lpython3.9")

# 清空环境变量
# 发现环境上的 CFLAGS只能通过这种形式去除，没有完全弄清原理，极可能是缓存的原因
cfg_vars = distutils.sysconfig.get_config_vars()
cfg_vars['CFLAGS'] = ''

# 置空，避免和环境上的冲突
sysconfig.get_config_vars()["LDFLAGS"] = ""

# 配置扩展模块
extensions = []
for py_file in find_py_files("."):
    # 将路径转换为模块名，如 iterative.config_space.module1
    module_name = py_file[:-3].replace('./', '').replace(os.path.sep, ".")

    extensions.append(
        Extension(
            module_name,
            [py_file],
            extra_compile_args=COMMON_COMPILE_ARGS,  # 优化选项
            extra_link_args=LINK_ARGS
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