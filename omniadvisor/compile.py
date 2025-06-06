# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
"""
"""
import fnmatch
import os
import shutil
import subprocess

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# 与src同级的其他资源文件夹
RESOURCES_LIST = ['config']

# 要编译的源文件夹的名称
COMPILE_FROM_NAME = 'src'

# 要编译的源码目录，一般就是 src
SOURCE_DIR = os.path.join(PROJECT_DIR, COMPILE_FROM_NAME)

# 编译后输出的根文件夹名称
COMPILE_DEST_ROOT_NAME = 'BoostKit-omniadvisor_2.0'

# 编译后输出的根目录
OUTPUT_ROOT_DIR = os.path.join(PROJECT_DIR, COMPILE_DEST_ROOT_NAME)

# 编译后输出的目录
OUTPUT_SRC_DIR = os.path.join(OUTPUT_ROOT_DIR, COMPILE_FROM_NAME)

# 原样保留的文件
RETAIN_FILES = ['*.so', '*.json', '*.cfg', '*.pyc']

# 这个是产生的临时的编译目录
TEMP_BUILD_DIR = os.path.join(OUTPUT_SRC_DIR, 'build')


def delete_files_recursively(root_dir, patterns):
    """
    递归删除指定目录下符合特定模式的文件

    :param root_dir: 要搜索的根目录
    :param patterns: 要删除的文件模式列表
    """
    # 保留必要文件，其他删除
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            # 检查文件名是否匹配任一模式
            if (filename in patterns or
                    any(fnmatch.fnmatch(filename, pattern) for pattern in patterns)):
                continue
            try:
                os.remove(filepath)
            except OSError as e:
                raise RuntimeError(f"删除 {filepath} 失败 ") from e

    # 重命名，默认是 utils.cpython-38-aarch64-linux-gnu.so，即 模块名.xxx.so
    for root, _, files in os.walk(root_dir):
        for filename in files:
            if not filename.endswith('.so'):
                continue
            parts = filename.split('.')
            if len(parts) >= 3:  # 确保文件名符合 xxx.yyy.so 格式
                new_name = f"{parts[0]}.so"  # 取第一个和最后一个部分

                old_path = os.path.join(root, filename)
                new_path = os.path.join(root, new_name)

                os.rename(old_path, new_path)

    # 清理build目录
    if os.path.exists(TEMP_BUILD_DIR):
        shutil.rmtree(TEMP_BUILD_DIR)


if __name__ == '__main__':
    if os.path.exists(OUTPUT_ROOT_DIR):
        shutil.rmtree(OUTPUT_ROOT_DIR)

    shutil.copytree(SOURCE_DIR, OUTPUT_SRC_DIR)
    for resource in RESOURCES_LIST:
        if os.path.exists(os.path.join(PROJECT_DIR, resource)):
            shutil.copytree(os.path.join(PROJECT_DIR, resource), os.path.join(OUTPUT_ROOT_DIR, resource))

    # 进入 build_output 目录
    original_dir = os.getcwd()
    os.chdir(OUTPUT_SRC_DIR)

    arg_list = ["python3", "setup.py", "build_ext", "--inplace"]

    try:
        # 这里可以根据你的实际打包命令进行修改
        subprocess.run(arg_list, check=True)

    finally:
        # 返回原始目录
        os.chdir(original_dir)

    # 删除build_output 目录下所有无用文件
    delete_files_recursively(OUTPUT_SRC_DIR, RETAIN_FILES)
