# -*- coding: utf-8 -*-
# Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
"""
"""
import os
import compileall
import shutil

# 要编译的文件夹的名称
TO_COMPLIES = ['src']
# 要保留的文件夹的名称
TO_RETAINS = ['config']
# 编译文件的输出路径
OUTPUT_DIR_NAME = 'BoostKit-omniadvisor_2.0'
# 本文件的路径
BASE_PATH = os.path.dirname(os.path.abspath(__file__))


def clean_pyc_files(project_dir):
    """删除旧的 .pyc 文件"""
    for root, _, files in os.walk(project_dir):
        for file in files:
            if file.endswith(".pyc"):
                pyc_path = os.path.join(root, file)
                os.remove(pyc_path)


def compile_python_files(project_dir):
    """编译所有 .py 文件为 .pyc"""
    compileall.compile_dir(project_dir, force=True, quiet=1)


def replace_pyc_files(project_dir):
    """将 __pycache__ 中的新 .pyc 移动到对应位置，替换旧的 .pyc"""
    for root, dirs, _ in os.walk(project_dir):  # 深度 1
        if "__pycache__" not in dirs:  # 提前跳过不相关的目录
            continue  # 减少一层缩进

        pycache_path = os.path.join(root, "__pycache__")
        for file in os.listdir(pycache_path):  # 深度 2
            if not file.endswith(".pyc"):  # 跳过非 .pyc 文件
                continue  # 减少一层缩进

            # 目标 .py 文件名
            module_name = file.partition(".")[0]  # 更安全的方式提取模块名
            if not module_name:  # 避免空模块名
                continue  # 减少一层缩进

            target_file = os.path.join(root, f"{module_name}.pyc")
            source_pyc = os.path.join(pycache_path, file)
            shutil.move(source_pyc, target_file)  # 深度 2

        # 可选：删除空 __pycache__ 目录
        if not os.listdir(pycache_path):  # 深度 2
            os.rmdir(pycache_path)  # 深度 3


def remove_source_files(project_dir):
    # 遍历文件夹
    for root, _, files in os.walk(project_dir):
        for file in files:
            # 检查文件扩展名是否为 .py
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                os.remove(file_path)


def rebuild_pyc_files(project_dir):
    clean_pyc_files(project_dir)
    compile_python_files(project_dir)
    replace_pyc_files(project_dir)
    remove_source_files(project_dir)


if __name__ == "__main__":
    # 将此处替换为你的项目路径
    output_path = os.path.join(BASE_PATH, OUTPUT_DIR_NAME)

    # 删除整个文件夹及其内容
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # 创建文件夹
    os.makedirs(output_path)

    # copy 需要编译的目录
    for name in TO_RETAINS + TO_COMPLIES:
        shutil.copytree(os.path.join(BASE_PATH, name), os.path.join(output_path, name))

    rebuild_pyc_files(os.path.join(output_path, TO_COMPLIES[0]))
