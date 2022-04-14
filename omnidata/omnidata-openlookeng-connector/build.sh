#!/usr/bin/env bash
# Building omniData openLooKeng Connector packages
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.

set -e

cpu_name=$(lscpu | grep Architecture | awk '{print $2}')
mvn -T12 clean install -Dos.detected.arch="${cpu_name}" 

