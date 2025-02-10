#!/bin/bash
# CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
set -e

cur_dir=$(cd $(dirname ${0});pwd)
parent_dir=$(dirname ${cur_dir})

logbackconfig=${parent_dir}/etc/logback.xml

logpath=$(readlink -e "${parent_dir}/log")

if test ! -f ${logpath}/loadsmetric.log
then
  touch ${logpath}/loadsmetric.log
  chomd 600 ${logpath}/loadsmetric.log
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')][INFO] [Deploy]: ${1}"
echo "[$(date '+%Y-%m-%d %H:%M:%S')][INFO] [Deploy]: ${1}" >> ${logpath}/loadsmetric.log
