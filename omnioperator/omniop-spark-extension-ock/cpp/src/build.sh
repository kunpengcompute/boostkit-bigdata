#!/bin/bash
# ***********************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
# script for ock compiling
# version: 1.0.0
# change log:
# ***********************************************************************
set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR
cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build

BUILD_MODE=$1
# options
if [ $# != 0 ] ; then
  options=""
  if [ "${BUILD_MODE}" = 'debug' ]; then
    echo "-- Enable Debug"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON"
  elif [ "${BUILD_MODE}" = 'trace' ]; then
    echo "-- Enable Trace"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DTRACE_RUNTIME=ON"
  elif [ "${BUILD_MODE}" = 'release' ];then
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  elif [ "${BUILD_MODE}" = 'test' ];then
    echo "-- Enable Test"
    options="$options -DCMAKE_BUILD_TYPE=Test -DBUILD_CPP_TESTS=TRUE"
  else
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  fi
  cmake .. $options
else
  echo "-- Enable Release"
  cmake .. -DCMAKE_BUILD_TYPE=Release
fi

make -j 32

set +eu