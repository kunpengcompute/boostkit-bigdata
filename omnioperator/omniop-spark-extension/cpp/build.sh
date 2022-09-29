#!/bin/bash
#  Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

set -e

if [ -z "$OMNI_HOME" ]; then
  echo "OMNI_HOME is empty"
  OMNI_HOME=/opt
fi

export OMNI_INCLUDE_PATH=$OMNI_HOME/lib/include
export CPLUS_INCLUDE_PATH=$OMNI_INCLUDE_PATH:$CPLUS_INCLUDE_PATH
echo "OMNI_INCLUDE_PATH=$OMNI_INCLUDE_PATH"

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR
cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build

# options
if [ $# != 0 ] ; then
  options=""
  if [ $1 = 'debug' ]; then
    echo "-- Enable Debug"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON"
  elif [ $1 = 'trace' ]; then
    echo "-- Enable Trace"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DTRACE_RUNTIME=ON"
  elif [ $1 = 'release' ];then
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  elif [ $1 = 'test' ];then
    echo "-- Enable Test"
    options="$options -DCMAKE_BUILD_TYPE=Test -DBUILD_CPP_TESTS=TRUE"
  elif [ $1 = 'coverage' ]; then
    echo "-- Enable Coverage"
    options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON -DCOVERAGE=ON"
  else
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  fi
  cmake .. $options
else
  echo "-- Enable Release"
  cmake .. -DCMAKE_BUILD_TYPE=Release
fi

make -j5

if [ $# != 0 ] ; then
  if [ $1 = 'coverage' ]; then
    ./test/tptest --gtest_output=xml:test_detail.xml
    lcov --d ../ --c --output-file test.info --rc lcov_branch_coverage=1
    lcov --remove test.info '*/opt/buildtools/include/*' '*/usr/include/*' '*/usr/lib/*' '*/usr/lib64/*' '*/usr/local/include/*' '*/usr/local/lib/*' '*/usr/local/lib64/*' -o final.info --rc lcov_branch_coverage=1
    genhtml final.info -o test_coverage --branch-coverage --rc lcov_branch_coverage=1
  fi
fi

set +eu
