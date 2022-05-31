#!/bin/bash
set -eu

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
  else
    echo "-- Enable Release"
    options="$options -DCMAKE_BUILD_TYPE=Release"
  fi
  cmake .. $options
else
  echo "-- Enable Release"
  cmake .. -DCMAKE_BUILD_TYPE=Release
fi

make

set +eu