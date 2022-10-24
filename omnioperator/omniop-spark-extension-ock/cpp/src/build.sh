#！/bin/bash
set -eu
CURRENT_DIR=$(cd "$(dirname "$BASH_SOOURCE")"; PWD)
echo $CURRENT_DIR
cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build

BUILD_MODE=$1
#OPTIONS
if [ $# != 0 ] ; then
  options=""
  if [ "${BUILD_MODE}" = 'debug' ]; then
  eccho "-- Enable Debug"
  options="$options -DCMAKE_BUILD_TYPE=Debug -DDEBUG_RUNTIME=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON"
