#!/bin/sh
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

filetype=$1
filepath=$2
imahashpath=$3
if [ "$filetype" == "file" ]; then
  echo "file"
  sha256sum $filepath
elif [ "$filetype" == "sparkdir" ]; then
  echo "spark dir"
  nowpath=$(pwd)
  cp -r $filepath $nowpath/spjars
  cd $nowpath/spjars
  chmod 500 *.jar
  find . -name '*.jar' -print0 | sort -z | tar --null --no-acls --owner=0 --group=0 --no-same-permissions --mtime='1970-01-01 12:00:00 UTC' -cvf $nowpath/dir.tar --files-from=-
  cd -
  rm -rf $nowpath/spjars
  sha256sum dir.tar
  rm -rf dir.tar
elif [ "$filetype" == "yarndir" ]; then
  echo "hadop dir"
  nowpath=$(pwd)
  cp -r $filepath $nowpath/yarnjars
  cd $nowpath/yarnjars
  find . -name '*.jar' -print0 | sort -z | tar --null --no-acls --owner=0 --group=0 --no-same-permissions --mtime='1970-01-01 12:00:00 UTC' -cvf $nowpath/dir.tar --files-from=-
  cd -
  rm -rf $nowpath/yarnjars
  sha256sum dir.tar
  rm -rf dir.tar
else
    echo "unknow file type"
fi