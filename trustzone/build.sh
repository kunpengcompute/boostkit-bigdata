#!/bin/bash

set -ux

work_dir=`pwd`
hook_dir=$work_dir
hook_lib_path=$hook_dir/libhook.so

build_hook_lib()
{
    cd $hook_dir
    rm -rf *.so

    gcc -shared -fPIC -o libhook.so hook.c -ldl

    if [ ! -f "libhook.so" ]; then
        echo "compile libhook.so failed"
        exit -1
    fi
}

run_example(){

    export LD_PRELOAD="$hook_lib_path"
    cd $hook_dir
    rm -rf posix.log
    #==========Write the code for the test here
    java -cp test.jar Start
    #==========
    unset LD_PRELOAD
}


build_hook_lib
run_example

exit 0
