#!/bin/bash
if [ -n "$1" ]; then
  package_name=$1
else
  package_name="BoostKit-omnimv_1.2.1"
fi

if [ ! -d "${package_name}" ]; then
  mkdir -p "${package_name}"
fi

\cp -rf config ${package_name}
\cp -rf src ${package_name}
\cp -f main.py omnimv_logging.py SPARK_README.md spark_task.py ${package_name}
chmod -R 750 ${package_name}