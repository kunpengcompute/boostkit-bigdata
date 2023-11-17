#!/usr/bin/env bash
#stop tee
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ ! -f "${SPARK_HOME}/trustzone/master/pid.log" ]; then
  echo "pid.log does not exist. "
  exit
fi
PID=$(head -n 1 ${SPARK_HOME}/trustzone/master/pid.log)
echo "tee pid is ${PID}"

NOWPID=$(ps -ef|grep ${PID}|grep -v grep|awk '{print $2}')

if [ -z "${NOWPID}" ]; then
  echo "The ${PID} process does not exist."
else
  kill -9 ${PID}
  rm -rf ${SPARK_HOME}/trustzone/master/pid.log
  echo "tee_teleport -ei ${SPARK_HOME}/trustzone/master/sessionID.txt"
  sleep 3
  tee_teleport -ei ${SPARK_HOME}/trustzone/master/sessionID.txt
fi
