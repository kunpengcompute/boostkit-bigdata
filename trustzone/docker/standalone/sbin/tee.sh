#!/bin/bash

function chcek_param {
  PARAM=$1
  case "$PARAM" in
        -cp)
                return 3;;
        -Xmx*)
                return 1;;
        org.apache.spark.deploy.worker.Worker)
          P_TYPE='worker'
                return 2;;
        org.apache.spark.deploy.master.Master)
          P_TYPE='master'
                return 2;;
        *)
                return;;
  esac
}

TEECMD='tee_teleport'
P_TYPE=''
P_PAEAM=''
P_MAIN_CALSS=''
RET=0
for i in ${*:2} ; do
if [ "$RET" = "3" ]; then
  P_PAEAM="-cp jars/*:jars/spark-core_2.12-3.1.1.jar "$P_PAEAM;
  RET=0
else
  chcek_param $i
  RET=$?
  case "$RET" in
        0)
                P_PAEAM=$P_PAEAM" "$i ;;
        1)
                echo "del param $i" ;;
        2)
                P_MAIN_CALSS="-r "$i".class";;
        *)
                 ;;
  esac
fi
done

if [ -f "${SPARK_HOME}/trustzone/${P_TYPE}/pid.log" ]; then
  echo "pid.log exists. Check whether the ${P_TYPE} is started."
  exit
fi

cd ${SPARK_HOME}/trustzone/${P_TYPE}
tee_teleport -c ${SPARK_HOME}/trustzone/sec/spconf.sec
tee_teleport -n ${SPARK_HOME}/trustzone/sec/jars.sec -t code -i sessionID.txt
echo "put spark sec success"

#create log dir
mkdir -p /home/spark/log/

#todo read env from conf
P_ENV='sep=:;'
if [ "$P_TYPE" = "master" ]; then
  P_ENV=$P_ENV'SPARK_HOME=.:OS=TEE:SPARK_DAEMON_MEMORY=10g'
else
  P_ENV=$P_ENV'SPARK_SCALA_VERSION=2.12;SPARK_HOME=.:OS=TEE:SPARK_USER=root:SPARK_WORKER_CORES1=8:SPARK_WORKER_DIR=/tmp/:SPARK_REE_LOG_DIR=/home/spark/log/'
fi

echo "cmd for tee os:"
echo "$TEECMD $P_MAIN_CALSS  -i sessionID.txt -p  \"$P_PAEAM\" --env \"$P_ENV\""
nohup $TEECMD $P_MAIN_CALSS  -i sessionID.txt -p  "$P_PAEAM" --env "$P_ENV" > ${SPARK_HOME}/trustzone/${P_TYPE}/ree.log 2>&1 & echo $! > ${SPARK_HOME}/trustzone/${P_TYPE}/pid.log
