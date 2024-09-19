#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
#set -e

function pathmunge () {
    case ":${PATH}:" in
        *:"$1":*)
            ;;
        *)
            if [ "$2" = "after" ] ; then
                PATH=$PATH:$1
            else
                PATH=$1:$PATH
            fi
    esac
}

function inject_path() {
  if [ "$EUID" = "0" ]; then
    pathmunge /usr/sbin
    pathmunge /usr/local/sbin
  else
    pathmunge /usr/local/sbin after
    pathmunge /usr/sbin after
  fi
}

function check_path() {
  path=$1
  if [[ "$path" =~ ^/([a-zA-Z0-9_-]+/?)*$ && ! "$op_lib_path" =~ // && ! "$op_lib_path" =~ (\./|\.\.) ]]; then
    echo "path check correct"
  else
    echo "check path format is not correct"
    exit 1
  fi
}

function log_info_echo() {
  local log_line="[$(date '+%Y-%m-%d %H:%M:%S')][INFO] [$(whoami)] : [${software_name}-deploy] ${1}"
  echo "${log_line}"
  echo "${log_line}" >> "${log_file}"
}

function log_err_echo() {
  local log_line="[$(date '+%Y-%m-%d %H:%M:%S')][ERROR] [$(whoami)] : [${software_name}-deploy] ${1}"
  echo "${log_line}"
  echo "${log_line}" >> "${log_file}"
}

function main()
{
  source /etc/profile
  local cur_dir=$(cd $(dirname ${0});pwd)
  local parent_dir=$(dirname ${cur_dir})
  local software_name="loadsmetric"

  local install_type="${1}"

  local log_path=${parent_dir}/${software_name}-${install_type}/log
  local log_file=${log_path}/loadsmetric.log

  for k in $( seq 1 6 )
  do
    # get service status
    status_info=$(sh "${parent_dir}/${software_name}-${install_type}/bin/launcher" "status")

    # start if the service is not running
    if [ "${status_info}" != "Running" ]; then
      # start service
      sh "${parent_dir}/${software_name}-${install_type}/bin/launcher" start
      # check and record result
      if [ $? -eq 0 ]; then
        log_info_echo "LoadsMetric started successfully by daemon"
      else
        log_err_echo "LoadsMetric started unsuccessfully by daemon"
      fi
    fi

    # sleep 9s then check again
    sleep 9s
  done
}

main "$@"