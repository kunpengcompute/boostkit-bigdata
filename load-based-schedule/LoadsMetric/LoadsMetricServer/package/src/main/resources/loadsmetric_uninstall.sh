#!/bin/bash
# CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
set -e

function log_audit()
{
      if [ "${log_file}" == "" ]; then
      echo "[loadsmetric-uninstall] log_audit needs one parameter and log_file needs to be initialized."
      exit 1
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [$(whoami)] : [loadsmetric-uninstall] ${1}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [$(whoami)] : [loadsmetric-uninstall] ${1}" >> ${log_file}
}

function log_warn_echo() {
  local log_line="[$(date '+%Y-%m-%d %H:%M:%S')][WARN] [$(whoami)] : [${software_name}-deploy] ${1}"
  echo "${log_line}"
  echo "${log_line}" >> "${2}"
}

function stop_loadsmetric()
{
  cd ${parent_dir}/loadsmetric-${install_type}/bin
  if [[ -n $(readlink launcher) ]] ; then
    log_audit "The launcher has soft link risks"
    exit 1
  fi
  sh launcher kill || (log_audit "Failed to stop service" && exit 1)
}

function remove_loadsmetric()
{
  chmod -R 700 ${parent_dir}/loadsmetric-${install_type}
  chmod -R 700 ${parent_dir}/loadsmetric-${install_type}-setup
  if [ -d "${parent_dir}/loadsmetric-${install_type}" ] || [ -d "${parent_dir}/loadsmetric-${install_type}-setup" ];then
    if [ -d "${parent_dir}/loadsmetric-${install_type}" ];then
      echo "remove loadsmetric path ${parent_dir}/loadsmetric-${install_type}"
      rm -rf ${parent_dir}/loadsmetric-${install_type}
    fi

    if [ -d "${parent_dir}/loadsmetric-${install_type}-setup" ];then
      echo "remove loadsmetric path ${parent_dir}/loadsmetric-${install_type}-setup"
      rm -rf ${parent_dir}/loadsmetric-${install_type}-setup
    fi

    log_audit "Uninstall LoadsMetric successfully"
  else
    echo "Uninstall LoadsMetric unsuccessfully"
    echo "${parent_dir}/loadsmetric-${install_type} and ${parent_dir}/loadsmetric-${install_type}-setup not exist, no need to delete"
  fi
}

function main()
{
  local cur_dir=$(cd $(dirname ${0});pwd)
  local parent_dir=$(dirname ${cur_dir})

  install_type="${1}"
  local log_path=$(readlink -e "${parent_dir}/loadsmetric-${install_type}/log")
  log_file=${log_path}/loadsmetric.log

  if pgrep -f "LoadsMetricApplication" > /dev/null; then
    log_warn_echo "Warning: LoadsMetricApplication process is currently running." "${log_file}"
    log_warn_echo "Warning: Please STOP the LoadsMetricApplication service first." "${log_file}"
    exit 1
  fi

  log_audit "Begin to uninstall LoadsMetric"

  stop_loadsmetric
  remove_loadsmetric
}

main $@