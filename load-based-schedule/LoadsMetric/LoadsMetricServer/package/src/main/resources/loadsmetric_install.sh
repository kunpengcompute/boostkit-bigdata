#!/bin/bash
# CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
set -e

function log_audit() {
    if [ "${log_file}" == "" ]; then
      echo "[loadsmetric-install] log_audit needs one parameter and log_file needs to be initialized."
      exit 1
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$(whoami)] : [loadsmetric-install] ${1}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$(whoami)] : [loadsmetric-install] ${1}" >> ${log_file}
}

check_and_create()
{
  if [ -h "${1}" ]; then
    echo "[check_and_create] The directory ${1} is a symlink"
    exit 1
  fi
  if [ ! -d "${1}" ]; then
    mkdir -p ${1}
  fi
}

function prepare_path()
{
  log_path=${parent_dir}/loadsmetric-${install_type}/log
  check_and_create ${log_path}

  sed -i "s#^.*name=\"LOG_HOME\".*#    <property name=\"LOG_HOME\" value=\"${log_path}\" />#" ${parent_dir}/loadsmetric-${install_type}/etc/logback.xml
}

function set_log_file()
{
  log_file=${log_path}/loadsmetric.log
  touch ${log_file}
}

function set_file_permission()
{
 chown -h -R ${cur_user}:${cur_group} ${parent_dir}/loadsmetric-${install_type}-setup
 chmod 500 ${parent_dir}/loadsmetric-${install_type}-setup/*

 chown -h -R ${cur_user}:${cur_group} ${parent_dir}/loadsmetric-${install_type}
 chmod 700 ${parent_dir}/loadsmetric-${install_type}

 chmod 500 ${parent_dir}/loadsmetric-${install_type}/lib/*
 chmod 500 ${parent_dir}/loadsmetric-${install_type}/deploy/*
 chmod 500 ${parent_dir}/loadsmetric-${install_type}/bin/*
 chmod 640 ${parent_dir}/loadsmetric-${install_type}/etc/*

 chmod 700 ${log_path}
 chmod 600 ${log_file}
}

function set_java()
{
  if [ ! -f "/usr/bin/java" ]; then
    local java_path=$(which java)
    local first_java=$(echo ${java_path} | awk -F':' '{print $1}')

    if [ ${cur_user} == "root" ]; then
      ln -s ${first_java} /usr/bin
    else
      log_audit "/usr/bin/java is not found, run the 'ln -s ${first_java} /usr/bin' command as the root user."
    fi
  fi
}

function main()
{
  local cur_dir=$(cd $(dirname ${0});pwd)
  local parent_dir=$(dirname ${cur_dir})

  local cur_user=$(whoami)
  local cur_group=$(groups)

  install_type="${1}"
  local log_path=""
  prepare_path
  local log_file=""
  set_log_file

  log_audit "Start installation LoadsMetric"
  set_file_permission
  log_audit "Install LoadsMetric Successfully"
}

main $@

