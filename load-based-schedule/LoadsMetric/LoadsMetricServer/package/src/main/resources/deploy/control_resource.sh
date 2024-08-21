#!/bin/bash
# CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
set -e

function main()
{
  local cur_dir=$(cd $(dirname ${0});pwd)
  local parent_dir=$(dirname ${cur_dir})
  local config_file=${parent_dir}/etc/config.properties

  if [ ! -f "${config_file}" ]
  then
    config_file=${parent_dir}/etc/application.properties
  fi

  local cur_user=$(whoami)
  local cur_group=$(groups)
  local max_number_cpu=$(cat /proc/cpuinfo| grep "processor"| wc -l)

  local quota=-1
  local curr_number_cpu=$((grep number.of.cpu.core ${config_file} || true) | cut -d'=' -f2 | sed 's/\r//')

  cpu_config_path=/sys/fs/cgroup/cpu/${cur_user}

  if [ "${curr_number_cpu}" = "" ]
  then
    sh ${parent_dir}/deploy/log.sh "The user does not set number.of.cpu.core, and the CPU used is not restricted by the cgroup."
  elif [[ -n "$(echo ${curr_number_cpu} | sed -n "/^[0-9]\+$/p")" && ${curr_number_cpu} -le ${max_number_cpu} && ${curr_number_cpu} -gt 0 ]]
  then
    unit=$(cat ${cpu_config_path}/cpu.cfs_period_us)
    quota=$(expr ${curr_number_cpu} \* ${unit})
    echo ${quota} > ${cpu_config_path}/cpu.cfs_quota_us
    echo $(cat ${parent_dir}/var/run/launcher.pid) > ${cpu_config_path}/cgroup.procs
    sh ${parent_dir}/deploy/log.sh "Set LoadsMetric server cpu resource to ${curr_number_cpu} core"
  else
    sh ${parent_dir}/deploy/log.sh "If number.of.cpu.core is set to ${curr_number_cpu}, the value is invalid. The value must be greater than 0 and less than or equal to ${max_number_cpu}."
  fi
}

main