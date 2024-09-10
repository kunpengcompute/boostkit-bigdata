#!/bin/bash
# CopyRight (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
stty intr undef
stty susp undef
stty quit undef
# Path
exporter_path="/opt/prometheus/node_exporter-1.7.0.linux-arm64"
prometheus_path="/opt/prometheus/prometheus-2.45.4.linux-arm64"
grafana_path="/opt/prometheus/grafana-v10.4.2/bin"

function exit_code() {
    if [ "${1}" -ne 0 ]; then
      echo "See log(${log_file}) for more details."
    fi

    set_file_permission
    stty intr ^C
    stty susp ^Z
    stty quit ^\\

    exit "${1}"
}

function check_file() {
  if [ ! -e "${1}" ]
  then
    log_err_echo "${2}"
    exit_code 1
  fi
}

function log_init() {
    if [ ! -d "${log_path}" ]; then
        mkdir -p "${log_path}"
        check_file "${log_path}" "Failed to create directory ${log_path}"
    fi
    if [ ! -f "${log_file}" ]; then
        touch "${log_file}"
        check_file "${log_file}" "Failed to create file ${log_file}"
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

function get_value_of_properties_file() {
  local result=""
  local pro_file_path="${1}"
  local key="${2}"
  local line_num=$(wc -l < "${pro_file_path}")
  OLD_IFS=${IFS}
  IFS='='
  for ((i = 1; i <= line_num; i++)); do
    local line=$(sed -n "${i}p" "${pro_file_path}" | sed 's/^ *//;s/ *$//')
    read -r -a line_split <<< "${line}"
    if ((${#line_split[@]} != 2))
    then
      continue
    fi
    if [ "${line_split[0]}" = "${key}" ]
    then
      result="${line_split[1]}"
      break
    fi
  done
  IFS=${OLD_IFS}
  echo "${result}"
}

function check_properties_value() {
    local value="${1}"
    local parameter="${2}"
    if [ "${value}" = "" ]
    then
      log_err_echo "Parameter ${parameter} in ${config_file} has not been configured"
      exit_code 1
    fi
}

function get_and_check_value_of_properties_file() {
    local parameter="${1}"
    local result=$(get_value_of_properties_file "${config_file}" "${parameter}")
    check_properties_value "${result}" "${parameter}"
    echo "${result}"
}

function validate_ip() {
  local ip="${1}"
  local parameter="${2}"
  if [[ "${ip}" =~ ^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$ ]]
  then
    return 1
  fi
  log_err_echo "Parameter ${parameter} in ${config_file} has a invalid ip: ${ip}"
  exit_code 1
}

function validate_port() {
  local port="${1}"
  local parameter="${2}"
  if [[ $(echo "${port}"|grep -P "^\d+$") == "" || "${port}" -gt 65534 || "${port}" -lt 1025 ]]; then
    log_err_echo "Parameter ${parameter} in ${config_file} has a invalid port: ${port}, port number should be 1024~65535"
    exit_code 1
  fi
}

function validate_address() {
  local ip_port="${1}"
  local parameter="${2}"
  local has_colon="$(echo "${ip_port}" | (grep : || true))"
  if [[ "${has_colon}" == "" ]]; then
    log_err_echo "Parameter ${parameter} in ${config_file} has a invalid address: ${ip_port}. The right format is ip:port"
    exit_code 1
  fi
  local ip="${1%:*}"
  local port="${1#*:}"
  validate_ip "${ip}" "${parameter}"
  validate_port "${port}" "${parameter}"
}

function validate_multiple_address () {
  local addresses="${1}"
  local parameter="${2}"
  for address in $(echo "${addresses}" | awk -F "," '{for(i=1;i<=NF;i++){print $i;}}')
  do
    validate_address "${address}" "${parameter}"
  done
}

function create_user() {
    if grep -q "^${exec_user}:" /etc/group ; then
        true
    else
      groupadd "${exec_user}"
      log_info_echo "Create group:${exec_user}"
    fi

    if id "${exec_user}" >> "${log_file}" 2>&1; then
        true
    else
      useradd -M -s /sbin/nologin -g "${exec_user}" "${exec_user}" >> "${log_file}" 2>&1
      log_info_echo "Create user:${exec_user} in group:${exec_user}"
    fi
}

function set_file_permission() {
  if [ -d "${install_dir}" ]
  then
    chown -R "${exec_user}:${exec_user}" "${install_dir}"
    chmod 750 "${install_dir}"
  fi

  if [ -d "${software_dir}" ]
  then
    chown -R "${exec_user}:${exec_user}" "${software_dir}"
    chmod 750 "${software_dir}"
    chmod 700 "${software_dir}/${software_name}_deploy.sh"
  fi

  if [ -d "${log_path}" ]
  then
    chown -R "${exec_user}:${exec_user}" "${log_path}"
    chmod 640 $(find ${log_path} -type f)
  fi
}

function check_already_install() {
  local install_dir_software="${install_dir}/${software_name}"
  local install_dir_setup="${install_dir}/${software_name}-setup"
  if [ -d "${install_dir_software}" ] || [ -d "${install_dir_setup}" ]
  then
    echo "The ${software_name} is installed. Are you sure to remove the ${install_dir_software} and ${install_dir_setup} directories in order to reinstall ${software_name}?"
    while((1))
    do
      read -rp "Please enter the Y/N:" para
      case "${para}" in
        [yY])
          local launcher_script="${install_dir}/${software_name}/bin/launcher"
          if [ -e "${launcher_script}" ]
          then
            sh "${launcher_script}" "stop"
          fi
          log_info_echo "Remove directory ${install_dir_software}"
          rm -rf "${install_dir_software}"
          log_info_echo "Remove directory ${install_dir_setup}"
          rm -rf "${install_dir_setup}"
          return 1
          ;;
        [nN])
          log_info_echo "Abandon reinstalling ${software_name}"
          exit_code 1
          ;;
        *)
          echo "Invalid input '${para}'"
      esac
    done
  fi
}

function version() {
  version=$(get_value_of_properties_file "${config_file}" "version")
  check_properties_value "${version}" "version"
}

function install(){
  # check
  log_info_echo "Start check dependency, configuration and directory"
  local insufficient_info="
    Insufficient dependency and configuration. Please complete the following steps first.
    1. Please put the ${software_dir}/${software_name}-${install_type}-${version}-aarch64.tar.gz in the ${software_dir} directory.
    2. Please correctly configure the ${config_file}."
  install_tar="${software_dir}/${software_name}-${install_type}-${version}-aarch64.tar.gz"

  check_file "${install_tar}" "${install_tar} doesn't exist."
  if [ ! -d "${install_dir}" ]
  then
    log_info_echo "${install_dir} doesn't exist, create dir ${install_dir}"
    mkdir -p "${install_dir}"
    check_file "${install_dir}" "Failed to create directory ${install_dir}"
  fi

  # decompress
  log_info_echo "Start decompressing the package."
  {
    tar -zxvf "${install_tar}" -C "${install_dir}"
  } >> "${log_file}" 2>&1
  log_info_echo "Complete decompressing the package."
  # install
  if ! sh "${install_dir}/${software_name}-${install_type}-setup/${software_name}_install.sh" "${install_type}"
  then
    log_err_echo "Failed to install the ${install_tar} package."
    exit_code 1
  fi

  # config
  log_info_echo "Start config ${software_name}-${install_type}"
  local config_path="${install_dir}/${software_name}-${install_type}/etc"
  local mem=$(free -g | sed -n '2p' | awk '{print $2}')

  if [ "${install_type}" = "server" ]
  then
    server_config_path="${config_path}/application.properties"
    replace_property "${server_config_path}" "server.port" "${server_port}"
    replace_property "${server_config_path}" "client.heartbeat.interval" "${client_heartbeat_interval}"
    replace_property "${server_config_path}" "client.lost.time" "${client_lost_time}"
    replace_property "${server_config_path}" "logical-resource-usage.levels.mem" "${logical_resource_usage_levels_mem}"
    replace_property "${server_config_path}" "logical-resource-usage.levels.vcores" "${logical_resource_usage_levels_vcores}"
    replace_property "${server_config_path}" "load.limit.cpu" "${load_limit_cpu}"
    replace_property "${server_config_path}" "load.limit.mem" "${load_limit_mem}"
    replace_property "${server_config_path}" "load.limit.diskio" "${load_limit_diskio}"
    replace_property "${server_config_path}" "load.limit.netio" "${load_limit_netio}"
    replace_property "${server_config_path}" "load.weight.cpu" "${load_weight_cpu}"
    replace_property "${server_config_path}" "load.weight.mem" "${load_weight_mem}"
    replace_property "${server_config_path}" "load.weight.diskio" "${load_weight_diskio}"
    replace_property "${server_config_path}" "load.weight.netio" "${load_weight_netio}"
    replace_property "${server_config_path}" "time.windowsize" "${time_windowsize}"
    replace_property "${server_config_path}" "overload.filter.able" "${overload_filter_able}"
    replace_property "${server_config_path}" "net.compute.type" "${net_compute_type}"
    replace_property "${server_config_path}" "node-exporter.pull.enable" "${node_exporter_pull_enable}"
    replace_property "${server_config_path}" "node-exporter.pull.protocol" "${node_exporter_pull_protocol}"
    replace_property "${server_config_path}" "node-exporter.pull.port" "${node_exporter_pull_port}"
    replace_property "${server_config_path}" "node-exporter.pull.thread-pool.size" "${node_exporter_pull_thread_pool_size}"
    cat ${workers_file} > ${config_path}/workers
  fi
  log_info_echo "Complete config ${software_name}"
}

function uninstall(){
  # check
  local uninstall_script="${install_dir}/${software_name}-${install_type}-setup/loadsmetric_uninstall.sh"
  check_file "${uninstall_script}" "${uninstall_script} doesn't exist. ${software_name} is not installed or is installed in an incorrect directory."

  # uninstall
  if ! sh "${uninstall_script}" "${install_type}"
  then
    log_err_echo "Failed to uninstall ${software_name}."
    exit_code 1
  fi
}

function manager(){
  manager_type="${1}"
  log_info_echo "${manager_type} ${software_name}-server"
  sh "${install_dir}/${software_name}-server/bin/launcher" "${manager_type}"  >> "${log_file}" 2>&1
}

# start node_exporter
start_node_exporter() {
  (
    ssh root@$1 "cd $exporter_path && nohup ./node_exporter --web.listen-address='0.0.0.0:9100' > node_exporter.log 2>&1 &"
  ) &
  log_info_echo "Started node_exporter on $1"
}

# stop node_exporter
stop_node_exporter() {
  ssh root@$1 "pkill -f node_exporter"
  log_info_echo "Stopped node_exporter on $1"
}
# restart node_exporter
restart_node_exporter() {
  stop_node_exporter "$1"
  start_node_exporter "$1"
}

function manager_node_exporter(){

  check_file "workers" "Thers is no workers!!!"
  # get agent lists from workers
  mapfile -t agents < workers
  manager_type="${1}"
  log_info_echo "========${manager_type} node_exporter========"
case $manager_type in
  "start")
    for agent in "${agents[@]}"; do
      if ! ssh root@$agent "[ -e \"$exporter_path\" ]"; then
         log_err_echo "node_exporter path is not exist"
      else
         start_node_exporter "$agent"
      fi
	    sleep 1
    done
    ;;
  "stop")
    for agent in "${agents[@]}"; do
      stop_node_exporter "$agent"
	  sleep 1
    done
    ;;
  "restart")
    for agent in "${agents[@]}"; do
      restart_node_exporter "$agent"
	  sleep 1
    done
    ;;
  *)
    exit 1
    ;;
esac
  log_info_echo "========completed ${manager_type} node_exporter====="
}

# start prometheus
start_prometheus() {
  (
    cd $prometheus_path && nohup ./prometheus --web.listen-address='0.0.0.0:9060' > prometheus.log 2>&1 &
  ) &
  log_info_echo "Started prometheus"
}

# stop prometheus
stop_prometheus() {
  (
    pkill -f prometheus
  ) &
  log_info_echo "Stopped prometheus"
}
# restart node_exporter
restart_prometheus() {
  stop_node_exporter
  start_node_exporter
}

function manager_prometheus(){
  check_file "$prometheus_path" "prometheus path is not exist"
  manager_type="${1}"
  log_info_echo "========${manager_type} prometheus========"
case $manager_type in
  "start")
    start_prometheus
    sleep 1
    ;;
  "stop")
    stop_prometheus
    sleep 1
    ;;
  "restart")
    restart_prometheus
    sleep 1
    ;;
  *)
    exit 1
    ;;
esac
log_info_echo "========completed ${manager_type} prometheus====="
}

# start grafana
start_grafana() {
  (
    cd $grafana_path && nohup ./grafana server > grafana.log 2>&1 &
  ) &
  log_info_echo "Started grafana"
}

# stop grafana
stop_grafana() {
  pkill -f grafana
  log_info_echo "Stopped grafana"
}
# restart node_exporter
restart_grafana() {
  stop_node_exporter
  start_node_exporter
}

function manager_grafana(){
  check_file "$grafana_path" "grafana path is not exist"
  manager_type="${1}"
  log_info_echo "========${manager_type} grafana========"
case $manager_type in
  "start")
    start_grafana
    sleep 1
    ;;
  "stop")
    stop_grafana
    sleep 1
    ;;
  "restart")
    restart_grafana
    sleep 1
    ;;
  *)
    exit 1
    ;;
esac
log_info_echo "========completed ${manager_type} grafana====="
}

function validate_properties() {

  server_port=$(get_value_of_properties_file "${config_file}" "server.port")
  check_properties_value "${server_port}" "server.port"

  client_lost_time=$(get_value_of_properties_file "${config_file}" "client.lost.time")
  check_properties_value "${client_lost_time}" "client.lost.time"

  loads_metric_server_address=$(get_value_of_properties_file "${config_file}" "loads-metric-server.address")
  check_properties_value "${loads_metric_server_address}" "loads-metric-server.address"

  client_heartbeat_interval=$(get_value_of_properties_file "${config_file}" "client.heartbeat.interval")
  check_properties_value "${client_heartbeat_interval}" "client.heartbeat.interval"

  net_compute_type=$(get_value_of_properties_file "${config_file}" "net.compute.type")
  check_properties_value "${net_compute_type}" "net.compute.type"

  logical_resource_usage_levels_mem=$(get_value_of_properties_file "${config_file}" "logical-resource-usage.levels.mem")
  check_properties_value "${logical_resource_usage_levels_mem}" "logical-resource-usage.levels.mem"

  logical_resource_usage_levels_vcores=$(get_value_of_properties_file "${config_file}" "logical-resource-usage.levels.vcores")
  check_properties_value "${logical_resource_usage_levels_vcores}" "logical-resource-usage.levels.vcores"

  load_limit_cpu=$(get_value_of_properties_file "${config_file}" "load.limit.cpu")
  check_properties_value "${load_limit_cpu}" "load.limit.cpu"

  load_limit_mem=$(get_value_of_properties_file "${config_file}" "load.limit.mem")
  check_properties_value "${load_limit_mem}" "load.limit.mem"

  load_limit_diskio=$(get_value_of_properties_file "${config_file}" "load.limit.diskio")
  check_properties_value "${load_limit_diskio}" "load.limit.diskio"

  load_limit_netio=$(get_value_of_properties_file "${config_file}" "load.limit.netio")
  check_properties_value "${load_limit_netio}" "load.limit.netio"

  load_weight_cpu=$(get_value_of_properties_file "${config_file}" "load.weight.cpu")
  check_properties_value "${load_weight_cpu}" "load.weight.cpu"

  load_weight_mem=$(get_value_of_properties_file "${config_file}" "load.weight.mem")
  check_properties_value "${load_weight_mem}" "load.weight.mem"

  load_weight_diskio=$(get_value_of_properties_file "${config_file}" "load.weight.diskio")
  check_properties_value "${load_weight_diskio}" "load.weight.diskio"

  load_weight_netio=$(get_value_of_properties_file "${config_file}" "load.weight.netio")
  check_properties_value "${load_weight_netio}" "load.weight.netio"

  time_windowsize=$(get_value_of_properties_file "${config_file}" "time.windowsize")
  check_properties_value "${time_windowsize}" "time.windowsize"

  overload_filter_able=$(get_value_of_properties_file "${config_file}" "overload.filter.able")
  check_properties_value "${overload_filter_able}" "overload.filter.able"

  node_exporter_pull_enable=$(get_value_of_properties_file "${config_file}" "node-exporter.pull.enable")
  check_properties_value "${node_exporter_pull_enable}" "node-exporter.pull.enable"

  node_exporter_pull_protocol=$(get_value_of_properties_file "${config_file}" "node-exporter.pull.protocol")
  check_properties_value "${node_exporter_pull_protocol}" "node-exporter.pull.protocol"

  node_exporter_pull_port=$(get_value_of_properties_file "${config_file}" "node-exporter.pull.port")
  check_properties_value "${node_exporter_pull_port}" "node-exporter.pull.port"

  node_exporter_pull_thread_pool_size=$(get_value_of_properties_file "${config_file}" "node-exporter.pull.thread-pool.size")
  check_properties_value "${node_exporter_pull_thread_pool_size}" "node-exporter.pull.thread-pool.size"
}

function replace_property() {
  local replace_config_path="${1}"
  local key="${2}"
  local value="${3}"
  sed -i "/^${key}=/c\\${key}=${value}" "${replace_config_path}"
}

function print_help() {
    echo "Usage: $0 [-h|--help] [install|uninstall|start|stop]"
    echo ""
    echo "Command:"
    echo "  install                                                            Install software"
    echo "  uninstall                                                          Uninstall software"
    echo "  start                                                              Start software"
    echo "  stop                                                               Stop software"
    echo "  restart                                                            Restart software"
    echo "  start_exporter     || start_exporter exporter_install_path         Start node_exporter"
    echo "  stop_exporter      || stop_exporter exporter_install_path          Stop node_exporter"
    echo "  restart_exporter   || restart_exporter exporter_install_path       Restart node_exporter"
    echo "  start_prometheus   || start_prometheus prometheus_install_path     Start prometheus"
    echo "  stop_prometheus    || stop_prometheus prometheus_install_path      Stop prometheus"
    echo "  restart_prometheus || restart_prometheus prometheus_install_path   Restart prometheus"
    echo "  start_grafana      || start_grafana grafana_install_path           Start grafana"
    echo "  stop_grafana       || stop_grafana grafana_install_path            Stop grafana"
    echo "  restart_grafana    || restart_grafana grafana_install_path         Restart grafana"
    echo ""
    echo "Example:"
    echo "  $0 install"
    echo "  $0 uninstall"
    echo "  $0 start"
    echo "  $0 stop"
    echo "  $0 restart"
    echo ""
}

set_paths() {
  local first_param="$1"
  local second_param="$2"

  if [[ "$first_param" == *exporter* ]]; then
    exporter_path="$second_param"
  elif [[ "$first_param" == *prometheus* ]]; then
    prometheus_path="$second_param"
  elif [[ "$first_param" == *grafana* ]]; then
    grafana_path="$second_param"
  else
    echo "Unknown parameter: $first_param"
    return 1
  fi

  log_info_echo "Paths updated:"
  log_info_echo "exporter_path: $exporter_path"
  log_info_echo "prometheus_path: $prometheus_path"
  log_info_echo "grafana_path: $grafana_path"
}

function main() {
  if [ "$1" = "-h" ] || [ "$1" = "--help" ]
  then
    print_help
    exit_code 0
  fi

  # init variable for directory and file
  software_name="loadsmetric"
  deploy_sh="${software_name}_deploy.sh"
  script_dir=$(cd $(dirname ${0});pwd)
  config_file="${script_dir}/${software_name}_configfile.properties"
  log_path="${script_dir}/log"
  log_file="${log_path}/${software_name}_deploy.log"
  # init log
  log_init
  # get user
  check_file "${config_file}" "${config_file} doesn't exist."
  exec_user=$(get_value_of_properties_file "${config_file}" "user")
  check_properties_value "${exec_user}" "user"
  # install at user dir
  install_dir="/home/${exec_user}/${software_name}-install"
  software_dir="/home/${exec_user}/${software_name}-software"
  workers_file="${software_dir}/workers"

  if [ "$script_dir" != "$software_dir" ]
  then
    echo "${0} The script and installation package must be stored in the ${software_dir} directory."
    exit_code 0
  fi

  install_type=$(get_value_of_properties_file "${config_file}" "install.type")
  check_properties_value "${install_type}" "install.type"
  client_type=$(get_value_of_properties_file "${config_file}" "client.type")
  check_properties_value "${client_type}" "client.type"
  if [ "install_type" == "server" ]
  then
    check_file "${workers_file}" "${workers_file} doesn't exist."
  fi
  # init version
  version
  # validate server/client properties
  validate_properties

  if [ $# -eq 2 ]; then
    set_paths "$1" "$2"
  fi

  local option="${1}"
  case "${option}" in
    "version")
      echo "${version}"
    ;;
    "install")
      install
    ;;
    "uninstall")
      uninstall
    ;;
    "start")
      # Check if the installation directory exists
      local install_dir_server="${install_dir}/${software_name}-${install_type}"
      local install_dir_setup="${install_dir}/${software_name}-${install_type}-setup"
      if [ -d "${install_dir_server}" ] || [ -d "${install_dir_setup}" ]; then
        # If the installation directory exists, perform the start operation
        manager "start"
      else
        # If the installation directory does not exist, prompt to install first
        log_err_echo "Installation directory not found. Please install ${software_name} before starting it."
        exit 1
      fi
    ;;
    "stop")
      manager "stop"
    ;;
    "restart")
      manager "restart"
    ;;
    "start_exporter")
      manager_node_exporter "start"
    ;;
    "restart_exporter")
      manager_node_exporter "restart"
    ;;
    "stop_exporter")
      manager_node_exporter "stop"
    ;;
    "start_prometheus")
      manager_prometheus "start"
    ;;
    "restart_prometheus")
      manager_prometheus "restart"
    ;;
    "stop_prometheus")
      manager_prometheus "stop"
    ;;
    "start_grafana")
      manager_grafana "start"
    ;;
    "restart_grafana")
      manager_grafana "restart"
    ;;
    "stop_grafana")
      manager_grafana "stop"
    ;;
    *)
      echo "Invalid option: ${option}"
      print_help
  esac
  exit_code 0
}

main "$@"
