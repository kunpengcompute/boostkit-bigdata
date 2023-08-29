start_time=$1
finish_time=$2

project_root=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
lib_dir=${project_root}/lib
conf_dir=${project_root}/conf
main_class=com.huawei.boostkit.omnituning.OmniTuning

java -Dlog4j.configuration=${conf_dir}/log4j.properties -cp ${project_root}/*:${lib_dir}/*:${conf_dir} ${main_class} "${start_time}" "${finish_time}"