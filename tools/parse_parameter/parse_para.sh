#!/bin/bash

input="$@"
# 借用内存的比例
memFraction=90
# 最终借用内存的大小
borrowedMemory=0
# 借用后堆内内存的大小
executorMemory=0
# 内存单位
memoryUnit="g"
# 是否获取conf文件中的参数
use_config_file="false"
# conf文件路径
config_file_path="${SPARK_HOME}/conf/spark-defaults.conf"

declare -A args_map

# 将整个命令行参数作为数组传入，去除命令名
params=("${@:2}")   # 如果你用 $0 是命令名，否则改为 "${@}" 也行

prev=""
for arg in "$@"; do
    if [[ "$arg" == "-e" ]]; then
        exit 2
    fi
    # 处理 --key=value 类型
    if [[ "$arg" =~ ^--([a-zA-Z0-9._-]+)=(.*)$ ]]; then
        key="--${BASH_REMATCH[1]}"
        echo "$key"
        value="${BASH_REMATCH[2]}"
        args_map["$key"]="$value"
        prev=""
    # 处理 --key value 类型
    elif [[ "$arg" == --* ]]; then
        prev="$arg"
    elif [[ -n "$prev" ]]; then
        args_map["$prev"]="$arg"
        prev=""
    fi
done

# 如果没有使用conf文件配置参数，直接在参数中读取
if [[ "$use_config_file" == "false" ]]; then
    # 查找是否有--conf spark.omni.enableBorrow
    if [[ "$input" =~ --conf[[:space:]]+spark.omni.enableBorrow=([^[:space:]]+) ]]; then
        value="${BASH_REMATCH[1]}"
        value=${value//\\/}
        if [[ "$value" != "true" ]]; then
            exit 2
        fi
    else
        exit 2
    fi

    # 检查堆外内存配置是否开启
    if [[ "$input" =~ --conf[[:space:]]+spark.memory.offHeap.enabled=([^[:space:]]+) ]]; then
        value=$(printf "%s\n" "$input" | grep -oP '.*\K--conf[[:space:]]+spark\.memory\.offHeap\.enabled=\K[^[:space:]]+')
        value=${value//\\/}
        # 如果堆外内存开启，则不能借用堆内内存
        if [[ "$value" == "true" ]]; then
            exit 2
        fi
    fi

    # 检查spark.omni.borrowMemory.Fraction参数是否存在
    if [[ "$input" =~ --conf[[:space:]]+spark.omni.borrowMemory.Fraction=([^[:space:]]+) ]]; then
        value="${BASH_REMATCH[1]}"
        value=${value//\\/}
        # 如果借用百分比存在且为正整数，则设定，如果参数不合法则退出
        if [[ "$value" =~ ^[0-9]+$ ]]; then
            memFraction=$value
            if (($(awk 'BEGIN{print ('$memFraction' > 100) ? 1 : 0}'))); then
                exit 2
            fi
        else
            exit 2
        fi
    fi
elif [[ "$use_config_file" == "true" ]]; then
    # 判断配置文件中是否开启内存借用
    value=$(awk '$1=="spark.omni.enableBorrow"{print $2}' "$config_file_path")
    if [ -z "$value" ]; then
        exit 2
    else
        if [ "$value" != "true" ]; then
            exit 2
        fi
    fi

    # 检查输入参数中堆外内存配置是否开启
    if [[ "$input" =~ --conf[[:space:]]+spark.memory.offHeap.enabled=([^[:space:]]+) ]]; then
        value=$(printf "%s\n" "$input" | grep -oP '.*\K--conf[[:space:]]+spark\.memory\.offHeap\.enabled=\K[^[:space:]]+')
        value=${value//\\/}
        # 如果堆外内存开启，则不能借用堆内内存
        if [[ "$value" == "true" ]]; then
            exit 2
        fi
    fi
    # 检查配置文件中堆外内存是否开启,如果开启堆外内存，则不启用内存借用
    value=$(awk '$1=="spark.memory.offHeap.enabled"{print $2}' "$config_file_path")
    if [ -n "$value" ]; then
        if [ "$value" == "true" ]; then
            exit 2
        fi
    fi

    # 检查spark.omni.borrowMemory.Fraction参数是否存在
    value=$(awk '$1=="spark.omni.borrowMemory.Fraction"{print $2}' "$config_file_path")
    if [ -n "$value" ]; then
        if [[ "$value" =~ ^[0-9]+$ ]]; then
            memFraction=$value
            if (($(awk 'BEGIN{print ('$memFraction' > 100) ? 1 : 0}'))); then
                # echo "错误：memFraction 不能大于100，当前值为 $memFraction"
                exit 2
            fi
        else
            exit 2
        fi
    fi
else
    exit 2
fi

if [[ -n "${args_map[--executor-memory]}" ]]; then
    em_val="${args_map[--executor-memory]}"
    if [[ "$em_val" =~ ^([0-9]+(\.[0-9]+)?)([a-zA-Z]+)$ ]]; then
        value="${BASH_REMATCH[1]}"
        memoryUnit="${BASH_REMATCH[3]}"
        borrowedMemory=$((value * memFraction / 100))
        executorMemory=$(awk "BEGIN {print $value - $borrowedMemory}")
    else
        exit 2
    fi
else
    exit 2
fi

# 1. 替换 --executor-memory 为 executorMemory+memoryUnit
output=$(printf "%s\n" "$input" | sed -E "s/(--executor-memory[[:space:]=]+)[^#[:space:]\\]*/\1${executorMemory}${memoryUnit}/g")

# 2. 增加新参数
output="${output} --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=${borrowedMemory}${memoryUnit}"

printf "%s\n" "$output"
