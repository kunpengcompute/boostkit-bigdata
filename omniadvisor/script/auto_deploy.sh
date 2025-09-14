#!/bin/bash

log() {  # 用法：log LEVEL "message"
  local level=${1:-INFO}; shift
  printf '%s [%s] %s\n' "$(date '+%F %T%z')" "$level" "$*" >&2
}

log INFO "即将开始自动部署OmniAdvisor2.0..."
# 获取软件包路径
prefix=$(dirname `dirname $(pwd)`)
core_dir=$prefix/BoostKit-omniadvisor_core_2.0.0
frame_dir=$prefix/BoostKit-omniadvisor_2.0.0

log INFO "检查目标路径是否存在..."
# 路径存在性检查
## 检测软件包路径
for d in "$core_dir" "$frame_dir"; do
  if [[ ! -d "$d" ]]; then
    log ERROR "错误：目录不存在 -> $d" >&2
    log ERROR "请检查软件包是否有按要求放置到指定位置" >&2
    exit 1
  else
    log INFO "$d 存在"
  fi
done

## 检查SPARK_HOME是否正确配置
if [ -z "${SPARK_HOME}" ]; then
  # 如果 SPARK_HOME 未设置，打印错误信息并退出
  log ERROR "错误: SPARK_HOME 环境变量未设置！" >&2
  log ERROR "请先配置 SPARK_HOME 环境变量，指向您的 Spark 安装目录" >&2
  log ERROR "例如: " >&2
  log ERROR "export SPARK_HOME=/path/to/your/spark" >&2
  log ERROR "然后将此行添加到您的 ~/.bashrc 或者 ~/.zshrc 文件中" >&2
  exit 1 # 以非零状态退出 表示错误
else
  log INFO "SPARK_HOME=${SPARK_HOME}环境变量已配置"
fi


# 合并核心代码与框架代码
log INFO "将算法模块合并到执行框架目录中..."
log INFO "OmniAdvisor2.0框架模块目录: $frame_dir"
log INFO "OmniAdvisor2.0算法模块目录: $core_dir"

log INFO "合并算法模块，为 $frame_dir/src/ 添加写入权限"
chmod 750 -- "$(readlink -e -- "$frame_dir/src/")"

log INFO "执行命令:cp -r $core_dir/src/algo $frame_dir/src/ 合并算法模块"
cp -r $core_dir/src/algo $frame_dir/src/

log INFO "合并算法模块完成，最小化 $frame_dir/src/ 权限"
chmod 550 -- "$(readlink -e -- "$frame_dir/src/")"

log INFO "执行命令:cp -r $core_dir/config/* $frame_dir/config/ 合并配置文件"
cp -r $core_dir/config/* $frame_dir/config/

log INFO "执行命令:mkdir -p $frame_dir/kmc_env && cp -r $core_dir/kmc_env/ $frame_dir/ 合并kmc模块"
mkdir -p $frame_dir/kmc_env && cp -r $core_dir/kmc_env/ $frame_dir/
log INFO "修改$frame_dir/kmc_env的目录权限为550"
chmod 550 "$(readlink -e -- "$frame_dir/kmc_env")"

# 修改spark_submit_modified的文件权限 用readlink获取真实路径防止提权
spark_submit_modified=$frame_dir/script/spark-submit
[[ -L "$spark_submit_modified" ]] && { log ERROR "Refuse symlink: $spark_submit_modified 不允许对符号链接进行权限修改" >&2; exit 1; }

# 修改spark_submit_modified 中的hijack.pyc路径
hijack_path=$frame_dir/src/hijack.pyc
log INFO "开始自动配置${spark_submit_modified}文件中的hijack_path路径..."
log INFO "原始hijack.pyc文件路径:`cat $spark_submit_modified | grep "hijack_path=" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'`"
chmod 750 -- "$(readlink -e -- "$spark_submit_modified")"
sed -i "s|hijack_path=\"\"|hijack_path=\"${hijack_path}\"|g" "$spark_submit_modified"
chmod 550 -- "$(readlink -e -- "$spark_submit_modified")"
log INFO "更新后的hijack.pyc文件路径:`cat $spark_submit_modified | grep "hijack_path=" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'`"

# 最小化spark_submit_modified的文件权限 不再允许写操作
log INFO "修改$spark_submit_modified的文件权限为550"
chmod 550 -- "$(readlink -e -- "$spark_submit_modified")"

# 替换spark-submit脚本
spark_submit_link=$SPARK_HOME/bin/spark-submit
original_spark_submit_file=spark_submit_link
log INFO "将软件所使用的spark-submit软链接到SPARK_HOME/bin/spark-submit"
# 如果spark-submit不存在则直接创建软链接
if [ -L "$spark_submit_link" ]; then
  log INFO "${spark_submit_link}是一个已存在的软链接`ls -l $spark_submit_link | cut -d' ' -f10-`"
  log INFO "即将对软链接进行刷新..."

  original_spark_submit_file=`readlink -f $spark_submit_link`
  ln -snf $spark_submit_modified $spark_submit_link
  log INFO "将$spark_submit_link链接到$spark_submit_modified"

# 如果spark-submit存在则备份并重置spark-submit的软连接
elif [ -f "$spark_submit_link" ]; then
  log INFO "${spark_submit_link}是一个普通文件，进行备份"
  cp -r $spark_submit_link $SPARK_HOME/bin/spark-submit.original.backup

  original_spark_submit_file="$SPARK_HOME/bin/spark-submit.original.backup"
  log INFO "文件已备份到$original_spark_submit_file"

  ln -snf $spark_submit_modified $spark_submit_link
  log INFO "将$spark_submit_link链接到$spark_submit_modified"

else
  log INFO "$spark_submit_link 不是软连接也不是文件 创建新的软链接"

  ln -sn $spark_submit_modified $spark_submit_link
  log INFO "将$spark_submit_link链接到$spark_submit_modified"
fi

# 展现修改的成果
log INFO "更新后$spark_submit_link的链接情况如下:`ls -l $spark_submit_link | cut -d' ' -f10-`"
log INFO "软链接刷新前原始spark_submit的位置如下: $original_spark_submit_file"

log INFO "Success! OmniAdvisor2.0自动部署完成"