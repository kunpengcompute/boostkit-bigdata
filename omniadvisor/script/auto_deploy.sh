prefix=$(dirname `dirname $(pwd)`)
core_dir=$prefix/BoostKit-omniadvisor_core_2.0.0
frame_dir=$prefix/BoostKit-omniadvisor_2.0.0

# 合并核心代码与框架代码
ls -l $frame_dir/src
ls -l $core_dir/src
cp -r $core_dir/src/algo $frame_dir/src/
cp -r $core_dir/config/* $frame_dir/config/

if [ -z "${SPARK_HOME}" ]; then
  # 如果 SPARK_HOME 未设置，打印错误信息并退出
  echo "错误: SPARK_HOME 环境变量未设置！" >&2
  echo "请先配置 SPARK_HOME 环境变量，指向您的 Spark 安装目录" >&2
  echo "例如: " >&2
  echo " export SPARK_HOME=/path/to/your/spark" >&2
  echo "然后将此行添加到您的 ~/.bashrc 或者 ~/.zshrc 文件中" >&2
  exit 1 # 以非零状态退出 表示错误
fi

# 替换spark-submit脚本
spark_submit_modified=$frame_dir/script/spark-submit
spark_submit_link=$SPARK_HOME/bin/spark-submit
original_spark_submit_file=spark_submit_link


# 修改spark_submit_modified 中的hijack.pyc路径
hijack_path=$frame_dir/src/hijack.pyc
cat $spark_submit_modified | grep "hijack_path="
sed -i "s|hijack_path=\"\"|hijack_path=\"${hijack_path}\"|g" "$spark_submit_modified"
# 显示修改后的路径
cat $spark_submit_modified | grep "hijack_path="
chmod +x $spark_submit_modified


# 如果spark-submit不存在则直接创建软链接
if [ -L "$spark_submit_link" ]; then
  echo -e "\n$spark_submit_link 是一个已存在的软链接"
  ls -l $spark_submit_link
  echo "即将对软链接进行刷新"
  original_spark_submit_file=`readlink -f $spark_submit_link`
  ln -snf $spark_submit_modified $spark_submit_link
  echo "将$spark_submit_link链接到$spark_submit_modified"
# 如果spark-submit存在则备份并重置spark-submit的软连接
elif [ -f "$spark_submit_link" ]; then
  echo -e "\n$spark_submit_link 是一个普通文件 进行备份"
  cp -r $spark_submit_link $SPARK_HOME/bin/spark-submit.original.backup
  original_spark_submit_file="$SPARK_HOME/bin/spark-submit.original.backup"
  echo "文件已备份到$original_spark_submit_file"
  ln -snf $spark_submit_modified $spark_submit_link
  echo "将$spark_submit_link链接到$spark_submit_modified"
else
  echo -e "\n$spark_submit_link 不是软连接也不是文件 创建新的软链接"
  ln -n $spark_submit_modified $spark_submit_link
  echo "将$spark_submit_link链接到$spark_submit_modified"
fi

# 展现修改的成果
echo -e "\更新后$spark_submit_link的链接情况如下"
ls -l $spark_submit_link
echo -e "\n软链接刷新前原始spark_submit的位置如下"
echo $original_spark_submit_file
echo -e "\n修改后与原始文件的修改差异如下:"
diff $spark_submit_link $original_spark_submit_file
