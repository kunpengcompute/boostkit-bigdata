#!/bin/bash
tool_root_dir=$(cd $(dirname "$0")||exit 1;pwd)
packages_dir="$tool_root_dir/omnioperator"
host_name=$(hostname)
user=$(whoami)
 
#环境变量生效
dos2unix ./conf/config
source ./conf/config

################################## 函数 ################################################
function_start() {
    echo ""
    echo "$1"
}

function_end() {
    echo "$1"
    echo ""
}

check_spark_version(){
    function_start "------Start checking spark version------"

    # 获取当前系统的spark版本
    full_spark_version=$(spark-sql --version 2>&1 | grep 'version' | head -n 1 | sed -E 's/.*version ([0-9]+\.[0-9]+\.[0-9])+.*/\1/')
    spark_version=$(spark-sql --version 2>&1 | grep 'version' | head -n 1 | sed -E 's/.*version ([0-9]+\.[0-9]+)\.[0-9]+.*/\1/')

    if [ $spark_version == $(echo ${expect_spark_version} | cut -d'.' -f1,2) ]; then
        if [ $full_spark_version == ${expect_spark_version} ]; then
            echo "INFO: Spark version is right. Expect Spark version is consistent with the SYSTEM spark version."
        else
            echo "INFO: Spark version is $full_spark_version."
            echo "INFO: Omni expected spark version is ${expect_spark_version}."
        fi
    else
        echo "ERROR: Spark version is wrong! Expect spark version is consistent with the SYSTEM spark version."
        echo "Spark version is $full_spark_version."
        echo "Omni expected spark version is ${expect_spark_version}."
        exit 1
    fi

    function_end "------Finish checking spark version------"
}

check_cpu_model(){
    function_start "------Start checking cpu model------"

    cpu_model=$(lscpu | grep "Model name" | sed 's/Model name:[[:space:]]*//')
    if [ $cpu_model == "Kunpeng-920" ] && [ ${sve_flag} == true ]; then
        echo "ERROR: Kunpeng-920 don't support omnioperator-SVE version!"
        exit 1
    else
        echo "INFO: Check over."
    fi

    function_end "------Finish checking cpu model------"
}

generate_dir(){
    function_start "------Start creating target dir------"

    # 检查用户是否有权限新建该文件夹
    if [ ! -w "$(dirname "$target_path")" ]; then  
        echo "Error: You do not have permission to create a directory in $(dirname "$target_path")."  
        exit 1
    fi  

    # 提示用户确认输入  
    read -p "Target path is: "$target_path", please confirm create it or not. (y/n): " confirm  

    # 检查用户确认的输入  
    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then  
        # 检查目录是否已存在  
        if [ -d "$target_path" ]; then  
            read -p "dir "$target_path" is existed, do you want to replace it? (y/n): " delete_confirm  
            if [[ "$delete_confirm" == "y" || "$delete_confirm" == "Y" ]]; then  
                rm -rf "$target_path"  
                echo "INFO: The old directory has been replaced: "$target_path"。"  
            else  
                echo "INFO: Process exit."  
                exit 1  
            fi  
        fi  

        # 创建目录  
        mkdir -p "${target_path}/lib" "${target_path}/conf"
        echo "INFO: dir "$target_path" has been created。"  
    else  
        echo "INFO: Process exit."
        echo "If you want to install OmniOperator in another dir,"
        echo "Please change configuration in ${tool_root_dir}/conf/config"
        exit 1  
    fi 

    function_end "------Finish creating target dir------"
}

unzip_package(){
    function_start "------Start unziping package------"
    # 检查系统类型
    os_type=$(grep -i "^NAME=" /etc/os-release | awk -F= '{print tolower($2)}' | tr -d '"')

    if [ "$os_type" != "openeuler" ] && [ "$os_type" != "centos" ]; then
        echo "Error: do not support: $os_type"  
        exit 1
    fi

    if [ "$os_type" == "centos" ] && [ ${sve_flag} == true ]; then
        echo "Error: CentOS don't support 'SVE' version."   
        exit 1
    fi

    if [ ${sve_flag} != true ] && [ ${sve_flag} != false ]; then
        echo "Error: sve_flag is not a boolean value."   
        exit 1
    fi

    # 创建unzip_file，检查 是否有权限 及 文件是否存在
    if [ -f "${packages_dir}/unzip_file" ]; then
        rm -rf ${packages_dir}/unzip_file
    fi

    if [ ! -w "$(dirname "${packages_dir}")" ]; then  
        echo "Error: You do not have permission to create a directory in $(dirname "${packages_dir}")."  
        exit 1
    else
        mkdir $packages_dir/unzip_file
    fi      
    unzip_file="$packages_dir/unzip_file"
    cd $packages_dir

    # 检查文件是否存在  
    missing_files=()
    if [[ ! -f "${packages_dir}/Dependency_library_${os_type}.zip" ]]; then
        missing_files+=("Dependency_library_${os_type}.zip")
    fi

    if [[ ! -f "${packages_dir}/BoostKit-omniop_${omnioperator_version}.zip" ]]; then
        missing_files+=("BoostKit-omniop_${omnioperator_version}.zip")
    fi

    if [[ ! -f "${packages_dir}/boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip" ]]; then
        missing_files+=("boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip")
    fi    
    
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        echo "ERROR: The following packages are missing in ${packages_dir}:"
        for file in "${missing_files[@]}"; do
            echo "- ${packages_dir}/$file"
        done
        exit 1
    fi

    #解压Dependency_library
    if [ ${sve_flag} == false ]; then
        unzip -q Dependency_library_${os_type}.zip  #大小写忽略
        mv ${packages_dir}/Dependency_library_${os_type}/* ${unzip_file}    
    else
        unzip -q Dependency_library_${os_type}-sve.zip  #大小写忽略
        mv ${packages_dir}/Dependency_library_${os_type}-sve/* ${unzip_file}   
    fi
    find . -type d -name "Dependency_library_${os_type}*" ! -name '*.zip' -exec rm -rf {} +
    echo "Info: Dependency_library_${os_type}.zip has been unzipped"

    #解压BoostKit-omniop_${omnioperator_version}.zip
    if [ ${sve_flag} == false ]; then
        unzip -q BoostKit-omniop_${omnioperator_version}.zip boostkit-omniop-operator-${omnioperator_version}-aarch64-${os_type}.tar.gz
        tar -zxf boostkit-omniop-operator-${omnioperator_version}-aarch64-${os_type}.tar.gz -C ${packages_dir}
    else
        unzip -q BoostKit-omniop_${omnioperator_version}.zip boostkit-omniop-operator-${omnioperator_version}-aarch64-${os_type}-sve.tar.gz 
        tar -zxf boostkit-omniop-operator-${omnioperator_version}-aarch64-${os_type}-sve.tar.gz -C ${packages_dir}
    fi
    mv ${packages_dir}/boostkit-omniop-operator-${omnioperator_version}-aarch64/* ${unzip_file}
    rm -rf boostkit-omniop-operator-${omnioperator_version}-aarch64*
    echo "Info: BoostKit-omniop_${omnioperator_version}.zip has been unzipped"

    #解压boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip
    if [ ${sve_flag} == false ]; then
        unzip -q boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}.zip
        unzip -q boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}.zip -d ${unzip_file}
        rm -rf boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}.zip
    else
        unzip -q boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}-sve.zip
        unzip -q boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}-sve.zip -d ${unzip_file}
        rm -rf boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64-${os_type}-sve.zip
    fi

    cd ${unzip_file}
    tar -zxf dependencies.tar.gz
    rm -rf dependencies.tar.gz
    cd ${packages_dir}
    echo "Info: boostkit-omniop-spark-${expect_spark_version}-${omnioperator_version}-aarch64.zip has been unzipped"

    mv ${unzip_file}/* ${target_path}/lib
    rm -rf ${unzip_file}
    echo "Info: all unzipped files have been moved to ${target_path}/lib"

    function_end "------Finish unziping package------"
}

generate_omniconf(){
    cp ${tool_root_dir}/conf/omni.conf ${target_path}/conf/
}

upload_hdfs(){
    function_start "------Start uploading hdfs------"
    username=$(whoami)
    hdfs_dir="/user/${username}"

    # 如果hdfs上用户目录不存在，新增一个
    hdfs dfs -test -e ${hdfs_dir}
    if [ $? -eq 1 ]; then
        echo "Info: hdfs: ${hdfs_dir} is not exist, creating now"
        hdfs dfs -mkdir ${hdfs_dir}
    fi

    # 打包omnioperator，准备传到hdfs ${hdfs_dir}，存在同名旧文件会直接覆盖
    tar -czf ${target_path}.tar.gz -C $(dirname "$target_path") $(basename "$target_path")

    # 检查 HDFS 路径下是否存在同名文件，如存在，则先删除再推送omnioperator包到hdfs
    hdfs dfs -test -e ${hdfs_dir}/${target_path##*/}.tar.gz
    if [ $? -eq 0 ]; then
        hadoop fs -rm ${hdfs_dir}/${target_path##*/}.tar.gz
    fi
    hadoop fs -put ${target_path}.tar.gz ${hdfs_dir}
    
    hdfs dfs -test -e ${hdfs_dir}/${target_path##*/}.tar.gz
    if [ $? -eq 0 ]; then
        echo "Info: successfully upload hdfs"
    else
        echo "ERROR: ${target_path##*/}.tar.gz didn't exist in hdfs."
        exit 1
    fi

    function_end "------Finish uploading hdfs------"
}

generate_spark_defaults(){
    function_start "------Start generating spark defaults------"

    tmp="${tool_root_dir}/conf/omnioperator_tmp.conf"
    target_dir="${spark_conf_path}" 

    # 检查 omni.conf 文件是否存在
    if [ ! -f "$tmp" ]; then
        echo "Error: $tmp does not exist!"
        exit 1
    fi

    conf_file_name="${omnioperator_${expect_spark_version}_${omnioperator_version}.conf}"
    conf_file_path="$(dirname "$tmp")/${conf_file_name}"
    cp $tmp $conf_file_path

    # 替换文件中的内容
    sed -i "s|{host_name}|${host_name}|g" "$conf_file_path"
    sed -i "s|{target_path}|${target_path}|g" "$conf_file_path"
    sed -i "s|{spark_version}|${expect_spark_version}|g" "$conf_file_path"
    sed -i "s|{omni_version}|${omnioperator_version}|g" "$conf_file_path"
    sed -i "s|{omni_package_name}|${target_path##*/}|g" "$conf_file_path"

    if [ -f "$target_dir/${conf_file_name}" ]; then
        echo "INFO: File ${conf_file_name} has been existed in target path:${target_dir}. Replace by new one..."
        rm "$target_dir/${conf_file_name}"
    fi

    # 移动修改后的文件到目标目录
    mv "$conf_file_path" "$target_dir"
    echo "File ${conf_file_name} has been moved to $target_dir"

    function_end "------Finish generating spark defaults------"
}

generate_command_line(){
    function_start "------Start generating command line------"
    command_line_tmp="${tool_root_dir}/command/command_line_tmp"

    # 检查 command_line 文件是否存在
    if [ ! -f "$command_line_tmp" ]; then
        echo "Error: $command_line_tmp does not exist!"
        exit 1
    fi

    # 生成目标文件副本
    command_line=$(dirname "$command_line_tmp")/command_line_${expect_spark_version}_${omnioperator_version}
    cp ${command_line_tmp} ${command_line}

    # 替换文件中的内容
    sed -i "s|{hostname}|${host_name}|g" "$command_line"
    sed -i "s|{user}|${user}|g" "$command_line"
    sed -i "s|{omni_tar_name}|${target_path##*/}|g" "$command_line"
    sed -i "s|{conf_file_name}|"omnioperator_${expect_spark_version}_${omnioperator_version}.conf"|g" "$command_line"
    sed -i "s|{spark_conf_path}|${spark_conf_path}|g" "$command_line"

    function_end "------Finish generating command line------"

    if [ ${sve_flag} == true ]; then
        echo "Deployment Successful. OmniOperator version: ${omnioperator_version}-sve; Spark version: ${expect_spark_version}."
    else
        echo "Deployment Successful. OmniOperator version: ${omnioperator_version}; Spark version: ${expect_spark_version}."
    fi
}

check_omni_function(){
    function_start "------Start checking omni function------"

    omni_start_command=$(cat "${tool_root_dir}/command/command_line_${expect_spark_version}_${omnioperator_version}")

    # 启动OmniOp后，执行下面sql建表，执行SQL82，验证OmniOp是否生效
    result=$(echo "
    CREATE DATABASE IF NOT EXISTS test_db;

    USE test_db;

    CREATE TABLE IF NOT EXISTS item (
        i_item_id INT,
        i_item_desc STRING,
        i_current_price DECIMAL(10, 2),
        i_manufact_id INT,
        i_item_sk INT
    );

    CREATE TABLE IF NOT EXISTS inventory (
        inv_item_sk INT,
        inv_quantity_on_hand INT,
        inv_date_sk INT
    );

    CREATE TABLE IF NOT EXISTS date_dim (
        d_date_sk INT,
        d_date STRING
    );

    CREATE TABLE IF NOT EXISTS store_sales (
        ss_item_sk INT,
        ss_item_id INT,
        ss_quantity INT
    );

    INSERT INTO item (i_item_id, i_item_desc, i_current_price, i_manufact_id, i_item_sk)
    VALUES
    (1, 'Item A', 80.00, 512, 1),
    (2, 'Item B', 90.00, 409, 2),
    (3, 'Item Omni', 100.00, 677, 3),
    (4, 'Item C', 95.00, 16, 4);

    INSERT INTO inventory (inv_item_sk, inv_quantity_on_hand, inv_date_sk)
    VALUES
    (1, 200, 1),
    (2, 150, 2),
    (3, 300, 3),
    (4, 250, 4);

    INSERT INTO date_dim (d_date_sk, d_date)
    VALUES
    (1, '1998-06-29'),
    (2, '1998-07-01'),
    (3, '1998-08-01'),
    (4, '1998-08-29');

    INSERT INTO store_sales (ss_item_sk, ss_item_id, ss_quantity)
    VALUES
    (1, 1, 50),
    (2, 2, 60),
    (3, 3, 70),
    (4, 4, 80);

    set spark.sql.adaptive.enabled=false;
    EXPLAIN SELECT i_item_id, i_item_desc, i_current_price
    FROM item, inventory, date_dim, store_sales
    WHERE i_current_price BETWEEN 76 AND 106
    AND inv_item_sk = i_item_sk
    AND d_date_sk = inv_date_sk
    AND d_date BETWEEN CAST('1998-06-29' AS DATE) AND CAST('1998-08-29' AS DATE)
    AND i_manufact_id IN (512, 409, 677, 16)
    AND inv_quantity_on_hand BETWEEN 100 AND 500
    AND ss_item_sk = i_item_sk
    GROUP BY i_item_id, i_item_desc, i_current_price
    ORDER BY i_item_id
    LIMIT 100;

    DROP TABLE IF EXISTS store_sales;
    DROP TABLE IF EXISTS date_dim;
    DROP TABLE IF EXISTS inventory;
    DROP TABLE IF EXISTS item;

    DROP DATABASE IF EXISTS test_db;
    " | $omni_start_command 2>&1)

    if [ $? -ne 0 ]; then
        echo "ERROR: Error occurred during spark-sql execution."
        echo "Error details: ${result}"
        exit 1
    fi 

    if echo "result" | grep -q "Omni"; then 
        echo "INFO: Omnioperator is effective."
    else
        echo "ERROR: Omnioperator is NOT effective."
    fi
    function_end "------Finish checking omni function------"
}

################################## 执行 ################################################
check_spark_version
check_cpu_model
generate_dir
unzip_package
generate_omniconf
upload_hdfs
generate_spark_defaults
generate_command_line
if [ ${omni_check,,} == true ]; then
    check_omni_function
fi
echo "-----------ALL Finish----------"
exit 0