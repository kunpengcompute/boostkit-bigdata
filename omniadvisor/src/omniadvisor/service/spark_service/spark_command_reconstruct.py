from omniadvisor.utils.logger import logger


def spark_command_reconstruct(load, conf):
    """
    :param load: {key:value} 保存spark-sql命令中形如 --driver-memory(key) 8G(value)的部分
    :param conf: {key:value} 保存spark-sql命令中形如 --conf spark.sql.shuffle.partitions(key)=3200(value)的部分
    :return:
    """
    cmd_prefix = "spark-sql"
    name = load.name
    submit_cmd_list = [cmd_prefix, "--name", name]

    def _normalize_key(key):
        if '_' in key:
            key = key.replace('_', '-')
        return key

    def _normalize_value(value):
        if ' ' in value:
            value = f'"{value}"'
        return value

    for key, value in load.exec_attr.items():
        # -i参数在处理上不支持--的形式 单独处理
        if key == "i":
            submit_cmd_list.append("-i")
        elif key == "f":
            submit_cmd_list.append("-f")
        # 单独处理 remainder = ['--uk1','xxxx','--uk2','xxxx'] 这部分未被解析的残余参数
        elif key == 'remainder' and isinstance(value, list):
            for i, cmd in enumerate(value):
                if cmd.startwith('--'):  # 处理remainder中以--开头的key值
                    value[i] = _normalize_key(cmd)
                else:
                    value[i] = _normalize_value(cmd)
            submit_cmd_list = submit_cmd_list + value
            continue
        else:
            submit_cmd_list.append("--" + _normalize_key(key))
        submit_cmd_list.append(_normalize_value(value))

    for key, value in conf.items():
        submit_cmd_list.append("--conf")
        if value is not None:
            submit_cmd_list.append(f"{_normalize_key(key)}={_normalize_value(value)}")

    submit_cmd = " ".join(submit_cmd_list)
    logger.info(f"拼接后的spark-sql命令如下{submit_cmd}")
    return submit_cmd
