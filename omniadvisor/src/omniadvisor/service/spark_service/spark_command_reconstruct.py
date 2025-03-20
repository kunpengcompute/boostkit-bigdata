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
    for key, value in load.exec_attr.items():
        # -i参数在处理上不支持--的形式 单独处理
        if key == "i":
            submit_cmd_list.append("-i")
        else:
            submit_cmd_list.append("--"+key)
        submit_cmd_list.append(value)
    for key, value in conf.items():
        submit_cmd_list.append("--conf")
        submit_cmd_list.append(f"{key}={value}")
    submit_cmd = " ".join(submit_cmd_list)
    logger.info(f"拼接后的spark-sql命令如下{submit_cmd}")
    return submit_cmd
