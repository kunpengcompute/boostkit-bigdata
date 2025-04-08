from omniadvisor.service.spark_service.spark_parameter_parser import SparkParameterParser
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.utils.logger import logger
from omniadvisor.service.spark_service.spark_run import spark_run
from common.constant import OA_CONF


def hijack_recommend(spark_sql_cmd):
    # 获取用户传入的Spark命令 并解析命令
    spark_parameter_parser = SparkParameterParser(spark_sql_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()

    # 提取名字 执行参数 租户默认配置
    name = parsed_parameter.name
    exec_attr = parsed_parameter.exec_attr
    user_config = parsed_parameter.conf_params

    # 从负载数据库中查询是否有相关联的Load
    loads = LoadRepository.query(name=name, default_config=user_config)

    def _get_load_and_config_for_test(loads, user_config):
        # 如果查询不到负载信息，则创建新的负载
        if not loads:
            logger.info("Not exit available load, create new one.")
            # TODO 如果用户在这里创建了一个格式异常的config, 会引起spark执行失败 这个应该如何处理
            load = LoadRepository.create(name=name, exec_attr=exec_attr, default_config=user_config)
            if not load:
                logger.error("Failed to create an available load")
                raise ValueError("Failed to create an available load")
        else:
            load = loads[0]
        # 如果负载不需要优化，直接使用默认配置
        if not load.tuning_needed:
            return load, user_config

        # 如果负载需要优化 根据优先级选择配置
        if load.test_config:  # 优先使用待测试配置
            config_for_test = load.test_config
        elif load.best_config:  # 否则使用推荐配置
            config_for_test = load.best_config
        else:
            config_for_test = user_config
        return load, config_for_test

    load, config_for_test = _get_load_and_config_for_test(loads, user_config)

    # 根据配置和负载执行Spark任务
    task = spark_run(load, config_for_test)

    # 执行结果分析 若执行失败则调度用户默认配置重新拉起任务
    if task.status is not OA_CONF.ExecStatus.success and config_for_test != user_config:
        spark_run(load, user_config)


def main(argv):
    spark_sql_cmd = argv
    hijack_recommend(spark_sql_cmd)
