from omniadvisor.service.spark_service.spark_parameter_parser import SparkParameterParser
from omniadvisor.repository.load_repository import LoadRepository
from omniadvisor.utils.logger import logger
from omniadvisor.service.spark_service.spark_run import spark_run
from omniadvisor.utils.constant import OA_CONF


def realtime_recommend(spark_sql_cmd):
    # 获取用户传入的Spark命令 并解析命令
    spark_parameter_parser = SparkParameterParser(spark_sql_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()

    # 提取名字 执行参数 租户默认配置
    name = parsed_parameter.get("name")
    exec_attr = parsed_parameter.get("exec_attr")
    user_config = parsed_parameter.get("default_config")

    # 从负载数据库中查询是否有相关联的Load
    load = LoadRepository.query(name=name, default_config=user_config)
    config_for_test = None
    if not load:  # 查询失败 创建load的同时使用租户默认配置进行测试
        load = LoadRepository.create(name=name, exec_attr=exec_attr, default_config=user_config)
        config_for_test = user_config
    elif load.tuning_needed():
        if load.test_config():  # TODO 关注一下这个函数查询失败时的返回值是不是bool类型的
            config_for_test = load.test_config()
        elif load.best_config():
            config_for_test = load.best_config()

    # TODO 感觉这个判断的位置可以换一换？
    if not config_for_test:
        logger.error("Failed to acquire an available execution configuration.")
        raise ValueError("Failed to acquire an available execution configuration.")

    # 根据配置和负载执行Spark任务
    task = spark_run(load, config_for_test)

    # 执行结果分析 若执行失败则调度用户默认配置重新拉起任务
    if task.status() is not OA_CONF.TaskStatus.success and config_for_test != user_config:
        task = spark_run(load, user_config)

    # TODO 复测逻辑部分


def main(argv):
    spark_sql_cmd = argv
    realtime_recommend(spark_sql_cmd)
