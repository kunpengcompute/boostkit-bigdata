from constants import COMMON_CONFIG
from omniadvisor.utils.logger import logger
from omniadvisor.service.spark_service.parameter_parser import SparkParameterParser
from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.repository.load_repository import LoadRepository

from omniadvisor.repository.task_repository import TaskRepository


def spark_run(submit_cmd):
    spark_parameter_parser = SparkParameterParser(submit_cmd)
    parsed_parameter = spark_parameter_parser.process_parameter()
    # 从解析后的参数列表中提取负载与任务的相关信息
    name = parsed_parameter.get("name")
    exec_attr = parsed_parameter.get("exec_attr")
    conf = parsed_parameter.get("conf")

    # 根据执行命令创建load和task
    logger.info("根据当前提交任务创建Load与Task")
    if LoadRepository.query(name, conf):
        # TODO  LoadRepository.query是否会在负载不存在的时候返回空值
        load = LoadRepository.query(name, conf)
    else:
        load = LoadRepository.create(name, exec_attr, conf)
    task = TaskRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    exitcode, result_dict = spark_executor.submit_spark_task(submit_cmd)

    # 根据ApplicantID获取Trace
    trace_dict = {}
    application_id = result_dict.get('application_id')
    history_server_url = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")
    spark_fetcher = SparkFetcher(history_server_url)
    trace_dict['sql'] = spark_fetcher.get_spark_sql_by_app(application_id)
    trace_dict['stages'] = spark_fetcher.get_spark_stages_by_app(application_id)
    trace_dict['executor'] = spark_fetcher.get_spark_executor_by_app(application_id)

    # trace与time_taken(runtime)刷新进入task变量
    runtime = result_dict.get("time_taken")
    # TODO 接下来要完成status状态的获取 所以status状态从哪里获取呢
    TaskRepository.update_task_result(task, "status", runtime, trace_dict)

    return task, exitcode, result_dict


