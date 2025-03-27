from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.repository.task_repository import TaskRepository
from omniadvisor.service.spark_service.spark_command_reconstruct import spark_command_reconstruct
from common.constant import OA_CONF
from omniadvisor.utils.utils import trace_data_saver


def spark_run(load, conf):
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = spark_command_reconstruct(load, conf)

    # 根据执行命令创建task
    task = TaskRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    exitcode, spark_output = spark_executor.submit_spark_task(submit_cmd)

    # 不存在application_id的情况下不提取time_taken 直接返回
    if exitcode != 0:
        status = OA_CONF.TaskStatus.fail
        return TaskRepository.update_task_result(task, status, -1, {}, spark_output)

    status = OA_CONF.TaskStatus.success
    # 提取application_id与 time_taken(runtime)

    spark_submit_cmd, application_id, runtime = spark_executor.parser_spark_output(spark_output)
    # 根据ApplicantID获取Trace
    trace_dict = {}
    history_server_url = OA_CONF.COMMON_CONFIG.SPARK_HISTORY_REST_URL
    spark_fetcher = SparkFetcher(history_server_url)
    trace_dict['sql'] = trace_data_saver(spark_fetcher.get_spark_sql_by_app(application_id), OA_CONF.DATA_DIR)
    trace_dict['stages'] = trace_data_saver(spark_fetcher.get_spark_stages_by_app(application_id), OA_CONF.DATA_DIR)
    trace_dict['executor'] = trace_data_saver(spark_fetcher.get_spark_executor_by_app(application_id), OA_CONF.DATA_DIR)

    return TaskRepository.update_task_result(task, status, runtime, trace_dict, spark_output)


