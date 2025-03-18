from src.omniadvisor.utils.constant import COMMON_CONFIG
from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.repository.task_repository import TaskRepository
from omniadvisor.service.reconstruction_command import reconstruction_command
from omniadvisor.utils.constant import OA_CONF


def spark_run(load, conf):
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = reconstruction_command(load, conf)

    # 根据执行命令创建task
    task = TaskRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    exitcode, spark_output, result_dict = spark_executor.submit_spark_task(submit_cmd)

    # 根据ApplicantID获取Trace
    trace_dict = {}
    application_id = result_dict.get('application_id')
    history_server_url = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")
    spark_fetcher = SparkFetcher(history_server_url)
    # TODO Trace的信息应该保存成文件的形式
    trace_dict['sql'] = spark_fetcher.get_spark_sql_by_app(application_id)
    trace_dict['stages'] = spark_fetcher.get_spark_stages_by_app(application_id)
    trace_dict['executor'] = spark_fetcher.get_spark_executor_by_app(application_id)

    # trace与time_taken(runtime)刷新进入task变量
    runtime = result_dict.get("time_taken")
    if exitcode == 0:
        status = OA_CONF.TaskStatus.success
    else:
        status = OA_CONF.TaskStatus.fail

    TaskRepository.update_task_result(task, status, runtime, trace_dict)
    return task


