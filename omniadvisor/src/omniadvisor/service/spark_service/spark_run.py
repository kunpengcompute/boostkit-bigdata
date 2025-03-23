from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.repository.task_repository import TaskRepository
from omniadvisor.service.spark_service.spark_command_reconstruct import spark_command_reconstruct
from omniadvisor.utils.constant import COMMON_CONFIG, TaskStatus, DATA_DIR
from omniadvisor.utils.utils import trace_data_saver


def spark_run(load, conf):
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = spark_command_reconstruct(load, conf)

    # 根据执行命令创建task
    task = TaskRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    exitcode, spark_output, result_dict = spark_executor.submit_spark_task(submit_cmd)

    # 根据ApplicantID获取Trace
    trace_dict = {}

    application_id = result_dict.get('application_id')
    if not application_id:
        # TODO 这里没有错误的处理逻辑
        pass
    history_server_url = COMMON_CONFIG.get("sparkfetcher", "spark.history.rest.url")
    spark_fetcher = SparkFetcher(history_server_url)
    trace_dict['sql'] = trace_data_saver(spark_fetcher.get_spark_sql_by_app(application_id), DATA_DIR)
    trace_dict['stages'] = trace_data_saver(spark_fetcher.get_spark_stages_by_app(application_id), DATA_DIR)
    trace_dict['executor'] = trace_data_saver(spark_fetcher.get_spark_executor_by_app(application_id), DATA_DIR)

    # trace与time_taken(runtime)刷新进入task变量
    runtime = result_dict.get("time_taken")
    if exitcode == 0:
        status = TaskStatus.success
    else:
        status = TaskStatus.fail

    TaskRepository.update_task_result(task, status, runtime, trace_dict)
    return task


