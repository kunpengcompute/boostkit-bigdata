from time import sleep

from omniadvisor.service.spark_service.spark_fetcher import SparkFetcher
from omniadvisor.service.spark_service.spark_executor import SparkExecutor
from omniadvisor.repository.exam_record_repository import ExamRecordRepository
from omniadvisor.service.spark_service.spark_command_reconstruct import spark_command_reconstruct
from common.constant import OA_CONF
from omniadvisor.utils.utils import trace_data_saver


def spark_run(load, conf):
    # 从解析后的参数列表中提取负载与任务的相关信息
    submit_cmd = spark_command_reconstruct(load, conf)

    # 根据执行命令创建测试记录
    # TODO Task对象的starttime 和 endtime基本上没有用到过？
    exam_record = ExamRecordRepository.create(load, conf)

    # 执行当前的submit_cmd
    spark_executor = SparkExecutor()
    exitcode, spark_output = spark_executor.submit_spark_task(submit_cmd)

    # 不存在application_id的情况下不提取time_taken 直接返回
    if exitcode != 0:
        return ExamRecordRepository.update_exam_result(
            exam_record=exam_record,
            status=OA_CONF.ExecStatus.fail,
            runtime=OA_CONF.exec_fail_return_runtime,
            trace=OA_CONF.exec_fail_return_trace,
            output=spark_output
        )

    status = OA_CONF.ExecStatus.success
    # 提取application_id与 time_taken(runtime)

    spark_submit_cmd, application_id, runtime = spark_executor.parser_spark_output(spark_output)
    # 根据ApplicantID获取Trace
    trace_dict = {}
    history_server_url = OA_CONF.spark_history_rest_url
    spark_fetcher = SparkFetcher(history_server_url)
    # TODO 这里需要一个等待时间 让spark-history上的记录刷新之后才能去抓取trace(考虑轮询)
    sleep(5)
    trace_dict['sql'] = trace_data_saver(spark_fetcher.get_spark_sql_by_app(application_id), OA_CONF.data_dir)
    trace_dict['stages'] = trace_data_saver(spark_fetcher.get_spark_stages_by_app(application_id), OA_CONF.data_dir)
    trace_dict['executor'] = trace_data_saver(spark_fetcher.get_spark_executor_by_app(application_id), OA_CONF.data_dir)

    return ExamRecordRepository.update_exam_result(exam_record, status, runtime, trace_dict, spark_output)


